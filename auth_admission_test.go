package ampacp

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

// authAdmissionBarrier stops every leg that reaches the native store assertion
// and releases them together. That assertion is the last thing a callback does
// before it touches the login child, so a leg parked here has passed every check
// the broker makes and has not yet acted on what it read — which is exactly the
// window a check-then-set leaves open.
type authAdmissionBarrier struct {
	arrived  chan struct{}
	release  chan struct{}
	admitted atomic.Int64
}

func newAuthAdmissionBarrier(t *testing.T) *authAdmissionBarrier {
	t.Helper()

	barrier := &authAdmissionBarrier{arrived: make(chan struct{}, 4), release: make(chan struct{})}
	original := authFileStoreAsserted

	t.Cleanup(func() { authFileStoreAsserted = original })

	authFileStoreAsserted = func(settingsFile string) (bool, error) {
		barrier.admitted.Add(1)
		barrier.arrived <- struct{}{}
		<-barrier.release

		return original(settingsFile)
	}

	return barrier
}

// awaitSecond waits for a second leg to reach the barrier. A discipline that
// admits only one never sends it, so the wait is bounded rather than blocking.
func (b *authAdmissionBarrier) awaitSecond() {
	select {
	case <-b.arrived:
	case <-time.After(300 * time.Millisecond):
	}
}

// countAuthLogins reports how many login children the surface started.
func countAuthLogins(t *testing.T) *atomic.Int64 {
	t.Helper()

	starts := &atomic.Int64{}
	original := authStartLogin

	t.Cleanup(func() { authStartLogin = original })

	authStartLogin = func(client *nativeamp.Client, ctx context.Context) (*nativeamp.AuthLogin, error) {
		starts.Add(1)

		return original(client, ctx)
	}

	return starts
}

func (f *authFixture) rawParams(params map[string]any) json.RawMessage {
	f.t.Helper()

	raw, err := json.Marshal(params)
	if err != nil {
		f.t.Fatalf("marshal params: %v", err)
	}

	return raw
}

func (f *authFixture) disconnect(connectionID string, generation int64) error {
	f.t.Helper()

	return f.call(AuthDisconnectMethod, map[string]any{
		authFieldSessionID:         string(f.session.id),
		authFieldProviderID:        authProviderID,
		authFieldConnectionID:      connectionID,
		authFieldBindingGeneration: generation,
	}, nil)
}

// TestDisconnectIsNotUndoneByAnAdmittedCompletion drives the two legs that
// rewrite one recorded binding at the same time. The owner disconnects while a
// paste is already admitted; without the slot gate the completion writes its own
// generation back over the bump, the host is told a disconnect it can no longer
// repeat succeeded, and the account key stays live in a residence the ledger
// claims is released.
func TestDisconnectIsNotUndoneByAnAdmittedCompletion(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	authorized := fixture.mustAuthorize("connection-1")

	barrier := newAuthAdmissionBarrier(t)
	params := fixture.rawParams(map[string]any{
		authFieldSessionID:  string(fixture.session.id),
		authFieldProviderID: authProviderID,
		authFieldMethod:     authMethodID,
		authFieldFlowID:     authorized.FlowID,
		authFieldInput:      "pasted",
	})

	answered := make(chan error, 1)

	go func() {
		_, callErr := fixture.agent.HandleExtensionMethod(context.Background(), AuthCallbackMethod, params)
		answered <- callErr
	}()

	<-barrier.arrived

	if err := fixture.disconnect("connection-1", 1); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	close(barrier.release)

	callbackErr := <-answered
	if callbackErr == nil {
		t.Fatal("a completion confirmed a binding the disconnect had already released")
	}

	requireAuthCause(t, callbackErr, authCauseBindingConflict)

	record, ok, err := fixture.broker.ledger.read(authProviderID, "connection-1")
	if err != nil || !ok || record.State != authLedgerRemoved || record.BindingGeneration != 2 {
		t.Fatalf("ledger after the raced completion = %#v/%v/%v, want removed at generation 2", record, ok, err)
	}

	// The generation never went backwards, so the fence the host was handed is
	// still the live one.
	if err := fixture.disconnect("connection-1", 2); err != nil {
		t.Fatalf("a disconnect at the recorded generation was refused: %v", err)
	}
}

// TestOneCallbackIsAdmittedForOneLoginChild puts two callbacks on one pending
// flow, which is what a host retrying after a client-side timeout produces. Both
// write the owner's value to the same child stdin and both close it, so an
// unclaimed flow loses a valid paste or reports a refusal of a login that in
// fact succeeded. Every field access is individually locked, so there is no data
// race for the detector to report.
func TestOneCallbackIsAdmittedForOneLoginChild(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	authorized := fixture.mustAuthorize("connection-1")

	barrier := newAuthAdmissionBarrier(t)
	params := fixture.rawParams(map[string]any{
		authFieldSessionID:  string(fixture.session.id),
		authFieldProviderID: authProviderID,
		authFieldMethod:     authMethodID,
		authFieldFlowID:     authorized.FlowID,
		authFieldInput:      "pasted",
	})

	answered := make(chan error, 2)

	for range 2 {
		go func() {
			_, callErr := fixture.agent.HandleExtensionMethod(context.Background(), AuthCallbackMethod, params)
			answered <- callErr
		}()
	}

	<-barrier.arrived
	barrier.awaitSecond()
	close(barrier.release)

	completed, refused := 0, 0

	for range 2 {
		if callErr := <-answered; callErr == nil {
			completed++
		} else {
			requireAuthCause(t, callErr, authCauseFlowState)

			refused++
		}
	}

	if got := barrier.admitted.Load(); got != 1 {
		t.Fatalf("callbacks admitted to one login child = %d, want 1", got)
	}

	if completed != 1 || refused != 1 {
		t.Fatalf("completed = %d, refused = %d, want one of each", completed, refused)
	}

	if status := fixture.status(authorized.FlowID); status.State != authStateAuthenticated {
		t.Fatalf("status after the raced callbacks = %#v", status)
	}
}

// TestConcurrentIdenticalAuthorizesMintOneFlow puts two copies of one request on
// the broker before either has published. The idempotency key can only mean
// something if the whole admission is atomic: without the key gate both miss
// each other's replay check, both start `amp login`, and the operator is shown
// two URLs of which the first is already superseded.
func TestConcurrentIdenticalAuthorizesMintOneFlow(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	starts := countAuthLogins(t)

	params := fixture.rawParams(map[string]any{
		authFieldSessionID:          string(fixture.session.id),
		authFieldProviderID:         authProviderID,
		authFieldConnectionID:       "connection-1",
		authFieldMethodsGeneration:  fixture.generation(),
		authFieldMethod:             authMethodID,
		authFieldAuthorizeRequestID: "request-a",
	})

	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	originalRand := authRandRead

	t.Cleanup(func() { authRandRead = originalRand })

	authRandRead = func(value []byte) (int, error) {
		entered <- struct{}{}
		<-release

		return originalRand(value)
	}

	type outcome struct {
		presentation authAuthorizeResult
		err          error
	}

	answered := make(chan outcome, 2)

	call := func() {
		result, callErr := fixture.agent.HandleExtensionMethod(context.Background(), AuthAuthorizeMethod, params)
		presentation, _ := result.(authAuthorizeResult)
		answered <- outcome{presentation: presentation, err: callErr}
	}

	go call()

	<-entered

	// The repeat arrives while the first request is still short of publishing.
	go call()

	select {
	case <-entered:
	case <-time.After(300 * time.Millisecond):
	}

	close(release)

	first, second := <-answered, <-answered
	if first.err != nil || second.err != nil {
		t.Fatalf("authorize = %v, repeat = %v", first.err, second.err)
	}

	if first.presentation != second.presentation {
		t.Fatalf("two identical requests minted two flows: %#v and %#v", first.presentation, second.presentation)
	}

	if got := starts.Load(); got != 1 {
		t.Fatalf("login children started = %d, want 1", got)
	}
}

// TestSessionCloseRefusesAnAuthorizeThatHasNotPublished races the close against
// an authorize that already passed its session lookup. Publication is the
// authoritative check because the sweep set close takes is only complete if
// nothing can join it afterwards: a flow published after the sweep starts a
// login child that writes into an isolated home the close is about to reclaim.
func TestSessionCloseRefusesAnAuthorizeThatHasNotPublished(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	starts := countAuthLogins(t)

	params := fixture.rawParams(map[string]any{
		authFieldSessionID:          string(fixture.session.id),
		authFieldProviderID:         authProviderID,
		authFieldConnectionID:       "connection-1",
		authFieldMethodsGeneration:  fixture.generation(),
		authFieldMethod:             authMethodID,
		authFieldAuthorizeRequestID: "request-a",
	})

	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	originalMarshal := ledgerMarshal

	t.Cleanup(func() { ledgerMarshal = originalMarshal })

	ledgerMarshal = func(value any) ([]byte, error) {
		arrived <- struct{}{}
		<-release

		return originalMarshal(value)
	}

	answered := make(chan error, 1)

	go func() {
		_, callErr := fixture.agent.HandleExtensionMethod(context.Background(), AuthAuthorizeMethod, params)
		answered <- callErr
	}()

	<-arrived

	fixture.broker.closeSession(fixture.session.id)
	close(release)

	callErr := <-answered
	if callErr == nil {
		t.Fatal("an authorize published a flow into a session whose sweep had already run")
	}

	requireInvalidAuthField(t, callErr, authFieldSessionID)

	if got := starts.Load(); got != 0 {
		t.Fatalf("login children started under a closed session = %d, want 0", got)
	}

	fixture.broker.mu.Lock()
	escaped := len(fixture.broker.byID)
	fixture.broker.mu.Unlock()

	if escaped != 0 {
		t.Fatalf("%d flow records escaped the session close", escaped)
	}
}

// TestARetiredAuthorizeRequestIdCannotCancelItsSuccessor delivers a transport
// retry of a request a later authorize already replaced. Only the newest record
// is answerable verbatim, so an older key is unanswerable — and minting in its
// place destroys the live flow whose URL the operator is looking at, which is
// the one thing an idempotency key exists to prevent.
func TestARetiredAuthorizeRequestIdCannotCancelItsSuccessor(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	starts := countAuthLogins(t)

	first, err := fixture.authorize("connection-1", "request-a")
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	second, err := fixture.authorize("connection-1", "request-b")
	if err != nil {
		t.Fatalf("superseding authorize: %v", err)
	}

	if _, retryErr := fixture.authorize("connection-1", "request-a"); retryErr == nil {
		t.Fatal("a retired request id minted a third flow")
	} else {
		requireInvalidAuthField(t, retryErr, authFieldAuthorizeRequestID)
	}

	if status := fixture.status(second.FlowID); status.State != authStatePending {
		t.Fatalf("the successor was closed under a retired retry: %#v", status)
	}

	replay, err := fixture.authorize("connection-1", "request-b")
	if err != nil || replay != second {
		t.Fatalf("the successor stopped replaying its own key: %#v/%v", replay, err)
	}

	if got := starts.Load(); got != 2 {
		t.Fatalf("login children started = %d, want 2", got)
	}

	// The retired request's own flow id addresses nothing either.
	err = fixture.call(AuthStatusMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: first.FlowID,
	}, nil)
	requireInvalidAuthField(t, err, authFieldFlowID)
}

// TestSupersessionRetiresACompletedFlowsResidence pins what a supersede owes the
// record it replaces. Amp's ledger is per connection, so a new connection's
// authorize does not fence the old one's entry: unless supersession retires the
// previous flow id and closes the residence behind it, that id keeps handing
// back the account key it installed long after it should address nothing.
func TestSupersessionRetiresACompletedFlowsResidence(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	first := fixture.mustAuthorize("connection-1")

	if err := fixture.callback(first.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, first.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	fixture.mustAuthorize("connection-2")

	err = fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: first.FlowID,
	}, nil)
	if err == nil {
		t.Fatal("a superseded flow handed back the credential it installed")
	}

	requireInvalidAuthField(t, err, authFieldFlowID)

	fixture.broker.mu.Lock()
	released := flow.login == nil
	fixture.broker.mu.Unlock()

	if !released {
		t.Fatal("the superseded flow kept the child holding its unharvested residence")
	}
}

// TestCompletionRefusesALineageThatMovedUnderIt pins the check the completion
// makes for itself. A post-hoc write would land on a record naming a generation
// this flow never bound to, leaving the credential resident under a ledger entry
// that says the slot was released — live, and invisible on every host surface.
func TestCompletionRefusesALineageThatMovedUnderIt(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	record, _, err := fixture.broker.ledger.read(authProviderID, "connection-1")
	if err != nil {
		t.Fatalf("ledger read: %v", err)
	}

	record.BindingGeneration++

	if writeErr := fixture.broker.ledger.write(record); writeErr != nil {
		t.Fatalf("ledger write: %v", writeErr)
	}

	requireAuthCause(t, fixture.broker.completeFlow(flow), authCauseBindingConflict)

	after, ok, err := fixture.broker.ledger.read(authProviderID, "connection-1")
	if err != nil || !ok || after != record {
		t.Fatalf("the refused completion wrote over the moved binding: %#v/%v/%v", after, ok, err)
	}

	if status := fixture.status(authorized.FlowID); status.State != authStatePending {
		t.Fatalf("a refused completion transitioned the flow: %#v", status)
	}
}

func TestALateLegIsRefusedOnAClosedSession(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	fixture.broker.closeSession(fixture.session.id)

	err := fixture.call(AuthStatusMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: authorized.FlowID,
	}, nil)
	if err == nil {
		t.Fatal("a leg answered for a session whose flows were already swept")
	}

	requireInvalidAuthField(t, err, authFieldSessionID)
}

// TestAGateOutlivesEverySweepAndOnlyItsOwnLastWaiter pins the one thing that
// makes a gate a gate. Its lifetime is its refcount and nothing else: a gate
// removed while a leg still holds it is replaced by a fresh one the next leg
// walks straight through, and the serialization stops happening while every map
// operation still looks correct. The same refcount is what keeps the per-session
// key gate from accumulating a dead entry for every session the agent outlives.
func TestAGateOutlivesEverySweepAndOnlyItsOwnLastWaiter(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	key := authFlowKey{sessionID: fixture.session.id, providerID: authProviderID}

	release, held := fixture.broker.admitKey(context.Background(), key)
	if !held {
		t.Fatal("the key gate was not free")
	}

	fixture.broker.closeSession(fixture.session.id)

	abandoned, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if _, passed := fixture.broker.admitKey(abandoned, key); passed {
		t.Fatal("a session close handed a gate away while a leg still held it")
	}

	release()

	fixture.broker.mu.Lock()
	gates := len(fixture.broker.admissions)
	fixture.broker.mu.Unlock()

	if gates != 0 {
		t.Fatalf("%d gates outlived every leg that used them", gates)
	}
}

// TestAReopenedSessionIdServesTheAuthSurfaceAgain pins the close mark against
// the id it is written under. session/close drops the id from the live map
// without tombstoning it, so a load rebuilds a session under exactly that id —
// and a mark that outlived the lifetime it described would refuse every
// provider-auth leg for that id for the rest of the agent's life.
func TestAReopenedSessionIdServesTheAuthSurfaceAgain(t *testing.T) {
	store := NewInMemorySessionStore()
	fixture := newAuthFixture(t, "login-hang", WithSessionStore(store))

	manifest, err := json.Marshal(ampManifest{
		Format: SessionStoreFormat, SessionID: string(fixture.session.id),
		NativeSessionID: string(fixture.session.id), Cwd: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	key := SessionKey{SessionID: string(fixture.session.id)}
	if replaceErr := store.Replace(t.Context(), key,
		[]SessionStoreReplacement{{Key: key, Entries: []SessionStoreEntry{manifest}}}); replaceErr != nil {
		t.Fatalf("seed manifest: %v", replaceErr)
	}

	if _, closeErr := fixture.agent.CloseSession(t.Context(),
		acp.CloseSessionRequest{SessionId: fixture.session.id}); closeErr != nil {
		t.Fatalf("close session: %v", closeErr)
	}

	if err := fixture.call(AuthMethodsMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, nil); err == nil {
		t.Fatal("a closed session still served the auth surface")
	}

	if _, loadErr := fixture.agent.LoadSession(t.Context(),
		LoadSessionRequest(fixture.session.id, t.TempDir())); loadErr != nil {
		t.Fatalf("load session: %v", loadErr)
	}

	var methods authMethodsResult
	if err := fixture.call(AuthMethodsMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &methods); err != nil {
		t.Fatalf("a reinstated session id was refused the auth surface: %v", err)
	}

	if methods.Generation == "" {
		t.Fatalf("methods after reopen = %#v", methods)
	}
}

// TestAdmissionRefusesALegTheCallerAbandoned pins what a leg does when the gate
// it needs is held and its own caller has gone. It answers the closed
// timeout cause and takes nothing, rather than sitting on a native sequence
// whose result nobody is waiting for.
func TestAdmissionRefusesALegTheCallerAbandoned(t *testing.T) {
	fixture := newAuthFixture(t, "login-settled")
	authorized := fixture.mustAuthorize("connection-1")

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	abandoned, cancel := context.WithCancel(context.Background())
	cancel()

	key := authFlowKey{sessionID: fixture.session.id, providerID: authProviderID}

	releaseKey, held := fixture.broker.admitKey(context.Background(), key)
	if !held {
		t.Fatal("the key gate was not free")
	}

	_, err = fixture.broker.authorize(abandoned, fixture.rawParams(map[string]any{
		authFieldSessionID:          string(fixture.session.id),
		authFieldProviderID:         authProviderID,
		authFieldConnectionID:       "connection-1",
		authFieldMethodsGeneration:  fixture.generation(),
		authFieldMethod:             authMethodID,
		authFieldAuthorizeRequestID: "request-b",
	}))
	requireAuthCause(t, err, authCauseTimeout)
	releaseKey()

	releaseSlot, held := fixture.broker.admitSlot(context.Background(), authProviderID)
	if !held {
		t.Fatal("the slot gate was not free")
	}

	defer releaseSlot()

	_, err = fixture.broker.recordIntent(abandoned,
		authorizeRequest{providerID: authProviderID, connectionID: "connection-1", method: authMethodID}, "flow", authNow())
	requireAuthCause(t, err, authCauseTimeout)

	_, err = fixture.broker.callback(abandoned, fixture.rawParams(map[string]any{
		authFieldSessionID:  string(fixture.session.id),
		authFieldProviderID: authProviderID,
		authFieldMethod:     authMethodID,
		authFieldFlowID:     authorized.FlowID,
		authFieldInput:      "pasted",
	}))
	requireAuthCause(t, err, authCauseTimeout)

	_, err = fixture.broker.disconnect(abandoned, fixture.rawParams(map[string]any{
		authFieldSessionID:         string(fixture.session.id),
		authFieldProviderID:        authProviderID,
		authFieldConnectionID:      "connection-1",
		authFieldBindingGeneration: 1,
	}))
	requireAuthCause(t, err, authCauseTimeout)

	// The status poll owns no answer of its own, so an abandoned probe simply
	// leaves the flow where it found it.
	fixture.awaitSettledLogin(authorized.FlowID)
	fixture.broker.probe(abandoned, flow)

	fixture.broker.mu.Lock()
	state := flow.state
	fixture.broker.mu.Unlock()

	if state != authStatePending {
		t.Fatalf("an abandoned probe completed the flow: %q", state)
	}
}
