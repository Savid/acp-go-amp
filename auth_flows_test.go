package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

// awaitFlowState polls the broker's own record until the flow leaves pending.
// A login that completes on its own is discovered by status, so the wait drives
// the same leg a consumer would.
func (f *authFixture) awaitFlowState(flowID string) authStatusResult {
	f.t.Helper()

	original := authNow
	authNow = func() time.Time { return original().Add(time.Hour) }

	defer func() { authNow = original }()

	var result authStatusResult

	for range 200 {
		result = f.status(flowID)
		if result.State != authStatePending {
			return result
		}

		time.Sleep(25 * time.Millisecond)
	}

	f.t.Fatalf("flow stayed pending: %#v", result)

	return result
}

func TestAuthorizeRelaysTheHostedPasteBackURL(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	result := fixture.mustAuthorize("connection-1")

	if result.Interaction != authInteractionCallback || result.CallbackInput != authCallbackInputCode {
		t.Fatalf("authorize = %#v, want a callback interaction", result)
	}

	if result.URL != fakeLoginURL {
		t.Fatalf("authorize URL = %q", result.URL)
	}

	if result.Message != authMethodLoginLabel || result.FlowID == "" || result.FlowExpiresAt == 0 {
		t.Fatalf("authorize presentation = %#v", result)
	}

	// Amp publishes no native poll hint, so none is synthesized.
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(encoded), "pollIntervalMs") || strings.Contains(string(encoded), "userCode") {
		t.Fatalf("authorize carried a field amp never supplies: %s", encoded)
	}

	// The flow's slot binding is persisted before authorize returns.
	record, ok, err := fixture.broker.ledger.read(authProviderID, "connection-1")
	if err != nil || !ok || record.FlowID != result.FlowID || record.State != authLedgerIntent {
		t.Fatalf("ledger after authorize = %#v/%v/%v", record, ok, err)
	}
}

func TestHostedStoreVetoPrecedesSupersessionAndLedgerMutation(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	existing := fixture.mustAuthorize("connection-existing")

	before, ok, err := fixture.broker.ledger.read(authProviderID, "connection-existing")
	if err != nil || !ok {
		t.Fatalf("read existing intent: %#v/%v/%v", before, ok, err)
	}

	if writeErr := os.WriteFile(fixture.session.settingsFile,
		[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	_, err = fixture.authorize("connection-replacement", "request-replacement")
	if err == nil {
		t.Fatal("hosted authorize crossed an unasserted native store")
	}
	requireAuthCause(t, err, authCauseNativeVeto)

	if status := fixture.status(existing.FlowID); status.State != authStatePending || status.Reason != "" {
		t.Fatalf("store veto superseded the existing flow: %#v", status)
	}

	after, ok, err := fixture.broker.ledger.read(authProviderID, "connection-existing")
	if err != nil || !ok || after != before {
		t.Fatalf("store veto changed the existing ledger intent: %#v/%v/%v, before %#v", after, ok, err, before)
	}

	if _, ok, err := fixture.broker.ledger.read(authProviderID, "connection-replacement"); err != nil || ok {
		t.Fatalf("store veto recorded replacement intent: ok=%v err=%v", ok, err)
	}
}

// TestAuthorizeRefusesANonDefaultDeployment pins the surface closed against an
// Amp deployment none of its pinned facts describe. The only host a relayed URL
// may name and the only store key a harvest reads are both the default
// deployment's, so a session pointed elsewhere can neither relay a URL nor find
// a credential — and the refusal belongs before a login child exists rather
// than after one has burned an authorization at the provider.
func TestAuthorizeRefusesANonDefaultDeployment(t *testing.T) {
	fixture := newAuthFixture(t, "login", WithEnv(map[string]string{
		nativeamp.AuthDeploymentEnv: "https://amp.example",
	}))

	_, err := fixture.authorize("connection-1", "request-1")
	if err == nil {
		t.Fatal("authorize minted a flow against a deployment this surface cannot serve")
	}

	requireAuthCause(t, err, authCauseUnsupportedVariant)

	flowID, _ := authFailure(t, err)[authFieldFlowID].(string)
	if status := fixture.status(flowID); status.State != authStateFailed || status.Reason != authReasonNativeVeto {
		t.Fatalf("state/reason = %q/%q, want failed/native_veto", status.State, status.Reason)
	}
}

func TestAuthorizeRefusesUnsupportedLoginPlatformsBeforeNativeMint(t *testing.T) {
	fixture := newAuthFixture(t, "login", func(options *Options) {
		options.testOnlyAuthLoginPlatform = platformWindows
	})

	original := authStartLogin
	calls := 0
	authStartLogin = func(*nativeamp.Client, context.Context) (*nativeamp.AuthLogin, error) {
		calls++

		return nil, errors.New("native login must not start")
	}
	t.Cleanup(func() { authStartLogin = original })

	_, err := fixture.authorize("connection-1", "request-1")
	requireAuthCause(t, err, authCauseUnsupportedVariant)
	if calls != 0 {
		t.Fatalf("native login calls = %d, want 0", calls)
	}

	flowID, _ := authFailure(t, err)[authFieldFlowID].(string)
	if status := fixture.status(flowID); status.State != authStateFailed || status.Reason != authReasonNativeVeto {
		t.Fatalf("state/reason = %q/%q, want failed/native_veto", status.State, status.Reason)
	}
}

// TestAuthorizeRefusesAnUnauditedBuildAsAVariant pins the cause an audit
// refusal carries: a build whose account-login shape the adapter cannot prove
// is a variant it does not broker, not a native process failure — no login
// child ever existed to fail.
func TestAuthorizeRefusesAnUnauditedBuildAsAVariant(t *testing.T) {
	fixture := newAuthFixture(t, "login", func(options *Options) {
		options.testOnlyAuthLoginPlatform = platformDarwin
	})

	original := authStartLogin
	authStartLogin = func(*nativeamp.Client, context.Context) (*nativeamp.AuthLogin, error) {
		return nil, fmt.Errorf("amp login: %w", nativeamp.ErrBrowserLaunchUnsupported)
	}
	t.Cleanup(func() { authStartLogin = original })

	_, err := fixture.authorize("connection-1", "request-1")
	requireAuthCause(t, err, authCauseUnsupportedVariant)

	flowID, _ := authFailure(t, err)[authFieldFlowID].(string)
	if status := fixture.status(flowID); status.State != authStateFailed || status.Reason != authReasonNativeVeto {
		t.Fatalf("state/reason = %q/%q, want failed/native_veto", status.State, status.Reason)
	}
}

func TestAuthorizeIsIdempotentAndSupersedes(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")

	first, err := fixture.authorize("connection-1", "request-a")
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	replay, err := fixture.authorize("connection-1", "request-a")
	if err != nil {
		t.Fatalf("replayed authorize: %v", err)
	}

	if replay != first {
		t.Fatalf("replay = %#v, want %#v", replay, first)
	}

	second, err := fixture.authorize("connection-1", "request-b")
	if err != nil {
		t.Fatalf("superseding authorize: %v", err)
	}

	if second.FlowID == first.FlowID {
		t.Fatal("a different request id reused the flow id")
	}

	// The superseded flow id addresses nothing afterwards.
	err = fixture.call(AuthStatusMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: first.FlowID,
	}, nil)
	if err == nil {
		t.Fatal("a superseded flow id was still addressable")
	}

	requireInvalidAuthField(t, err, authFieldFlowID)

	// The superseded flow keeps its revision history in the ledger.
	record, _, err := fixture.broker.ledger.read(authProviderID, "connection-1")
	if err != nil || record.Revision != 2 {
		t.Fatalf("ledger revision = %#v/%v", record, err)
	}
}

func TestAuthorizeReplaysAcrossTerminalization(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	first := fixture.mustAuthorize("connection-1")

	if err := fixture.callback(first.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	// The key is answerable for as long as the session lives, so a repeat that
	// arrives after the flow went terminal still replays it: no supersede, no
	// ledger revision, no second login, and the same flow id.
	native := len(readHelperJSON[[]string](t, filepath.Join(fixture.state, "args.jsonl")))

	replay, err := fixture.authorize("connection-1", "request-connection-1")
	if err != nil {
		t.Fatalf("replayed authorize: %v", err)
	}

	if replay != first {
		t.Fatalf("replay = %#v, want %#v", replay, first)
	}

	if again := len(readHelperJSON[[]string](t, filepath.Join(fixture.state, "args.jsonl"))); again != native {
		t.Fatalf("the repeat drove %d native calls", again-native)
	}

	record, _, err := fixture.broker.ledger.read(authProviderID, "connection-1")
	if err != nil || record.Revision != 1 || record.FlowID != first.FlowID {
		t.Fatalf("ledger after the repeat = %#v/%v", record, err)
	}

	if status := fixture.status(first.FlowID); status.State != authStateAuthenticated || status.Reason != "" {
		t.Fatalf("the repeat disturbed the flow: %#v", status)
	}
}

func TestAuthorizeReplaysAMintFailureAgainstARealFlow(t *testing.T) {
	fixture := newAuthFixture(t, "login-no-url")

	_, err := fixture.authorize("connection-1", "request-a")
	if err == nil {
		t.Fatal("a failed mint returned a presentation")
	}

	requireAuthCause(t, err, authCauseProcess)

	// The flow is registered before the mint runs, so the id a mint failure
	// reports addresses the terminal record it made rather than nothing.
	flowID, _ := authFailure(t, err)[authFieldFlowID].(string)
	if flowID == "" {
		t.Fatalf("the mint failure carried no flow id: %#v", authFailure(t, err))
	}

	if status := fixture.status(flowID); status.State != authStateFailed || status.Reason != authReasonProcess {
		t.Fatalf("status after a failed mint = %#v", status)
	}

	_, replayErr := fixture.authorize("connection-1", "request-a")
	if replayErr == nil {
		t.Fatal("the repeat started a second login")
	}

	requireAuthCause(t, replayErr, authCauseProcess)

	if replayed, _ := authFailure(t, replayErr)[authFieldFlowID].(string); replayed != flowID {
		t.Fatalf("the repeat reported flow %q, want %q", replayed, flowID)
	}
}

// blockAuthLogin holds every mint at the native seam until the returned channel
// is closed, which is what an authorize still waiting on `amp login` looks like
// from another caller's side.
func blockAuthLogin(t *testing.T, started chan<- struct{}) (chan struct{}, func() *nativeamp.AuthLogin) {
	t.Helper()

	release := make(chan struct{})
	original := authStartLogin

	var child *nativeamp.AuthLogin

	authStartLogin = func(client *nativeamp.Client, ctx context.Context) (*nativeamp.AuthLogin, error) {
		close(started)
		<-release

		login, err := original(client, ctx)
		child = login

		return login, err
	}

	t.Cleanup(func() { authStartLogin = original })

	return release, func() *nativeamp.AuthLogin { return child }
}

func TestAuthorizeReplayWaitsForAnInFlightMint(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")

	params, err := json.Marshal(map[string]any{
		authFieldSessionID:          string(fixture.session.id),
		authFieldProviderID:         authProviderID,
		authFieldConnectionID:       "connection-1",
		authFieldMethodsGeneration:  fixture.generation(),
		authFieldMethod:             authMethodLogin,
		authFieldAuthorizeRequestID: "request-a",
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release, _ := blockAuthLogin(t, started)

	type outcome struct {
		result any
		err    error
	}

	call := func() chan outcome {
		answered := make(chan outcome, 1)

		go func() {
			result, callErr := fixture.agent.HandleExtensionMethod(context.Background(), AuthAuthorizeMethod, params)
			answered <- outcome{result: result, err: callErr}
		}()

		return answered
	}

	minting := call()
	<-started

	// The repeat arrives while the first mint is still running. It waits for
	// that mint instead of starting a second login.
	repeating := call()

	select {
	case got := <-repeating:
		t.Fatalf("the repeat answered before the mint settled: %#v", got)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	minted, repeated := <-minting, <-repeating
	if minted.err != nil || repeated.err != nil {
		t.Fatalf("authorize = %v, repeat = %v", minted.err, repeated.err)
	}

	if minted.result != repeated.result {
		t.Fatalf("repeat = %#v, want %#v", repeated.result, minted.result)
	}

	presentation, _ := minted.result.(authAuthorizeResult)
	if presentation.URL != fakeLoginURL {
		t.Fatalf("the waiting repeat answered with %#v", presentation)
	}
}

func TestAuthorizeReplayAbandonsAWaitTheCallerCancelled(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	key := authFlowKey{sessionID: fixture.session.id, providerID: authProviderID}

	fixture.broker.mu.Lock()
	fixture.broker.retained[key] = &authFlow{authorizeRequestID: "request-a", ready: make(chan struct{})}
	fixture.broker.mu.Unlock()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, replayed, err := fixture.broker.replayAuthorize(ctx, key, "request-a")
	if !replayed || err == nil {
		t.Fatalf("a repeat the caller abandoned kept waiting: %v/%v", replayed, err)
	}

	requireAuthCause(t, err, authCauseTimeout)

	// A key that names no retained flow is not a repeat at all.
	if _, replayed, err := fixture.broker.replayAuthorize(t.Context(), key, "request-b"); replayed || err != nil {
		t.Fatalf("an unrecorded key replayed: %v/%v", replayed, err)
	}
}

func TestAuthorizeReleasesALoginChildTheSessionCloseRacedPastIt(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")

	params, err := json.Marshal(map[string]any{
		authFieldSessionID:          string(fixture.session.id),
		authFieldProviderID:         authProviderID,
		authFieldConnectionID:       "connection-1",
		authFieldMethodsGeneration:  fixture.generation(),
		authFieldMethod:             authMethodLogin,
		authFieldAuthorizeRequestID: "request-a",
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release, child := blockAuthLogin(t, started)
	answered := make(chan error, 1)

	go func() {
		_, callErr := fixture.agent.HandleExtensionMethod(context.Background(), AuthAuthorizeMethod, params)
		answered <- callErr
	}()

	<-started

	// The session closes while the mint is still in flight, so the flow is
	// already torn down when its login child finally exists.
	fixture.broker.closeSession(fixture.session.id)
	close(release)

	if callErr := <-answered; callErr != nil {
		t.Fatalf("authorize: %v", callErr)
	}

	if settled, _ := child().Settled(); !settled {
		t.Fatal("the mint's login child outlived the session that owned it")
	}
}

func TestAuthorizeRejectsAddressingFailures(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	generation := fixture.generation()

	base := func() map[string]any {
		return map[string]any{
			authFieldSessionID:          string(fixture.session.id),
			authFieldProviderID:         authProviderID,
			authFieldConnectionID:       "connection-1",
			authFieldMethodsGeneration:  generation,
			authFieldMethod:             authMethodLogin,
			authFieldAuthorizeRequestID: "request-a",
		}
	}

	cases := map[string]struct {
		mutate func(map[string]any)
		field  string
	}{
		"generation":  {mutate: func(p map[string]any) { p[authFieldMethodsGeneration] = "stale" }, field: authFieldMethodsGeneration},
		"method":      {mutate: func(p map[string]any) { p[authFieldMethod] = "other" }, field: authFieldMethod},
		"provider":    {mutate: func(p map[string]any) { p[authFieldProviderID] = "openai" }, field: authFieldProviderID},
		"inputs":      {mutate: func(p map[string]any) { p[authFieldInputs] = map[string]any{"account": "x"} }, field: authFieldInputs},
		"badInputs":   {mutate: func(p map[string]any) { p[authFieldInputs] = 7 }, field: authFieldInputs},
		"session":     {mutate: func(p map[string]any) { delete(p, authFieldSessionID) }, field: authFieldSessionID},
		"connection":  {mutate: func(p map[string]any) { delete(p, authFieldConnectionID) }, field: authFieldConnectionID},
		"requestId":   {mutate: func(p map[string]any) { delete(p, authFieldAuthorizeRequestID) }, field: authFieldAuthorizeRequestID},
		"noProvider":  {mutate: func(p map[string]any) { delete(p, authFieldProviderID) }, field: authFieldProviderID},
		"noGenerated": {mutate: func(p map[string]any) { delete(p, authFieldMethodsGeneration) }, field: authFieldMethodsGeneration},
		"noMethod":    {mutate: func(p map[string]any) { delete(p, authFieldMethod) }, field: authFieldMethod},
	}

	for name, testCase := range cases {
		params := base()
		testCase.mutate(params)

		err := fixture.call(AuthAuthorizeMethod, params, nil)
		if err == nil {
			t.Fatalf("%s: authorize accepted the request", name)
		}

		requireInvalidAuthField(t, err, testCase.field)
	}

	unknownSession := base()
	unknownSession[authFieldSessionID] = "T-unknown"

	if err := fixture.call(AuthAuthorizeMethod, unknownSession, nil); err == nil {
		t.Fatal("authorize answered for an unknown session")
	}

	unknownField := base()
	unknownField["unexpected"] = 1

	if err := fixture.call(AuthAuthorizeMethod, unknownField, nil); err == nil {
		t.Fatal("authorize accepted an unknown field")
	}
}

func TestAuthorizeFailsClosedOnANativeRefusal(t *testing.T) {
	cases := map[string]string{
		"login-no-url":       authCauseProcess,
		"login-fragment-url": authCauseNativeVeto,
	}

	for mode, cause := range cases {
		fixture := newAuthFixture(t, mode)

		_, err := fixture.authorize("connection-1", "request-a")
		if err == nil {
			t.Fatalf("%s: authorize returned a presentation", mode)
		}

		requireAuthCause(t, err, cause)
	}

	// A binary that cannot be launched at all is a process failure.
	missing := newAuthFixture(t, "login", WithExecutablePath(filepath.Join(t.TempDir(), "absent")))
	if _, err := missing.authorize("connection-1", "request-a"); err == nil {
		t.Fatal("a missing binary produced a presentation")
	} else {
		requireAuthCause(t, err, authCauseProcess)
	}
}

func TestAuthorizeFailsClosedOnAnUnassertedNativeStore(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	if err := os.WriteFile(fixture.session.settingsFile,
		[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := fixture.authorize("connection-1", "request-a")
	if err == nil {
		t.Fatal("authorize ran against an unasserted native store")
	}

	requireAuthCause(t, err, authCauseNativeVeto)
}

func TestAuthorizeFailsClosedOnALedgerOrTokenFailure(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	generation := fixture.generation()

	params := map[string]any{
		authFieldSessionID:          string(fixture.session.id),
		authFieldProviderID:         authProviderID,
		authFieldConnectionID:       "connection-1",
		authFieldMethodsGeneration:  generation,
		authFieldMethod:             authMethodLogin,
		authFieldAuthorizeRequestID: "request-a",
	}

	originalRand := authRandRead
	authRandRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }

	err := fixture.call(AuthAuthorizeMethod, params, nil)
	authRandRead = originalRand

	if err == nil {
		t.Fatal("authorize minted a flow with no entropy")
	}

	requireAuthCause(t, err, authCauseProcess)

	originalMarshal := ledgerMarshal
	ledgerMarshal = func(any) ([]byte, error) { return nil, errors.New("no encoder") }

	err = fixture.call(AuthAuthorizeMethod, params, nil)
	ledgerMarshal = originalMarshal

	if err == nil {
		t.Fatal("authorize returned without a persisted binding")
	}

	requireAuthCause(t, err, authCauseProcess)
}

func TestCallbackCompletesTheFlowAndHarvestsOnce(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	authorized := fixture.mustAuthorize("connection-1")

	if err := fixture.callback(authorized.FlowID, "pasted-envelope"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	if status := fixture.status(authorized.FlowID); status.State != authStateAuthenticated || status.Reason != "" {
		t.Fatalf("status after callback = %#v", status)
	}

	// Amp's key never expires, so no credential expiry is ever reported.
	if status := fixture.status(authorized.FlowID); status.ExpiresAt != 0 {
		t.Fatalf("status reported a credential expiry: %#v", status)
	}

	var harvest authCredentialResult
	if err := fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: authorized.FlowID,
	}, &harvest); err != nil {
		t.Fatalf("credential: %v", err)
	}

	if harvest.Credential.Type != ProviderCredentialAPI || harvest.Credential.API == nil {
		t.Fatalf("harvest variant = %#v", harvest.Credential)
	}

	if harvest.Credential.API.Key != "secret-for-pasted-envelope" || harvest.Credential.API.Metadata != nil {
		t.Fatalf("harvest = %#v", harvest.Credential.API)
	}

	if harvest.ConnectionID != "connection-1" || harvest.Revision != 1 || harvest.BindingGeneration != 1 {
		t.Fatalf("harvest binding = %#v", harvest)
	}

	// A second harvest on the same flow is a flow-state failure.
	err := fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: authorized.FlowID,
	}, nil)
	if err == nil {
		t.Fatal("a second harvest succeeded")
	}

	requireAuthCause(t, err, authCauseFlowState)

	// The pasted value reaches the login child and nothing else.
	pasted := readHelperJSON[string](t, filepath.Join(fixture.state, "login-stdin.jsonl"))
	if len(pasted) != 1 || pasted[0] != "pasted-envelope" {
		t.Fatalf("login stdin = %#v", pasted)
	}
}

func TestCallbackRejectsAddressingFailures(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	base := func() map[string]any {
		return map[string]any{
			authFieldSessionID:  string(fixture.session.id),
			authFieldProviderID: authProviderID,
			authFieldMethod:     authMethodLogin,
			authFieldFlowID:     authorized.FlowID,
			authFieldInput:      "pasted",
		}
	}

	cases := map[string]struct {
		mutate func(map[string]any)
		field  string
	}{
		"method":     {mutate: func(p map[string]any) { p[authFieldMethod] = "other" }, field: authFieldMethod},
		"flow":       {mutate: func(p map[string]any) { p[authFieldFlowID] = "unknown" }, field: authFieldFlowID},
		"provider":   {mutate: func(p map[string]any) { p[authFieldProviderID] = "openai" }, field: authFieldFlowID},
		"empty":      {mutate: func(p map[string]any) { p[authFieldInput] = "" }, field: authFieldInput},
		"oversize":   {mutate: func(p map[string]any) { p[authFieldInput] = strings.Repeat("x", authMaxCallbackBytes+1) }, field: authFieldInput},
		"noSession":  {mutate: func(p map[string]any) { delete(p, authFieldSessionID) }, field: authFieldSessionID},
		"noProvider": {mutate: func(p map[string]any) { delete(p, authFieldProviderID) }, field: authFieldProviderID},
		"noMethod":   {mutate: func(p map[string]any) { delete(p, authFieldMethod) }, field: authFieldMethod},
		"noFlow":     {mutate: func(p map[string]any) { delete(p, authFieldFlowID) }, field: authFieldFlowID},
		"noInput":    {mutate: func(p map[string]any) { delete(p, authFieldInput) }, field: authFieldInput},
	}

	for name, testCase := range cases {
		params := base()
		testCase.mutate(params)

		err := fixture.call(AuthCallbackMethod, params, nil)
		if err == nil {
			t.Fatalf("%s: callback accepted the request", name)
		}

		requireInvalidAuthField(t, err, testCase.field)
	}

	unknownSession := base()
	unknownSession[authFieldSessionID] = "T-unknown"

	if err := fixture.call(AuthCallbackMethod, unknownSession, nil); err == nil {
		t.Fatal("callback answered for an unknown session")
	}

	unknownField := base()
	unknownField["unexpected"] = 1

	if err := fixture.call(AuthCallbackMethod, unknownField, nil); err == nil {
		t.Fatal("callback accepted an unknown field")
	}

	// A flow another session owns is not addressable from this one.
	other := fixture.newSession("T-other")
	err := fixture.call(AuthCallbackMethod, map[string]any{
		authFieldSessionID: string(other.id), authFieldProviderID: authProviderID,
		authFieldMethod: authMethodLogin, authFieldFlowID: authorized.FlowID, authFieldInput: "pasted",
	}, nil)

	if err == nil {
		t.Fatal("a cross-session flow id was addressable")
	}

	requireInvalidAuthField(t, err, authFieldFlowID)
}

func TestCallbackOnATerminalFlowIsAFlowStateFailure(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	if err := fixture.call(AuthCancelMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: authorized.FlowID,
	}, nil); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	err := fixture.callback(authorized.FlowID, "pasted")
	if err == nil {
		t.Fatal("a terminal flow accepted a callback")
	}

	requireAuthCause(t, err, authCauseFlowState)
}

func TestCallbackFailsClosedOnARefusal(t *testing.T) {
	fixture := newAuthFixture(t, "login-refuse")
	authorized := fixture.mustAuthorize("connection-1")

	err := fixture.callback(authorized.FlowID, "bad-code")
	if err == nil {
		t.Fatal("a refused paste reported success")
	}

	requireAuthCause(t, err, authCauseProviderRefused)

	// The refusal transitions the flow and the native message never crosses.
	if status := fixture.status(authorized.FlowID); status.State != authStateFailed || status.Reason != authReasonProviderRefused {
		t.Fatalf("status after refusal = %#v", status)
	}

	if data := authFailure(t, err); len(data) != 6 {
		t.Fatalf("failure data carried native text: %#v", data)
	}
}

func TestCallbackFailsClosedOnAnUnassertedStoreOrLedgerFailure(t *testing.T) {
	veto := newAuthFixture(t, "login-hang")
	vetoed := veto.mustAuthorize("connection-1")

	if err := os.WriteFile(veto.session.settingsFile,
		[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	err := veto.callback(vetoed.FlowID, "pasted")
	if err == nil {
		t.Fatal("callback ran against an unasserted native store")
	}

	requireAuthCause(t, err, authCauseNativeVeto)

	ledgerFailure := newAuthFixture(t, "login")
	authorized := ledgerFailure.mustAuthorize("connection-1")

	original := ledgerMarshal
	ledgerMarshal = func(any) ([]byte, error) { return nil, errors.New("no encoder") }

	err = ledgerFailure.callback(authorized.FlowID, "pasted")
	ledgerMarshal = original

	if err == nil {
		t.Fatal("callback confirmed a flow it could not record")
	}

	requireAuthCause(t, err, authCauseProcess)
}

func TestCallbackFailsClosedWithNoLoginChild(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	fixture.broker.closeLogin(flow)

	callbackErr := fixture.callback(authorized.FlowID, "pasted")
	if callbackErr == nil {
		t.Fatal("a flow with no login child accepted a callback")
	}

	requireAuthCause(t, callbackErr, authCauseTransport)
}

func TestStatusDiscoversAnIndependentCompletion(t *testing.T) {
	fixture := newAuthFixture(t, "login-settled-secret")
	authorized := fixture.mustAuthorize("connection-1")

	if status := fixture.awaitFlowState(authorized.FlowID); status.State != authStateAuthenticated {
		t.Fatalf("independently completed flow = %#v", status)
	}

	var harvest authCredentialResult
	if err := fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: authorized.FlowID,
	}, &harvest); err != nil {
		t.Fatalf("credential: %v", err)
	}

	if harvest.Credential.API.Key != "secret-for-independent" {
		t.Fatalf("harvest = %#v", harvest.Credential.API)
	}
}

// awaitSettledLogin blocks until the flow's login child has exited, which is
// what a login the owner completed in a browser looks like from here.
func (f *authFixture) awaitSettledLogin(flowID string) *authFlow {
	f.t.Helper()

	flow, err := f.broker.addressFlow(f.session.id, authProviderID, flowID)
	if err != nil {
		f.t.Fatalf("addressFlow: %v", err)
	}

	for range 200 {
		f.broker.mu.Lock()
		login := flow.login
		f.broker.mu.Unlock()

		if login == nil {
			f.t.Fatal("the flow released its login child")
		}

		if settled, _ := login.Settled(); settled {
			return flow
		}

		time.Sleep(25 * time.Millisecond)
	}

	f.t.Fatal("the login child never settled")

	return nil
}

// captureLoginTeardown reports when a native login teardown has returned. The
// broker clears its handle on the child before tearing it down, so a completer
// firing in the background is still inside the child's native cleanup after the
// flow record says the child was released; a test that waits on the handle
// alone leaves that cleanup running past its own end.
func captureLoginTeardown(t *testing.T) <-chan struct{} {
	t.Helper()

	torn := make(chan struct{}, 1)
	original := authCloseLogin

	authCloseLogin = func(login *nativeamp.AuthLogin) error {
		err := original(login)

		select {
		case torn <- struct{}{}:
		default:
		}

		return err
	}

	t.Cleanup(func() { authCloseLogin = original })

	return torn
}

func TestCallbackAcceptsAFlowThatAlreadyCompletedOnItsOwn(t *testing.T) {
	fixture := newAuthFixture(t, "login-settled-secret")
	authorized := fixture.mustAuthorize("connection-1")

	fixture.awaitSettledLogin(authorized.FlowID)

	// The paste arrives after the login finished by itself. Nothing is written
	// to the dead child, and the flow is the completion it already was.
	if err := fixture.callback(authorized.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	if status := fixture.status(authorized.FlowID); status.State != authStateAuthenticated {
		t.Fatalf("status = %#v", status)
	}
}

func TestCallbackFailsClosedWhenAnIndependentCompletionCannotBeRecorded(t *testing.T) {
	fixture := newAuthFixture(t, "login-settled-secret")
	authorized := fixture.mustAuthorize("connection-1")

	fixture.awaitSettledLogin(authorized.FlowID)

	original := ledgerMarshal
	ledgerMarshal = func(any) ([]byte, error) { return nil, errors.New("no encoder") }

	err := fixture.callback(authorized.FlowID, "pasted")
	ledgerMarshal = original

	if err == nil {
		t.Fatal("callback confirmed a completion it could not record")
	}

	requireAuthCause(t, err, authCauseProcess)
}

func TestCallbackReportsAFlowThatAlreadyFailedOnItsOwn(t *testing.T) {
	fixture := newAuthFixture(t, "login-settled-fail")
	authorized := fixture.mustAuthorize("connection-1")

	fixture.awaitSettledLogin(authorized.FlowID)

	err := fixture.callback(authorized.FlowID, "pasted")
	if err == nil {
		t.Fatal("a callback against a failed login reported success")
	}

	requireAuthCause(t, err, authCauseProviderRefused)

	if status := fixture.status(authorized.FlowID); status.State != authStateFailed || status.Reason != authReasonProviderRefused {
		t.Fatalf("status = %#v", status)
	}
}

func TestStatusReportsAnIndependentFailure(t *testing.T) {
	fixture := newAuthFixture(t, "login-settled-fail")
	authorized := fixture.mustAuthorize("connection-1")

	status := fixture.awaitFlowState(authorized.FlowID)
	if status.State != authStateFailed || status.Reason != authReasonProviderRefused {
		t.Fatalf("independently failed flow = %#v", status)
	}
}

func TestStatusHoldsTheNativePollFloor(t *testing.T) {
	fixture := newAuthFixture(t, "login-settled")
	authorized := fixture.mustAuthorize("connection-1")

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	// The first status arms the interval; a second inside it serves the cached
	// state without touching the child again.
	fixture.broker.probe(t.Context(), flow)

	fixture.broker.mu.Lock()
	armed := flow.nextProbeAt
	fixture.broker.mu.Unlock()

	if armed.IsZero() {
		t.Fatal("status did not arm the poll interval")
	}

	fixture.broker.probe(t.Context(), flow)

	fixture.broker.mu.Lock()
	again := flow.nextProbeAt
	fixture.broker.mu.Unlock()

	if !again.Equal(armed) {
		t.Fatal("a status inside the poll floor drove a second native read")
	}

	if armed.Sub(authNow()) > authPollFloor {
		t.Fatalf("the poll interval %v exceeds the floor", armed.Sub(authNow()))
	}

	// A terminal flow, and one whose login child is gone, are both no-ops.
	fixture.broker.terminalize(flow, authStateCancelled, authReasonOwnerCancel)
	fixture.broker.probe(t.Context(), flow)

	fixture.broker.closeLogin(flow)
	fixture.broker.probe(t.Context(), flow)
}

func TestStatusAndCancelRejectAddressingFailures(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")

	for _, method := range []string{AuthStatusMethod, AuthCancelMethod} {
		if err := fixture.call(method, map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: "unknown",
		}, nil); err == nil {
			t.Fatalf("%s answered an unknown flow", method)
		}

		if err := fixture.call(method, map[string]any{
			authFieldProviderID: authProviderID, authFieldFlowID: "x",
		}, nil); err == nil {
			t.Fatalf("%s answered with no session", method)
		}

		if err := fixture.call(method, map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldFlowID: "x",
		}, nil); err == nil {
			t.Fatalf("%s answered with no provider", method)
		}

		if err := fixture.call(method, map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID,
		}, nil); err == nil {
			t.Fatalf("%s answered with no flow", method)
		}

		if err := fixture.call(method, map[string]any{
			authFieldSessionID: "T-unknown", authFieldProviderID: authProviderID, authFieldFlowID: "x",
		}, nil); err == nil {
			t.Fatalf("%s answered for an unknown session", method)
		}

		if err := fixture.call(method, map[string]any{"unexpected": 1}, nil); err == nil {
			t.Fatalf("%s accepted an unknown field", method)
		}
	}
}

func TestCancelIsIdempotentAndClaimsNoProviderSideCancellation(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	params := map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: authorized.FlowID,
	}

	var first authFlowIDResult
	if err := fixture.call(AuthCancelMethod, params, &first); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if first.FlowID != authorized.FlowID {
		t.Fatalf("cancel = %#v", first)
	}

	if status := fixture.status(authorized.FlowID); status.State != authStateCancelled || status.Reason != authReasonOwnerCancel {
		t.Fatalf("status after cancel = %#v", status)
	}

	var second authFlowIDResult
	if err := fixture.call(AuthCancelMethod, params, &second); err != nil {
		t.Fatalf("second cancel: %v", err)
	}

	if second.FlowID != authorized.FlowID {
		t.Fatalf("second cancel = %#v", second)
	}
}

// TestTerminalizeKeepsTheFirstTerminalTransition pins the record itself: a flow
// has one terminal transition, and a later one is dropped rather than
// overwriting the owner's.
func TestTerminalizeKeepsTheFirstTerminalTransition(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	fixture.broker.terminalize(flow, authStateCancelled, authReasonOwnerCancel)
	fixture.broker.terminalize(flow, authStateAuthenticated, "")

	if status := fixture.status(authorized.FlowID); status.State != authStateCancelled || status.Reason != authReasonOwnerCancel {
		t.Fatalf("status = %#v, want cancelled/owner_cancel", status)
	}
}

// TestCompletionAnswersForAFlowClosedUnderIt pins the leg against the record: a
// login child that settled after the owner closed the flow owns no transition
// and writes no confirmation. The confirmation would bind a credential to a
// connection generation the owner already ended, and a cause naming the
// provider would report a refusal nobody made.
func TestCompletionAnswersForAFlowClosedUnderIt(t *testing.T) {
	cases := []struct {
		name   string
		state  string
		reason string
		cause  string
	}{
		{name: "cancelled", state: authStateCancelled, reason: authReasonOwnerCancel, cause: authCauseFlowCancelled},
		{name: "expired", state: authStateExpired, reason: authReasonDeadline, cause: authCauseFlowState},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newAuthFixture(t, "login-hang")
			authorized := fixture.mustAuthorize("connection-1")

			flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
			if err != nil {
				t.Fatalf("addressFlow: %v", err)
			}

			fixture.broker.terminalize(flow, testCase.state, testCase.reason)

			requireAuthCause(t, fixture.broker.completeFlow(flow), testCase.cause)
			requireAuthCause(t, fixture.broker.failSettled(flow, authCauseProviderRefused, true), testCase.cause)

			record, ok, readErr := fixture.broker.ledger.read(authProviderID, "connection-1")
			if readErr != nil || !ok || record.State != authLedgerIntent {
				t.Fatalf("ledger = %#v/%v/%v, want intent", record, ok, readErr)
			}

			if status := fixture.status(authorized.FlowID); status.State != testCase.state || status.Reason != testCase.reason {
				t.Fatalf("status = %#v, want %s/%s", status, testCase.state, testCase.reason)
			}
		})
	}
}

func TestFlowExpiresOnItsEffectiveDeadline(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	torn := captureLoginTeardown(t)

	original := authNow
	authNow = func() time.Time { return original().Add(-authSafetyDeadline - time.Minute) }

	authorized, err := fixture.authorize("connection-1", "request-a")

	authNow = original

	if err != nil {
		t.Fatalf("authorize: %v", err)
	}

	var status authStatusResult

	for range 200 {
		status = fixture.status(authorized.FlowID)
		if status.State != authStatePending {
			break
		}

		time.Sleep(25 * time.Millisecond)
	}

	if status.State != authStateExpired || status.Reason != authReasonDeadline {
		t.Fatalf("expired flow = %#v", status)
	}

	// The completer fires once; a second expiry against a terminal flow is a
	// no-op rather than a second transition.
	flow, addressErr := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if addressErr != nil {
		t.Fatalf("addressFlow: %v", addressErr)
	}

	// The completer tears the child down on its own goroutine, so the test waits
	// for that teardown rather than for the handle the broker clears first.
	<-torn

	fixture.broker.expire(flow)

	if again := fixture.status(authorized.FlowID); again != status {
		t.Fatalf("second expiry changed the flow: %#v", again)
	}
}

func TestSessionCloseCancelsEveryFlowItOwns(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	// A flow another session owns is untouched by this session's close.
	other := fixture.newSession("T-other")

	fixture.broker.closeSession(other.id)

	if status := fixture.status(authorized.FlowID); status.State != authStatePending {
		t.Fatalf("an unrelated session close terminalized the flow: %#v", status)
	}

	if err := fixture.session.Close(context.Background()); err != nil {
		t.Fatalf("session close: %v", err)
	}

	fixture.broker.mu.Lock()
	remaining := len(fixture.broker.byID)
	fixture.broker.mu.Unlock()

	if remaining != 0 {
		t.Fatalf("%d flow records survived the session", remaining)
	}
}

func TestSessionCloseReleasesACompletedFlowsLoginChild(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	authorized := fixture.mustAuthorize("connection-1")

	if err := fixture.callback(authorized.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	// The completed flow keeps its login child alive so the harvest can still
	// read the residence; the session close is what reclaims it.
	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	fixture.broker.mu.Lock()
	live := flow.login != nil
	fixture.broker.mu.Unlock()

	if !live {
		t.Fatal("a completed flow released the residence before the harvest")
	}

	fixture.broker.closeSession(fixture.session.id)

	fixture.broker.mu.Lock()
	released := flow.login == nil
	fixture.broker.mu.Unlock()

	if !released {
		t.Fatal("the session close left a login child running")
	}
}

func TestSupersedeIgnoresAnAbsentFlow(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	fixture.broker.supersede(authFlowKey{sessionID: acp.SessionId("T-none"), providerID: authProviderID}, authReasonSuperseded)
}

func TestCloseLoginLogsATeardownFailure(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	original := authCloseLogin
	authCloseLogin = func(login *nativeamp.AuthLogin) error {
		return errors.Join(errors.New("teardown refused"), original(login))
	}

	fixture.broker.closeLogin(flow)
	authCloseLogin = original
}

func TestNewAuthTokenReportsAnEntropyFailure(t *testing.T) {
	original := authRandRead
	authRandRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }

	t.Cleanup(func() { authRandRead = original })

	if _, err := newAuthToken(); err == nil {
		t.Fatal("a token was minted with no entropy")
	}
}

// adversarialConnectionIDs are the caller-minted values the bound refuses. Each
// is a shape the id would otherwise carry into a durable ledger entry, into the
// name that entry is hashed into, and into the adapter's own logs, and the two
// replacement-rune spellings are one Go string reached from two different wire
// encodings, which aliases one connection onto another's entry.
func adversarialConnectionIDs() map[string]string {
	return map[string]string{
		"empty":              "",
		"path separators":    "../../../etc/passwd",
		"windows separators": `..\..\connection`,
		"newline":            "connection\n1",
		"nul":                "connection\x00 1",
		"bidi override":      "connection\u202e1",
		"space":              "connection 1",
		"colon":              "connection:1",
		"replacement rune":   "connection-�",
		"non ascii":          "connection-é",
		"unbounded":          strings.Repeat("c", authConnectionIDMaxBytes+1),
	}
}

func TestConnectionIDIsRefusedAtEverySurfaceEntry(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	seeded := authLedgerRecord{
		ProviderID: authProviderID, Method: authMethodLogin, ConnectionID: "connection-1",
		Revision: 1, BindingGeneration: 1, State: authLedgerConfirmed,
	}
	if err := fixture.broker.ledger.write(seeded); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}

	for name, connectionID := range adversarialConnectionIDs() {
		t.Run(name, func(t *testing.T) {
			_, err := fixture.authorize(connectionID, "request-1")
			requireInvalidAuthField(t, err, authFieldConnectionID)

			err = fixture.call(AuthDisconnectMethod, map[string]any{
				authFieldSessionID:         string(fixture.session.id),
				authFieldProviderID:        authProviderID,
				authFieldConnectionID:      connectionID,
				authFieldBindingGeneration: seeded.BindingGeneration,
			}, nil)
			requireInvalidAuthField(t, err, authFieldConnectionID)
		})
	}

	// Every refusal landed before the leg derived a ledger name from the id or
	// read the entry the live binding names, so nothing recorded a value the
	// bound rejects.
	live, ok, err := fixture.broker.ledger.read(authProviderID, seeded.ConnectionID)
	if err != nil || !ok {
		t.Fatalf("ledger read: ok=%v err=%v", ok, err)
	}

	if live != seeded {
		t.Fatalf("the live entry changed under a refused connection id: %#v", live)
	}
}

func TestConnectionIDAcceptsTheOpaqueTokenAConsumerMints(t *testing.T) {
	for _, connectionID := range []string{
		"pac_2f1c9b4e-8d3a-4c17-9f21-0b6e5a7c8d90",
		"connection-1",
		"C0",
		strings.Repeat("c", authConnectionIDMaxBytes),
	} {
		if !authValidConnectionID(connectionID) {
			t.Fatalf("connection id %q was refused", connectionID)
		}
	}
}

func TestManualAPIKeyAuthorizeMintsOnlyASecretInteraction(t *testing.T) {
	fixture := newAuthFixture(t, "login", WithEnv(map[string]string{
		"AMP_URL": "https://private.amp.example",
	}))

	if err := os.WriteFile(fixture.session.settingsFile,
		[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	original := authStartLogin
	loginCalls := 0
	authStartLogin = func(*nativeamp.Client, context.Context) (*nativeamp.AuthLogin, error) {
		loginCalls++

		return nil, errors.New("manual authorization must not start amp login")
	}
	t.Cleanup(func() { authStartLogin = original })

	result := fixture.mustAuthorizeMethod("connection-manual", authMethodAPIKey)
	if result.Interaction != authInteractionSecret || result.URL != "" ||
		result.Message != authMethodAPIKeyMessage || result.CallbackInput != authMethodAPIKeyInput ||
		result.FlowID == "" || result.FlowExpiresAt == 0 {
		t.Fatalf("manual authorize = %#v", result)
	}

	if loginCalls != 0 {
		t.Fatalf("manual authorize started %d native logins", loginCalls)
	}

	record, ok, err := fixture.broker.ledger.read(authProviderID, "connection-manual")
	if err != nil || !ok || record.Method != authMethodAPIKey || record.State != authLedgerIntent || record.FlowID != result.FlowID {
		t.Fatalf("manual intent = %#v/%v/%v", record, ok, err)
	}
}

func TestManualAPIKeyPresentationFailsClosedOnUnsafeGuidance(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	original := pinnedAuthCatalog
	pinnedAuthCatalog = func() []authCatalogMethod {
		return []authCatalogMethod{{
			ID: authMethodAPIKey, Type: authMethodTypeAPI, Label: authMethodAPIKeyLabel,
			Message: "unsafe\nmessage", Interaction: authInteractionSecret, CallbackInput: authMethodAPIKeyInput,
		}}
	}
	t.Cleanup(func() { pinnedAuthCatalog = original })

	_, err := fixture.authorizeMethod("connection-unsafe", "request-unsafe", authMethodAPIKey)
	if err == nil {
		t.Fatal("manual authorize relayed unsafe guidance")
	}
	requireAuthCause(t, err, authCauseNativeVeto)
}

func TestManualAPIKeyMaterialValidationAndMethodFence(t *testing.T) {
	invalid := []string{
		"",
		"line\nbreak",
		"carriage\rreturn",
		"control\x00byte",
		strings.Repeat("x", authMaxSecretBytes+1),
	}

	for index, value := range invalid {
		fixture := newAuthFixture(t, "login-hang")
		flow := fixture.mustAuthorizeMethod("connection-invalid", authMethodAPIKey)
		err := fixture.callbackMethod(flow.FlowID, authMethodAPIKey, value)
		if err == nil {
			t.Fatalf("invalid material %d was accepted", index)
		}
		requireInvalidAuthField(t, err, authFieldInput)

		if status := fixture.status(flow.FlowID); status.State != authStatePending {
			t.Fatalf("invalid material %d moved the flow: %#v", index, status)
		}
	}

	fixture := newAuthFixture(t, "login-hang")
	flow := fixture.mustAuthorizeMethod("connection-fenced", authMethodAPIKey)
	if err := fixture.callbackMethod(flow.FlowID, authMethodLogin, manualAmpKeyCanary); err == nil {
		t.Fatal("manual material crossed the method fence")
	} else {
		requireInvalidAuthField(t, err, authFieldMethod)
	}
}

func TestManualAPIKeyMaterialFailurePathsRemainFenced(t *testing.T) {
	t.Run("already saved", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flow := fixture.mustAuthorizeMethod("connection-saved", authMethodAPIKey)
		if err := fixture.callbackMethod(flow.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
			t.Fatal(err)
		}

		err := fixture.callbackMethod(flow.FlowID, authMethodAPIKey, "second-copy")
		requireAuthCause(t, err, authCauseFlowState)
	})

	t.Run("lineage moved", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flow := fixture.mustAuthorizeMethod("connection-moved", authMethodAPIKey)
		record, _, err := fixture.broker.ledger.read(authProviderID, "connection-moved")
		if err != nil {
			t.Fatal(err)
		}
		record.BindingGeneration++
		if writeErr := fixture.broker.ledger.write(record); writeErr != nil {
			t.Fatal(writeErr)
		}

		err = fixture.callbackMethod(flow.FlowID, authMethodAPIKey, manualAmpKeyCanary)
		requireAuthCause(t, err, authCauseBindingConflict)
		if status := fixture.status(flow.FlowID); status.State != authStatePending {
			t.Fatalf("lineage refusal moved the flow: %#v", status)
		}
	})

	t.Run("slot timeout", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flowResult := fixture.mustAuthorizeMethod("connection-timeout", authMethodAPIKey)
		flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, flowResult.FlowID)
		if err != nil {
			t.Fatal(err)
		}

		release, admitted := fixture.broker.admitSlot(context.Background(), authProviderID)
		if !admitted {
			t.Fatal("could not hold the slot gate")
		}
		defer release()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = fixture.broker.saveCredential(ctx, flow, manualAmpKeyCanary)
		requireAuthCause(t, err, authCauseTimeout)
	})

	t.Run("cancel after durable confirmation", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flowResult := fixture.mustAuthorizeMethod("connection-race", authMethodAPIKey)

		originalRename := ledgerRename
		started := make(chan struct{})
		release := make(chan struct{})
		ledgerRename = func(oldPath string, newPath string) error {
			close(started)
			<-release

			return originalRename(oldPath, newPath)
		}
		t.Cleanup(func() { ledgerRename = originalRename })

		answered := make(chan error, 1)
		go func() {
			answered <- fixture.callbackMethod(flowResult.FlowID, authMethodAPIKey, manualAmpKeyCanary)
		}()
		<-started

		if err := fixture.call(AuthCancelMethod, map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: flowResult.FlowID,
		}, nil); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		close(release)

		requireAuthCause(t, <-answered, authCauseFlowState)
		if status := fixture.status(flowResult.FlowID); status.State != authStateCancelled {
			t.Fatalf("raced cancel status = %#v", status)
		}
	})
}

func TestManualAPIKeyCancelWipesUnharvestedMaterial(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")

	pending := fixture.mustAuthorizeMethod("connection-pending", authMethodAPIKey)
	if err := fixture.call(AuthCancelMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: pending.FlowID,
	}, nil); err != nil {
		t.Fatalf("cancel pending manual flow: %v", err)
	}
	if status := fixture.status(pending.FlowID); status.State != authStateCancelled || status.Reason != authReasonOwnerCancel {
		t.Fatalf("pending cancel status = %#v", status)
	}

	saved := fixture.mustAuthorizeMethod("connection-saved", authMethodAPIKey)
	if err := fixture.callbackMethod(saved.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}
	if err := fixture.call(AuthCancelMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: saved.FlowID,
	}, nil); err != nil {
		t.Fatalf("cancel saved manual flow: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, saved.FlowID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.broker.mu.Lock()
	retained := len(flow.credential)
	fixture.broker.mu.Unlock()
	if retained != 0 {
		t.Fatalf("cancelled flow retained %d credential bytes", retained)
	}
	if status := fixture.status(saved.FlowID); status.State != authStateCancelled || status.Reason != authReasonOwnerCancel {
		t.Fatalf("saved cancel status = %#v", status)
	}
	if err := fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: saved.FlowID,
	}, nil); err == nil {
		t.Fatal("cancelled manual material remained harvestable")
	}
}

func TestManualAPIKeyLateExpiryCannotWipeSavedMaterial(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	result := fixture.mustAuthorizeMethod("connection-late-expiry", authMethodAPIKey)
	if err := fixture.callbackMethod(result.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, result.FlowID)
	if err != nil {
		t.Fatal(err)
	}

	// Model the deadline goroutine winning its timer-vs-disarm select just
	// before callback saved the material, then reaching expire after callback
	// returned. A terminal flow already has its owner; the losing expiry is a
	// true no-op and must not perform terminal cleanup on that owner's state.
	fixture.broker.expire(flow)

	if status := fixture.status(result.FlowID); status.State != authStateSaved || status.Reason != "" {
		t.Fatalf("late expiry moved the saved flow: %#v", status)
	}

	credential := harvestAuthCredential(t, fixture, result.FlowID)
	if credential.Credential.API == nil || credential.Credential.API.Key != manualAmpKeyCanary {
		t.Fatalf("late expiry wiped the saved material: %#v", credential)
	}
}
