package ampacp

import (
	"context"
	"encoding/json"
	"errors"
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

	if result.Message != authProviderName || result.FlowID == "" || result.FlowExpiresAt == 0 {
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
	record, ok, err := fixture.broker.ledger.read(authProviderID)
	if err != nil || !ok || record.FlowID != result.FlowID || record.State != authLedgerIntent {
		t.Fatalf("ledger after authorize = %#v/%v/%v", record, ok, err)
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
	record, _, err := fixture.broker.ledger.read(authProviderID)
	if err != nil || record.Revision != 2 {
		t.Fatalf("ledger revision = %#v/%v", record, err)
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
			authFieldMethod:             authMethodID,
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
		authFieldMethod:             authMethodID,
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
			authFieldMethod:     authMethodID,
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
		authFieldMethod: authMethodID, authFieldFlowID: authorized.FlowID, authFieldInput: "pasted",
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

// awaitReleasedLogin blocks until a flow's login child has been torn down, so
// the native root it owned is reclaimed before the test's scratch is.
func (f *authFixture) awaitReleasedLogin(flow *authFlow) {
	f.t.Helper()

	for range 200 {
		f.broker.mu.Lock()
		released := flow.login == nil
		f.broker.mu.Unlock()

		if released {
			return
		}

		time.Sleep(25 * time.Millisecond)
	}

	f.t.Fatal("the login child was never released")
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
	fixture.broker.probe(flow)

	fixture.broker.mu.Lock()
	armed := flow.nextProbeAt
	fixture.broker.mu.Unlock()

	if armed.IsZero() {
		t.Fatal("status did not arm the poll interval")
	}

	fixture.broker.probe(flow)

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
	fixture.broker.probe(flow)

	fixture.broker.closeLogin(flow)
	fixture.broker.probe(flow)
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

func TestFlowExpiresOnItsEffectiveDeadline(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")

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

	fixture.awaitReleasedLogin(flow)
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
