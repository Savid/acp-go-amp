package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

// fakeLoginURL is the hosted paste-back URL the fake amp binary prints, and the
// only URL this surface may relay.
const fakeLoginURL = "https://ampcode.com/auth/cli-login?authToken=deadbeef"

// authFixture is one agent with a usable durable ledger root, one live session,
// and the fake amp binary behind it.
type authFixture struct {
	t       *testing.T
	agent   *Agent
	broker  *providerAuth
	session *agentSession
	root    string
	state   string
}

func newAuthFixture(t *testing.T, mode string, extra ...Option) *authFixture {
	t.Helper()

	path, state := fakeAgentAmpPath(t, mode)
	root := t.TempDir()
	agent := newTestAgent(append([]Option{
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithProviderAuthRoot(root),
	}, extra...)...)

	fixture := &authFixture{t: t, agent: agent, broker: agent.providerAuth, root: root, state: state}
	fixture.session = fixture.newSession("T-auth")

	return fixture
}

func (f *authFixture) newSession(id acp.SessionId) *agentSession {
	f.t.Helper()

	session, err := newAgentSession(f.t.Context(), f.agent, id, f.t.TempDir(), parsedSessionMeta{}, "", nil)
	if err != nil {
		f.t.Fatalf("newAgentSession: %v", err)
	}

	f.agent.mu.Lock()
	f.agent.sessions[session.id] = session
	f.agent.mu.Unlock()

	f.t.Cleanup(func() { _ = session.Close(context.Background()) })

	return session
}

func (f *authFixture) call(method string, params map[string]any, out any) error {
	f.t.Helper()

	raw, err := json.Marshal(params)
	if err != nil {
		f.t.Fatalf("marshal %s params: %v", method, err)
	}

	result, callErr := f.agent.HandleExtensionMethod(f.t.Context(), method, raw)
	if callErr != nil {
		return callErr
	}

	if out == nil {
		return nil
	}

	encoded, err := json.Marshal(result)
	if err != nil {
		f.t.Fatalf("marshal %s result: %v", method, err)
	}

	if err := json.Unmarshal(encoded, out); err != nil {
		f.t.Fatalf("decode %s result: %v", method, err)
	}

	return nil
}

// generation runs the methods leg and returns the token naming its result.
func (f *authFixture) generation() string {
	f.t.Helper()

	var methods authMethodsResult
	if err := f.call(AuthMethodsMethod, map[string]any{authFieldSessionID: string(f.session.id)}, &methods); err != nil {
		f.t.Fatalf("methods: %v", err)
	}

	return methods.Generation
}

func (f *authFixture) authorize(connectionID string, requestID string) (authAuthorizeResult, error) {
	f.t.Helper()

	var result authAuthorizeResult
	err := f.call(AuthAuthorizeMethod, map[string]any{
		authFieldSessionID:          string(f.session.id),
		authFieldProviderID:         authProviderID,
		authFieldConnectionID:       connectionID,
		authFieldMethodsGeneration:  f.generation(),
		authFieldMethod:             authMethodID,
		authFieldAuthorizeRequestID: requestID,
	}, &result)

	return result, err
}

func (f *authFixture) mustAuthorize(connectionID string) authAuthorizeResult {
	f.t.Helper()

	result, err := f.authorize(connectionID, "request-"+connectionID)
	if err != nil {
		f.t.Fatalf("authorize: %v", err)
	}

	return result
}

func (f *authFixture) callback(flowID string, input string) error {
	f.t.Helper()

	return f.call(AuthCallbackMethod, map[string]any{
		authFieldSessionID:  string(f.session.id),
		authFieldProviderID: authProviderID,
		authFieldMethod:     authMethodID,
		authFieldFlowID:     flowID,
		authFieldInput:      input,
	}, nil)
}

func (f *authFixture) status(flowID string) authStatusResult {
	f.t.Helper()

	var result authStatusResult
	if err := f.call(AuthStatusMethod, map[string]any{
		authFieldSessionID:  string(f.session.id),
		authFieldProviderID: authProviderID,
		authFieldFlowID:     flowID,
	}, &result); err != nil {
		f.t.Fatalf("status: %v", err)
	}

	return result
}

// authFailure decodes the closed data shape of a provider-auth leg failure.
func authFailure(t *testing.T, err error) map[string]any {
	t.Helper()

	var requestErr *acp.RequestError
	if !errors.As(err, &requestErr) {
		t.Fatalf("error %v is not a request error", err)
	}

	encoded, marshalErr := json.Marshal(requestErr.Data)
	if marshalErr != nil {
		t.Fatalf("marshal error data: %v", marshalErr)
	}

	data := map[string]any{}
	if unmarshalErr := json.Unmarshal(encoded, &data); unmarshalErr != nil {
		t.Fatalf("decode error data: %v", unmarshalErr)
	}

	return data
}

func requireAuthCause(t *testing.T, err error, cause string) {
	t.Helper()

	data := authFailure(t, err)
	if data[jsonFieldError] != authFailedErrorTag || data[jsonFieldCause] != cause {
		t.Fatalf("failure = %#v, want cause %q", data, cause)
	}
}

func requireInvalidAuthField(t *testing.T, err error, field string) {
	t.Helper()

	data := authFailure(t, err)
	if data[jsonFieldField] != field {
		t.Fatalf("error = %#v, want invalid field %q", data, field)
	}
}

func TestProviderAuthIsUnadvertisedWithoutAUsableRoot(t *testing.T) {
	bare := newTestAgent()
	if bare.providerAuth != nil {
		t.Fatal("an unset provider-auth root advertised the surface")
	}

	resp, err := bare.Initialize(t.Context(), acp.InitializeRequest{})
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}

	ampMeta, _ := resp.AgentCapabilities.Meta[ampMetaKey].(map[string]any)
	if _, advertised := ampMeta[providerAuthCapabilityKey]; advertised {
		t.Fatalf("capability advertised without a root: %#v", ampMeta)
	}

	if _, err := bare.HandleExtensionMethod(t.Context(), AuthMethodsMethod, json.RawMessage(`{}`)); err == nil {
		t.Fatal("an unadvertised leg answered")
	}

	// A root that was asked for and cannot be prepared is the same verdict as
	// one nobody asked for.
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	if blocked := newTestAgent(WithProviderAuthRoot(file)); blocked.providerAuth != nil {
		t.Fatal("an unusable provider-auth root advertised the surface")
	}
}

func TestProviderAuthAdvertisesEightLegsAndNoInjectionKey(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	resp, err := fixture.agent.Initialize(t.Context(), acp.InitializeRequest{})
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}

	ampMeta, _ := resp.AgentCapabilities.Meta[ampMetaKey].(map[string]any)

	capability, ok := ampMeta[providerAuthCapabilityKey].(map[string]any)
	if !ok {
		t.Fatalf("provider auth capability absent: %#v", ampMeta)
	}

	if _, injectable := capability["injectionKey"]; injectable {
		t.Fatalf("amp advertised an injection key: %#v", capability)
	}

	names, _ := capability[providerAuthMethodsField].([]string)
	want := []string{
		AuthMethodsMethod, AuthAuthorizeMethod, AuthCallbackMethod, AuthStatusMethod,
		AuthCancelMethod, AuthInventoryMethod, AuthCredentialMethod, AuthDisconnectMethod,
	}

	if !slices.Equal(names, want) {
		t.Fatalf("advertised legs = %v, want %v", names, want)
	}

	for _, name := range want {
		if !strings.HasPrefix(name, "_amp/auth/") {
			t.Fatalf("leg %q is outside the vendor extension prefix", name)
		}
	}
}

func TestProviderAuthBootstrapsSessionWithoutAPIKey(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "login")
	t.Setenv("AMP_API_KEY", "")

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithProviderAuthRoot(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": ""}),
	)
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	resp, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	session, err := agent.session(resp.SessionId)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	fixture := &authFixture{
		t:       t,
		agent:   agent,
		broker:  agent.providerAuth,
		session: session,
		root:    agent.options.ProviderAuthRoot,
		state:   state,
	}
	presentation := fixture.mustAuthorize("connection-keyless")
	if presentation.URL != fakeLoginURL || presentation.FlowID == "" {
		t.Fatalf("authorize presentation = %#v", presentation)
	}
	if err := fixture.call(AuthCancelMethod, map[string]any{
		authFieldSessionID:  string(session.id),
		authFieldProviderID: authProviderID,
		authFieldFlowID:     presentation.FlowID,
	}, nil); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}

func TestProviderAuthRejectsAnInjectionOption(t *testing.T) {
	// Redelivery is AMP_API_KEY through WithEnv, so the per-session injection
	// key is not a supported option in any form.
	_, err := parseSessionMeta(map[string]any{ampMetaKey: map[string]any{
		ampOptionsKey: map[string]any{"providerAuth": map[string]any{}},
	}})
	requireInvalidAuthField(t, err, "_meta.amp.options.providerAuth")
}

func TestProviderAuthDirectHomeIsRejectedFailClosed(t *testing.T) {
	agent := newTestAgent(WithProviderAuthDirectHome("/consented/home"))
	if err := agent.validateSessionStartOptions(AmpOptions{}); err == nil {
		t.Fatal("a consented exact home was accepted")
	} else {
		requireInvalidAuthField(t, err, optionFieldProviderAuthDirectHome)
	}

	// A relative value is a construction-time configuration failure instead.
	if err := newTestAgent(WithProviderAuthDirectHome("relative")).optionsError(); err == nil {
		t.Fatal("a relative exact home was accepted at construction")
	}

	if err := newTestAgent(WithProviderAuthRoot("relative")).optionsError(); err == nil {
		t.Fatal("a relative provider-auth root was accepted at construction")
	}

	if err := validateProviderAuthRoots(Options{ProviderAuthRoot: "/abs", ProviderAuthDirectHome: "/abs"}); err != nil {
		t.Fatalf("absolute roots rejected: %v", err)
	}
}

func TestAuthLegsRejectAClosedRequestObject(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	if _, err := authParamFields(json.RawMessage(`[]`), authFieldSessionID); err == nil {
		t.Fatal("a non-object body was accepted")
	}

	if _, err := authParamFields(json.RawMessage(`{"a":1`), "a"); err == nil {
		t.Fatal("a truncated object was accepted")
	}

	if _, err := authParamFields(json.RawMessage(`{} trailing`), "a"); err == nil {
		t.Fatal("trailing content was accepted")
	}

	if _, err := authParamFields(json.RawMessage(`{"a":1,"a":2}`), "a"); err == nil {
		t.Fatal("a duplicate field was accepted")
	}

	if _, err := authParamFields(json.RawMessage(`{"a":}`), "a"); err == nil {
		t.Fatal("a malformed value was accepted")
	}

	if _, err := authParamFields(json.RawMessage(`{1:2}`), "a"); err == nil {
		t.Fatal("a non-string key was accepted")
	}

	if _, err := authParamFields(json.RawMessage(`{"unknown":1}`), "a"); err == nil {
		t.Fatal("an unknown field was accepted")
	}

	if err := fixture.call(AuthMethodsMethod, map[string]any{"unexpected": 1}, nil); err == nil {
		t.Fatal("methods accepted an unknown field")
	}

	if err := fixture.call(AuthMethodsMethod, map[string]any{}, nil); err == nil {
		t.Fatal("methods accepted a missing sessionId")
	}

	if err := fixture.call(AuthMethodsMethod, map[string]any{authFieldSessionID: "T-unknown"}, nil); err == nil {
		t.Fatal("methods answered for an unknown session")
	}
}

func TestAuthFieldDecoders(t *testing.T) {
	fields, err := authParamFields(json.RawMessage(`{"s":"","n":7,"bad":"x"}`), "s", "n", "bad")
	if err != nil {
		t.Fatalf("authParamFields: %v", err)
	}

	if _, decodeErr := authRequiredString(fields, "s"); decodeErr == nil {
		t.Fatal("an empty required string was accepted")
	}

	if _, decodeErr := authRequiredString(fields, "missing"); decodeErr == nil {
		t.Fatal("an absent required string was accepted")
	}

	if _, decodeErr := authRequiredString(fields, "n"); decodeErr == nil {
		t.Fatal("a number decoded as a required string")
	}

	value, err := authString(fields, "s")
	if err != nil || value != "" {
		t.Fatalf("authString = %q/%v", value, err)
	}

	if _, decodeErr := authString(fields, "missing"); decodeErr == nil {
		t.Fatal("an absent string was accepted")
	}

	if _, decodeErr := authString(fields, "n"); decodeErr == nil {
		t.Fatal("a number decoded as a string")
	}

	number, err := authRequiredInt64(fields, "n")
	if err != nil || number != 7 {
		t.Fatalf("authRequiredInt64 = %d/%v", number, err)
	}

	if _, decodeErr := authRequiredInt64(fields, "missing"); decodeErr == nil {
		t.Fatal("an absent int was accepted")
	}

	if _, decodeErr := authRequiredInt64(fields, "bad"); decodeErr == nil {
		t.Fatal("a string decoded as an int")
	}
}

func TestAuthFailureShapeIsClosed(t *testing.T) {
	failure := &authFailedError{cause: authCauseTransport, providerID: "amp", method: "login", flowID: "F1"}
	if failure.Error() != "amp_auth_failed: transport" {
		t.Fatalf("Error() = %q", failure.Error())
	}

	data := authFailure(t, failure.requestError())
	for _, key := range []string{jsonFieldError, jsonFieldCause, "retryable", authFieldProviderID, authFieldMethod, authFieldFlowID} {
		if _, ok := data[key]; !ok {
			t.Fatalf("failure data missing %q: %#v", key, data)
		}
	}

	bare := authFailure(t, authFailed(authCausePolicy, "", "", ""))
	if len(bare) != 3 || bare["retryable"] != false {
		t.Fatalf("bare failure = %#v", bare)
	}

	retryable := map[string]bool{
		authCauseTransport: true, authCauseProcess: true, authCauseTimeout: true,
		authCauseNativeVeto: false, authCauseProviderRefused: false, authCauseHarvestFailed: false,
		authCauseUnsupportedVariant: false, authCauseFlowExpired: false, authCauseFlowState: false,
		authCauseFlowCancelled: false, authCausePolicy: false, authCauseBindingConflict: false,
	}
	for cause, want := range retryable {
		if got := authCauseRetryable(cause); got != want {
			t.Fatalf("authCauseRetryable(%q) = %v, want %v", cause, got, want)
		}
	}
}

func TestAuthFlowTransitionTable(t *testing.T) {
	cases := []struct {
		cause    string
		inFlight bool
		state    string
		reason   string
	}{
		{cause: authCauseNativeVeto, state: authStateFailed, reason: authReasonNativeVeto},
		{cause: authCauseUnsupportedVariant, state: authStateFailed, reason: authReasonNativeVeto},
		{cause: authCauseProviderRefused, state: authStateFailed, reason: authReasonProviderRefused},
		{cause: authCauseTransport, state: authStateFailed, reason: authReasonTransport},
		{cause: authCauseTransport, inFlight: true, state: authStateFailed, reason: authReasonAcceptanceUnknown},
		{cause: authCauseProcess, state: authStateFailed, reason: authReasonProcess},
		{cause: authCauseProcess, inFlight: true, state: authStateFailed, reason: authReasonAcceptanceUnknown},
		{cause: authCauseTimeout, state: authStateFailed, reason: authReasonTransport},
		{cause: authCauseTimeout, inFlight: true, state: authStateFailed, reason: authReasonAcceptanceUnknown},
		{cause: authCauseHarvestFailed, state: authStateFailed, reason: authReasonHarvestFailed},
		{cause: authCauseFlowExpired, state: authStateExpired, reason: authReasonDeadline},
		{cause: authCausePolicy},
		{cause: authCauseBindingConflict},
		{cause: authCauseFlowState},
		{cause: authCauseFlowCancelled},
	}

	for _, testCase := range cases {
		state, reason := authFlowTransition(testCase.cause, testCase.inFlight)
		if state != testCase.state || reason != testCase.reason {
			t.Fatalf("authFlowTransition(%q,%v) = %q/%q, want %q/%q",
				testCase.cause, testCase.inFlight, state, reason, testCase.state, testCase.reason)
		}
	}
}

func TestAuthExtensionDispatchIgnoresUnknownMethods(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	if _, handled, err := fixture.agent.handleAuthExtensionMethod(t.Context(), "_amp/other", nil); handled || err != nil {
		t.Fatalf("an unrelated method was handled: %v/%v", handled, err)
	}

	if _, err := fixture.agent.HandleExtensionMethod(t.Context(), "_amp/other", json.RawMessage(`{}`)); err == nil {
		t.Fatal("an unrelated method answered")
	}

	// Every leg is reachable through the agent's extension dispatch.
	for _, method := range authMethodNames() {
		if _, handled, _ := fixture.agent.handleAuthExtensionMethod(t.Context(), method, json.RawMessage(`{}`)); !handled {
			t.Fatalf("leg %q is not dispatched", method)
		}
	}
}

func TestAuthSessionAccessorsAndPanicGuard(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	if home := fixture.session.authDataHome(); home == "" {
		t.Fatal("the session reports no isolated data home")
	}

	if err := fixture.session.authFileStore(); err != nil {
		t.Fatalf("the wrapper's own settings file does not assert the file store: %v", err)
	}

	// A settings file something else rewrote fails closed rather than reading a
	// store the adapter cannot prove is authoritative.
	if err := os.WriteFile(fixture.session.settingsFile,
		[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := fixture.session.authFileStore(); !errors.Is(err, errAuthNativeStore) {
		t.Fatalf("authFileStore = %v, want the native-store sentinel", err)
	}

	done := make(chan struct{})

	fixture.broker.goSafe("probe", func() {
		defer close(done)

		panic("boom")
	})

	<-done

	// A session with no broker is a no-op rather than a nil dereference.
	plain := newTestAgent(WithScratchDir(t.TempDir()))
	(&agentSession{agent: plain, id: "T-none"}).closeProviderAuth()
}

func TestAuthNativeSeamsAreTheOnlyNativeEntryPoints(t *testing.T) {
	// The package reaches amp's credential state through exactly these, so a
	// second reader cannot appear without changing one of them.
	if authReadSecret == nil || authSecretPresent == nil || authFileStoreAsserted == nil || authStartLogin == nil {
		t.Fatal("a native provider-auth entry point is unset")
	}

	if _, _, err := authReadSecret(t.TempDir()); err != nil {
		t.Fatalf("authReadSecret over an empty home: %v", err)
	}

	if _, err := authFileStoreAsserted(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("an absent settings file asserted the file store")
	}

	if path := nativeamp.AuthSecretsPath("/data"); path != filepath.Join("/data", "amp", "secrets.json") {
		t.Fatalf("AuthSecretsPath = %q", path)
	}
}
