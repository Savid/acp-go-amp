package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

// Session-scoped provider-auth extension methods. Amp brokers exactly one
// entry — the Amp account itself — through its hosted paste-back login, and
// hands the resulting long-lived key back out once.
const (
	AuthMethodsMethod   = "_amp/auth/methods"
	AuthAuthorizeMethod = "_amp/auth/authorize"
	AuthCallbackMethod  = "_amp/auth/callback"
	AuthStatusMethod    = "_amp/auth/status"
	AuthCancelMethod    = "_amp/auth/cancel"
	AuthInventoryMethod = "_amp/auth/inventory"
	//nolint:gosec // G101 false positive: this is an ACP method name, not a credential.
	AuthCredentialMethod = "_amp/auth/credential"
	AuthDisconnectMethod = "_amp/auth/disconnect"
)

const (
	providerAuthCapabilityKey = "providerAuth"
	providerAuthMethodsField  = "methods"

	authFailedErrorTag = "amp_auth_failed"

	authFieldSessionID          = "sessionId"
	authFieldProviderID         = "providerId"
	authFieldConnectionID       = "connectionId"
	authFieldMethodsGeneration  = "methodsGeneration"
	authFieldMethod             = "method"
	authFieldAuthorizeRequestID = "authorizeRequestId"
	authFieldInputs             = "inputs"
	authFieldFlowID             = "flowId"
	authFieldInput              = "input"
	authFieldBindingGeneration  = "bindingGeneration"
	authFieldParams             = "params"

	authValueInvalid = "invalid"
	jsonFieldCause   = "cause"
	jsonFieldKey     = "key"
)

// Closed cause enum carried by a provider-auth leg failure.
const (
	authCauseNativeVeto         = "native_veto"
	authCauseProviderRefused    = "provider_refused"
	authCauseTransport          = "transport"
	authCauseProcess            = "process"
	authCauseTimeout            = "timeout"
	authCauseHarvestFailed      = "harvest_failed"
	authCauseUnsupportedVariant = "unsupported_variant"
	authCauseFlowExpired        = "flow_expired"
	authCauseFlowState          = "flow_state"
	authCauseFlowCancelled      = "flow_cancelled"
	authCausePolicy             = "policy"
)

// Native entry points. Every read and every login this surface performs goes
// through exactly these.
var (
	authStartLogin        = (*nativeamp.Client).StartAuthLogin
	authCloseLogin        = (*nativeamp.AuthLogin).Close
	authReadSecret        = nativeamp.AuthReadSecret
	authSecretPresent     = nativeamp.AuthSecretPresent
	authFileStoreAsserted = nativeamp.AuthFileStoreAsserted
)

// authMethodNames lists every advertised leg in the order the capability
// reports them.
func authMethodNames() []string {
	return []string{
		AuthMethodsMethod,
		AuthAuthorizeMethod,
		AuthCallbackMethod,
		AuthStatusMethod,
		AuthCancelMethod,
		AuthInventoryMethod,
		AuthCredentialMethod,
		AuthDisconnectMethod,
	}
}

// providerAuth is the agent-scoped broker behind the provider-auth legs. It
// owns the generation naming the current catalog, the per-session flow records,
// and the durable values-free ledger.
type providerAuth struct {
	agent  *Agent
	ledger *authLedger

	mu         sync.Mutex
	generation string
	flows      map[authFlowKey]*authFlow
	byID       map[string]*authFlow
	// retained holds the most recent flow per key whatever its state, because
	// every terminal transition frees the pending slot while an idempotent
	// repeat must still be answered verbatim.
	retained map[authFlowKey]*authFlow
}

type authFlowKey struct {
	sessionID  acp.SessionId
	providerID string
}

// newProviderAuth builds the broker when a usable durable ledger root is
// configured. A root that was asked for and could not be prepared leaves the
// surface unadvertised, exactly as an unset one does: a leg that cannot record
// what it did must not be offered.
func newProviderAuth(agent *Agent) *providerAuth {
	if !authLedgerRootConfigured(agent.options) {
		return nil
	}

	ledger, err := newAuthLedger(agent.options)
	if err != nil {
		agent.log.WarnContext(context.Background(), "provider auth surface is unavailable", slog.String(jsonFieldError, err.Error()))

		return nil
	}

	return &providerAuth{
		agent:    agent,
		ledger:   ledger,
		flows:    make(map[authFlowKey]*authFlow),
		byID:     make(map[string]*authFlow),
		retained: make(map[authFlowKey]*authFlow),
	}
}

// capability reports the enabled leg names. No injection key is advertised:
// a brokered Amp key is redelivered as AMP_API_KEY through WithEnv, so this
// surface accepts no per-session binding at all.
func (p *providerAuth) capability() map[string]any {
	return map[string]any{providerAuthMethodsField: authMethodNames()}
}

func (a *Agent) handleAuthExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, bool, error) {
	broker := a.providerAuth
	if broker == nil {
		return nil, false, nil
	}

	switch method {
	case AuthMethodsMethod:
		result, err := broker.methods(ctx, params)

		return result, true, err
	case AuthAuthorizeMethod:
		result, err := broker.authorize(ctx, params)

		return result, true, err
	case AuthCallbackMethod:
		result, err := broker.callback(ctx, params)

		return result, true, err
	case AuthStatusMethod:
		result, err := broker.status(ctx, params)

		return result, true, err
	case AuthCancelMethod:
		result, err := broker.cancel(ctx, params)

		return result, true, err
	case AuthInventoryMethod:
		result, err := broker.inventory(ctx, params)

		return result, true, err
	case AuthCredentialMethod:
		result, err := broker.credential(ctx, params)

		return result, true, err
	case AuthDisconnectMethod:
		result, err := broker.disconnect(ctx, params)

		return result, true, err
	default:
		return nil, false, nil
	}
}

// authFailedError is the uniform provider-auth leg failure. Native message
// text, native stdout, and child stderr never reach it: every failure becomes
// this closed shape.
type authFailedError struct {
	cause      string
	providerID string
	method     string
	flowID     string
}

func (f *authFailedError) Error() string {
	return authFailedErrorTag + ": " + f.cause
}

func (f *authFailedError) requestError() *acp.RequestError {
	data := map[string]any{
		jsonFieldError: authFailedErrorTag,
		jsonFieldCause: f.cause,
		"retryable":    authCauseRetryable(f.cause),
	}
	if f.providerID != "" {
		data[authFieldProviderID] = f.providerID
	}

	if f.method != "" {
		data[authFieldMethod] = f.method
	}

	if f.flowID != "" {
		data[authFieldFlowID] = f.flowID
	}

	return acp.NewAuthRequired(data)
}

// authCauseRetryable reports whether the same call could succeed unchanged. The
// three transport-shaped causes can; a refusal, a veto, and every flow-state
// answer cannot, because repeating them changes nothing.
func authCauseRetryable(cause string) bool {
	switch cause {
	case authCauseTransport, authCauseProcess, authCauseTimeout:
		return true
	default:
		return false
	}
}

func authFailed(cause string, providerID string, method string, flowID string) error {
	failure := &authFailedError{cause: cause, providerID: providerID, method: method, flowID: flowID}

	return failure.requestError()
}

// authFlowTransition maps a leg cause to the flow transition it must also
// perform. An empty state means the cause carries no transition: a refusal the
// adapter made itself never consumes the owner's authorization.
func authFlowTransition(cause string, materialInFlight bool) (string, string) {
	switch cause {
	case authCauseNativeVeto, authCauseUnsupportedVariant:
		return authStateFailed, authReasonNativeVeto
	case authCauseProviderRefused:
		return authStateFailed, authReasonProviderRefused
	case authCauseTransport:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonTransport
	case authCauseProcess:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonProcess
	case authCauseTimeout:
		if materialInFlight {
			return authStateFailed, authReasonAcceptanceUnknown
		}

		return authStateFailed, authReasonTransport
	case authCauseHarvestFailed:
		return authStateFailed, authReasonHarvestFailed
	case authCauseFlowExpired:
		return authStateExpired, authReasonDeadline
	default:
		return "", ""
	}
}

// authSession resolves the session a leg addresses. An unknown, unloaded, or
// tombstoned session gets the uniform unknown-session rejection.
func (p *providerAuth) authSession(id string) (*agentSession, error) {
	return p.agent.session(acp.SessionId(id))
}

// authDataHome reports the session's isolated XDG_DATA_HOME, which is where amp
// keeps the account credential in file mode.
func (s *agentSession) authDataHome() string {
	return s.env[envXDGDataHome]
}

// closeProviderAuth terminalizes every flow the session owns before its scratch
// is reclaimed, so a login child never outlives the isolated home it writes to.
func (s *agentSession) closeProviderAuth() {
	if s.agent.providerAuth == nil {
		return
	}

	s.agent.providerAuth.closeSession(s.id)
}

// authFileStore establishes that the file store is still the authoritative one
// before any leg reads or writes a credential. The wrapper owns the settings
// file it points amp at and asserts the native-secrets flag false there; a flag
// that reads back true moves the credential to an item keyed by hostname alone,
// which every session on the host would share.
func (s *agentSession) authFileStore() error {
	asserted, err := authFileStoreAsserted(s.settingsFile)
	if err != nil || !asserted {
		return errAuthNativeStore
	}

	return nil
}

var errAuthNativeStore = errors.New("amp native secrets storage is not asserted off")

// authParamFields walks a leg's params object once, rejecting an unknown field,
// a duplicate field, and a non-object body with the offending field path. Every
// request object on this surface is closed, and encoding/json alone would let a
// duplicate key silently win.
func authParamFields(raw json.RawMessage, allowed ...string) (map[string]json.RawMessage, error) {
	permitted := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		permitted[name] = struct{}{}
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))

	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, invalidAuthField(authFieldParams)
	}

	fields := make(map[string]json.RawMessage, len(allowed))

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, invalidAuthField(authFieldParams)
		}

		key, _ := keyToken.(string)
		if _, ok := permitted[key]; !ok {
			return nil, unsupportedField(key)
		}

		if _, duplicate := fields[key]; duplicate {
			return nil, unsupportedField(key)
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, invalidAuthField(key)
		}

		fields[key] = value
	}

	if _, err := decoder.Token(); err != nil {
		return nil, invalidAuthField(authFieldParams)
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, invalidAuthField(authFieldParams)
	}

	return fields, nil
}

// authRequiredString decodes a non-empty string field.
func authRequiredString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", invalidAuthField(name)
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return "", invalidAuthField(name)
	}

	return value, nil
}

// authString decodes a string field that may be empty but must be present.
func authString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", invalidAuthField(name)
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", invalidAuthField(name)
	}

	return value, nil
}

func authRequiredInt64(fields map[string]json.RawMessage, name string) (int64, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, invalidAuthField(name)
	}

	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, invalidAuthField(name)
	}

	return value, nil
}

func (p *providerAuth) goSafe(name string, fn func()) {
	go func() {
		defer recoverAgentGoroutine(context.Background(), p.agent.log, name)

		fn()
	}()
}

func invalidAuthField(path string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: authValueInvalid,
		jsonFieldField: path,
	})
}
