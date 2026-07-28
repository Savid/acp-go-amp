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
	authCauseBindingConflict    = "binding_conflict"
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
//
// The ACP connection runs every inbound request on its own goroutine and
// cancels that goroutine's context when the handler returns; only notifications
// are processed in order. So two legs addressing the same flow, the same
// session, or the same recorded binding run at the same time, and every
// read state → native call → write state sequence below is a check-then-set
// whose window is the whole native call. The admissions in auth_admission.go
// exist for that reason and for no other: without them a wide check-then-set
// hands two legs the same authorization to act on, and because each field
// access is individually locked there is no data race for the detector to
// report — only a lost update the host sees as a disconnect that came back or a
// paste that vanished.
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
	// retired holds the request ids a supersede replaced, so a delayed retry of
	// one fails on its own key instead of destroying the flow that replaced it.
	retired map[authFlowKey]map[string]struct{}
	// closedSessions holds the ids whose sweep already ran, which is what a
	// publication is refused against.
	closedSessions map[acp.SessionId]struct{}
	// admissions serialises authorize per (session, provider); slots serialise
	// every rewrite of one provider's recorded binding. Both are refcounted and
	// drop their own entries, so neither grows with the sessions an agent
	// outlives and neither can hand a held gate away.
	admissions map[authFlowKey]*authGate
	slots      map[string]*authGate
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
		agent:          agent,
		ledger:         ledger,
		flows:          make(map[authFlowKey]*authFlow),
		byID:           make(map[string]*authFlow),
		retained:       make(map[authFlowKey]*authFlow),
		retired:        make(map[authFlowKey]map[string]struct{}),
		closedSessions: make(map[acp.SessionId]struct{}),
		admissions:     make(map[authFlowKey]*authGate),
		slots:          make(map[string]*authGate),
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
// tombstoned session gets the uniform unknown-session rejection, and so does one
// whose close already swept its flows: that is the cheap refusal of an ordinary
// late leg, while publication is what refuses the leg that passed here a moment
// before the close ran.
func (p *providerAuth) authSession(id string) (*agentSession, error) {
	session, err := p.agent.session(acp.SessionId(id))
	if err != nil {
		return nil, err
	}

	if p.sessionClosed(session.id) {
		return nil, unknownSessionError()
	}

	return session, nil
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

// reopenProviderAuth clears the close mark a reinstated id carries. session/close
// drops the id from the live session map without tombstoning it — only delete
// tombstones — so a later load or resume rebuilds a session under exactly that
// id. A mark left behind would refuse every provider-auth leg for it for the
// rest of the agent's life, on the strength of a lifetime that already ended.
func (a *Agent) reopenProviderAuth(sessionID acp.SessionId) {
	if a.providerAuth == nil {
		return
	}

	a.providerAuth.reopenSession(sessionID)
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

// authConnectionIDMaxBytes bounds the caller-minted connection id. The value is
// durable — it lands in a ledger entry a later leg equality-checks against what
// the caller sent — and the bound leaves room for the opaque token a consumer
// mints, of which a prefixed UUID is forty bytes.
const authConnectionIDMaxBytes = 128

// authRequiredConnectionID decodes and validates the connection id a leg
// addresses. It runs where the value enters, ahead of every comparison, every
// write, and the ledger file name the id is hashed into, so no leg ever fences
// against, records, or derives a path from an id this bound refuses. The value
// is never normalised: a later leg compares it byte for byte with what the
// caller sent, so rewriting it would break that comparison.
func authRequiredConnectionID(fields map[string]json.RawMessage) (string, error) {
	value, err := authRequiredString(fields, authFieldConnectionID)
	if err != nil {
		return "", err
	}

	if !authValidConnectionID(value) {
		return "", invalidAuthField(authFieldConnectionID)
	}

	return value, nil
}

// authValidConnectionID reports whether id is an opaque bounded ASCII token.
// The alphabet keeps the id safe in every position it reaches — a path segment,
// a native label, and a log line — and admits no non-ASCII spelling, so two
// wire encodings can never decode to one Go string and alias one connection
// onto another's entry.
func authValidConnectionID(id string) bool {
	if id == "" || len(id) > authConnectionIDMaxBytes {
		return false
	}

	for index := range len(id) {
		if !authConnectionIDByte(id[index]) {
			return false
		}
	}

	return true
}

func authConnectionIDByte(char byte) bool {
	return (char >= 'A' && char <= 'Z') ||
		(char >= 'a' && char <= 'z') ||
		(char >= '0' && char <= '9') ||
		char == '-' || char == '_'
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
