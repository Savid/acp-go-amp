package ampacp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

// Closed flow states.
const (
	authStatePending       = "pending"
	authStateAuthenticated = "authenticated"
	authStateSaved         = "saved"
	authStateFailed        = "failed"
	authStateCancelled     = "cancelled"
	authStateExpired       = "expired"
)

// Closed flow reasons, legal only against the state each pairs with.
const (
	authReasonProviderRefused   = "provider_refused"
	authReasonNativeVeto        = "native_veto"
	authReasonTransport         = "transport"
	authReasonProcess           = "process"
	authReasonAcceptanceUnknown = "acceptance_unknown"
	authReasonHarvestFailed     = "harvest_failed"
	authReasonOwnerCancel       = "owner_cancel"
	authReasonSuperseded        = "superseded"
	authReasonSessionClosed     = "session_closed"
	authReasonDeadline          = "deadline"
)

// Closed interaction discriminators. Hosted login pastes a value back into its
// native child; manual account material is collected through a secret
// interaction after authorize has minted the flow.
const (
	authInteractionCallback = "callback"
	authInteractionSecret   = "secret"
	authCallbackInputCode   = "code"
)

const (
	// authSafetyDeadline bounds a flow independently of the harness. Amp's login
	// publishes no expiry of its own, so this is the effective deadline.
	authSafetyDeadline = 15 * time.Minute
	// authPollFloor is the fastest cadence a status call may drive a native read
	// at, so consumer poll cadence never propagates into a provider.
	authPollFloor = 5 * time.Second
	// authNativeCallTimeout bounds one native auth call.
	authNativeCallTimeout = 30 * time.Second
)

var (
	authRandRead = rand.Read
	authNow      = time.Now
)

// authFlow is the session-scoped record of one login. The presentation it can
// replay lives here and nowhere else: its url embeds the flow's own auth token,
// so it is code-bearing for the flow's life.
type authFlow struct {
	id                 string
	sessionID          acp.SessionId
	providerID         string
	connectionID       string
	revision           int64
	bindingGeneration  int64
	method             authCatalogMethod
	authorizeRequestID string
	presentation       authAuthorizeResult
	login              *nativeamp.AuthLogin
	// residence is the exact data home this flow's login wrote its credential
	// into. It is read off the child rather than assumed from the session,
	// because a containment boundary may redirect the child's own data home.
	residence string

	createdAt int64
	state     string
	reason    string
	expiresAt time.Time
	harvested bool
	// credential holds manual account material only from the successful secret
	// callback until its one credential harvest. Every cleanup path wipes it.
	credential []byte
	// claimed is held by the one leg driving this flow's login child, so a
	// second callback or a status poll cannot write to a stdin another leg is
	// already writing to and closing.
	claimed bool

	nextProbeAt time.Time

	// ready is closed once the mint has settled, and mintErr carries the verdict
	// it settled on. A repeat of the idempotency key that arrives while the mint
	// is still running waits here instead of starting a second login.
	ready   chan struct{}
	mintErr error

	disarm chan struct{}
}

type authAuthorizeResult struct {
	Interaction   string `json:"interaction"`
	URL           string `json:"url,omitempty"`
	Message       string `json:"message"`
	CallbackInput string `json:"callbackInput,omitempty"`
	FlowID        string `json:"flowId"`
	FlowExpiresAt int64  `json:"flowExpiresAt"`
}

type authFlowIDResult struct {
	FlowID string `json:"flowId"`
}

type authStatusResult struct {
	FlowID    string `json:"flowId"`
	State     string `json:"state"`
	ExpiresAt int64  `json:"expiresAt,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func authTerminal(state string) bool {
	return state != authStatePending
}

// newAuthToken mints an opaque adapter-owned identifier from 16 CSPRNG bytes,
// encoded unpadded base64url. Native flow handles never cross the boundary.
func newAuthToken() (string, error) {
	var value [16]byte
	if _, err := authRandRead(value[:]); err != nil {
		return "", fmt.Errorf("create provider auth token: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(value[:]), nil
}

type authorizeRequest struct {
	sessionID    string
	providerID   string
	connectionID string
	generation   string
	method       string
	// authorizeRequestID is the caller-minted idempotency key. authorize is the
	// only leg that takes one because it is the most destructive leg here.
	authorizeRequestID string
	inputs             map[string]string
}

// authorize starts exactly one flow per (sessionId, providerId). It records the
// idempotency key before any native mint and has persisted the flow's slot
// binding before it returns.
func (p *providerAuth) authorize(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params,
		authFieldSessionID, authFieldProviderID, authFieldConnectionID,
		authFieldMethodsGeneration, authFieldMethod, authFieldAuthorizeRequestID, authFieldInputs)
	if err != nil {
		return nil, err
	}

	request, err := decodeAuthorizeRequest(fields)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(request.sessionID)
	if err != nil {
		return nil, err
	}

	key := authFlowKey{sessionID: session.id, providerID: request.providerID}

	releaseKey, admitted := p.admitKey(ctx, key)
	if !admitted {
		return nil, authFailed(authCauseTimeout, request.providerID, request.method, "")
	}

	defer releaseKey()

	// The retired check precedes the replay because the two can never both
	// answer: a supersede retires the key it replaced and the successor becomes
	// the retained one. Asking first is what keeps a supersede whose successor
	// never published from replaying the record it already tore down.
	if p.requestRetired(key, request.authorizeRequestID) {
		return nil, invalidAuthField(authFieldAuthorizeRequestID)
	}

	if replay, replayed, replayErr := p.replayAuthorize(ctx, key, request.authorizeRequestID); replayed {
		if replayErr != nil {
			return nil, replayErr
		}

		return replay, nil
	}

	method, err := p.authResolveMethod(request.providerID, request.generation, request.method)
	if err != nil {
		return nil, err
	}

	if inputErr := validateAuthInputs(request.inputs); inputErr != nil {
		return nil, inputErr
	}

	// Manual material never enters Amp's native store. Hosted login does, so
	// assert that store before minting an id or superseding any retained flow:
	// a local-policy refusal must leave the owner's existing flow and durable
	// lineage untouched.
	if method.Type != authMethodTypeAPI {
		if storeErr := session.authFileStore(); storeErr != nil {
			return nil, authFailed(authCauseNativeVeto, request.providerID, request.method, "")
		}
	}

	flowID, err := newAuthToken()
	if err != nil {
		return nil, authFailed(authCauseProcess, request.providerID, request.method, "")
	}

	p.supersede(key, authReasonSuperseded)

	now := authNow()

	record, recordErr := p.recordIntent(ctx, request, flowID, now)
	if recordErr != nil {
		return nil, recordErr
	}

	flow := &authFlow{
		id:                 flowID,
		sessionID:          session.id,
		providerID:         request.providerID,
		connectionID:       request.connectionID,
		revision:           record.Revision,
		bindingGeneration:  record.BindingGeneration,
		method:             method,
		authorizeRequestID: request.authorizeRequestID,
		createdAt:          record.CreatedAt,
		state:              authStatePending,
		expiresAt:          now.Add(authSafetyDeadline),
		ready:              make(chan struct{}),
		disarm:             make(chan struct{}),
	}

	if !p.publishFlow(key, flow, session) {
		return nil, unknownSessionError()
	}

	presentation, cause := p.mintPresentation(ctx, session, flow)
	if cause != "" {
		mintErr := p.fail(flow, cause, false)
		p.settleMint(flow, authAuthorizeResult{}, mintErr)

		return nil, mintErr
	}

	p.settleMint(flow, presentation, nil)
	p.armCompleter(flow)

	return presentation, nil
}

// recordIntent persists the flow's slot binding before any login child exists.
// The read that carries the prior generation forward and the write that
// supersedes it are one sequence under the slot gate: a disconnect landing
// between them would have its own bump read back as this flow's generation and
// silently undone by the write that follows.
func (p *providerAuth) recordIntent(ctx context.Context, request authorizeRequest, flowID string, now time.Time) (authLedgerRecord, error) {
	release, admitted := p.admitSlot(ctx, request.providerID)
	if !admitted {
		return authLedgerRecord{}, authFailed(authCauseTimeout, request.providerID, request.method, "")
	}

	defer release()

	record := authLedgerRecord{
		ProviderID:         request.providerID,
		Method:             request.method,
		ConnectionID:       request.connectionID,
		Revision:           1,
		BindingGeneration:  1,
		FlowID:             flowID,
		AuthorizeRequestID: request.authorizeRequestID,
		State:              authLedgerIntent,
		CreatedAt:          now.UnixMilli(),
		UpdatedAt:          now.UnixMilli(),
	}

	if prior, ok, readErr := p.ledger.read(request.providerID, request.connectionID); readErr == nil && ok {
		record.Revision = prior.Revision + 1
		record.BindingGeneration = prior.BindingGeneration
		record.CreatedAt = prior.CreatedAt
	}

	if writeErr := p.ledger.write(record); writeErr != nil {
		return authLedgerRecord{}, authFailed(authCauseProcess, request.providerID, request.method, "")
	}

	return record, nil
}

// settleMint publishes the mint's verdict to every waiting repeat. A flow the
// session closed or a newer authorize superseded while the mint was still
// running is already torn down and unreachable, so the child this mint started
// is released here rather than left holding a native root nobody can reclaim.
func (p *providerAuth) settleMint(flow *authFlow, presentation authAuthorizeResult, mintErr error) {
	p.mu.Lock()

	flow.presentation = presentation
	flow.mintErr = mintErr
	orphaned := authTerminal(flow.state)

	close(flow.ready)
	p.mu.Unlock()

	if orphaned {
		p.closeLogin(flow)
	}
}

func decodeAuthorizeRequest(fields map[string]json.RawMessage) (authorizeRequest, error) {
	request := authorizeRequest{}

	var err error
	if request.sessionID, err = authRequiredString(fields, authFieldSessionID); err != nil {
		return request, err
	}

	if request.providerID, err = authRequiredString(fields, authFieldProviderID); err != nil {
		return request, err
	}

	if request.connectionID, err = authRequiredConnectionID(fields); err != nil {
		return request, err
	}

	if request.generation, err = authRequiredString(fields, authFieldMethodsGeneration); err != nil {
		return request, err
	}

	if request.method, err = authRequiredString(fields, authFieldMethod); err != nil {
		return request, err
	}

	if request.authorizeRequestID, err = authRequiredString(fields, authFieldAuthorizeRequestID); err != nil {
		return request, err
	}

	if raw, ok := fields[authFieldInputs]; ok {
		if err := json.Unmarshal(raw, &request.inputs); err != nil {
			return request, invalidAuthField(authFieldInputs)
		}
	}

	return request, nil
}

// replayAuthorize answers a repeated idempotency key verbatim from the retained
// record: no supersede, no completer disarm, no destruction of flow or broker
// state, and no native call. The record outlives every terminal transition, so
// a repeat is answerable for as long as the session lives.
func (p *providerAuth) replayAuthorize(ctx context.Context, key authFlowKey, requestID string) (authAuthorizeResult, bool, error) {
	p.mu.Lock()

	flow, ok := p.retained[key]
	if !ok || flow.authorizeRequestID != requestID {
		p.mu.Unlock()

		return authAuthorizeResult{}, false, nil
	}

	ready := flow.ready
	p.mu.Unlock()

	select {
	case <-ready:
	case <-ctx.Done():
		// The caller stopped waiting on a mint that is still running, so nothing
		// about the flow is decided and nothing is consumed.
		return authAuthorizeResult{}, true, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return flow.presentation, true, flow.mintErr
}

// mintPresentation runs `amp login` in the session's isolated data home and
// relays the hosted paste-back URL it prints. The URL carries the flow's own
// auth token, so it is validated against its bound before it is relayed and is
// treated as code-bearing everywhere afterwards. A session pointed at a
// deployment this surface has no measured URL host or store key for is refused
// here, before any login child exists. A refusal is reported as its cause
// alone: the flow is already registered, so the caller terminalizes it through
// the transition that cause pairs with.
func (p *providerAuth) mintPresentation(ctx context.Context, session *agentSession, flow *authFlow) (authAuthorizeResult, string) {
	if flow.method.Type == authMethodTypeAPI {
		message, ok := authDisplayText(flow.method.Message, authMaxMessageBytes)
		if !ok {
			return authAuthorizeResult{}, authCauseNativeVeto
		}

		return authAuthorizeResult{
			Interaction:   flow.method.Interaction,
			Message:       message,
			CallbackInput: flow.method.CallbackInput,
			FlowID:        flow.id,
			FlowExpiresAt: flow.expiresAt.UnixMilli(),
		}, ""
	}

	client := session.client()
	if !client.AuthDeploymentSupported() {
		return authAuthorizeResult{}, authCauseUnsupportedVariant
	}

	login, err := authStartLogin(client, ctx)
	if err != nil {
		// A launch the audit refused is a variant this adapter does not
		// broker, not a native process failure: no child ever existed.
		if errors.Is(err, nativeamp.ErrBrowserLaunchUnsupported) {
			return authAuthorizeResult{}, authCauseUnsupportedVariant
		}

		return authAuthorizeResult{}, authCauseProcess
	}

	p.mu.Lock()
	flow.login = login
	flow.residence = login.DataHome()
	p.mu.Unlock()

	urlCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	minted, err := login.URL(urlCtx)
	if err != nil {
		return authAuthorizeResult{}, authCauseProcess
	}

	authorizeURL, ok := authDisplayURL(minted)
	if !ok {
		return authAuthorizeResult{}, authCauseNativeVeto
	}

	return authAuthorizeResult{
		Interaction:   authInteractionCallback,
		URL:           authorizeURL,
		Message:       flow.method.Label,
		CallbackInput: authCallbackInputCode,
		FlowID:        flow.id,
		FlowExpiresAt: flow.expiresAt.UnixMilli(),
	}, ""
}

// armCompleter bounds the flow by its effective deadline. It is armed exactly
// once, at authorize, and status never starts, extends, or rearms it.
func (p *providerAuth) armCompleter(flow *authFlow) {
	deadline := time.Until(flow.expiresAt)
	disarm := flow.disarm

	p.goSafe("provider auth completer", func() {
		timer := time.NewTimer(deadline)
		defer timer.Stop()

		select {
		case <-disarm:
			return
		case <-timer.C:
			p.expire(flow)
		}
	})
}

func (p *providerAuth) expire(flow *authFlow) {
	p.mu.Lock()

	if authTerminal(flow.state) {
		p.mu.Unlock()

		return
	}

	flow.state = authStateExpired
	flow.reason = authReasonDeadline
	flow.dropCredential()

	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	p.mu.Unlock()

	p.closeLogin(flow)
}

// supersede retires the record a new authorize replaces, whatever state it
// reached. Its flow id stops addressing anything, its idempotency key is retired
// so a delayed retry of it cannot mint in place of the successor, and its login
// child is torn down — a flow that completed still holds the credential it
// installed under that child's own root, and a record nobody can address any
// more is one nobody can harvest, so leaving the residence up would leave a live
// key resident for a connection this broker no longer answers for. A flow that
// already reached a terminal state keeps it: being replaced is what ends its
// life, not the transition it happened to end on.
func (p *providerAuth) supersede(key authFlowKey, reason string) {
	p.mu.Lock()

	flow, ok := p.retained[key]
	if !ok {
		p.mu.Unlock()

		return
	}

	delete(p.flows, key)
	delete(p.byID, flow.id)
	p.retire(key, flow.authorizeRequestID)
	flow.dropCredential()

	if !authTerminal(flow.state) {
		flow.state = authStateCancelled
		flow.reason = reason
	}

	flow.stopCompleter()
	p.mu.Unlock()

	p.closeLogin(flow)
}

// closeLogin tears the login child down through its containment boundary. Amp
// has no native flow-cancel route, so this claims no provider-side
// cancellation: the hosted flow stays open at Amp until it expires there.
func (p *providerAuth) closeLogin(flow *authFlow) {
	p.mu.Lock()
	login := flow.login
	flow.login = nil
	p.mu.Unlock()

	if login == nil {
		return
	}

	if err := authCloseLogin(login); err != nil {
		p.agent.log.WarnContext(context.Background(), "amp login teardown failed", slog.String(jsonFieldError, err.Error()))
	}
}

func (f *authFlow) stopCompleter() {
	select {
	case <-f.disarm:
	default:
		close(f.disarm)
	}
}

func (f *authFlow) dropCredential() {
	for index := range f.credential {
		f.credential[index] = 0
	}

	f.credential = nil
}

// callback submits the value the owner pasted back. That value is not an
// authorization code to be exchanged: it carries the account credential in the
// clear, so it is written straight to the login child and is never logged,
// echoed, persisted, or retained after delivery.
func (p *providerAuth) callback(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldMethod, authFieldFlowID, authFieldInput)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, err
	}

	method, err := authRequiredString(fields, authFieldMethod)
	if err != nil {
		return nil, err
	}

	flowID, err := authRequiredString(fields, authFieldFlowID)
	if err != nil {
		return nil, err
	}

	input, err := authString(fields, authFieldInput)
	if err != nil {
		return nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, err
	}

	flow, err := p.addressFlow(session.id, providerID, flowID)
	if err != nil {
		return nil, err
	}

	if flow.method.ID != method {
		return nil, invalidAuthField(authFieldMethod)
	}

	if flow.method.Type == authMethodTypeAPI {
		if secretErr := validateAuthSecret(input); secretErr != nil {
			return nil, secretErr
		}

		return p.saveCredential(ctx, flow, input)
	}

	if claimErr := p.claimFlow(flow); claimErr != nil {
		return nil, claimErr
	}

	defer p.releaseFlow(flow)

	if input == "" || len(input) > authMaxCallbackBytes {
		return nil, invalidAuthField(authFieldInput)
	}

	p.mu.Lock()
	login := flow.login
	p.mu.Unlock()

	if login == nil {
		return nil, p.fail(flow, authCauseTransport, false)
	}

	if storeErr := session.authFileStore(); storeErr != nil {
		return nil, p.fail(flow, authCauseNativeVeto, false)
	}

	releaseSlot, admitted := p.admitSlot(ctx, providerID)
	if !admitted {
		return nil, authFailed(authCauseTimeout, providerID, method, flow.id)
	}

	defer releaseSlot()

	// Amp races a hook that can finish the login without any paste. A child that
	// has already settled wants no input, and writing to it would report a dead
	// pipe as a refusal of a login that in fact succeeded.
	if settled, settleErr := login.Settled(); settled {
		if settleErr != nil {
			return nil, p.failSettled(flow, authCauseProviderRefused, false)
		}

		if err := p.completeFlow(flow); err != nil {
			return nil, err
		}

		return authFlowIDResult{FlowID: flow.id}, nil
	}

	// The recorded binding is re-read before the paste reaches the child, not
	// only before the confirmation is written. A disconnect that already released
	// the slot leaves nothing for this paste to install against, and a paste
	// submitted anyway would put the account key in the child's residence with
	// the ledger reading `removed` over it.
	if !p.lineageCurrent(flow) {
		return nil, authFailed(authCauseBindingConflict, providerID, method, flow.id)
	}

	submitCtx, cancel := context.WithTimeout(ctx, authNativeCallTimeout)
	defer cancel()

	if submitErr := login.Submit(submitCtx, input); submitErr != nil {
		return nil, p.failSettled(flow, authCauseProviderRefused, true)
	}

	if err := p.completeFlow(flow); err != nil {
		return nil, err
	}

	return authFlowIDResult{FlowID: flow.id}, nil
}

// saveCredential binds manually supplied material to this flow without writing
// it to Amp's native store, the ledger, session state, or configuration. The
// host receives the exact opaque key from the credential leg and redelivers it
// as AMP_API_KEY through the existing environment boundary.
func (p *providerAuth) saveCredential(ctx context.Context, flow *authFlow, input string) (any, error) {
	if claimErr := p.claimFlow(flow); claimErr != nil {
		return nil, claimErr
	}

	defer p.releaseFlow(flow)

	releaseSlot, admitted := p.admitSlot(ctx, flow.providerID)
	if !admitted {
		return nil, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	}

	defer releaseSlot()

	if err := p.confirmFlow(flow, false); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if authTerminal(flow.state) {
		p.mu.Unlock()

		return nil, authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	flow.credential = []byte(input)
	flow.state = authStateSaved
	flow.reason = ""
	flow.stopCompleter()
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	p.mu.Unlock()

	return authFlowIDResult{FlowID: flow.id}, nil
}

// completeFlow records the post-mutation confirmation and terminalizes the flow
// as authenticated. The credential the login just wrote is not read here: the
// harvest leg is the only reader, and it runs at most once. Its caller holds the
// slot gate, so the binding it checks here cannot move before the write below
// lands.
func (p *providerAuth) completeFlow(flow *authFlow) error {
	if err := p.confirmFlow(flow, true); err != nil {
		return err
	}

	p.terminalize(flow, authStateAuthenticated, "")

	return nil
}

// confirmFlow persists the post-material confirmation under the caller's slot
// gate. It records lineage only; secret material never enters the ledger.
func (p *providerAuth) confirmFlow(flow *authFlow, materialInFlight bool) error {
	if cause, abandoned := p.abandonedCause(flow); abandoned {
		return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
	}

	if !p.lineageCurrent(flow) {
		return authFailed(authCauseBindingConflict, flow.providerID, flow.method.ID, flow.id)
	}

	record := authLedgerRecord{
		ProviderID:         flow.providerID,
		Method:             flow.method.ID,
		ConnectionID:       flow.connectionID,
		Revision:           flow.revision,
		BindingGeneration:  flow.bindingGeneration,
		FlowID:             flow.id,
		AuthorizeRequestID: flow.authorizeRequestID,
		State:              authLedgerConfirmed,
		CreatedAt:          flow.createdAt,
		UpdatedAt:          authNow().UnixMilli(),
	}

	if err := p.ledger.write(record); err != nil {
		return p.fail(flow, authCauseProcess, materialInFlight)
	}

	return nil
}

// abandonedCause reports the cause a leg answers with when the flow reached a
// terminal state while the native call this leg started was still in flight.
// Such a leg owns no transition and confirms nothing: the record it addressed
// is already closed, and the outcome it carries is no longer the flow's.
func (p *providerAuth) abandonedCause(flow *authFlow) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch {
	case !authTerminal(flow.state):
		return "", false
	case flow.state == authStateCancelled:
		return authCauseFlowCancelled, true
	default:
		return authCauseFlowState, true
	}
}

// failSettled answers a native outcome that could have arrived after the flow
// closed. The transition it would otherwise perform belongs to whoever closed
// the flow first, and a cause naming the provider over a login the owner
// abandoned reports a refusal nobody made.
func (p *providerAuth) failSettled(flow *authFlow, cause string, materialInFlight bool) error {
	if abandoned, ok := p.abandonedCause(flow); ok {
		return authFailed(abandoned, flow.providerID, flow.method.ID, flow.id)
	}

	return p.fail(flow, cause, materialInFlight)
}

// fail returns the leg's closed error and performs the transition its cause
// pairs with. A cause with no transition consumes nothing.
func (p *providerAuth) fail(flow *authFlow, cause string, materialInFlight bool) error {
	if state, reason := authFlowTransition(cause, materialInFlight); state != "" {
		p.terminalize(flow, state, reason)
		p.closeLogin(flow)
	}

	return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
}

// terminalize records the flow's one terminal transition and frees the pending
// slot. A flow that already reached one keeps it: a login child still running
// when the owner cancelled settles into a record the owner already closed, and
// what it settled on is no longer the flow's outcome. It deliberately leaves the
// login child's handle in place: on a successful completion the credential is
// resident under that child's own root, and tearing the child down reclaims the
// root the harvest still has to read.
func (p *providerAuth) terminalize(flow *authFlow, state string, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if authTerminal(flow.state) {
		return
	}

	flow.state = state
	flow.reason = reason

	if state != authStateSaved {
		flow.dropCredential()
	}

	flow.stopCompleter()
	delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
}

// addressFlow resolves a flowId a caller supplied. A missing, unknown,
// superseded, or cross-session id is a caller addressing failure and never a
// flow failure.
func (p *providerAuth) addressFlow(sessionID acp.SessionId, providerID string, flowID string) (*authFlow, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	flow, ok := p.byID[flowID]
	if !ok || flow.sessionID != sessionID || flow.providerID != providerID {
		return nil, invalidAuthField(authFieldFlowID)
	}

	return flow, nil
}

// status reports the flow, not the connection. Amp's key never expires, so no
// credential expiry is ever reported.
func (p *providerAuth) status(ctx context.Context, params json.RawMessage) (any, error) {
	_, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.probe(ctx, flow)

	p.mu.Lock()
	defer p.mu.Unlock()

	return authStatusResult{FlowID: flow.id, State: flow.state, Reason: flow.reason}, nil
}

// probe refreshes a pending flow from the login child behind the adapter's own
// interval, serving the cached state in between so a consumer's poll cadence
// never reaches the provider. Amp races a loopback hook against the paste, so a
// flow can complete without any callback arriving at all. It drives the same
// login child a callback does, so it takes the same claim — and takes it only if
// it is free, because a poll has nothing to add to a flow a callback is already
// completing.
func (p *providerAuth) probe(ctx context.Context, flow *authFlow) {
	if !p.tryClaimFlow(flow) {
		return
	}

	defer p.releaseFlow(flow)

	p.mu.Lock()

	now := authNow()
	if flow.login == nil || now.Before(flow.nextProbeAt) {
		p.mu.Unlock()

		return
	}

	flow.nextProbeAt = now.Add(authPollFloor)
	login := flow.login
	p.mu.Unlock()

	settled, err := login.Settled()
	if !settled {
		return
	}

	if err != nil {
		p.terminalize(flow, authStateFailed, authReasonProviderRefused)
		p.closeLogin(flow)

		return
	}

	releaseSlot, admitted := p.admitSlot(ctx, flow.providerID)
	if !admitted {
		return
	}

	defer releaseSlot()

	_ = p.completeFlow(flow)
}

// cancel disarms the completer, terminalizes the flow record, frees the pending
// slot, and tears the login child down. It never claims provider-side
// cancellation: the hosted flow stays open at Amp until it expires there.
func (p *providerAuth) cancel(_ context.Context, params json.RawMessage) (any, error) {
	_, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()

	if authTerminal(flow.state) {
		if flow.state == authStateSaved {
			flow.dropCredential()
			flow.state = authStateCancelled
			flow.reason = authReasonOwnerCancel
		}
		p.mu.Unlock()

		return authFlowIDResult{FlowID: flow.id}, nil
	}

	p.mu.Unlock()

	p.terminalize(flow, authStateCancelled, authReasonOwnerCancel)
	p.closeLogin(flow)

	return authFlowIDResult{FlowID: flow.id}, nil
}

func (p *providerAuth) addressedFlowLeg(params json.RawMessage) (*agentSession, *authFlow, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldFlowID)
	if err != nil {
		return nil, nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, nil, err
	}

	flowID, err := authRequiredString(fields, authFieldFlowID)
	if err != nil {
		return nil, nil, err
	}

	session, err := p.authSession(sessionID)
	if err != nil {
		return nil, nil, err
	}

	flow, err := p.addressFlow(session.id, providerID, flowID)
	if err != nil {
		return nil, nil, err
	}

	return session, flow, nil
}

// closeSession cancels every pending flow the session owns, terminalizing each
// as cancelled/session_closed, and tears down every login child the session
// still holds — including the completed ones a harvest never came for. It runs
// before the session's scratch is reclaimed, so a login child never outlives
// the isolated home it was writing into. It is also the one place a retained
// record is dropped: an idempotent repeat is answerable for exactly as long as
// the session that owns the flow.
//
// The id is marked closed in the same critical section that takes the sweep set,
// which is what makes the set complete: an authorize already past its session
// lookup cannot publish afterwards, so there is no flow the sweep can miss and
// no need to make close wait for a native call it does not own.
func (p *providerAuth) closeSession(sessionID acp.SessionId) {
	p.mu.Lock()

	p.closedSessions[sessionID] = struct{}{}

	owned := make([]*authFlow, 0, len(p.byID))

	for id, flow := range p.byID {
		if flow.sessionID != sessionID {
			continue
		}

		delete(p.byID, id)
		delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
		flow.dropCredential()

		if !authTerminal(flow.state) {
			flow.state = authStateCancelled
			flow.reason = authReasonSessionClosed
		}

		flow.stopCompleter()

		owned = append(owned, flow)
	}

	for key := range p.retained {
		if key.sessionID == sessionID {
			delete(p.retained, key)
		}
	}

	for key := range p.retired {
		if key.sessionID == sessionID {
			delete(p.retired, key)
		}
	}

	p.mu.Unlock()

	for _, flow := range owned {
		p.closeLogin(flow)
	}
}
