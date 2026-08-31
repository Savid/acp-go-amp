package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/savid/acp-go-amp/internal/observer"
)

const agentClosedMessage = "agent closed"

type Agent struct {
	options             Options
	log                 *slog.Logger
	store               SessionStore
	observe             *observer.Observer
	ordinaryEnvironment map[string]string
	nativeEnvironment   map[string]string

	callShutdown chan struct{}
	callWG       sync.WaitGroup
	shutdownOnce sync.Once
	callbackMu   sync.Mutex
	callbacks    map[uint64]*agentCallbackAuthority
	nextCallback uint64
	closeMu      sync.Mutex
	closeFlight  *agentCloseFlight
	closeDone    bool
	closeErr     error

	mu       sync.Mutex
	closed   bool
	conn     agentClient
	sessions map[acp.SessionId]*agentSession
	deleted  map[acp.SessionId]struct{}
	// sessionFlights publish close/delete intent before any external work. A
	// flight owns one generation and, once known, the exact wrapper it settles.
	sessionFlights map[acp.SessionId]*agentSessionFlight
	// sessionUses are load/resume leases. Delete publishes its flight first, then
	// waits for the exact already-admitted use without holding mu.
	sessionUses map[acp.SessionId]*agentSessionUse
	// cleanupOwners retain private or tombstoned wrappers whose local/native
	// cleanup failed. Wire operations may retry them until Agent.Close takes its
	// final, last-word ownership pass.
	cleanupOwners           map[acp.SessionId][]agentCleanupOwner
	cleanupResidences       map[uint64]*agentCleanupResidence
	nextSessionGeneration   uint64
	nextCallGeneration      uint64
	nextCleanupResidence    uint64
	nextCallbackGeneration  atomic.Uint64
	pending                 int
	clientCalls             chan struct{}
	lifecycleContainmentErr error

	activeLimitErr   error
	configurationErr error
	providerAuth     *providerAuth
	// lifecycle is the answer this connection negotiated at initialize. It is
	// the contract for the connection: with no answer present there is no
	// envelope, no correlation read, and no lifecycle fact at all.
	lifecycle lifecycle.Negotiated

	// harnessMu guards harnessPath, the exact absolute amp binary the version
	// and startup probes validated against the static base. Every child this
	// agent launches afterwards runs that file, so no session environment can
	// substitute a different harness later.
	harnessMu   sync.Mutex
	harnessPath string
}

type agentSessionFlightKind uint8

const (
	agentSessionCloseFlight agentSessionFlightKind = iota + 1
	agentSessionDeleteFlight
)

type agentSessionFlight struct {
	generation uint64
	kind       agentSessionFlightKind
	session    *agentSession
	use        *agentSessionUse
	done       chan struct{}
	panicErr   error
	abandoned  bool
	reclaiming bool
	waiters    int
}

type agentSessionUse struct {
	generation uint64
	session    *agentSession
	done       chan struct{}
}

type agentCleanupKind uint8

const (
	agentCleanupConstructing agentCleanupKind = iota + 1
	agentCleanupPrepared
	agentCleanupDeleted
)

type agentCleanupOwner struct {
	session *agentSession
	kind    agentCleanupKind
}

type agentCallGeneration struct {
	agent      *Agent
	generation uint64
}

type agentCallbackGeneration struct {
	generation uint64
	kind       string
}

type agentCloseFlight struct {
	done    chan struct{}
	err     error
	waiters int
}

// retainHarnessPath records the harness a probe validated. An empty or relative
// answer is a broken probe rather than a reason to resolve again.
func (a *Agent) retainHarnessPath(path string) error {
	if !a.options.hostAuthoritySupplied && !filepath.IsAbs(path) {
		return fmt.Errorf("amp startup probe reported unusable harness path %q", path)
	}

	a.harnessMu.Lock()
	a.harnessPath = path
	a.harnessMu.Unlock()

	return nil
}

// retainedHarnessPath is the validated harness every later launch runs, or ""
// before any probe has validated one.
func (a *Agent) retainedHarnessPath() string {
	a.harnessMu.Lock()
	defer a.harnessMu.Unlock()

	return a.harnessPath
}

var newAgentForServe = NewAgent

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)

	log := options.Logger
	if log == nil {
		log = slog.Default()
	}

	store := options.SessionStore
	if store == nil {
		store = NewInMemorySessionStore()
	}

	store = callbackSessionStore{store: store}

	observe := observer.New(observer.Config{
		TracerProvider: options.TracerProvider,
		MeterProvider:  options.MeterProvider,
		Propagator:     options.TextMapPropagator,
		Version:        options.AgentVersion,
	})

	var (
		nativeEnvironment map[string]string
		authorityErr      error
	)
	if options.hostAuthoritySupplied {
		nativeEnvironment, authorityErr = readHostEnvironment(options.HostAuthority)
	}

	agent := &Agent{
		options:             options,
		log:                 log,
		store:               store,
		observe:             observe,
		ordinaryEnvironment: nativeamp.CaptureOrdinaryEnvironment(),
		nativeEnvironment:   nativeEnvironment,
		sessions:            make(map[acp.SessionId]*agentSession),
		deleted:             make(map[acp.SessionId]struct{}),
		sessionFlights:      make(map[acp.SessionId]*agentSessionFlight),
		sessionUses:         make(map[acp.SessionId]*agentSessionUse),
		cleanupOwners:       make(map[acp.SessionId][]agentCleanupOwner),
		cleanupResidences:   make(map[uint64]*agentCleanupResidence),
		clientCalls:         make(chan struct{}, maxConcurrentClientCalls(options.ConcurrencyLimits)),
		callShutdown:        make(chan struct{}),
		callbacks:           make(map[uint64]*agentCallbackAuthority),
		activeLimitErr:      validateConcurrencyLimits(options.ConcurrencyLimits),
		configurationErr: errors.Join(
			authorityErr,
			validateEnvironment(nativeEnvironment),
			validateContainmentOptions(options),
			validateImageLimits(options.ImageLimits),
			validateInputHandoffRoot(options.InputHandoffRoot),
			validateProviderAuthRoots(options),
		),
	}
	agent.providerAuth = newProviderAuth(agent)

	return agent
}

func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	agent := newAgentForServe(opts...)
	defer func() {
		if closeErr := agent.Close(); closeErr != nil {
			returnErr = closeErr
		}
	}()

	conn := newLocalAgentConnection(agent, output, input)
	agent.setConnection(conn)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

func (a *Agent) Close() (err error) {
	if a.hasActiveCallbackAuthority() {
		return closedCallbackRefusal()
	}

	a.closeMu.Lock()
	if a.closeDone {
		closeErr := a.closeErr
		a.closeMu.Unlock()

		return closeErr
	}

	if existing := a.closeFlight; existing != nil {
		existing.waiters++
		a.closeMu.Unlock()
		<-existing.done

		return existing.err
	}

	flight := &agentCloseFlight{done: make(chan struct{})}
	a.closeFlight = flight
	a.closeMu.Unlock()

	err = invokeShutdownStep(a.closeAttempt)
	a.finishCloseFlight(flight, err)

	return err
}

func (a *Agent) finishCloseFlight(flight *agentCloseFlight, err error) {
	a.closeMu.Lock()
	defer a.closeMu.Unlock()

	if a.closeFlight != flight {
		return
	}

	flight.err = err
	a.closeDone = true
	a.closeErr = err

	a.closeFlight = nil

	close(flight.done)
}

func (a *Agent) closeAttempt() error {
	a.mu.Lock()
	a.closed = true
	shutdown := a.callShutdown
	a.mu.Unlock()

	a.shutdownOnce.Do(func() {
		if shutdown != nil {
			close(shutdown)
		}
	})

	// A lifecycle request can own native discovery/session roots before it
	// installs an active session. Fence those requests before snapshotting
	// ownership so a construction either publishes its exact residence or fully
	// unwinds before shutdown evaluates it.
	a.callWG.Wait()

	a.mu.Lock()

	sessions := make(map[acp.SessionId]*agentSession, len(a.sessions))
	for id, session := range a.sessions {
		sessions[id] = session
	}

	cleanupOwners := make(map[acp.SessionId][]agentCleanupOwner, len(a.cleanupOwners))
	for id, owners := range a.cleanupOwners {
		cleanupOwners[id] = append(cleanupOwners[id], owners...)
	}

	cleanupResidences := make([]*agentCleanupResidence, 0, len(a.cleanupResidences))
	for _, residence := range a.cleanupResidences {
		cleanupResidences = append(cleanupResidences, residence)
	}

	boundaryErr := a.lifecycleContainmentErr
	a.mu.Unlock()

	closeErr := boundaryErr
	removed := int64(0)

	for id, session := range sessions {
		sessionErr := invokeShutdownStep(func() error {
			return session.closeAtShutdown(context.Background())
		})
		if errors.Is(sessionErr, errAgentGoroutinePanic) {
			sessionErr = errors.Join(sessionErr, invokeShutdownStep(func() error {
				return session.closeAtShutdown(context.Background())
			}))
		}

		closeErr = errors.Join(closeErr, sessionErr)

		a.mu.Lock()
		if a.sessions[id] == session {
			delete(a.sessions, id)

			removed++
		}

		a.clearCleanupOwnerLocked(id, session)
		a.mu.Unlock()
	}

	for id, owners := range cleanupOwners {
		for _, owner := range owners {
			cleanupErr := invokeShutdownStep(func() error {
				if owner.kind == agentCleanupDeleted {
					return owner.session.deleteAtShutdown(context.Background())
				}

				return owner.session.Close(context.Background())
			})
			if errors.Is(cleanupErr, errAgentGoroutinePanic) {
				cleanupErr = errors.Join(cleanupErr, invokeShutdownStep(func() error {
					if owner.kind == agentCleanupDeleted {
						return owner.session.deleteAtShutdown(context.Background())
					}

					return owner.session.Close(context.Background())
				}))
			}

			closeErr = errors.Join(closeErr, cleanupErr)

			a.clearCleanupOwner(id, owner.session)
		}
	}

	for _, residence := range cleanupResidences {
		cleanupErr := invokeShutdownStep(residence.finalize)
		if errors.Is(cleanupErr, errAgentGoroutinePanic) {
			cleanupErr = errors.Join(cleanupErr, invokeShutdownStep(residence.finalize))
		}

		closeErr = errors.Join(closeErr, cleanupErr)

		a.clearCleanupResidence(residence)
	}

	a.mu.Lock()
	a.conn = nil
	clear(a.sessions)
	clear(a.cleanupOwners)
	clear(a.cleanupResidences)
	clear(a.sessionFlights)
	clear(a.sessionUses)
	a.pending = 0
	a.mu.Unlock()

	if removed != 0 {
		a.observe.AddActiveSession(context.Background(), -removed)
	}

	return publicContainmentError(closeErr)
}

func invokeShutdownStep(step func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = errAgentGoroutinePanic
		}
	}()

	return step()
}

// optionsError reports every construction-time option failure as one uniform
// internal error, or nil when all of them validated. The code is -32603 because
// the caller's params are fine — the embedding host built an agent that cannot
// serve anything — and the data carries only the joined prose because no wire
// field is at fault to name. Both the handshake and session establishment
// report it, because an embedded host can open a session and prompt without
// ever calling initialize.
func (a *Agent) optionsError() error {
	configurationErr := errors.Join(a.activeLimitErr, a.configurationErr)
	if configurationErr == nil {
		return nil
	}

	return errors.Join(acp.NewInternalError(map[string]any{jsonFieldError: configurationErr.Error()}), configurationErr)
}

func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx)
	if err != nil {
		return acp.InitializeResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodInitialize)
	defer func() { finish(err) }()

	if optionsErr := a.optionsError(); optionsErr != nil {
		return acp.InitializeResponse{}, optionsErr
	}

	if sweepErr := a.sweepExpiredImageArtifacts(ctx); sweepErr != nil {
		a.log.WarnContext(ctx, "image artifact sweep failed", slog.String(jsonFieldError, sweepErr.Error()))
	}

	// The lifecycle answer rides the response's own top-level `_meta`, never
	// agentCapabilities._meta: later protocol work relocates capability objects
	// and initialize `_meta` survives that move unchanged.
	lifecycleMeta, err := a.negotiateLifecycle(params.Meta)
	if err != nil {
		return acp.InitializeResponse{}, err
	}

	title := a.options.AgentTitle
	position := selectPositionEncoding(params.ClientCapabilities.PositionEncodings)

	return acp.InitializeResponse{
		Meta:            lifecycleMeta,
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			LoadSession:      true,
			McpCapabilities:  acp.McpCapabilities{Http: true},
			PositionEncoding: &position,
			PromptCapabilities: acp.PromptCapabilities{
				EmbeddedContext: true,
				Image:           true,
			},
			SessionCapabilities: acp.SessionCapabilities{
				AdditionalDirectories: &acp.SessionAdditionalDirectoriesCapabilities{},
				Close:                 &acp.SessionCloseCapabilities{},
				Delete:                &acp.SessionDeleteCapabilities{},
				List:                  &acp.SessionListCapabilities{},
				Resume:                &acp.SessionResumeCapabilities{},
			},
			Meta: a.agentCapabilityMeta(),
		},
	}, nil
}

// agentCapabilityMeta builds the advertised agentCapabilities._meta block: the
// Amp vendor block, the family-reserved media envelope — always emitted, and
// never conditioned on any other advertisement — the handoff advertisement only
// when a handoff read root is configured, and the provider-auth legs only when
// a usable durable ledger root is.
func (a *Agent) agentCapabilityMeta() map[string]any {
	ampMeta := map[string]any{
		metaRawEventKey: map[string]any{
			jsonFieldMethod:  RawEventMethod,
			"enabledBy":      "_meta.amp.rawEvent.enabled",
			keyMaxBytes:      rawEventMaxBytes,
			"defaultEnabled": false,
		},
		"sessionStore": map[string]any{
			"format":     SessionStoreFormat,
			jsonFieldKey: []string{jsonFieldSessionID, "subpath"},
		},
	}

	if a.providerAuth != nil {
		ampMeta[providerAuthCapabilityKey] = a.providerAuth.capability()
	}

	meta := map[string]any{
		ampMetaKey:           ampMeta,
		metaMediaEnvelopeKey: mediaEnvelopeMeta(a.options.ImageLimits),
	}

	if a.options.InputHandoffRoot != "" {
		meta[metaHandoffKey] = handoffAdvertisement()
	}

	return meta
}

func (a *Agent) Authenticate(ctx context.Context, params acp.AuthenticateRequest) (resp acp.AuthenticateResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx)
	if err != nil {
		return acp.AuthenticateResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodAuthenticate)
	defer func() { finish(err) }()

	// A family literal is never foreign, so it is rejected by name before the
	// method's own refusal: this adapter advertises no auth method, but "the
	// method does not exist" and "the key is not read here" are different
	// answers and the host is owed the second one.
	if refusal := rejectLifecycleMeta(params.Meta); refusal != nil {
		return acp.AuthenticateResponse{}, refusal
	}

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

func (a *Agent) Logout(ctx context.Context, params acp.LogoutRequest) (resp acp.LogoutResponse, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx)
	if err != nil {
		return acp.LogoutResponse{}, err
	}
	defer finishPublicCall(&err, finishCall)

	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodLogout)
	defer func() { finish(err) }()

	if refusal := rejectLifecycleMeta(params.Meta); refusal != nil {
		return acp.LogoutResponse{}, refusal
	}

	return acp.LogoutResponse{}, nil
}

func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (result any, err error) {
	ctx, finishCall, err := a.beginAgentCall(ctx)
	if err != nil {
		return nil, err
	}
	defer finishPublicCall(&err, finishCall)

	_, finish := a.observe.StartACPRequest(ctx, method)
	defer func() { finish(err) }()

	// The reserved key is read before the method name is resolved, so it is
	// refused on every method this surface dispatches — including one this
	// adapter does not have. A method name is the caller's guess; the key is a
	// family literal that means something here, and answering "no such method"
	// would reply to the guess and leave the key unanswered, as if it had been
	// another namespace's business all along.
	if refusal := rejectLifecycleMetaParams(params); refusal != nil {
		return nil, refusal
	}

	switch method {
	case ForkSessionMethod:
		return acp.UnstableForkSessionResponse{}, acp.NewInvalidParams(map[string]any{
			jsonFieldError: valUnsupported,
			jsonFieldField: ForkSessionMethod,
		})
	default:
		if result, handled, authErr := a.handleAuthExtensionMethod(ctx, method, params); handled {
			return result, authErr
		}

		return nil, acp.NewMethodNotFound(method)
	}
}

// ensureOpen rejects any call on a closed agent with the uniform -32600
// "agent closed" error before dispatch or parameter validation runs.
func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: agentClosedMessage})
	}

	return nil
}

func maxConcurrentClientCalls(limits ConcurrencyLimits) int {
	if limits.MaxConcurrentClientCalls > 0 {
		return limits.MaxConcurrentClientCalls
	}

	return defaultMaxConcurrentCalls
}

func validateConcurrencyLimits(limits ConcurrencyLimits) error {
	switch {
	case limits.MaxActiveSessions < 0:
		return errors.New("max active sessions must be non-negative")
	case limits.MaxConcurrentClientCalls < 0:
		return errors.New("max concurrent client calls must be non-negative")
	default:
		return nil
	}
}

func selectPositionEncoding(values []acp.PositionEncodingKind) acp.PositionEncodingKind {
	if slices.Contains(values, acp.PositionEncodingKindUtf8) {
		return acp.PositionEncodingKindUtf8
	}

	return acp.PositionEncodingKindUtf16
}
