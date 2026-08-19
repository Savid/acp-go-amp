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

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/savid/acp-go-amp/internal/observer"
)

type Agent struct {
	options             Options
	log                 *slog.Logger
	store               SessionStore
	observe             *observer.Observer
	ordinaryEnvironment map[string]string

	lifecycleDone chan struct{}
	lifecycleWG   sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error

	mu       sync.Mutex
	closed   bool
	conn     agentClient
	sessions map[acp.SessionId]*agentSession
	deleted  map[acp.SessionId]struct{}
	// pendingNativeDeletes maps a tombstoned session to the native thread id
	// whose server-side delete still needs to be retried.
	pendingNativeDeletes    map[acp.SessionId]string
	pending                 int
	clientCalls             chan struct{}
	providerProcesses       *providerProcessSnapshotTracker
	lifecycleContainmentErr error

	activeLimitErr   error
	containmentMode  RuntimeContainmentMode
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

// retainHarnessPath records the harness a probe validated. An empty or relative
// answer is a broken probe rather than a reason to resolve again.
func (a *Agent) retainHarnessPath(path string) error {
	if !filepath.IsAbs(path) {
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

	observe := observer.New(observer.Config{
		TracerProvider: options.TracerProvider,
		MeterProvider:  options.MeterProvider,
		Propagator:     options.TextMapPropagator,
		Version:        options.AgentVersion,
	})
	options.RuntimeResourceHooks = instrumentRuntimeResourceHooks(options.RuntimeResourceHooks, observe)
	mode := containmentMode(options)

	providerProcesses := newProviderProcessSnapshotTracker(options.RuntimeResourceHooks, mode.provesWholeTreeLifecycle())
	if options.RuntimeResourceHooks.ObserveContainment != nil {
		options.RuntimeResourceHooks.ObserveContainment(context.Background(), mode)
	}

	if mode == RuntimeContainmentBestEffort {
		log.Warn("Darwin best-effort process containment is enabled; escaped descendants may survive, numeric PGID reuse can cause collateral signalling, marker correlation is not ownership, markers can be scrubbed, and native-root permits do not bound escaped provider work",
			slog.String("containment", string(mode)),
		)
	}

	agent := &Agent{
		options:              options,
		log:                  log,
		store:                store,
		observe:              observe,
		ordinaryEnvironment:  nativeamp.CaptureOrdinaryEnvironment(),
		sessions:             make(map[acp.SessionId]*agentSession),
		deleted:              make(map[acp.SessionId]struct{}),
		pendingNativeDeletes: make(map[acp.SessionId]string),
		clientCalls:          make(chan struct{}, maxConcurrentClientCalls(options.ConcurrencyLimits)),
		providerProcesses:    providerProcesses,
		lifecycleDone:        make(chan struct{}),
		activeLimitErr:       validateConcurrencyLimits(options.ConcurrencyLimits),
		containmentMode:      mode,
		configurationErr: errors.Join(
			validateContainmentOptions(options),
			validateImageLimits(options.ImageLimits),
			validateInputHandoffRoot(options.InputHandoffRoot),
			validateProviderAuthRoots(options),
		),
	}
	agent.providerAuth = newProviderAuth(agent)

	return agent
}

func (a *Agent) ContainmentMode() RuntimeContainmentMode {
	if a == nil {
		return RuntimeContainmentUnavailable
	}

	return a.containmentMode
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

func (a *Agent) Close() error {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		a.conn = nil
		shutdown := a.lifecycleDone
		a.mu.Unlock()

		if shutdown != nil {
			close(shutdown)
		}

		// A lifecycle request can own native discovery/session roots before it
		// installs an active session. Fence those requests before snapshotting
		// sessions so no root can outlive a settled Close and no late install can
		// escape this shutdown pass.
		a.lifecycleWG.Wait()

		a.mu.Lock()

		sessions := make([]*agentSession, 0, len(a.sessions))
		for _, session := range a.sessions {
			sessions = append(sessions, session)
		}

		a.sessions = map[acp.SessionId]*agentSession{}
		boundaryErr := a.lifecycleContainmentErr
		a.mu.Unlock()

		// Each session runs the whole ladder, durable rung included: the shutdown
		// ladder applies identically to a wire close and an embedded one, and a
		// settlement whose commit failed retains frames this is the last chance to
		// land. A commit this close cannot make is reported rather than dropped
		// with the wrapper.
		closeErr := boundaryErr
		for _, session := range sessions {
			closeErr = errors.Join(closeErr, session.closeAtShutdown(context.Background()))
		}

		a.observe.AddActiveSession(context.Background(), -int64(len(sessions)))
		a.closeErr = closeErr
	})

	return a.closeErr
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

	return acp.NewInternalError(map[string]any{jsonFieldError: configurationErr.Error()})
}

func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
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
	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodLogout)
	defer func() { finish(err) }()

	if refusal := rejectLifecycleMeta(params.Meta); refusal != nil {
		return acp.LogoutResponse{}, refusal
	}

	return acp.LogoutResponse{}, nil
}

func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (result any, err error) {
	_, finish := a.observe.StartACPRequest(ctx, method)
	defer func() { finish(err) }()

	// A closed agent rejects every call before dispatch: -32600 first, then the
	// reserved family key, then -32601 for unknown methods, then parameter
	// validation.
	if err := a.ensureOpen(); err != nil {
		return nil, err
	}

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
		return acp.NewInvalidRequest(map[string]any{jsonFieldError: "agent closed"})
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
