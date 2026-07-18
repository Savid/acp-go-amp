package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/observer"
)

type Agent struct {
	options Options
	log     *slog.Logger
	store   SessionStore
	observe *observer.Observer

	lifecycleDone chan struct{}
	lifecycleWG   sync.WaitGroup
	closeOnce     sync.Once
	closeErr      error

	mu                      sync.Mutex
	closed                  bool
	conn                    agentClient
	sessions                map[acp.SessionId]*agentSession
	deleted                 map[acp.SessionId]struct{}
	pendingNativeDeletes    map[acp.SessionId]struct{}
	pending                 int
	clientCalls             chan struct{}
	providerProcesses       *providerProcessSnapshotTracker
	lifecycleContainmentErr error

	activeLimitErr   error
	containmentMode  RuntimeContainmentMode
	configurationErr error
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

	providerProcesses := newProviderProcessSnapshotTracker(options.RuntimeResourceHooks, mode == RuntimeContainmentAuthoritative)
	if options.RuntimeResourceHooks.ObserveContainment != nil {
		options.RuntimeResourceHooks.ObserveContainment(context.Background(), mode)
	}

	if mode == RuntimeContainmentBestEffort {
		log.Warn("Darwin best-effort process containment is enabled; escaped descendants may survive, numeric PGID reuse can cause collateral signalling, marker correlation is not ownership, markers can be scrubbed, and native-root permits do not bound escaped provider work",
			slog.String("containment", string(mode)),
		)
	}

	return &Agent{
		options:              options,
		log:                  log,
		store:                store,
		observe:              observe,
		sessions:             make(map[acp.SessionId]*agentSession),
		deleted:              make(map[acp.SessionId]struct{}),
		pendingNativeDeletes: make(map[acp.SessionId]struct{}),
		clientCalls:          make(chan struct{}, maxConcurrentClientCalls(options.ConcurrencyLimits)),
		providerProcesses:    providerProcesses,
		lifecycleDone:        make(chan struct{}),
		activeLimitErr:       validateConcurrencyLimits(options.ConcurrencyLimits),
		containmentMode:      mode,
		configurationErr:     validateContainmentOptions(options),
	}
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

		closeErr := boundaryErr
		for _, session := range sessions {
			closeErr = errors.Join(closeErr, session.Close(context.Background()))
		}

		a.observe.AddActiveSession(context.Background(), -int64(len(sessions)))
		a.closeErr = closeErr
	})

	return a.closeErr
}

func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodInitialize)
	defer func() { finish(err) }()

	if configurationErr := errors.Join(a.activeLimitErr, a.configurationErr); configurationErr != nil {
		return acp.InitializeResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldError: configurationErr.Error()})
	}

	title := a.options.AgentTitle
	position := selectPositionEncoding(params.ClientCapabilities.PositionEncodings)

	return acp.InitializeResponse{
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
			Meta: map[string]any{
				ampMetaKey: map[string]any{
					metaRawEventKey: map[string]any{
						"method":         RawEventMethod,
						"enabledBy":      "_meta.amp.rawEvent.enabled",
						keyMaxBytes:      rawEventMaxBytes,
						"defaultEnabled": false,
					},
					"sessionStore": map[string]any{
						"format": SessionStoreFormat,
						"key":    []string{jsonFieldSessionID, "subpath"},
					},
				},
			},
		},
	}, nil
}

func (a *Agent) Authenticate(ctx context.Context, params acp.AuthenticateRequest) (resp acp.AuthenticateResponse, err error) {
	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodAuthenticate)
	defer func() { finish(err) }()

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

func (a *Agent) Logout(ctx context.Context, params acp.LogoutRequest) (resp acp.LogoutResponse, err error) {
	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodLogout)
	defer func() { finish(err) }()

	return acp.LogoutResponse{}, nil
}

func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (result any, err error) {
	_, finish := a.observe.StartACPRequest(ctx, method)
	defer func() { finish(err) }()

	// A closed agent rejects every call before dispatch: -32600 first, then
	// -32601 for unknown methods, then parameter validation.
	if err := a.ensureOpen(); err != nil {
		return nil, err
	}

	switch method {
	case ForkSessionMethod:
		return acp.UnstableForkSessionResponse{}, acp.NewInvalidParams(map[string]any{
			jsonFieldError: valUnsupported,
			jsonFieldField: ForkSessionMethod,
		})
	default:
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
