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

	mu                   sync.Mutex
	closed               bool
	conn                 agentClient
	sessions             map[acp.SessionId]*agentSession
	deleted              map[acp.SessionId]struct{}
	pendingNativeDeletes map[acp.SessionId]struct{}
	pending              int
	clientCalls          chan struct{}

	activeLimitErr error
}

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

	return &Agent{
		options: options,
		log:     log,
		store:   store,
		observe: observer.New(observer.Config{
			TracerProvider: options.TracerProvider,
			MeterProvider:  options.MeterProvider,
			Propagator:     options.TextMapPropagator,
			Version:        options.AgentVersion,
		}),
		sessions:             make(map[acp.SessionId]*agentSession),
		deleted:              make(map[acp.SessionId]struct{}),
		pendingNativeDeletes: make(map[acp.SessionId]struct{}),
		clientCalls:          make(chan struct{}, maxConcurrentClientCalls(options.ConcurrencyLimits)),
		activeLimitErr:       validateConcurrencyLimits(options.ConcurrencyLimits),
	}
}

func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) error {
	agent := NewAgent(opts...)
	defer func() { _ = agent.Close() }()

	conn := newLocalAgentConnection(agent, output, input)
	agent.setConnection(conn)

	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

func (a *Agent) Close() error {
	a.mu.Lock()

	sessions := make([]*agentSession, 0, len(a.sessions))
	for _, session := range a.sessions {
		sessions = append(sessions, session)
	}

	a.sessions = map[acp.SessionId]*agentSession{}
	a.closed = true
	a.conn = nil
	a.mu.Unlock()

	var err error
	for _, session := range sessions {
		err = errors.Join(err, session.Close(context.Background()))
	}

	a.observe.AddActiveSession(context.Background(), -int64(len(sessions)))

	return err
}

func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	_, finish := a.observe.StartACPRequest(ctx, acp.AgentMethodInitialize)
	defer func() { finish(err) }()

	if a.activeLimitErr != nil {
		return acp.InitializeResponse{}, acp.NewInvalidParams(map[string]any{jsonFieldError: a.activeLimitErr.Error()})
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
