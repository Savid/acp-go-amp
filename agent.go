package ampacp

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-amp/internal/amp"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
)

const (
	// RawEventMethod is the notification carrying one raw Amp event when a
	// session opted in through _meta.amp.rawEvent.enabled.
	RawEventMethod = "_amp/rawEvent"
	// SessionStoreFormat identifies the store layout this package writes: raw
	// Amp thread exports under the main subpath plus the adapter's session
	// record under the config subpath.
	SessionStoreFormat = "amp-thread-json-v1"

	vendor = "amp"

	capabilityMethodKey = "method"
)

// client is the host side of the connection, as the sessions use it.
type client interface {
	SessionUpdate(ctx context.Context, params acp.SessionNotification) error
	NotifyExtension(ctx context.Context, method string, params any) error
}

// Agent exposes the Amp coding agent through ACP.
type Agent struct {
	options   Options
	log       *slog.Logger
	observe   *observer.Observer
	optionErr *acp.RequestError
	// processEnv is the adapter's own environment, read once at construction.
	processEnv []string
	store      acpcore.SessionStore

	mu        sync.Mutex
	conn      client
	transport *wire.Transport
	closed    bool
	// lifecycle is the answer this connection gave at initialize. An absent
	// answer leaves the extension dormant for every session on it.
	lifecycle    lifecycle.Negotiated
	restores     wire.SessionRequests
	sessions     map[acp.SessionId]*session
	deleted      map[acp.SessionId]bool
	incarnations uint64
}

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

// NewAgent creates an ACP agent for the Amp coding agent CLI. Construction
// never fails; a refused option is reported by Initialize and every
// session-establishing method as amp_invalid_options.
func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)

	log := options.Logger
	if log == nil {
		log = slog.Default()
	}

	store := options.SessionStore
	if store == nil {
		store = acpcore.NewInMemorySessionStore()
	}

	agent := &Agent{
		options: options,
		log:     log,
		observe: observer.New(observer.Config{
			Vendor: vendor, NativeClient: "amp",
			MeterProvider:  options.MeterProvider,
			Propagator:     options.TextMapPropagator,
			TracerProvider: options.TracerProvider,
			Version:        options.AgentVersion,
		}),
		processEnv: os.Environ(),
		store:      store,
		sessions:   make(map[acp.SessionId]*session),
		deleted:    make(map[acp.SessionId]bool),
	}
	agent.optionErr = agent.validateOptions()

	return agent
}

// validateOptions reports the first refused option. The reason goes to the
// log; the wire answer names only the option.
func (a *Agent) validateOptions() *acp.RequestError {
	options := a.options

	checks := []struct {
		field string
		err   error
	}{
		{"home", refuseNonempty(options.Home)},
		{"scratchDir", process.ValidateOptionalAbsolutePath(options.ScratchDir)},
		{"inputHandoffRoot", image.ValidateHandoffRoot(options.InputHandoffRoot)},
		{"defaultModel", refuseNonempty(options.DefaultModel)},
		{"configuredModels", validateConfiguredModels(options.ConfiguredModels)},
		{metaEnvKey, process.ValidateNames(options.Env)},
		{"concurrencyLimits", wire.ValidateConcurrencyLimits(options.ConcurrencyLimits.MaxActiveSessions, options.ConcurrencyLimits.MaxConcurrentClientCalls)},
		{"imageLimits", options.ImageLimits.core().Validate()},
	}

	for _, check := range checks {
		if check.err == nil {
			continue
		}

		a.log.Error("amp agent option rejected", slog.String("field", check.field), slog.String("reason", check.err.Error()))

		return wire.InvalidOptions(vendor, check.field)
	}

	return nil
}

func refuseNonempty(value string) error {
	if value != "" {
		return errors.New("option is unsupported by Amp")
	}

	return nil
}

func validateConfiguredModels(ids []string) error {
	if len(ids) > 0 {
		return errors.New("amp has no model catalog")
	}

	return nil
}

// Serve runs an ACP agent over the provided streams. It blocks until the
// context is cancelled or the peer closes the connection, then closes the
// agent.
func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	agent := NewAgent(opts...)
	defer func() {
		if closeErr := agent.Close(); closeErr != nil {
			returnErr = closeErr
		}
	}()

	transport := wire.NewTransport(input, output)
	defer transport.Close()

	conn := acp.NewAgentSideConnection(agent, transport.Writer(), transport.Reader())
	conn.SetLogger(agent.log)
	agent.attach(conn, transport)
	transport.Start()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

// attach binds the host connection the sessions emit through.
func (a *Agent) attach(conn client, transport *wire.Transport) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
	a.transport = transport
}

func (a *Agent) connection() client {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.conn
}

// Close runs the shutdown ladder for every session and refuses every later
// request.
func (a *Agent) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return nil
	}

	a.closed = true
	sessions := slices.Collect(maps.Values(a.sessions))
	a.mu.Unlock()

	var errs []error

	// The ladder's terminal events still need the connection, so it is cleared
	// only once every session has run its own shutdown.
	for _, s := range sessions {
		if err := s.close(context.Background()); err != nil {
			errs = append(errs, err)
		}

		a.detach(context.Background(), s)
	}

	a.mu.Lock()
	a.conn = nil
	a.mu.Unlock()

	return errors.Join(errs...)
}

func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return wire.AgentClosed()
	}

	return nil
}

// Initialize implements ACP initialize.
func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.InitializeResponse{}, openErr
	}

	_, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodInitialize)
	defer func() { finish(err) }()

	if a.optionErr != nil {
		return acp.InitializeResponse{}, a.optionErr
	}

	meta := params.Meta
	if t := a.transportRef(); t != nil {
		meta = lifecycle.RetainRequestMetadata(meta, t.TakeRaw(acp.AgentMethodInitialize))
	}

	present, paramErr := lifecycle.DecodeOffer(meta)
	if paramErr != nil {
		return acp.InitializeResponse{}, wire.ParamRefusal(paramErr)
	}

	var negotiated lifecycle.Negotiated
	if present {
		negotiated = lifecycle.Answer(lifecycle.Negotiated{UpdatesOutsidePrompt: false, ActivityKinds: []lifecycle.ActivityKind{}})
	}

	encoding := wire.SelectPositionEncoding(params.ClientCapabilities.PositionEncodings)

	a.mu.Lock()
	a.lifecycle = negotiated
	a.mu.Unlock()

	title := a.options.AgentTitle

	capabilityMeta := map[string]any{
		vendor: map[string]any{
			metaRawEventKey: map[string]any{
				capabilityMethodKey: RawEventMethod, "enabledBy": "_meta.amp.rawEvent.enabled",
				"maxBytes": wire.RawEventMaxBytes, "defaultEnabled": false,
			},
			"sessionStore": map[string]any{"format": SessionStoreFormat, "key": []string{"sessionId", "subpath"}},
		},
		wire.MediaEnvelopeKey: image.MediaEnvelope(a.options.ImageLimits.core(), image.Envelope{NativeCeiling: amp.MaxInputImageBytes, MaxDimension: amp.MaxImageDimension, DocumentFormats: []string{}}),
	}
	if a.options.InputHandoffRoot != "" {
		capabilityMeta[wire.HandoffKey] = image.HandoffAdvertisement()
	}

	var responseMeta map[string]any
	if negotiated.Present() {
		responseMeta = map[string]any{wire.LifecycleKey: negotiated.Advertisement()}
	}

	return acp.InitializeResponse{
		Meta:            responseMeta,
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			Meta:             capabilityMeta,
			LoadSession:      true,
			PositionEncoding: &encoding,
			PromptCapabilities: acp.PromptCapabilities{
				EmbeddedContext: true,
				Image:           true,
			},
			SessionCapabilities: acp.SessionCapabilities{
				Close:                 &acp.SessionCloseCapabilities{},
				Delete:                &acp.SessionDeleteCapabilities{},
				List:                  &acp.SessionListCapabilities{},
				Resume:                &acp.SessionResumeCapabilities{},
				AdditionalDirectories: &acp.SessionAdditionalDirectoriesCapabilities{},
			},
		},
	}, nil
}

// Authenticate exists because the SDK interface requires it. The harness
// authenticates itself in its own home, outside ACP.
func (a *Agent) Authenticate(_ context.Context, params acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.AuthenticateResponse{}, openErr
	}

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.AuthenticateResponse{}, wire.ParamRefusal(refusal)
	}

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

// Logout exists because the SDK interface requires it.
func (a *Agent) Logout(_ context.Context, params acp.LogoutRequest) (acp.LogoutResponse, error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.LogoutResponse{}, openErr
	}

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.LogoutResponse{}, wire.ParamRefusal(refusal)
	}

	return acp.LogoutResponse{}, acp.NewMethodNotFound(acp.AgentMethodLogout)
}

// SetSessionMode exists because the SDK interface requires it. Native modes
// are config options, never ACP session modes.
func (a *Agent) SetSessionMode(_ context.Context, params acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.SetSessionModeResponse{}, openErr
	}

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.SetSessionModeResponse{}, wire.ParamRefusal(refusal)
	}

	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

// HandleExtensionMethod answers every extension method with method-not-found.
// The only extension surface is the outbound RawEventMethod notification.
func (a *Agent) HandleExtensionMethod(_ context.Context, method string, params json.RawMessage) (any, error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	var envelope struct {
		Meta map[string]any `json:"_meta"` //nolint:tagliatelle // ACP reserves this wire spelling.
	}

	if err := json.Unmarshal(params, &envelope); err == nil {
		if refusal := lifecycle.RejectKey(envelope.Meta); refusal != nil {
			return nil, wire.ParamRefusal(refusal)
		}
	}

	return nil, acp.NewMethodNotFound(method)
}

func (a *Agent) transportRef() *wire.Transport {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.transport
}

func (a *Agent) lifecycleNegotiated() lifecycle.Negotiated {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lifecycle
}

// nextIncarnation mints a stream identity no earlier incarnation of any
// session on this agent used.
func (a *Agent) nextIncarnation() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.incarnations++

	return a.incarnations
}

// ensureExecutable resolves the amp executable against the base
// environment, so a session directory can never shadow it.
func (a *Agent) ensureExecutable(ctx context.Context) (string, error) {
	base, err := a.environment(nil, nil).Base()
	if err == nil {
		var executable string
		if executable, err = process.ResolveExecutable(cmp.Or(a.options.ExecutablePath, vendor), base); err == nil {
			return executable, nil
		}
	}

	a.log.ErrorContext(ctx, "amp executable resolution failed", slog.String("reason", err.Error()))

	return "", wire.InternalFailure(vendor, internalClassNativeStart)
}

// environment merges the inherited environment with the two caller overlays.
func (a *Agent) environment(sessionEnv, owned map[string]string) process.Environment {
	return process.Environment{Process: a.processEnv, Agent: a.options.Env, Session: sessionEnv, Owned: owned, InternalPrefix: amp.InternalEnvPrefix}
}

// internalClassNativeStart is the amp_internal_failure class of a native amp
// process that could not be started or configured for a session.
const internalClassNativeStart = "native_start"
