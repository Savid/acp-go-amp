package ampacp

import (
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultAgentName             = "acp-go-amp"
	defaultAgentTitle            = "acp-go-amp"
	defaultAgentVersion          = "0.1.0"
	defaultSessionStoreTimeout   = 10 * time.Second
	sessionStoreWriteTimeout     = 60 * time.Second
	defaultMaxActiveSessions     = 32
	defaultMaxConcurrentCalls    = 16
	defaultNativeCancelTimeout   = 5 * time.Second
	defaultNativeCloseTurnWait   = 5 * time.Second
	defaultNativeCommandTimeout  = 30 * time.Second
	defaultNativePromptLineLimit = 10 * 1024 * 1024
)

// Option configures an Agent.
type Option func(*Options)

// ConcurrencyLimits configures ACP backpressure limits.
type ConcurrencyLimits struct {
	MaxActiveSessions        int
	MaxConcurrentClientCalls int
}

// Options contains package-level agent configuration.
type Options struct {
	AgentName    string
	AgentTitle   string
	AgentVersion string

	ExecutablePath string
	Home           string
	DefaultModel   string
	Env            map[string]string

	Logger            *slog.Logger
	TracerProvider    trace.TracerProvider
	MeterProvider     metric.MeterProvider
	TextMapPropagator propagation.TextMapPropagator

	SessionStore            SessionStore
	SessionStoreLoadTimeout time.Duration
	ConcurrencyLimits       ConcurrencyLimits
	SeedFiles               map[string]string
	TurnTimeout             time.Duration
	runtime                 runtimeOptions
}

type runtimeOptions struct {
	nativeCancelTimeout  time.Duration
	nativeCloseTurnWait  time.Duration
	nativeCommandTimeout time.Duration
	maxJSONLineBytes     int
	// newTurnTimer builds the per-turn deadline channel. It is a seam so tests
	// can drive the timeout branch deterministically against a coincident
	// cancel; production always uses a real time.Timer.
	newTurnTimer func(d time.Duration) (<-chan time.Time, func())
}

// newRealTurnTimer is the production turn-deadline source: a real time.Timer
// whose channel fires after d, paired with a stop func for the caller to defer.
func newRealTurnTimer(d time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(d)

	return timer.C, func() { timer.Stop() }
}

func applyOptions(opts []Option) Options {
	options := Options{
		AgentName:               defaultAgentName,
		AgentTitle:              defaultAgentTitle,
		AgentVersion:            defaultAgentVersion,
		SessionStoreLoadTimeout: defaultSessionStoreTimeout,
		runtime: runtimeOptions{
			nativeCancelTimeout:  defaultNativeCancelTimeout,
			nativeCloseTurnWait:  defaultNativeCloseTurnWait,
			nativeCommandTimeout: defaultNativeCommandTimeout,
			maxJSONLineBytes:     defaultNativePromptLineLimit,
			newTurnTimer:         newRealTurnTimer,
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	if options.Env == nil {
		options.Env = map[string]string{}
	}

	return options
}

// WithLogger sets the agent's structured logger.
func WithLogger(logger *slog.Logger) Option {
	return func(options *Options) {
		options.Logger = logger
	}
}

// WithAgentName overrides the advertised agent name.
func WithAgentName(name string) Option {
	return func(options *Options) {
		if name != "" {
			options.AgentName = name
		}
	}
}

// WithAgentTitle overrides the advertised agent title.
func WithAgentTitle(title string) Option {
	return func(options *Options) {
		if title != "" {
			options.AgentTitle = title
		}
	}
}

// WithAgentVersion overrides the advertised agent version.
func WithAgentVersion(version string) Option {
	return func(options *Options) {
		if version != "" {
			options.AgentVersion = version
		}
	}
}

// WithExecutablePath sets the Amp CLI path.
func WithExecutablePath(path string) Option {
	return func(options *Options) {
		options.ExecutablePath = path
	}
}

// WithHome sets the parent directory for isolated per-session Amp home state.
func WithHome(path string) Option {
	return func(options *Options) {
		options.Home = path
	}
}

// WithDefaultModel records a default model, but Amp does not support model
// selection. Amp advertises no default model at initialize; instead, when a
// default model is set every session start is rejected fail-closed with the
// uniform unsupported "model" field error.
func WithDefaultModel(model string) Option {
	return func(options *Options) {
		options.DefaultModel = model
	}
}

// WithEnv sets base environment variables for spawned Amp processes.
func WithEnv(env map[string]string) Option {
	return func(options *Options) {
		options.Env = cloneStringMap(env)
	}
}

// WithTracerProvider sets the OpenTelemetry tracer provider.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(options *Options) {
		options.TracerProvider = provider
	}
}

// WithMeterProvider sets the OpenTelemetry meter provider.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(options *Options) {
		options.MeterProvider = provider
	}
}

// WithTextMapPropagator sets the OpenTelemetry context propagator.
func WithTextMapPropagator(propagator propagation.TextMapPropagator) Option {
	return func(options *Options) {
		options.TextMapPropagator = propagator
	}
}

// WithSessionStore sets the durable session store.
func WithSessionStore(store SessionStore) Option {
	return func(options *Options) {
		options.SessionStore = store
	}
}

// WithSessionStoreLoadTimeout sets the session-store load timeout.
func WithSessionStoreLoadTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.SessionStoreLoadTimeout = timeout
	}
}

// WithConcurrencyLimits sets ACP backpressure limits.
func WithConcurrencyLimits(limits ConcurrencyLimits) Option {
	return func(options *Options) {
		options.ConcurrencyLimits = limits
	}
}

// WithTurnTimeout sets a per-turn native deadline. The default of 0 means no
// deadline. When positive, a prompt turn that has not completed within the
// duration aborts the native turn and returns the uniform turn-failure error
// with cause "timeout" — a timeout is a failure, never a cancellation.
func WithTurnTimeout(timeout time.Duration) Option {
	return func(options *Options) {
		options.TurnTimeout = timeout
	}
}

// WithSeedFiles registers relative-path file contents that the wrapper writes
// into each session's resolved native root before the amp CLI launches, so the
// short-lived amp process reads them as its own on-disk state. See
// writeSeedFiles for the chosen anchor and path-confinement rules. The map is
// cloned like WithEnv so later caller mutation cannot change agent state.
func WithSeedFiles(files map[string]string) Option {
	return func(options *Options) { options.SeedFiles = cloneStringMap(files) }
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}

// AmpOptions contains per-session Amp configuration accepted under _meta.amp.options.
type AmpOptions struct {
	Model        string            `json:"model,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	OutputSchema map[string]any    `json:"outputSchema,omitempty"`
	Mode         string            `json:"mode,omitempty"`
	Effort       string            `json:"effort,omitempty"`
}

// AmpOption configures AmpOptions.
type AmpOption func(*AmpOptions)

// NewAmpOptions builds AmpOptions from functional options, cloning caller-owned
// maps so the result shares no memory with the caller.
func NewAmpOptions(opts ...AmpOption) AmpOptions {
	options := AmpOptions{}

	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	options.Env = cloneStringMap(options.Env)
	options.OutputSchema = cloneAnyMap(options.OutputSchema)

	return options
}

// Meta renders the AmpOptions as an _meta.amp.options payload.
func (options AmpOptions) Meta() map[string]any {
	return map[string]any{ampMetaKey: map[string]any{ampOptionsKey: ampOptionsPayload(options)}}
}

// WithAmpModel sets the per-session model.
func WithAmpModel(model string) AmpOption {
	return func(options *AmpOptions) {
		options.Model = model
	}
}

// WithAmpEnv sets per-session environment overrides.
func WithAmpEnv(env map[string]string) AmpOption {
	return func(options *AmpOptions) {
		options.Env = cloneStringMap(env)
	}
}

// WithAmpOutputSchema sets the per-session structured-output schema.
func WithAmpOutputSchema(schema map[string]any) AmpOption {
	return func(options *AmpOptions) {
		options.OutputSchema = cloneAnyMap(schema)
	}
}

// WithAmpMode sets the per-session mode.
func WithAmpMode(mode string) AmpOption {
	return func(options *AmpOptions) {
		options.Mode = mode
	}
}

// WithAmpEffort sets the per-session reasoning effort.
func WithAmpEffort(effort string) AmpOption {
	return func(options *AmpOptions) {
		options.Effort = effort
	}
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}

	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneAny(value)
	}

	return out
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		return cloneAnySlice(typed)
	default:
		return value
	}
}

func cloneAnySlice(in []any) []any {
	if in == nil {
		return nil
	}

	out := make([]any, len(in))
	for index, value := range in {
		out[index] = cloneAny(value)
	}

	return out
}
