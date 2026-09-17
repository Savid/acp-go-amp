package ampacp

import (
	"log/slog"
	"maps"
	"slices"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
)

// Option configures the amp ACP agent.
type Option func(*Options)

// Options configures the ACP agent process and the native Amp prompt processes.
type Options struct {
	// AgentName is the protocol identifier advertised during ACP initialize.
	AgentName string
	// AgentTitle is the human-readable agent name advertised during ACP initialize.
	AgentTitle string
	// AgentVersion is the agent version advertised during ACP initialize.
	AgentVersion string

	// ExecutablePath selects the amp executable. A bare name is searched on the
	// base PATH; a path containing a separator is used as given. Empty means
	// "amp".
	ExecutablePath string
	// Home is unsupported; Amp resolves its native files from the inherited environment.
	Home string
	// ScratchDir is the parent directory for ephemeral adapter state. Empty
	// means the system temp directory.
	ScratchDir string
	// InputHandoffRoot is the absolute directory under which handoff-form
	// prompt images are read. Empty rejects the handoff form.
	InputHandoffRoot string
	// DefaultModel is unsupported; use the native mode option.
	DefaultModel string
	// ConfiguredModels is unsupported because Amp has no model catalog.
	ConfiguredModels []string
	// Env is the static agent-scoped overlay on the inherited process
	// environment every amp process runs with.
	Env map[string]string

	// Logger receives structured diagnostic logs. If nil, the default logger is used.
	Logger *slog.Logger
	// TracerProvider records adapter spans. If nil, tracing is a no-op.
	TracerProvider trace.TracerProvider
	// MeterProvider records adapter metrics. If nil, metrics are no-ops.
	MeterProvider metric.MeterProvider
	// TextMapPropagator extracts trace context from ACP _meta. If nil, W3C
	// trace context plus baggage propagation is used.
	TextMapPropagator propagation.TextMapPropagator

	// SessionStore is the durability boundary for session rows. Nil installs a
	// fresh in-memory store.
	SessionStore acpcore.SessionStore
	// ConcurrencyLimits controls process-local backpressure.
	ConcurrencyLimits ConcurrencyLimits
	// SeedFiles maps paths relative to amp's config root to file contents
	// written there before each launch.
	SeedFiles map[string]string
	// ImageLimits bounds decoded image bytes on prompt input and emitted
	// output. Every field defaults to 6 MiB when the option is omitted.
	ImageLimits ImageLimits

	imageLimitsSet bool
}

// ConcurrencyLimits controls per-agent backpressure. Zero fields use defaults.
type ConcurrencyLimits struct {
	MaxActiveSessions        int
	MaxConcurrentClientCalls int
}

// ImageLimits bounds decoded image bytes. A zero field disables that policy
// limit; the frame clamp still applies.
type ImageLimits struct {
	MaxInputBytesPerImage     int64
	MaxInputBytesPerPrompt    int64
	MaxOutputBytesPerImage    int64
	MaxOutputBytesPerToolCall int64
}

func (l ImageLimits) core() image.Limits {
	return image.Limits{
		MaxInputBytesPerImage:     l.MaxInputBytesPerImage,
		MaxInputBytesPerPrompt:    l.MaxInputBytesPerPrompt,
		MaxOutputBytesPerImage:    l.MaxOutputBytesPerImage,
		MaxOutputBytesPerToolCall: l.MaxOutputBytesPerToolCall,
	}
}

const (
	defaultMaxActiveSessions        = 32
	defaultMaxConcurrentClientCalls = 16
)

func applyOptions(opts []Option) Options {
	options := Options{
		AgentName:    "acp-go-amp",
		AgentTitle:   "acp-go-amp",
		AgentVersion: "0.1.0",
	}

	for _, opt := range opts {
		opt(&options)
	}

	if !options.imageLimitsSet {
		limits := image.DefaultLimits()
		options.ImageLimits = ImageLimits{
			MaxInputBytesPerImage:     limits.MaxInputBytesPerImage,
			MaxInputBytesPerPrompt:    limits.MaxInputBytesPerPrompt,
			MaxOutputBytesPerImage:    image.DefaultLimitBytes,
			MaxOutputBytesPerToolCall: image.DefaultLimitBytes,
		}
	}

	if options.ConcurrencyLimits.MaxActiveSessions == 0 {
		options.ConcurrencyLimits.MaxActiveSessions = defaultMaxActiveSessions
	}

	if options.ConcurrencyLimits.MaxConcurrentClientCalls == 0 {
		options.ConcurrencyLimits.MaxConcurrentClientCalls = defaultMaxConcurrentClientCalls
	}

	return options
}

// WithLogger configures structured diagnostic logging.
func WithLogger(logger *slog.Logger) Option {
	return func(options *Options) { options.Logger = logger }
}

// WithAgentName sets the protocol identifier advertised during ACP initialize.
func WithAgentName(name string) Option {
	return func(options *Options) { options.AgentName = name }
}

// WithAgentTitle sets the human-readable agent name advertised during ACP initialize.
func WithAgentTitle(title string) Option {
	return func(options *Options) { options.AgentTitle = title }
}

// WithAgentVersion sets the agent version advertised during ACP initialize.
func WithAgentVersion(version string) Option {
	return func(options *Options) { options.AgentVersion = version }
}

// WithExecutablePath selects the amp executable.
func WithExecutablePath(path string) Option {
	return func(options *Options) { options.ExecutablePath = path }
}

// WithHome is unsupported by Amp. A nonempty path fails initialization.
func WithHome(path string) Option {
	return func(options *Options) { options.Home = path }
}

// WithScratchDir sets the parent directory for ephemeral adapter state.
func WithScratchDir(dir string) Option {
	return func(options *Options) { options.ScratchDir = dir }
}

// WithInputHandoffRoot sets the absolute directory under which handoff-form
// prompt images are read. The adapter never writes there.
func WithInputHandoffRoot(dir string) Option {
	return func(options *Options) { options.InputHandoffRoot = dir }
}

// WithDefaultModel is unsupported by Amp.
func WithDefaultModel(model string) Option {
	return func(options *Options) { options.DefaultModel = model }
}

// WithConfiguredModels is unsupported by Amp.
func WithConfiguredModels(ids []string) Option {
	return func(options *Options) { options.ConfiguredModels = slices.Clone(ids) }
}

// WithEnv sets the static agent-scoped environment overlay applied to every
// amp process after the inherited environment and before the session env.
func WithEnv(env map[string]string) Option {
	return func(options *Options) { options.Env = maps.Clone(env) }
}

// WithTracerProvider configures the OpenTelemetry tracer provider.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(options *Options) { options.TracerProvider = provider }
}

// WithMeterProvider configures the OpenTelemetry meter provider.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(options *Options) { options.MeterProvider = provider }
}

// WithTextMapPropagator configures trace-context extraction from ACP _meta.
func WithTextMapPropagator(propagator propagation.TextMapPropagator) Option {
	return func(options *Options) { options.TextMapPropagator = propagator }
}

// WithSessionStore configures the session store.
func WithSessionStore(store acpcore.SessionStore) Option {
	return func(options *Options) { options.SessionStore = store }
}

// WithConcurrencyLimits sets process-local backpressure limits.
func WithConcurrencyLimits(limits ConcurrencyLimits) Option {
	return func(options *Options) { options.ConcurrencyLimits = limits }
}

// WithImageLimits bounds decoded image bytes. A zero field disables that
// policy limit; a negative field fails construction.
func WithImageLimits(limits ImageLimits) Option {
	return func(options *Options) {
		options.ImageLimits = limits
		options.imageLimitsSet = true
	}
}

// WithSeedFiles registers files written into amp's config root before each
// launch. Keys are paths relative to that root; values are the contents.
func WithSeedFiles(files map[string]string) Option {
	return func(options *Options) { options.SeedFiles = maps.Clone(files) }
}
