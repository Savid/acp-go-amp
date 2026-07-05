//nolint:goconst,wsl_v5,nlreturn // public options use literal JSON field names.
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
	defaultAgentVersion          = "0.0.0"
	defaultSessionStoreTimeout   = 10 * time.Second
	defaultMaxActiveSessions     = 32
	defaultMaxConcurrentPrompts  = 1
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
	MaxConcurrentPrompts     int
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
	runtime                 runtimeOptions
}

type runtimeOptions struct {
	nativeCancelTimeout  time.Duration
	nativeCloseTurnWait  time.Duration
	nativeCommandTimeout time.Duration
	maxJSONLineBytes     int
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

func WithLogger(logger *slog.Logger) Option {
	return func(options *Options) { options.Logger = logger }
}

func WithAgentName(name string) Option {
	return func(options *Options) {
		if name != "" {
			options.AgentName = name
		}
	}
}

func WithAgentTitle(title string) Option {
	return func(options *Options) {
		if title != "" {
			options.AgentTitle = title
		}
	}
}

func WithAgentVersion(version string) Option {
	return func(options *Options) {
		if version != "" {
			options.AgentVersion = version
		}
	}
}

func WithExecutablePath(path string) Option {
	return func(options *Options) { options.ExecutablePath = path }
}

func WithHome(path string) Option {
	return func(options *Options) { options.Home = path }
}

func WithDefaultModel(model string) Option {
	return func(options *Options) { options.DefaultModel = model }
}

func WithEnv(env map[string]string) Option {
	return func(options *Options) { options.Env = cloneStringMap(env) }
}

func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(options *Options) { options.TracerProvider = provider }
}

func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(options *Options) { options.MeterProvider = provider }
}

func WithTextMapPropagator(propagator propagation.TextMapPropagator) Option {
	return func(options *Options) { options.TextMapPropagator = propagator }
}

func WithSessionStore(store SessionStore) Option {
	return func(options *Options) { options.SessionStore = store }
}

func WithSessionStoreLoadTimeout(timeout time.Duration) Option {
	return func(options *Options) { options.SessionStoreLoadTimeout = timeout }
}

func WithConcurrencyLimits(limits ConcurrencyLimits) Option {
	return func(options *Options) { options.ConcurrencyLimits = limits }
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

func NewAmpOptions(opts ...AmpOption) AmpOptions {
	options := AmpOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.Env != nil {
		options.Env = cloneStringMap(options.Env)
	}
	if options.OutputSchema != nil {
		options.OutputSchema = cloneAnyMap(options.OutputSchema)
	}
	return options
}

func (options AmpOptions) Meta() map[string]any {
	payload := map[string]any{}
	if options.Model != "" {
		payload["model"] = options.Model
	}
	if len(options.Env) > 0 {
		payload["env"] = cloneStringMap(options.Env)
	}
	if options.OutputSchema != nil {
		payload["outputSchema"] = cloneAnyMap(options.OutputSchema)
	}
	if options.Mode != "" {
		payload["mode"] = options.Mode
	}
	if options.Effort != "" {
		payload["effort"] = options.Effort
	}
	return map[string]any{"amp": map[string]any{"options": payload}}
}

func WithAmpModel(model string) AmpOption {
	return func(options *AmpOptions) { options.Model = model }
}

func WithAmpEnv(env map[string]string) AmpOption {
	return func(options *AmpOptions) { options.Env = cloneStringMap(env) }
}

func WithAmpOutputSchema(schema map[string]any) AmpOption {
	return func(options *AmpOptions) { options.OutputSchema = cloneAnyMap(schema) }
}

func WithAmpMode(mode string) AmpOption {
	return func(options *AmpOptions) { options.Mode = mode }
}

func WithAmpEffort(effort string) AmpOption {
	return func(options *AmpOptions) { options.Effort = effort }
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
