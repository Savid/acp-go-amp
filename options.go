package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	platformDarwin   = "darwin"
	platformLinux    = "linux"
	platformWindows  = "windows"
	privateEnvPrefix = "ACP_" + "GO_AMP_INTERNAL_"
)

var runtimeGOOS = runtime.GOOS

// RuntimeResourceKind identifies the lifecycle scope consuming a host-managed resource.
type RuntimeResourceKind string

const (
	RuntimeResourceRuntime   RuntimeResourceKind = "runtime"
	RuntimeResourceSession   RuntimeResourceKind = "session"
	RuntimeResourcePrompt    RuntimeResourceKind = "prompt"
	RuntimeResourceDiscovery RuntimeResourceKind = "discovery"
)

type RuntimeProcessKind string

const (
	RuntimeProcessHomeLockSupervisor RuntimeProcessKind = "home_lock_supervisor"
	RuntimeProcessProviderDescendant RuntimeProcessKind = "provider_descendant"
)

type RuntimeContainmentMode string

const (
	RuntimeContainmentAuthoritative RuntimeContainmentMode = "authoritative"
	RuntimeContainmentBestEffort    RuntimeContainmentMode = "best_effort"
	// RuntimeContainmentSharedIdentity is the boundary a supervisor proves when
	// the native identity is the identity it already runs as. The subreaper
	// tree, the descendant reaping and the process-group teardown are the
	// authoritative ones, so whole-tree lifecycle is still proven; what is
	// absent is the credential separation between the supervisor and the agent,
	// and the host-global record of who holds the identity.
	RuntimeContainmentSharedIdentity RuntimeContainmentMode = "shared_identity"
	RuntimeContainmentUnavailable    RuntimeContainmentMode = "unavailable"
)

type RuntimeStartupStage string

const (
	RuntimeStartupSpawn         RuntimeStartupStage = "spawn"
	RuntimeStartupReadiness     RuntimeStartupStage = "readiness"
	RuntimeStartupConfiguration RuntimeStartupStage = "configuration"
	// RuntimeStartupSession marks the thread-creating first-prompt spawn: amp
	// mints the server-side thread lazily on a session's first `-x` turn, and
	// session/new runs readiness probes but no prompt or thread-creating work.
	RuntimeStartupSession RuntimeStartupStage = "session"
)

// RuntimeResourceHooks lets an embedding host enforce native-root and scratch-root limits.
type RuntimeResourceHooks struct {
	AcquireNativeRoot      func(context.Context, RuntimeResourceKind) (func(), error)
	ReserveScratchRoot     func(context.Context, RuntimeResourceKind) (func(), error)
	ObserveProcess         func(context.Context, RuntimeProcessKind, int64)
	ObserveProcessSnapshot func(context.Context, RuntimeProcessKind, int)
	ObserveStartupStage    func(context.Context, RuntimeResourceKind, RuntimeStartupStage, time.Duration, error)
	ObserveContainment     func(context.Context, RuntimeContainmentMode)
}

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
	defaultImageLimitBytes       = 6 * 1024 * 1024
)

// Option configures an Agent.
type Option func(*Options)

type ProcessIdentityLockCapability interface {
	Duplicate() (*os.File, error)
}

// ProcessIsolation is the mandatory operating-system identity and complete
// base environment for every native Amp process.
type ProcessIsolation struct {
	UID             uint32
	GID             uint32
	BaseEnvironment map[string]string
	// IdentityLock is an optional trusted-supervisor descriptor for the
	// host-global UID lock. Linux supervisors validate it and never expose it to
	// the native Amp process. Standalone embeddings should leave it nil.
	IdentityLock        ProcessIdentityLockCapability
	AuthorityDomain     ProcessIdentityLockCapability
	StandaloneOwnerID   string
	StandaloneStateRoot string
}

// ConcurrencyLimits configures ACP backpressure limits.
type ConcurrencyLimits struct {
	MaxActiveSessions        int
	MaxConcurrentClientCalls int
}

// ImageLimits configures decoded image byte limits.
type ImageLimits struct {
	MaxInputBytesPerImage     int64
	MaxInputBytesPerPrompt    int64
	MaxOutputBytesPerImage    int64
	MaxOutputBytesPerToolCall int64
}

// Options contains package-level agent configuration.
type Options struct {
	AgentName    string
	AgentTitle   string
	AgentVersion string

	ExecutablePath   string
	ProcessIsolation *ProcessIsolation
	// Home is unsupported: Amp has no native config/auth root, so a non-empty
	// value is rejected at every session start. See WithHome and WithScratchDir.
	Home         string
	DefaultModel string
	// ScratchDir is the sole parent for all ephemeral on-disk materialization
	// (per-session isolated HOME/XDG dirs, startup probe dirs, any temp). Empty
	// falls back to the system temp directory. See WithScratchDir.
	ScratchDir string
	// InputHandoffRoot is the read root for the local handoff input form. It
	// materializes nothing, so it is not a materialization root. Empty rejects
	// every handoff-form image block. See WithInputHandoffRoot.
	InputHandoffRoot string
	// ProviderAuthRoot is the absolute, host-owned, durable directory that
	// houses the values-free provider-auth ledger. It is not ephemeral
	// materialization: the ledger deliberately outlives every session and every
	// isolated home, which is the one class of state a scratch parent must not
	// hold. Without it no provider-auth method is advertised at all.
	ProviderAuthRoot string
	// ProviderAuthDirectHome is unsupported: Amp's disconnect releases the one
	// ledger slot a connection owns and performs no native removal, so there is
	// no canonical operator home for an exact-home gate to authorize. A
	// non-empty value is rejected at every session start.
	ProviderAuthDirectHome string
	Env                    map[string]string

	Logger            *slog.Logger
	TracerProvider    trace.TracerProvider
	MeterProvider     metric.MeterProvider
	TextMapPropagator propagation.TextMapPropagator

	SessionStore                SessionStore
	SessionStoreLoadTimeout     time.Duration
	ConcurrencyLimits           ConcurrencyLimits
	ImageLimits                 ImageLimits
	SeedFiles                   map[string]string
	TurnTimeout                 time.Duration
	RuntimeResourceHooks        RuntimeResourceHooks
	DarwinBestEffortContainment bool
	testOnlyNoCredential        bool
	testOnlyIdentityLockRoot    string
	testOnlyAuthLoginPlatform   string
	runtime                     runtimeOptions
}

type runtimeOptions struct {
	nativeCancelTimeout  time.Duration
	nativeCloseTurnWait  time.Duration
	nativeCommandTimeout time.Duration
	maxJSONLineBytes     int
	startupProbe         func(context.Context, *nativeamp.Client) error
	// executeThread launches the thread-less `amp -x` turn that lazily creates
	// the server-side thread on a session's first prompt.
	executeThread  func(context.Context, *nativeamp.Client, any) (*nativeamp.Turn, error)
	continueThread func(context.Context, *nativeamp.Client, string, any) (*nativeamp.Turn, error)
	exportThread   func(context.Context, *nativeamp.Client, string) (json.RawMessage, error)
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
		ImageLimits: ImageLimits{
			MaxInputBytesPerImage:     defaultImageLimitBytes,
			MaxInputBytesPerPrompt:    defaultImageLimitBytes,
			MaxOutputBytesPerImage:    defaultImageLimitBytes,
			MaxOutputBytesPerToolCall: defaultImageLimitBytes,
		},
		runtime: runtimeOptions{
			nativeCancelTimeout:  defaultNativeCancelTimeout,
			nativeCloseTurnWait:  defaultNativeCloseTurnWait,
			nativeCommandTimeout: defaultNativeCommandTimeout,
			maxJSONLineBytes:     defaultNativePromptLineLimit,
			newTurnTimer:         newRealTurnTimer,
			startupProbe: func(ctx context.Context, client *nativeamp.Client) error {
				return client.StartupProbe(ctx)
			},
			executeThread: func(ctx context.Context, client *nativeamp.Client, input any) (*nativeamp.Turn, error) {
				return client.Execute(ctx, input)
			},
			continueThread: func(ctx context.Context, client *nativeamp.Client, threadID string, input any) (*nativeamp.Turn, error) {
				return client.Continue(ctx, threadID, input)
			},
			exportThread: func(ctx context.Context, client *nativeamp.Client, threadID string) (json.RawMessage, error) {
				return client.ExportThread(ctx, threadID)
			},
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

	if options.ProcessIsolation != nil {
		cloned := *options.ProcessIsolation
		cloned.BaseEnvironment = cloneStringMap(options.ProcessIsolation.BaseEnvironment)
		options.ProcessIsolation = &cloned
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

// WithProcessIsolation requires every Amp command, probe, and authentication
// leg to run as the supplied non-root identity with no
// supplementary groups. BaseEnvironment replaces the adapter environment;
// WithEnv and per-session values overlay it.
func WithProcessIsolation(isolation ProcessIsolation) Option {
	return func(options *Options) {
		cloned := isolation
		cloned.BaseEnvironment = cloneStringMap(isolation.BaseEnvironment)
		options.ProcessIsolation = &cloned
	}
}

// WithHome records a native config/auth root, but Amp has no such root: it runs
// each session inside an ephemeral isolated home under WithScratchDir instead.
// The option stays in the surface for symmetry; a non-empty value is rejected
// fail-closed at every session start with the uniform unsupported "home" field
// error. Use WithScratchDir to control where the ephemeral state is
// materialized.
func WithHome(path string) Option {
	return func(options *Options) {
		options.Home = path
	}
}

// WithScratchDir sets the sole parent directory for all ephemeral on-disk
// materialization: the per-session isolated HOME/XDG settings directories, the
// startup/discovery probes' isolated HOME/XDG/settings/MCP residence, and any
// other temporary state. An empty value falls back to the system temp directory.
// The directory is created 0700 when it does not yet exist.
func WithScratchDir(dir string) Option {
	return func(options *Options) {
		options.ScratchDir = dir
	}
}

// WithInputHandoffRoot sets the read root for the local handoff input form. An
// image content block may arrive with empty data, a file URI naming a path under
// this directory, and an _meta "acp-go.dev/handoff" envelope carrying the sha256
// digest and byte size of the referenced file; the wrapper then reads and
// verifies those bytes instead of decoding embedded base64. The directory must
// be absolute.
//
// It is a read root only: the wrapper never writes, moves, or removes anything
// under it, so it is not an ephemeral-materialization root and does not compete
// with WithScratchDir. Leaving it unset withholds the handoff capability
// advertisement at initialize and rejects every handoff-form block with the
// uniform invalid_handoff input error.
func WithInputHandoffRoot(dir string) Option {
	return func(options *Options) {
		options.InputHandoffRoot = dir
	}
}

// WithProviderAuthRoot sets the durable directory that houses the values-free
// provider-auth ledger. The path must be absolute; a relative path is rejected
// at agent construction. The directory is created 0700 when missing and ledger
// entries are written 0600. Omitting the option, or supplying a root that is
// not a writable directory, leaves every provider-auth method unadvertised and
// answering method-not-found: a leg that cannot record what it did is never
// offered. The root carries no config or auth-resolution semantics and is never
// a scratch parent.
func WithProviderAuthRoot(path string) Option {
	return func(options *Options) {
		options.ProviderAuthRoot = path
	}
}

// WithProviderAuthDirectHome is unsupported. Amp's disconnect bumps the binding
// generation and releases the ledger slot a connection owns; it performs no
// native removal and reads no operator home, so there is nothing for an
// exact-home consent gate to authorize. Establishing a session with a non-empty
// value is rejected as an unsupported option.
func WithProviderAuthDirectHome(path string) Option {
	return func(options *Options) {
		options.ProviderAuthDirectHome = path
	}
}

func WithDarwinBestEffortContainment() Option {
	return func(options *Options) {
		options.DarwinBestEffortContainment = true
	}
}

// WithRuntimeResourceHooks installs host-facing native-root and scratch-root admission hooks.
func WithRuntimeResourceHooks(hooks RuntimeResourceHooks) Option {
	return func(options *Options) {
		options.RuntimeResourceHooks = hooks
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

// WithImageLimits sets decoded image byte limits.
func WithImageLimits(limits ImageLimits) Option {
	return func(options *Options) {
		options.ImageLimits = limits
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

func containmentMode(options Options) RuntimeContainmentMode {
	if options.DarwinBestEffortContainment && runtimeGOOS != platformDarwin {
		return RuntimeContainmentUnavailable
	}

	switch runtimeGOOS {
	case platformLinux:
		if sharedProcessIdentity(options.ProcessIsolation) {
			return RuntimeContainmentSharedIdentity
		}

		return RuntimeContainmentAuthoritative
	case platformDarwin:
		if options.DarwinBestEffortContainment {
			return RuntimeContainmentBestEffort
		}
	}

	return RuntimeContainmentUnavailable
}

func validateContainmentOptions(options Options) error {
	if options.DarwinBestEffortContainment && runtimeGOOS != platformDarwin {
		return errors.New("darwin best-effort containment is supported only on darwin")
	}

	for key := range options.Env {
		if strings.HasPrefix(strings.ToUpper(key), privateEnvPrefix) {
			return fmt.Errorf("environment key %q uses the reserved %s prefix", key, privateEnvPrefix)
		}
	}

	return nil
}

// validateInputHandoffRoot rejects a relative handoff read root. Containment
// checks are meaningless against a root whose meaning depends on the process
// working directory, so a relative value is a configuration error rather than a
// per-block verdict.
func validateInputHandoffRoot(root string) error {
	if root != "" && !filepath.IsAbs(root) {
		return errors.New("input handoff root must be an absolute path")
	}

	return nil
}

func validateImageLimits(limits ImageLimits) error {
	switch {
	case limits.MaxInputBytesPerImage < 0:
		return errors.New("max input bytes per image must be non-negative")
	case limits.MaxInputBytesPerPrompt < 0:
		return errors.New("max input bytes per prompt must be non-negative")
	case limits.MaxOutputBytesPerImage < 0:
		return errors.New("max output bytes per image must be non-negative")
	case limits.MaxOutputBytesPerToolCall < 0:
		return errors.New("max output bytes per tool call must be non-negative")
	default:
		return nil
	}
}
