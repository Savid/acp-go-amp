package amp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// MinimumVersion is the oldest amp CLI verified to support the wrapper's
	// full flag surface, including thread-less `-x --stream-json-input` turns
	// and --no-archive-after-execute.
	MinimumVersion              = "0.0.1784765892"
	ampExecutableName           = authSecretsDirName
	envHome                     = "HOME"
	envXDGCacheHome             = "XDG_CACHE_HOME"
	envXDGConfigHome            = "XDG_CONFIG_HOME"
	envXDGStateHome             = "XDG_STATE_HOME"
	maxCapturedStderrBytes      = 64 * 1024
	defaultCloseKillAfter       = 500 * time.Millisecond
	defaultCloseWait            = 5 * time.Second
	ampArgThreads               = "threads"
	ampThreadContinue           = "continue"
	ampThreadDelete             = "delete"
	ampThreadExport             = "export"
	ampArgNoIDE                 = "--no-ide"
	ampArgNoColor               = "--no-color"
	ampArgNoNotifications       = "--no-notifications"
	ampArgSettingsFile          = "--settings-file"
	ampArgMCPConfig             = "--mcp-config"
	ampArgStreamJSON            = "--stream-json"
	ampArgStreamJSONInput       = "--stream-json-input"
	ampArgExecute               = "-x"
	ampArgNoArchiveAfterExecute = "--no-archive-after-execute"
	adapterPrivateEnvPrefix     = "ACP_GO_AMP_INTERNAL_"
	scrubbedTracebackEnv        = "GOTRACEBACK"
	scrubbedRedactionEnv        = "AMP_DISABLE_SECRET_REDACTION"
)

var (
	getwd            = os.Getwd
	closeWriteCloser = func(closer io.Closer) error { return closer.Close() }
	probeCache       sync.Map
)

type Options struct {
	CLIPath      string
	Cwd          string
	SettingsFile string
	// Env is the complete child environment overlay this client's children
	// receive. A prompt client carries the logical session's own values
	// including its raw PATH; a probe or one-shot client carries the static
	// agent-scoped values plus only the operation values it was given.
	Env map[string]string
	// ResolutionEnv is the static agent-scoped overlay the executable is
	// resolved and version-probed against. Session values never take part, so a
	// directory a host places on a session PATH reaches the child it was meant
	// for and can never shadow the amp harness itself.
	ResolutionEnv map[string]string
	// ResolvedExecutable is the exact absolute harness the version and startup
	// probes validated. When it is set every child of this client launches that
	// file directly: no lookup runs at all, so no later environment can select
	// a different binary than the one that passed validation.
	ResolvedExecutable  string
	OrdinaryEnvironment map[string]string
	NativeEnvironment   map[string]string
	ReadNativeAppendLog func(context.Context, string, uint64) ([][]byte, error)
	StartNative         NativeStarter
	Mode                string
	MCPConfigPath       string
	MaxLineBytes        int
	// OnGoroutinePanic is invoked with the recovered value when a turn-owned
	// goroutine panics, so the embedding agent can log the panic instead of
	// crashing the process. A nil handler leaves the panic to propagate.
	OnGoroutinePanic          func(ctx context.Context, name string, recovered any)
	WritableRoot              string
	BrowserShim               string
	NewProbeClient            func(context.Context) (*Client, func() error, error)
	AfterNativeWait           func(context.Context) error
	CleanupResidence          func() error
	TestOnlyAuthLoginPlatform string
}

type Client struct {
	log                  *slog.Logger
	options              Options
	checkAuthLoginSafety func(string) error
}

func NewClient(log *slog.Logger, options Options) *Client {
	if log == nil {
		log = slog.Default()
	}

	if options.MaxLineBytes <= 0 {
		options.MaxLineBytes = defaultMaxJSONLineBytes
	}

	if options.StartNative == nil && options.OrdinaryEnvironment == nil {
		options.OrdinaryEnvironment = CaptureOrdinaryEnvironment()
	}

	checkAuthLoginSafety := CheckAuthLoginBrowserSafety

	if options.TestOnlyAuthLoginPlatform != "" {
		if options.TestOnlyAuthLoginPlatform == linuxPlatform || options.TestOnlyAuthLoginPlatform == darwinPlatform {
			// Fake Amp binaries used by the login tests model the supported
			// variants behaviorally; they do not carry Amp's bundled JS.
			checkAuthLoginSafety = func(string) error { return nil }
		} else {
			checkAuthLoginSafety = func(path string) error {
				return checkAuthLoginBrowserSafetyOnPlatform(options.TestOnlyAuthLoginPlatform, path)
			}
		}
	}

	return &Client{
		log:                  log,
		options:              options,
		checkAuthLoginSafety: checkAuthLoginSafety,
	}
}

func (c *Client) Version(ctx context.Context) (string, error) {
	path, err := c.resolveExecutable(ctx, c.options.Cwd)
	if err != nil {
		return "", err
	}

	return c.versionAt(ctx, path)
}

// versionAt runs `amp version` on one already-resolved harness. The probe
// resolves once and reports the version of exactly that file, so the path the
// caller retains is the path whose version was validated.
func (c *Client) versionAt(ctx context.Context, path string) (string, error) {
	out, err := c.outputAtPath(ctx, path, "version")
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(out)), nil
}

// startupProbeThreadID is a deliberately non-existent thread id used for the
// method-present probes. amp answers with a domain "missing thread" error when a
// subcommand exists but the id is unknown, which lets us distinguish a present
// subcommand from a removed one without spending tokens or touching real threads.
const startupProbeThreadID = "T-00000000-0000-0000-0000-000000000000"

const startupProbeTimeout = 30 * time.Second

// StartupProbe validates the harness and returns the exact absolute file that
// passed. Every probe child runs that file, and the caller retains it for the
// session launches that follow.
func (c *Client) StartupProbe(ctx context.Context) (string, error) {
	if c.options.NewProbeClient == nil {
		if err := c.validateProbeResidence(); err != nil {
			return "", err
		}
	}

	path, version, err := c.discoverVersion(ctx)
	if err != nil {
		return "", err
	}

	cacheKey := path + "\x00" + version
	cacheable := c.options.StartNative == nil

	if cacheable {
		if _, ok := probeCache.Load(cacheKey); ok {
			return path, nil
		}
	}

	if err := c.pinnedTo(path).probeSubcommands(ctx); err != nil {
		return "", err
	}

	if cacheable {
		probeCache.Store(cacheKey, struct{}{})
	}

	return path, nil
}

// DiscoveryProbe verifies the executable and version without running commands
// that require an authenticated Amp account.
func (c *Client) DiscoveryProbe(ctx context.Context) (string, error) {
	if c.options.NewProbeClient == nil {
		if err := c.validateProbeResidence(); err != nil {
			return "", err
		}
	}

	path, _, err := c.discoverVersion(ctx)
	if err != nil {
		return "", err
	}

	return path, nil
}

// pinnedTo returns a copy of this client whose every child launches the exact
// resolved harness, without re-running discovery.
func (c *Client) pinnedTo(path string) *Client {
	pinned := *c
	pinned.options.ResolvedExecutable = path

	return &pinned
}

func (c *Client) validateProbeResidence() error {
	root := c.options.WritableRoot
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" || strings.IndexByte(root, 0) >= 0 {
		return errors.New("amp probe requires a clean absolute isolated writable root")
	}

	environment, err := c.buildEnvironment(c.options.Env, c.options.Cwd)
	if err != nil {
		return err
	}

	values := environmentMap(environment)

	expected := []struct {
		name string
		got  string
		want string
	}{
		{name: envHome, got: values[envHome], want: filepath.Join(root, "home")},
		{name: envXDGConfigHome, got: values[envXDGConfigHome], want: filepath.Join(root, "xdg-config")},
		{name: envXDGCacheHome, got: values[envXDGCacheHome], want: filepath.Join(root, "xdg-cache")},
		{name: dataHomeEnv, got: values[dataHomeEnv], want: filepath.Join(root, "xdg-data")},
		{name: envXDGStateHome, got: values[envXDGStateHome], want: filepath.Join(root, "xdg-state")},
		{name: "settings file", got: c.options.SettingsFile, want: filepath.Join(root, "xdg-config", "amp", "settings.json")},
		{name: "MCP config", got: c.options.MCPConfigPath, want: filepath.Join(root, "mcp.json")},
	}
	for _, value := range expected {
		if value.got != value.want {
			return fmt.Errorf("amp probe %s must equal %q", value.name, value.want)
		}
	}

	return nil
}

func (c *Client) discoverVersion(ctx context.Context) (string, string, error) {
	path, err := c.resolveExecutable(ctx, c.options.Cwd)
	if err != nil {
		return "", "", err
	}

	version, err := c.versionAt(ctx, path)
	if err != nil {
		return "", "", err
	}

	if !versionAtLeast(version, MinimumVersion) {
		return "", "", fmt.Errorf("amp version %q is below required %s", version, MinimumVersion)
	}

	return path, version, nil
}

// probeSubcommands executes the required Amp subcommands for real instead of
// grepping help text: it runs `threads list --json` and issues method-present
// probes for `threads export/continue/delete` against a missing id. The continue
// probe uses an isolated settings file and the same real-turn flag surface; the
// known-missing thread must fail before any model turn can start.
func (c *Client) probeSubcommands(ctx context.Context) error {
	probeCtx, cancel := context.WithTimeout(ctx, startupProbeTimeout)
	defer cancel()

	if _, err := c.ListThreads(probeCtx); err != nil {
		return fmt.Errorf("amp threads list --json probe failed: %w", err)
	}

	continueClient := *c
	continueClient.options.Mode = "medium"
	// --no-archive-after-execute rides on the probe so an amp build that does
	// not parse the flag fails startup closed instead of failing the first
	// prompt: the missing-thread domain error still proves the flag was
	// accepted, and no extra process or token is spent.
	continueArgs := []string{ampArgThreads, ampThreadContinue, startupProbeThreadID, ampArgStreamJSON, ampArgStreamJSONInput, ampArgExecute, ampArgNoArchiveAfterExecute}

	probes := []struct {
		name                 string
		client               *Client
		args                 []string
		requireMissingThread bool
	}{
		{name: "threads export", client: c, args: []string{ampArgThreads, ampThreadExport, startupProbeThreadID}},
		{name: "threads continue", client: &continueClient, args: continueArgs, requireMissingThread: true},
		{name: "threads delete", client: c, args: []string{ampArgThreads, ampThreadDelete, startupProbeThreadID}},
	}
	for _, probe := range probes {
		if _, err := probe.client.output(probeCtx, probe.args...); err != nil {
			if methodErr := methodProbeError(probe.name, err, probe.requireMissingThread); methodErr != nil {
				return methodErr
			}
		} else if probe.requireMissingThread {
			return fmt.Errorf("amp %s probe unexpectedly succeeded for missing thread %s", probe.name, startupProbeThreadID)
		}
	}

	return nil
}

// methodProbeError classifies a method-present probe result: a domain
// missing-thread error means the subcommand exists (probe passes, nil); any
// other error means the subcommand is missing or broken (probe fails).
func methodProbeError(name string, err error, requireMissingThread bool) error {
	if errors.Is(err, ErrContainmentIncomplete) {
		return fmt.Errorf("amp %s probe containment failed: %w", name, err)
	}

	if err == nil || isMissingThreadMessage(err.Error()) {
		return nil
	}

	if requireMissingThread {
		return fmt.Errorf("amp %s probe did not return missing-thread domain error: %w", name, err)
	}

	return fmt.Errorf("amp %s probe failed: %w", name, err)
}

func isMissingThreadMessage(msg string) bool {
	return strings.Contains(msg, "does not exist") || strings.Contains(msg, "Thread not found")
}

func (c *Client) ListThreads(ctx context.Context) ([]ThreadSummary, error) {
	out, err := c.output(ctx, ampArgThreads, "list", "--json")
	if err != nil {
		return nil, err
	}

	var summaries []ThreadSummary
	if err := json.Unmarshal(out, &summaries); err != nil {
		return nil, fmt.Errorf("decode amp threads list: %w", err)
	}

	return summaries, nil
}

func (c *Client) ExportThread(ctx context.Context, threadID string) (json.RawMessage, error) {
	out, err := c.output(ctx, ampArgThreads, ampThreadExport, threadID)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(bytes.TrimSpace(out)), nil
}

func (c *Client) DeleteThread(ctx context.Context, threadID string) error {
	_, err := c.output(ctx, ampArgThreads, ampThreadDelete, threadID)
	if err != nil && !errors.Is(err, ErrContainmentIncomplete) && strings.Contains(err.Error(), "does not exist") {
		return nil
	}

	return err
}

func (c *Client) Continue(ctx context.Context, threadID string, input any) (*Turn, error) {
	args := c.globalArgs()
	args = append(args, ampArgThreads, ampThreadContinue, threadID, ampArgStreamJSON, ampArgStreamJSONInput, ampArgExecute)

	return c.startTurn(ctx, args, input)
}

// Execute launches a thread-less `amp -x` turn: amp mints the server-side
// thread itself and reports its id on the stream-json init frame, so no
// remote thread exists until a prompt actually runs. --no-archive-after-execute
// keeps the minted thread visible in amp's thread list, matching the
// visibility of interactively created threads.
func (c *Client) Execute(ctx context.Context, input any) (*Turn, error) {
	args := c.globalArgs()
	args = append(args, ampArgNoArchiveAfterExecute, ampArgStreamJSON, ampArgStreamJSONInput, ampArgExecute)

	return c.startTurn(ctx, args, input)
}

func (c *Client) startTurn(ctx context.Context, args []string, input any) (*Turn, error) {
	cwd := c.options.Cwd
	if cwd == "" {
		var err error

		cwd, err = getwd()
		if err != nil {
			return nil, fmt.Errorf("get working directory: %w", err)
		}
	}

	environment, err := c.buildEnvironment(c.options.Env, cwd)
	if err != nil {
		return nil, err
	}

	path, err := c.resolveExecutable(ctx, cwd)
	if err != nil {
		return nil, err
	}

	process, err := c.startNative(ctx, NativeRequest{
		Executable: path, Arguments: append([]string(nil), args...),
		Environment: environment, WorkingDirectory: cwd,
	})
	if err != nil {
		return nil, fmt.Errorf("start amp: %w", err)
	}

	turn := &Turn{
		log:           c.log,
		process:       process,
		authoritative: c.options.StartNative != nil,
		stdin:         process.Stdin(),
		stdout:        process.Stdout(),
		stderr:        process.Stderr(),
		maxLineBytes:  c.options.MaxLineBytes,
		messages:      make(chan Message),
		errs:          make(chan error, 4),
		onPanic:       c.options.OnGoroutinePanic,
	}
	turn.start(ctx)

	// A launch that fails after the process started still owns a contained tree.
	// The cleanup close's own error travels with the transport failure so the
	// caller's scratch containment sees an incomplete boundary: a discarded one
	// would let a surviving native process escape both the prompt boundary and
	// the session poison latch.
	if err := turn.Send(ctx, input); err != nil {
		return nil, errors.Join(err, turn.Close())
	}

	if err := closeWriteCloser(turn.stdin); err != nil {
		return nil, errors.Join(fmt.Errorf("close amp stdin: %w", err), turn.Close())
	}

	return turn, nil
}

func (c *Client) output(ctx context.Context, args ...string) ([]byte, error) {
	return c.outputWithArgs(ctx, append(c.globalArgs(), args...)...)
}

func (c *Client) outputWithArgs(ctx context.Context, args ...string) ([]byte, error) {
	path, err := c.resolveExecutable(ctx, c.commandCwd())
	if err != nil {
		return nil, err
	}

	return c.outputAtPath(ctx, path, args...)
}

// commandCwd is the directory a one-shot command runs in.
func (c *Client) commandCwd() string {
	cwd := c.options.Cwd
	if cwd == "" {
		cwd, _ = getwd()
	}

	return cwd
}

// outputAtPath runs one short-lived command on an already-resolved harness, so
// the caller decides which file runs and the child environment never re-enters
// executable selection.
func (c *Client) outputAtPath(ctx context.Context, path string, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if c.options.NewProbeClient != nil {
		probe, cleanup, err := c.options.NewProbeClient(ctx)
		if err != nil {
			return nil, err
		}

		if err := probe.validateProbeResidence(); err != nil {
			return nil, errors.Join(err, cleanup())
		}

		probe.options.ResolvedExecutable = path
		probe.options.NewProbeClient = nil
		out, runErr := probe.outputAtPath(ctx, path, args...)

		return out, errors.Join(runErr, cleanup())
	}

	cwd := c.commandCwd()

	environment, err := c.buildEnvironment(c.options.Env, cwd)
	if err != nil {
		return nil, err
	}

	process, err := c.startNative(ctx, NativeRequest{
		Executable: path, Arguments: append([]string(nil), args...),
		Environment: environment, WorkingDirectory: cwd,
	})
	if err != nil {
		return nil, fmt.Errorf("amp %s: %w", strings.Join(args, " "), err)
	}

	stdin := process.Stdin()
	stdoutStream := process.Stdout()
	stderrStream := process.Stderr()
	_ = stdin.Close()

	var stdout, stderr bytes.Buffer

	readDone := make(chan struct{}, 2)

	go func() { _, _ = io.Copy(&stdout, stdoutStream); readDone <- struct{}{} }()
	go func() { _, _ = io.Copy(&stderr, stderrStream); readDone <- struct{}{} }()

	result, waitErr := process.Wait(ctx)
	terminal := waitErr == nil

	detachedWait := errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded)
	if waitErr != nil && (c.options.StartNative != nil || detachedWait) {
		initialWaitErr := waitErr
		revokeCtx, cancelRevoke := context.WithTimeout(context.Background(), defaultCloseWait)
		revokeErr := process.Revoke(revokeCtx)

		cancelRevoke()

		terminalCtx, cancelTerminal := context.WithTimeout(context.Background(), defaultCloseWait)
		result, waitErr = process.Wait(terminalCtx)

		cancelTerminal()

		if waitErr != nil {
			waitErr = errors.Join(initialWaitErr, revokeErr, waitErr, ErrContainmentIncomplete)
		} else {
			terminal = true

			if detachedWait {
				waitErr = revokeErr
			} else {
				waitErr = errors.Join(initialWaitErr, revokeErr)
			}
		}
	}

	if !terminal {
		_ = stdoutStream.Close()
		_ = stderrStream.Close()
	}

	<-readDone
	<-readDone

	if terminal {
		_ = stdoutStream.Close()
		_ = stderrStream.Close()
	}

	if waitErr != nil || result.ExitCode != 0 || result.Signal != 0 || result.Revoked {
		msg := strings.TrimSpace(stripANSI(stderr.String()))
		if msg == "" {
			msg = fmt.Sprintf("exit code %d signal %d", result.ExitCode, result.Signal)
		}

		return nil, errors.Join(fmt.Errorf("amp %s: %s", strings.Join(args, " "), msg), waitErr)
	}

	return stdout.Bytes(), nil
}

func (c *Client) startNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	starter := c.options.StartNative
	if starter == nil {
		starter = startOrdinaryNative
	}

	process, err := starter(ctx, request)
	if err != nil {
		return nil, err
	}

	if process == nil {
		return nil, errors.New("native process returned unusable host stdio")
	}

	tracked := trackProcess(process)
	if tracked.Stdin() == nil || tracked.Stdout() == nil || tracked.Stderr() == nil {
		revokeCtx, cancelRevoke := context.WithTimeout(context.Background(), defaultCloseWait)
		revokeErr := tracked.Revoke(revokeCtx)

		cancelRevoke()

		waitCtx, cancelWait := context.WithTimeout(context.Background(), defaultCloseWait)
		_, waitErr := tracked.Wait(waitCtx)

		cancelWait()

		if c.options.StartNative != nil && waitErr != nil {
			waitErr = errors.Join(waitErr, ErrContainmentIncomplete)
		}

		return nil, errors.Join(errors.New("native process returned unusable host stdio"), revokeErr, waitErr)
	}

	return tracked, nil
}

func (c *Client) globalArgs() []string {
	args := []string{ampArgNoIDE, ampArgNoColor, ampArgNoNotifications}
	if c.options.SettingsFile != "" {
		args = append(args, "--settings-file", c.options.SettingsFile)
	}

	if c.options.MCPConfigPath != "" {
		args = append(args, "--mcp-config", c.options.MCPConfigPath)
	}

	if c.options.Mode != "" {
		args = append(args, "-m", c.options.Mode)
	}

	return args
}

func Discover(ctx context.Context, cliPath string, environments ...[]string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	file := strings.TrimSpace(cliPath)
	if file == "" {
		file = ampExecutableName
	}

	if len(environments) == 0 {
		return "", errors.New("complete process environment is required for executable discovery")
	}

	path, err := lookPathInEnvironment(file, environments[0])
	if err != nil {
		return "", fmt.Errorf("find amp in PATH: %w", err)
	}

	return path, nil
}

// resolveExecutable yields the amp harness this client launches. A retained
// path is the file the version and startup probes already validated, so it is
// used verbatim and no lookup runs. Otherwise the harness is found against the
// static base — the policy or ordinary environment plus the agent-scoped
// overlay — and never against the logical session's own environment.
func (c *Client) resolveExecutable(ctx context.Context, cwd string) (string, error) {
	if retained := c.options.ResolvedExecutable; retained != "" {
		if c.options.StartNative == nil && !filepath.IsAbs(retained) {
			return "", fmt.Errorf("retained amp harness %q is not an absolute path", retained)
		}

		return retained, nil
	}

	environment, err := c.buildEnvironment(c.options.ResolutionEnv, cwd)
	if err != nil {
		return "", err
	}

	return c.discover(ctx, environment, cwd)
}

func (c *Client) discover(ctx context.Context, environment []string, cwd string) (string, error) {
	if c.options.StartNative != nil {
		file := strings.TrimSpace(c.options.CLIPath)
		if file == "" {
			file = ampExecutableName
		}

		return file, nil
	}

	file := strings.TrimSpace(c.options.CLIPath)
	if file == "" {
		file = ampExecutableName
	}

	path, err := lookPathInOrdinaryEnvironment(file, environment, cwd)
	if err != nil {
		return "", fmt.Errorf("find amp in PATH: %w", err)
	}

	return path, nil
}

// HasAPIKey reports whether the supplied environment delivers a non-empty
// AMP_API_KEY under this platform's environment key identity. Equal-fold
// spellings are resolved exactly as buildEnvironment resolves them within one
// phase — sorted, last wins — so the gate reads the value the child would
// actually receive rather than an arbitrary map entry.
func HasAPIKey(overrides map[string]string) bool {
	delivered, found := "", false

	for _, key := range sortedEnvironmentKeys(overrides) {
		if launchEnvironmentKey(key) == AuthAPIKeyEnv {
			delivered, found = overrides[key], true
		}
	}

	return found && strings.TrimSpace(delivered) != ""
}

func BuildEnv(overrides map[string]string, cwd string) []string {
	env, _ := buildEnvironment(cwd, overrides)

	return env
}

func (c *Client) buildEnvironment(overrides map[string]string, cwd string) ([]string, error) {
	if c.options.StartNative != nil {
		return buildEnvironment(cwd, c.options.NativeEnvironment, overrides)
	}

	return buildEnvironment(cwd, c.options.OrdinaryEnvironment, overrides)
}

// buildEnvironment applies the supplied phases left to right and appends the
// adapter-managed working directory last. A later phase replaces an earlier
// key under the platform environment key identity, so the child receives
// exactly one entry per variable and the final phase always wins.
func buildEnvironment(cwd string, phases ...map[string]string) ([]string, error) {
	capacity := 1
	for _, phase := range phases {
		capacity += len(phase)
	}

	values := make(map[string]string, capacity)

	set := func(key, value string) {
		key = launchEnvironmentKey(key)
		if isPrivateAdapterEnv(key) || isScrubbedEnv(key) {
			return
		}

		values[key] = value
	}

	for _, phase := range phases {
		for _, key := range sortedEnvironmentKeys(phase) {
			if key == "" || strings.ContainsRune(key, '=') {
				return nil, fmt.Errorf("invalid environment key %q", key)
			}

			set(key, phase[key])
		}
	}

	if cwd != "" {
		set("PWD", cwd)
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+values[key])
	}

	return out, nil
}

func sortedEnvironmentKeys(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

// isScrubbedEnv reports the variables that never reach a spawned amp process,
// from the live environment or from an explicit override. Neither is disarmable
// once the child is running: GOTRACEBACK=crash dumps every goroutine to stderr
// before any code of ours runs, and AMP_DISABLE_SECRET_REDACTION turns off
// amp's own redaction wholesale.
func isScrubbedEnv(key string) bool {
	switch key {
	case scrubbedTracebackEnv, scrubbedRedactionEnv:
		return true
	default:
		return false
	}
}

func isPrivateAdapterEnv(key string) bool {
	return strings.HasPrefix(strings.ToUpper(key), adapterPrivateEnvPrefix)
}

func versionAtLeast(got string, floor string) bool {
	gotParts := versionParts(got)

	minParts := versionParts(floor)
	for len(gotParts) < len(minParts) {
		gotParts = append(gotParts, 0)
	}

	for len(minParts) < len(gotParts) {
		minParts = append(minParts, 0)
	}

	for i := range gotParts {
		switch {
		case gotParts[i] > minParts[i]:
			return true
		case gotParts[i] < minParts[i]:
			return false
		}
	}

	return true
}

func versionParts(value string) []int64 {
	head := strings.Fields(strings.TrimSpace(value))
	if len(head) == 0 {
		return nil
	}

	version, _, _ := strings.Cut(head[0], "-")
	rawParts := strings.Split(version, ".")

	parts := make([]int64, 0, len(rawParts))
	for _, raw := range rawParts {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil
		}

		parts = append(parts, n)
	}

	return parts
}

type Turn struct {
	log           *slog.Logger
	process       NativeProcess
	authoritative bool
	stdin         io.WriteCloser
	stdout        io.ReadCloser
	stderr        io.ReadCloser
	maxLineBytes  int
	messages      chan Message
	errs          chan error
	stderrMu      sync.Mutex
	stderrTail    bytes.Buffer
	stderrDone    chan struct{}
	stdoutDone    chan struct{}
	stopTerminal  context.CancelFunc
	closeOnce     sync.Once
	closeErr      error
	onPanic       func(ctx context.Context, name string, recovered any)
}

// recoverGoroutine is deferred at the top of every turn-owned goroutine. It
// must be the deferred function itself so recover() observes the goroutine's
// panic; without a handler the panic propagates unchanged.
func (t *Turn) recoverGoroutine(ctx context.Context, name string) {
	if t.onPanic == nil {
		return
	}

	if recovered := recover(); recovered != nil {
		t.onPanic(ctx, name, recovered)
	}
}

func (t *Turn) Messages() <-chan Message { return t.messages }
func (t *Turn) Errors() <-chan error     { return t.errs }

func (t *Turn) Send(ctx context.Context, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal amp input: %w", err)
	}

	if len(data)+1 > t.maxLineBytes {
		return fmt.Errorf("amp stdin json line exceeds %d bytes", t.maxLineBytes)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	done := make(chan error, 1)

	go func() {
		defer t.recoverGoroutine(ctx, "amp stdin writer")

		if _, err := t.stdin.Write(append(data, '\n')); err != nil {
			done <- fmt.Errorf("write amp stdin: %w", err)

			return
		}

		done <- nil
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Turn) start(ctx context.Context) {
	t.stderrDone = make(chan struct{})
	t.stdoutDone = make(chan struct{})

	terminalCtx, stopTerminal := context.WithCancel(context.Background())
	t.stopTerminal = stopTerminal

	go t.drainStderr(ctx)
	go t.readStdout(ctx, terminalCtx)
}

func (t *Turn) readStdout(ctx context.Context, terminalCtx context.Context) {
	defer t.recoverGoroutine(ctx, "amp stdout reader")
	defer func() {
		if t.stdoutDone != nil {
			close(t.stdoutDone)
		}
	}()
	defer close(t.messages)
	defer close(t.errs)
	defer func() {
		if err := t.wait(terminalCtx); err != nil {
			select {
			case <-t.stderrDone:
			case <-time.After(defaultCloseKillAfter):
			}

			t.sendErr(t.exitError(err))
		}
	}()

	scanner := bufio.NewScanner(t.stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), t.maxLineBytes)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] != '{' {
			continue
		}

		msg, err := ParseJSONLine(line)
		if err != nil {
			t.sendErr(fmt.Errorf("decode amp json line: %w", err))

			continue
		}

		select {
		case t.messages <- msg:
		case <-ctx.Done():
			t.sendErr(ctx.Err())

			return
		case <-terminalCtx.Done():
			return
		}
	}

	if err := scanner.Err(); err != nil {
		t.sendErr(fmt.Errorf("read amp stdout: %w", err))
	}
}

func (t *Turn) drainStderr(ctx context.Context) {
	defer t.recoverGoroutine(ctx, "amp stderr drain")
	defer close(t.stderrDone)

	scanner := bufio.NewScanner(t.stderr)
	for scanner.Scan() {
		t.captureStderr(scanner.Text())

		if t.log != nil {
			t.log.DebugContext(ctx, "amp stderr", slog.String("line", scanner.Text()))
		}
	}
}

func (t *Turn) Interrupt(ctx context.Context) error {
	if t.process == nil {
		return nil
	}

	_ = t.stdin.Close()
	revokeErr := t.process.Revoke(ctx)

	result, waitErr := t.process.Wait(ctx)
	waitErr = markDetachedWaitIncomplete(waitErr)

	if (result.Revoked || result.Signal != 0) && !errors.Is(waitErr, ErrContainmentIncomplete) {
		waitErr = nil
	}

	if result.Revoked && waitErr == nil && (errors.Is(revokeErr, context.Canceled) || errors.Is(revokeErr, context.DeadlineExceeded)) {
		revokeErr = nil
	}

	return errors.Join(revokeErr, waitErr)
}

func (t *Turn) Close() error {
	t.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultCloseWait)
		t.closeErr = t.closeWithContext(ctx)

		cancel()
	})

	return t.closeErr
}

func (t *Turn) closeWithContext(ctx context.Context) error {
	var err error
	if t.process != nil {
		err = errors.Join(err, t.Interrupt(ctx))
	}

	if t.stopTerminal != nil {
		t.stopTerminal()
	}

	if t.stdin != nil {
		err = errors.Join(err, t.stdin.Close())
	}

	if t.stdout != nil {
		err = errors.Join(err, t.stdout.Close())
	}

	if t.stderr != nil {
		err = errors.Join(err, t.stderr.Close())
	}

	if t.stdoutDone != nil {
		<-t.stdoutDone
	}

	if t.stderrDone != nil {
		<-t.stderrDone
	}

	return err
}

func (t *Turn) wait(ctx context.Context) error {
	if t.process == nil {
		return nil
	}

	result, err := t.process.Wait(ctx)
	if err == nil && (result.ExitCode != 0 || result.Signal != 0 || result.Revoked) {
		return fmt.Errorf("native exit code %d signal %d revoked %t", result.ExitCode, result.Signal, result.Revoked)
	}

	return err
}

func (t *Turn) sendErr(err error) {
	if err == nil {
		return
	}

	select {
	case t.errs <- err:
	default:
		if t.log != nil {
			t.log.Debug("drop amp turn error", slog.String("error", err.Error()))
		}
	}
}

func (t *Turn) captureStderr(line string) {
	t.stderrMu.Lock()
	defer t.stderrMu.Unlock()

	if t.stderrTail.Len() > 0 {
		t.stderrTail.WriteByte('\n')
	}

	t.stderrTail.WriteString(line)

	for t.stderrTail.Len() > maxCapturedStderrBytes {
		_, _ = t.stderrTail.ReadByte()
	}
}

func (t *Turn) stderrText() string {
	t.stderrMu.Lock()
	defer t.stderrMu.Unlock()

	return strings.TrimSpace(stripANSI(t.stderrTail.String()))
}

func (t *Turn) exitError(err error) error {
	detail := t.stderrText()
	if detail == "" {
		return fmt.Errorf("amp process exited: %w", err)
	}

	return fmt.Errorf("amp process exited: %w: %s", err, detail)
}

func stripANSI(s string) string {
	var b strings.Builder

	inEscape := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inEscape {
			if ch == '[' || (ch >= '0' && ch <= '?') {
				continue
			}

			if ch >= '@' && ch <= '~' {
				inEscape = false
			}

			continue
		}

		if ch == 0x1b {
			inEscape = true

			continue
		}

		b.WriteByte(ch)
	}

	return b.String()
}
