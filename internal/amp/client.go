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
	"os/exec"
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
	adapterSupervisorModeEnv    = "ACP_GO_AMP_INTERNAL_NATIVE_SUPERVISOR"
	adapterOneShotDeathPhaseEnv = "ACP_GO_AMP_TEST_ONE_SHOT_DEATH_PHASE"
	adapterOneShotDeathPathEnv  = "ACP_GO_AMP_TEST_ONE_SHOT_DEATH_PATH"
	adapterOneShotDeathStateEnv = "ACP_GO_AMP_TEST_ONE_SHOT_DEATH_STATE"
	adapterPrivateEnvPrefix     = "ACP_" + "GO_AMP_INTERNAL_"
	scrubbedTracebackEnv        = "GOTRACEBACK"
	scrubbedRedactionEnv        = "AMP_DISABLE_SECRET_REDACTION"
)

var (
	commandContext             = exec.CommandContext
	getwd                      = os.Getwd
	closeWriteCloser           = func(closer io.Closer) error { return closer.Close() }
	openPipe                   = os.Pipe
	probeCache                 sync.Map
	processTreeDescendantCount = func(tree *processTree) (int, bool) {
		return tree.descendantCount()
	}
	processTreeTerminateAndWait = func(tree *processTree, timeout time.Duration) error {
		return tree.terminateAndWait(timeout)
	}
	prepareProcessTree = prepareProcessTreeCommand
	newCommandWait     = startCommandWait
	commandWaitTimeout = defaultCloseWait
)

type Options struct {
	CLIPath      string
	Cwd          string
	SettingsFile string
	// ScratchParent is the already-resolved parent for the account-login browser
	// shim. The embedding package owns temp-directory resolution.
	ScratchParent string
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
	Isolation           *ProcessIsolation
	OrdinaryEnvironment map[string]string
	Mode                string
	MCPConfigPath       string
	MaxLineBytes        int
	// OnGoroutinePanic is invoked with the recovered value when a turn-owned
	// goroutine panics, so the embedding agent can log the panic instead of
	// crashing the process. A nil handler leaves the panic to propagate.
	OnGoroutinePanic func(ctx context.Context, name string, recovered any)
	// NewProcessSnapshotObserver registers one successfully started contained
	// native root with the embedding agent's absolute descendant inventory.
	NewProcessSnapshotObserver func(context.Context, ProcessInventory) ProcessSnapshotObserver
	DarwinBestEffort           bool
	AcquireNativeRoot          func(context.Context) (func(), error)
	NewDarwinGeneration        func(context.Context) (*DarwinGeneration, error)
	WritableRoot               string
	TestOnlyAuthLoginPlatform  string
}

// ProcessInventory queries the current absolute inventory exposed by an
// authoritative containment boundary. False means no absolute count is available.
type ProcessInventory func() (count int, available bool)

// ProcessSnapshotObserver reports inventory only for authoritative containment.
// Refresh is optional when no absolute live inventory exists; Complete follows
// successful completion of the authoritative boundary.
type ProcessSnapshotObserver struct {
	Refresh    func(context.Context)
	Complete   func(context.Context)
	Incomplete func()
}

type Client struct {
	log                         *slog.Logger
	options                     Options
	checkAuthLoginCompatibility func(string) error
}

func (c *Client) newProcessSnapshotObserver(ctx context.Context, tree *processTree) ProcessSnapshotObserver {
	if c == nil || c.options.NewProcessSnapshotObserver == nil {
		return ProcessSnapshotObserver{}
	}

	return c.options.NewProcessSnapshotObserver(ctx, func() (int, bool) {
		return processTreeDescendantCount(tree)
	})
}

func observeProcessTreeSnapshot(ctx context.Context, observer ProcessSnapshotObserver) {
	if observer.Refresh != nil {
		observer.Refresh(ctx)
	}
}

func finishProcessTreeObservation(ctx context.Context, observer ProcessSnapshotObserver, containmentErr error) {
	if ProcessContainmentComplete(containmentErr) {
		if observer.Complete != nil {
			observer.Complete(ctx)
		}

		return
	}

	if observer.Incomplete != nil {
		observer.Incomplete()
	}
}

func NewClient(log *slog.Logger, options Options) *Client {
	if log == nil {
		log = slog.Default()
	}

	if options.MaxLineBytes <= 0 {
		options.MaxLineBytes = defaultMaxJSONLineBytes
	}

	if options.Isolation == nil && options.OrdinaryEnvironment == nil {
		options.OrdinaryEnvironment = CaptureOrdinaryEnvironment()
	}

	checkAuthLoginCompatibility := CheckAuthLoginBrowserCompatibility

	if options.TestOnlyAuthLoginPlatform != "" {
		if options.TestOnlyAuthLoginPlatform == linuxPlatform {
			// Fake Amp binaries used by the login tests model the supported
			// Linux variant behaviorally; they do not carry Amp's bundled JS.
			checkAuthLoginCompatibility = func(string) error { return nil }
		} else {
			checkAuthLoginCompatibility = func(path string) error {
				return checkAuthLoginBrowserCompatibilityOnPlatform(options.TestOnlyAuthLoginPlatform, path)
			}
		}
	}

	return &Client{
		log:                         log,
		options:                     options,
		checkAuthLoginCompatibility: checkAuthLoginCompatibility,
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
	if err := c.validateProbeResidence(); err != nil {
		return "", err
	}

	path, version, err := c.discoverVersion(ctx)
	if err != nil {
		return "", err
	}

	cacheKey := path + "\x00" + version
	if _, ok := probeCache.Load(cacheKey); ok {
		return path, nil
	}

	if err := c.pinnedTo(path).probeSubcommands(ctx); err != nil {
		return "", err
	}

	probeCache.Store(cacheKey, struct{}{})

	return path, nil
}

// DiscoveryProbe verifies the executable and version without running commands
// that require an authenticated Amp account.
func (c *Client) DiscoveryProbe(ctx context.Context) (string, error) {
	if err := c.validateProbeResidence(); err != nil {
		return "", err
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
	if errors.Is(err, ErrProcessContainmentIncomplete) {
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
	if err != nil && !errors.Is(err, ErrProcessContainmentIncomplete) && strings.Contains(err.Error(), "does not exist") {
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

	cmd := commandContext(context.Background(), path, args...)
	cmd.Dir = cwd
	cmd.Env = environment

	if cmd.Stdin != nil {
		return nil, errors.New("create amp stdin: exec: Stdin already set")
	}

	if cmd.Stdout != nil {
		return nil, errors.New("create amp stdout: exec: Stdout already set")
	}

	if cmd.Stderr != nil {
		return nil, errors.New("create amp stderr: exec: Stderr already set")
	}

	launch, err := c.prepareProcessLaunch(ctx, cmd)
	if err != nil {
		return nil, err
	}

	cmd = launch.cmd
	if cmd.Stdin != nil {
		return nil, errors.Join(errors.New("create amp stdin: exec: Stdin already set"), launch.close())
	}

	if cmd.Stdout != nil {
		return nil, errors.Join(errors.New("create amp stdout: exec: Stdout already set"), launch.close())
	}

	if cmd.Stderr != nil {
		return nil, errors.Join(errors.New("create amp stderr: exec: Stderr already set"), launch.close())
	}

	stdinReader, stdin, err := openPipe()
	if err != nil {
		closeErr := launch.close()

		return nil, errors.Join(fmt.Errorf("create amp stdin: %w", err), closeErr)
	}

	cmd.Stdin = stdinReader

	stdout, stdoutWriter, err := openPipe()
	if err != nil {
		_ = stdinReader.Close()
		_ = stdin.Close()
		closeErr := launch.close()

		return nil, errors.Join(fmt.Errorf("create amp stdout: %w", err), closeErr)
	}

	cmd.Stdout = stdoutWriter

	stderr, stderrWriter, err := openPipe()
	if err != nil {
		_ = stdinReader.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		closeErr := launch.close()

		return nil, errors.Join(fmt.Errorf("create amp stderr: %w", err), closeErr)
	}

	cmd.Stderr = stderrWriter
	cmd.WaitDelay = defaultCloseKillAfter

	if contextErr := ctx.Err(); contextErr != nil {
		_ = stdinReader.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		_ = stderr.Close()
		_ = stderrWriter.Close()

		return nil, errors.Join(contextErr, launch.close())
	}

	tree, err := startProcessTree(launch)
	if err != nil {
		_ = stdinReader.Close()
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		_ = stderr.Close()
		_ = stderrWriter.Close()

		return nil, fmt.Errorf("start amp: %w", err)
	}

	_ = stdinReader.Close()
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()

	processObserver := c.newProcessSnapshotObserver(ctx, tree)
	observeProcessTreeSnapshot(ctx, processObserver)

	turn := &Turn{
		log:             c.log,
		cmd:             cmd,
		tree:            tree,
		processObserver: processObserver,
		stdin:           stdin,
		stdout:          stdout,
		stderr:          stderr,
		maxLineBytes:    c.options.MaxLineBytes,
		messages:        make(chan Message),
		errs:            make(chan error, 4),
		onPanic:         c.options.OnGoroutinePanic,
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

	if err := closeWriteCloser(stdin); err != nil {
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

	cwd := c.commandCwd()

	environment, err := c.buildEnvironment(c.options.Env, cwd)
	if err != nil {
		return nil, err
	}

	cmd := commandContext(context.Background(), path, args...)
	cmd.Dir = cwd
	cmd.Env = environment

	launch, err := c.prepareProcessLaunch(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("amp %s: %w", strings.Join(args, " "), err)
	}

	cmd = launch.cmd
	if cmd.Stdin != nil {
		return nil, errors.Join(errors.New("create amp stdin: exec: Stdin already set"), launch.close())
	}

	if cmd.Stdout != nil {
		return nil, errors.Join(errors.New("create amp stdout: exec: Stdout already set"), launch.close())
	}

	if cmd.Stderr != nil {
		return nil, errors.Join(errors.New("create amp stderr: exec: Stderr already set"), launch.close())
	}

	var stdout, stderr bytes.Buffer

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// A provider descendant may inherit stdout/stderr after the native root
	// exits. Bound exec's copy-goroutine wait so the selected containment
	// boundary can settle instead of deadlocking in Cmd.Wait.
	cmd.WaitDelay = defaultCloseKillAfter

	if contextErr := ctx.Err(); contextErr != nil {
		return nil, errors.Join(contextErr, launch.close())
	}

	tree, err := startProcessTree(launch)
	if err != nil {
		return nil, fmt.Errorf("amp %s: %w", strings.Join(args, " "), err)
	}

	processObserver := c.newProcessSnapshotObserver(ctx, tree)
	observeProcessTreeSnapshot(ctx, processObserver)

	waiter := tree.commandWait()
	waitErr, completed := waiter.await(ctx)

	var cancellationErr error

	if !completed {
		cancellationErr = errors.Join(waitErr, tree.kill())
		waitErr = nil
	}

	observeProcessTreeSnapshot(ctx, processObserver)

	containmentErr := processTreeTerminateAndWait(tree, defaultCloseWait)
	if ProcessContainmentComplete(containmentErr) && !completed {
		waitCtx, cancelWait := context.WithTimeout(context.Background(), commandWaitTimeout)
		waitErr, completed = waiter.await(waitCtx)

		cancelWait()

		if !completed {
			containmentErr = errors.Join(
				containmentErr,
				fmt.Errorf("%w: wait for contained Amp command: %v", ErrProcessContainmentIncomplete, waitErr),
			)
			waitErr = nil
		}
	}

	finishProcessTreeObservation(ctx, processObserver, containmentErr)

	waitErr = normalizeWaitDelay(waitErr, containmentErr)

	if cancellationErr != nil || waitErr != nil || containmentErr != nil {
		var msg string
		if completed {
			msg = strings.TrimSpace(stripANSI(stderr.String()))
		}

		if msg == "" {
			msg = errors.Join(cancellationErr, waitErr, containmentErr).Error()
		}

		return nil, errors.Join(
			fmt.Errorf("amp %s: %s", strings.Join(args, " "), msg),
			cancellationErr,
			containmentErr,
		)
	}

	return stdout.Bytes(), nil
}

func (c *Client) prepareProcessLaunch(ctx context.Context, cmd *exec.Cmd) (*processTreeCommand, error) {
	if c.options.Isolation != nil {
		if err := validateProcessIsolation(c.options.Isolation); err != nil {
			return nil, fmt.Errorf("validate Amp process isolation: %w", err)
		}
	}

	if c.options.DarwinBestEffort && c.options.Isolation != nil {
		return nil, errors.New("darwin best-effort containment cannot be combined with process isolation")
	}

	var generation *DarwinGeneration

	if c.options.DarwinBestEffort {
		if c.options.NewDarwinGeneration == nil {
			return nil, fmt.Errorf("%w: Darwin generation factory is unavailable", ErrProcessContainmentIncomplete)
		}

		var err error

		generation, err = c.options.NewDarwinGeneration(ctx)
		if err != nil {
			return nil, err
		}

		if err := generation.prepareCommand(cmd, c.options.WritableRoot); err != nil {
			finishErr := generation.finish(true)

			return nil, errors.Join(err, finishErr)
		}
	}

	launch, err := prepareProcessTree(cmd, processLaunchOptions{
		DarwinBestEffort: c.options.DarwinBestEffort,
		Generation:       generation,
		Isolation:        c.options.Isolation,
	})
	if err != nil {
		finishErr := generation.finish(true)

		return nil, errors.Join(err, finishErr)
	}

	if c.options.Isolation != nil && !launch.nativeIsolation {
		if err := applyProcessIsolation(launch.cmd, c.options.Isolation); err != nil {
			closeErr := launch.close()

			return nil, errors.Join(fmt.Errorf("apply Amp process isolation: %w", err), closeErr)
		}
	}

	launch.nativeEnv = append([]string(nil), cmd.Env...)
	launch.onStartCancel = func(cancel func()) func() bool { return context.AfterFunc(ctx, cancel) }
	launch.startError = ctx.Err
	launch.bestEffort = c.options.DarwinBestEffort
	launch.generation = generation
	launch.acquireNative = func() (func(), error) {
		if c.options.AcquireNativeRoot == nil {
			return func() {}, nil
		}

		release, acquireErr := c.options.AcquireNativeRoot(ctx)
		if acquireErr != nil {
			return nil, acquireErr
		}

		if release == nil {
			return nil, errors.New("native root hook returned nil release")
		}

		return release, nil
	}

	return launch, nil
}

func normalizeWaitDelay(waitErr error, containmentErr error) error {
	if errors.Is(waitErr, exec.ErrWaitDelay) && containmentErr == nil {
		return nil
	}

	return waitErr
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
		if !filepath.IsAbs(retained) {
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
	if c.options.Isolation != nil {
		return Discover(ctx, c.options.CLIPath, environment)
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

// BuildEnvWithIsolation constructs the complete child environment from the
// policy base and explicit overlays. Ambient os.Environ is never consulted.
func BuildEnvWithIsolation(isolation *ProcessIsolation, overrides map[string]string, cwd string) ([]string, error) {
	return buildIsolatedEnvironment(isolation, cwd, overrides)
}

func buildIsolatedEnvironment(isolation *ProcessIsolation, cwd string, phases ...map[string]string) ([]string, error) {
	if err := validateProcessIsolation(isolation); err != nil {
		return nil, err
	}

	return buildEnvironment(cwd, append([]map[string]string{isolation.BaseEnvironment}, phases...)...)
}

func (c *Client) buildEnvironment(overrides map[string]string, cwd string) ([]string, error) {
	if c.options.Isolation != nil {
		return buildIsolatedEnvironment(c.options.Isolation, cwd, overrides)
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
	keys := make([]string, 0, capacity)

	set := func(key, value string) {
		key = launchEnvironmentKey(key)
		if isPrivateAdapterEnv(key) || isScrubbedEnv(key) {
			return
		}

		if _, ok := values[key]; !ok {
			keys = append(keys, key)
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
	if strings.HasPrefix(strings.ToUpper(key), adapterPrivateEnvPrefix) {
		return true
	}

	switch key {
	case adapterSupervisorModeEnv,
		adapterOneShotDeathPhaseEnv,
		adapterOneShotDeathPathEnv,
		adapterOneShotDeathStateEnv:
		return true
	default:
		return false
	}
}

func withoutPrivateAdapterEnv(entries []string) []string {
	env := make([]string, 0, len(entries))

	for _, entry := range entries {
		key, _, ok := strings.Cut(entry, "=")
		if ok && isPrivateAdapterEnv(key) {
			continue
		}

		env = append(env, entry)
	}

	return env
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
	log             *slog.Logger
	cmd             *exec.Cmd
	tree            *processTree
	stdin           io.WriteCloser
	stdout          io.ReadCloser
	stderr          io.ReadCloser
	maxLineBytes    int
	messages        chan Message
	errs            chan error
	stderrMu        sync.Mutex
	stderrTail      bytes.Buffer
	stderrDone      chan struct{}
	waitOnce        sync.Once
	waitState       *commandWait
	waitFunc        func() error
	closeOnce       sync.Once
	onPanic         func(ctx context.Context, name string, recovered any)
	processObserver ProcessSnapshotObserver
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
	go t.drainStderr(ctx)
	go t.readStdout(ctx)
}

func (t *Turn) readStdout(ctx context.Context) {
	defer t.recoverGoroutine(ctx, "amp stdout reader")
	defer close(t.messages)
	defer close(t.errs)
	defer func() {
		if err := t.wait(); err != nil {
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

func (t *Turn) Interrupt(ctx context.Context, killAfter time.Duration) error {
	if t.cmd == nil || t.cmd.Process == nil {
		return nil
	}

	interruptErr := t.interruptProcess()
	waiter := t.commandWait()

	if killAfter <= 0 {
		killErr := t.killProcess()
		containmentErr := processTreeTerminateAndWait(t.tree, defaultCloseWait)

		var waitErr error

		if ProcessContainmentComplete(containmentErr) {
			completedErr, _ := waiter.await(ctx)
			waitErr = interruptWaitResult(completedErr)
		}

		return errors.Join(interruptErr, killErr, waitErr, containmentErr)
	}

	timer := time.NewTimer(killAfter)
	defer timer.Stop()

	var waitErr error

	select {
	case <-waiter.done:
		waitErr = interruptWaitResult(waiter.err)
	case <-timer.C:
		waitErr = t.killProcess()
	case <-ctx.Done():
		waitErr = ctx.Err()
	}

	// Direct-child exit does not complete the selected containment boundary.
	// Drive the platform cleanup once before the cancel control path returns;
	// Turn.Close joins the same memoized result.
	containmentErr := processTreeTerminateAndWait(t.tree, defaultCloseWait)
	if ProcessContainmentComplete(containmentErr) {
		completedErr, _ := waiter.await(ctx)
		waitErr = errors.Join(waitErr, interruptWaitResult(completedErr))
	}

	return errors.Join(interruptErr, waitErr, containmentErr)
}

func interruptWaitResult(err error) error {
	if expectedExit(err) {
		return nil
	}

	return err
}

func (t *Turn) Close() error {
	var err error

	t.closeOnce.Do(func() {
		var boundaryErr error

		if t.cmd != nil && t.cmd.Process != nil {
			ctx, cancel := context.WithTimeout(context.Background(), defaultCloseKillAfter+defaultCloseWait)
			boundaryErr = t.Interrupt(ctx, defaultCloseKillAfter)

			cancel()

			err = errors.Join(err, boundaryErr)
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

		if ProcessContainmentComplete(boundaryErr) {
			waitCtx, cancelWait := context.WithTimeout(context.Background(), commandWaitTimeout)
			waitErr, completed := t.waitWithin(waitCtx)

			cancelWait()

			if !completed {
				waitErr = fmt.Errorf("%w: wait for Amp command close: %v", ErrProcessContainmentIncomplete, waitErr)
			}

			err = errors.Join(err, waitErr)
		}

		if t.tree != nil && ProcessContainmentComplete(err) {
			observeProcessTreeSnapshot(context.Background(), t.processObserver)
			containmentErr := processTreeTerminateAndWait(t.tree, defaultCloseWait)
			err = errors.Join(err, containmentErr)
		}

		finishProcessTreeObservation(context.Background(), t.processObserver, err)
	})

	return err
}

func (t *Turn) interruptProcess() error {
	if t.tree != nil {
		return t.tree.interrupt()
	}

	return interruptProcess(t.cmd)
}

func (t *Turn) killProcess() error {
	if t.tree != nil {
		return t.tree.kill()
	}

	return killProcess(t.cmd)
}

func (t *Turn) wait() error {
	waiter := t.commandWait()
	<-waiter.done

	return waiter.err
}

func (t *Turn) waitWithin(ctx context.Context) (error, bool) {
	return t.commandWait().await(ctx)
}

func (t *Turn) commandWait() *commandWait {
	t.waitOnce.Do(func() {
		if t.tree != nil {
			t.waitState = t.tree.commandWait()

			return
		}

		wait := t.waitFunc
		if wait == nil && t.cmd != nil {
			wait = t.cmd.Wait
		}

		t.waitState = newCommandWait(wait)
	})

	return t.waitState
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

func expectedExit(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return true
	}

	var exitErr *exec.ExitError

	return errors.As(err, &exitErr)
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
