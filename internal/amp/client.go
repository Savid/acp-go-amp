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
	MinimumVersion         = "0.0.1783155105"
	maxCapturedStderrBytes = 64 * 1024
	defaultCloseKillAfter  = 100 * time.Millisecond
	defaultCloseWait       = 5 * time.Second
	ampArgThreads          = "threads"
	ampThreadContinue      = "continue"
	ampThreadDelete        = "delete"
	ampThreadExport        = "export"
	ampArgNoIDE            = "--no-ide"
	ampArgNoColor          = "--no-color"
	ampArgNoNotifications  = "--no-notifications"
)

var (
	commandContext             = exec.CommandContext
	getwd                      = os.Getwd
	lookPath                   = exec.LookPath
	mkdirTemp                  = os.MkdirTemp
	removeAll                  = os.RemoveAll
	writeFile                  = os.WriteFile
	closeWriteCloser           = func(closer io.Closer) error { return closer.Close() }
	probeCache                 sync.Map
	processTreeDescendantCount = func(tree *processTree) (int, bool) {
		return tree.descendantCount()
	}
	processTreeTerminateAndWait = func(tree *processTree, timeout time.Duration) error {
		return tree.terminateAndWait(timeout)
	}
	prepareProcessTree = prepareProcessTreeCommand
)

type Options struct {
	CLIPath      string
	Cwd          string
	SettingsFile string
	// ScratchParent is the already-resolved parent directory the root package
	// supplies for the startup probe's ephemeral settings directory. The root
	// package owns temp-directory resolution; this package never consults the
	// system temp directory itself.
	ScratchParent string
	Env           map[string]string
	ThreadID      string
	Mode          string
	MCPConfigPath string
	MaxLineBytes  int
	// OnGoroutinePanic is invoked with the recovered value when a turn-owned
	// goroutine panics, so the embedding agent can log the panic instead of
	// crashing the process. A nil handler leaves the panic to propagate.
	OnGoroutinePanic func(ctx context.Context, name string, recovered any)
	// NewProcessSnapshotObserver registers one successfully started contained
	// native root with the embedding agent's absolute descendant inventory.
	NewProcessSnapshotObserver func(context.Context, ProcessInventory) ProcessSnapshotObserver
}

// ProcessInventory queries the current absolute inventory of one containment
// boundary. False means the platform cannot prove the count at this boundary.
type ProcessInventory func() (count int, available bool)

// ProcessSnapshotObserver reports only containment-proven process inventory.
// Observe is optional on platforms without an absolute live inventory;
// Quiescent is called only after containment proves the root empty.
type ProcessSnapshotObserver struct {
	Refresh   func(context.Context)
	Quiescent func(context.Context)
	Unproven  func()
}

type Client struct {
	log     *slog.Logger
	options Options
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

func finishProcessTreeObservation(ctx context.Context, observer ProcessSnapshotObserver, quiescenceErr error) {
	if ProcessTreeQuiescent(quiescenceErr) {
		if observer.Quiescent != nil {
			observer.Quiescent(ctx)
		}

		return
	}

	if observer.Unproven != nil {
		observer.Unproven()
	}
}

func NewClient(log *slog.Logger, options Options) *Client {
	if log == nil {
		log = slog.Default()
	}

	if options.MaxLineBytes <= 0 {
		options.MaxLineBytes = defaultMaxJSONLineBytes
	}

	return &Client{log: log, options: options}
}

func (c *Client) Version(ctx context.Context) (string, error) {
	out, err := c.outputRaw(ctx, "version")
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

func (c *Client) StartupProbe(ctx context.Context) error {
	path, err := Discover(ctx, c.options.CLIPath)
	if err != nil {
		return err
	}

	version, err := c.Version(ctx)
	if err != nil {
		return err
	}

	if !versionAtLeast(version, MinimumVersion) {
		return fmt.Errorf("amp version %q is below required %s", version, MinimumVersion)
	}

	cacheKey := path + "\x00" + version
	if _, ok := probeCache.Load(cacheKey); ok {
		return nil
	}

	if err := c.probeSubcommands(ctx); err != nil {
		return err
	}

	probeCache.Store(cacheKey, struct{}{})

	return nil
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

	settingsFile, cleanup, err := startupProbeSettingsFile(c.options.ScratchParent)
	if err != nil {
		return err
	}
	defer cleanup()

	continueClient := *c
	continueClient.options.SettingsFile = settingsFile

	continueClient.options.MCPConfigPath = filepath.Join(filepath.Dir(settingsFile), "mcp.json")
	if err := writeFile(continueClient.options.MCPConfigPath, []byte("{}\n"), 0o600); err != nil {
		return fmt.Errorf("write amp startup MCP config: %w", err)
	}

	continueClient.options.Mode = "medium"
	continueArgs := []string{ampArgThreads, ampThreadContinue, startupProbeThreadID, "--stream-json", "--stream-json-input", "-x"}

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

func startupProbeSettingsFile(parent string) (string, func(), error) {
	dir, err := mkdirTemp(parent, "acp-go-amp-startup-*")
	if err != nil {
		return "", nil, fmt.Errorf("create amp startup settings dir: %w", err)
	}

	cleanup := func() { _ = removeAll(dir) }

	settingsFile := filepath.Join(dir, "settings.json")
	if err := writeFile(settingsFile, []byte("{}\n"), 0o600); err != nil {
		cleanup()

		return "", nil, fmt.Errorf("write amp startup settings file: %w", err)
	}

	return settingsFile, cleanup, nil
}

// methodProbeError classifies a method-present probe result: a domain
// missing-thread error means the subcommand exists (probe passes, nil); any
// other error means the subcommand is missing or broken (probe fails).
func methodProbeError(name string, err error, requireMissingThread bool) error {
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

func (c *Client) NewThread(ctx context.Context) (string, error) {
	out, err := c.output(ctx, ampArgThreads, "new")
	if err != nil {
		return "", err
	}

	threadID := strings.TrimSpace(stripANSI(string(out)))
	if err := ValidateThreadID(threadID); err != nil {
		return "", fmt.Errorf("amp threads new returned invalid id: %w", err)
	}

	return threadID, nil
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
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		return nil
	}

	return err
}

func (c *Client) Continue(ctx context.Context, threadID string, input any) (*Turn, error) {
	path, err := Discover(ctx, c.options.CLIPath)
	if err != nil {
		return nil, err
	}

	args := c.globalArgs()
	args = append(args, ampArgThreads, ampThreadContinue, threadID, "--stream-json", "--stream-json-input", "-x")

	cmd := commandContext(context.Background(), path, args...)

	cmd.Dir = c.options.Cwd
	if cmd.Dir == "" {
		cmd.Dir, err = getwd()
		if err != nil {
			return nil, fmt.Errorf("get working directory: %w", err)
		}
	}

	cmd.Env = BuildEnv(c.options.Env, cmd.Dir)
	if cmd.Stdin != nil {
		return nil, errors.New("create amp stdin: exec: Stdin already set")
	}

	if cmd.Stdout != nil {
		return nil, errors.New("create amp stdout: exec: Stdout already set")
	}

	if cmd.Stderr != nil {
		return nil, errors.New("create amp stderr: exec: Stderr already set")
	}

	launch, err := prepareProcessTree(cmd)
	if err != nil {
		return nil, err
	}

	cmd = launch.cmd

	stdin, err := cmd.StdinPipe()
	if err != nil {
		launch.close()

		return nil, fmt.Errorf("create amp stdin: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		launch.close()

		return nil, fmt.Errorf("create amp stdout: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		launch.close()

		return nil, fmt.Errorf("create amp stderr: %w", err)
	}

	tree, err := startProcessTree(launch)
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()

		return nil, fmt.Errorf("start amp: %w", err)
	}

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

	if err := turn.Send(ctx, input); err != nil {
		_ = turn.Close()

		return nil, err
	}

	if err := closeWriteCloser(stdin); err != nil {
		_ = turn.Close()

		return nil, fmt.Errorf("close amp stdin: %w", err)
	}

	return turn, nil
}

func (c *Client) output(ctx context.Context, args ...string) ([]byte, error) {
	return c.outputWithArgs(ctx, append(c.globalArgs(), args...)...)
}

func (c *Client) outputRaw(ctx context.Context, args ...string) ([]byte, error) {
	return c.outputWithArgs(ctx, args...)
}

func (c *Client) outputWithArgs(ctx context.Context, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	path, err := Discover(ctx, c.options.CLIPath)
	if err != nil {
		return nil, err
	}

	cmd := commandContext(context.Background(), path, args...)

	cmd.Dir = c.options.Cwd
	if cmd.Dir == "" {
		cmd.Dir, _ = getwd()
	}

	cmd.Env = BuildEnv(c.options.Env, cmd.Dir)
	configureCommand(cmd)
	launch := &processTreeCommand{cmd: cmd}

	var stdout, stderr bytes.Buffer

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// A provider descendant may inherit stdout/stderr after the native root
	// exits. Bound exec's copy-goroutine wait so containment cleanup can kill the
	// descendant and prove the group empty instead of deadlocking in Cmd.Wait.
	cmd.WaitDelay = defaultCloseKillAfter

	tree, err := startProcessTree(launch)
	if err != nil {
		return nil, fmt.Errorf("amp %s: %w", strings.Join(args, " "), err)
	}

	processObserver := c.newProcessSnapshotObserver(ctx, tree)
	observeProcessTreeSnapshot(ctx, processObserver)

	cancellationDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		defer close(cancellationDone)

		_ = tree.kill()
	})
	waitErr := cmd.Wait()

	if stopCancellation() {
		close(cancellationDone)
	}

	<-cancellationDone

	observeProcessTreeSnapshot(ctx, processObserver)

	quiescenceErr := processTreeTerminateAndWait(tree, defaultCloseWait)
	finishProcessTreeObservation(ctx, processObserver, quiescenceErr)

	waitErr = normalizeWaitDelay(waitErr, quiescenceErr)

	if waitErr != nil || quiescenceErr != nil {
		msg := strings.TrimSpace(stripANSI(stderr.String()))
		if msg == "" {
			msg = errors.Join(waitErr, quiescenceErr).Error()
		}

		return nil, errors.Join(
			fmt.Errorf("amp %s: %s", strings.Join(args, " "), msg),
			quiescenceErr,
		)
	}

	return stdout.Bytes(), nil
}

func normalizeWaitDelay(waitErr error, quiescenceErr error) error {
	if errors.Is(waitErr, exec.ErrWaitDelay) && quiescenceErr == nil {
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

func Discover(ctx context.Context, cliPath string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if strings.TrimSpace(cliPath) != "" {
		return cliPath, nil
	}

	path, err := lookPath("amp")
	if err != nil {
		return "", fmt.Errorf("find amp in PATH: %w", err)
	}

	return path, nil
}

// HasAPIKey reports whether the amp CLI would see a non-empty AMP_API_KEY
// under BuildEnv semantics: an explicit override wins even when empty,
// otherwise the live process environment supplies the value.
func HasAPIKey(overrides map[string]string) bool {
	value, ok := overrides["AMP_API_KEY"]
	if !ok {
		value = os.Getenv("AMP_API_KEY")
	}

	return strings.TrimSpace(value) != ""
}

func BuildEnv(overrides map[string]string, cwd string) []string {
	values := map[string]string{}
	keys := make([]string, 0, len(os.Environ())+len(overrides)+1)

	set := func(key, value string) {
		if key == "" {
			return
		}

		if _, ok := values[key]; !ok {
			keys = append(keys, key)
		}

		values[key] = value
	}
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			set(key, value)
		}
	}

	overrideKeys := make([]string, 0, len(overrides))
	for key := range overrides {
		overrideKeys = append(overrideKeys, key)
	}

	sort.Strings(overrideKeys)

	for _, key := range overrideKeys {
		set(key, overrides[key])
	}

	if cwd != "" {
		set("PWD", cwd)
	}

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+values[key])
	}

	return out
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
	waitOnce        sync.Once
	waitErr         error
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
	go t.drainStderr(ctx)
	go t.readStdout(ctx)
}

func (t *Turn) readStdout(ctx context.Context) {
	defer t.recoverGoroutine(ctx, "amp stdout reader")
	defer close(t.messages)
	defer close(t.errs)
	defer func() {
		if err := t.wait(); err != nil {
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

	if killAfter <= 0 {
		return interruptErr
	}

	done := make(chan error, 1)

	go func() {
		defer t.recoverGoroutine(ctx, "amp interrupt waiter")

		done <- t.wait()
	}()

	timer := time.NewTimer(killAfter)
	defer timer.Stop()

	var waitErr error

	select {
	case err := <-done:
		waitErr = interruptWaitResult(err)
	case <-timer.C:
		killErr := t.killProcess()

		select {
		case err := <-done:
			waitErr = errors.Join(killErr, interruptWaitResult(err))
		case <-ctx.Done():
			waitErr = errors.Join(killErr, ctx.Err())
		}
	case <-ctx.Done():
		waitErr = ctx.Err()
	}

	// Waiting for the native/supervisor root is not descendant proof. A Linux
	// tool may leave the original process group with setsid(2), and Windows Job
	// Object accounting can lag root exit. Always drive the platform containment
	// boundary to zero before the cancel control path returns. Turn.Close repeats
	// this idempotently so prompt finalization retains the same proof guarantee.
	quiescenceErr := processTreeTerminateAndWait(t.tree, defaultCloseWait)

	return errors.Join(interruptErr, waitErr, quiescenceErr)
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
		if t.cmd != nil && t.cmd.Process != nil {
			ctx, cancel := context.WithTimeout(context.Background(), defaultCloseKillAfter+defaultCloseWait)
			defer cancel()

			err = errors.Join(err, t.Interrupt(ctx, defaultCloseKillAfter))
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

		err = errors.Join(err, t.wait())
		if t.tree != nil {
			observeProcessTreeSnapshot(context.Background(), t.processObserver)
			quiescenceErr := processTreeTerminateAndWait(t.tree, defaultCloseWait)
			finishProcessTreeObservation(context.Background(), t.processObserver, quiescenceErr)
			err = errors.Join(err, quiescenceErr)
		}
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
	t.waitOnce.Do(func() {
		if t.waitFunc != nil {
			t.waitErr = t.waitFunc()
		} else if t.cmd != nil {
			t.waitErr = t.cmd.Wait()
		}
	})

	return t.waitErr
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
