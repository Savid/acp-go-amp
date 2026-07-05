//nolint:wsl_v5,nlreturn // process transport keeps shutdown branches compact.
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MinimumVersion         = "0.0.1783155105"
	maxCapturedStderrBytes = 64 * 1024
	ampArgThreads          = "threads"
	ampThreadContinue      = "continue"
	ampThreadDelete        = "delete"
	ampThreadExport        = "export"
)

var (
	commandContext   = exec.CommandContext
	getwd            = os.Getwd
	lookPath         = exec.LookPath
	closeWriteCloser = func(closer io.Closer) error { return closer.Close() }
	probeCache       sync.Map
)

type Options struct {
	CLIPath       string
	Cwd           string
	SettingsFile  string
	Env           map[string]string
	ThreadID      string
	Mode          string
	Effort        string
	MCPConfigJSON string
	MaxLineBytes  int
}

type Client struct {
	log     *slog.Logger
	options Options
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
// probe relies on cmd.Output leaving stdin as /dev/null, so amp sees immediate
// EOF and rejects the missing thread without spending tokens.
func (c *Client) probeSubcommands(ctx context.Context) error {
	probeCtx, cancel := context.WithTimeout(ctx, startupProbeTimeout)
	defer cancel()

	if _, err := c.ListThreads(probeCtx); err != nil {
		return fmt.Errorf("amp threads list --json probe failed: %w", err)
	}
	probes := []struct {
		name string
		args []string
	}{
		{name: "threads export", args: []string{ampArgThreads, ampThreadExport, startupProbeThreadID}},
		{name: "threads continue", args: []string{ampArgThreads, ampThreadContinue, startupProbeThreadID, "--stream-json", "--stream-json-input", "-x"}},
		{name: "threads delete", args: []string{ampArgThreads, ampThreadDelete, startupProbeThreadID}},
	}
	for _, probe := range probes {
		if _, err := c.output(probeCtx, probe.args...); err != nil {
			if methodErr := methodProbeError(probe.name, err); methodErr != nil {
				return methodErr
			}
		}
	}
	return nil
}

// methodProbeError classifies a method-present probe result: a domain
// missing-thread error means the subcommand exists (probe passes, nil); any
// other error means the subcommand is missing or broken (probe fails).
func methodProbeError(name string, err error) error {
	if err == nil || isMissingThreadMessage(err.Error()) {
		return nil
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
	if !strings.HasPrefix(threadID, "T-") {
		return "", fmt.Errorf("amp threads new returned unexpected id %q", threadID)
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

	cmd := commandContext(ctx, path, args...)
	configureCommand(cmd)
	cmd.Dir = c.options.Cwd
	if cmd.Dir == "" {
		cmd.Dir, err = getwd()
		if err != nil {
			return nil, fmt.Errorf("get working directory: %w", err)
		}
	}
	cmd.Env = BuildEnv(c.options.Env, cmd.Dir)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create amp stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create amp stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("create amp stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start amp: %w", err)
	}

	turn := &Turn{
		log:          c.log,
		cmd:          cmd,
		stdin:        stdin,
		stdout:       stdout,
		stderr:       stderr,
		maxLineBytes: c.options.MaxLineBytes,
		messages:     make(chan Message),
		errs:         make(chan error, 4),
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
	path, err := Discover(ctx, c.options.CLIPath)
	if err != nil {
		return nil, err
	}
	cmd := commandContext(ctx, path, args...)
	configureCommand(cmd)
	cmd.Dir = c.options.Cwd
	if cmd.Dir == "" {
		cmd.Dir, _ = getwd()
	}
	cmd.Env = BuildEnv(c.options.Env, cmd.Dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stripANSI(stderr.String()))
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("amp %s: %s", strings.Join(args, " "), msg)
	}
	return out, nil
}

func (c *Client) globalArgs() []string {
	args := []string{"--no-ide", "--no-color", "--no-notifications"}
	if c.options.SettingsFile != "" {
		args = append(args, "--settings-file", c.options.SettingsFile)
	}
	if c.options.MCPConfigJSON != "" {
		args = append(args, "--mcp-config", c.options.MCPConfigJSON)
	}
	if c.options.Mode != "" {
		args = append(args, "-m", c.options.Mode)
	}
	if c.options.Effort != "" {
		args = append(args, "--effort", c.options.Effort)
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
	log          *slog.Logger
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	stderr       io.ReadCloser
	maxLineBytes int
	messages     chan Message
	errs         chan error
	stderrMu     sync.Mutex
	stderrTail   bytes.Buffer
	waitOnce     sync.Once
	waitErr      error
	waitFunc     func() error
	closeOnce    sync.Once
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
	if _, err := t.stdin.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write amp stdin: %w", err)
	}
	return nil
}

func (t *Turn) start(ctx context.Context) {
	go t.drainStderr()
	go t.readStdout(ctx)
}

func (t *Turn) readStdout(ctx context.Context) {
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

func (t *Turn) drainStderr() {
	scanner := bufio.NewScanner(t.stderr)
	for scanner.Scan() {
		t.captureStderr(scanner.Text())
		if t.log != nil {
			t.log.Debug("amp stderr", slog.String("line", scanner.Text()))
		}
	}
}

func (t *Turn) Interrupt(ctx context.Context, killAfter time.Duration) error {
	if t.cmd == nil || t.cmd.Process == nil {
		return nil
	}
	if err := interruptProcess(t.cmd); err != nil {
		return err
	}
	if killAfter <= 0 {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- t.wait() }()
	timer := time.NewTimer(killAfter)
	defer timer.Stop()
	select {
	case err := <-done:
		if expectedExit(err) {
			return nil
		}
		return err
	case <-timer.C:
		return killProcess(t.cmd)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Turn) Close() error {
	var err error
	t.closeOnce.Do(func() {
		if t.stdin != nil {
			err = errors.Join(err, t.stdin.Close())
		}
		if t.stdout != nil {
			err = errors.Join(err, t.stdout.Close())
		}
		if t.stderr != nil {
			err = errors.Join(err, t.stderr.Close())
		}
		err = errors.Join(err, killProcess(t.cmd), t.wait())
	})
	return err
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
