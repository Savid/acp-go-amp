//nolint:gocyclo,nlreturn // Fake executable harness keeps process cases in one place.
package amp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestFakeAmpHelper(t *testing.T) {
	if os.Getenv("GO_WANT_FAKE_AMP") != "1" {
		return
	}
	args := helperArgs()
	state := os.Getenv("FAKE_AMP_STATE")
	mode := os.Getenv("FAKE_AMP_MODE")
	recordHelperJSON(state, "args.jsonl", args)

	if slices.Contains(args, "version") {
		if mode == "bad-version" {
			os.Stdout.WriteString("0.0.1\n")
			os.Exit(0)
		}
		os.Stdout.WriteString("0.0.1783155105-gfake\n")
		os.Exit(0)
	}
	if len(args) > 0 && args[len(args)-1] == "--help" {
		if mode == "help-fail" {
			os.Stderr.WriteString("help failed\n")
			os.Exit(1)
		}
		if mode == "bad-help" {
			os.Stdout.WriteString("threads only\n")
			os.Exit(0)
		}
		os.Stdout.WriteString("--settings-file --mcp-config -m --json --stream-json-input threads continue threads export threads delete\n")
		os.Exit(0)
	}
	threads := slices.Index(args, "threads")
	if threads < 0 || threads+1 >= len(args) {
		os.Stderr.WriteString("missing threads subcommand\n")
		os.Exit(2)
	}
	if mode == "probe-export-absent" && args[threads+1] == "export" {
		os.Stderr.WriteString("error: unknown command 'export'\n")
		os.Exit(1)
	}
	if slices.Contains(args, startupProbeThreadID) {
		if mode != "probe-continue-success" || args[threads+1] != "continue" {
			os.Stderr.WriteString("Thread not found\n")
			os.Exit(1)
		}
	}

	switch args[threads+1] {
	case "new":
		if mode == "bad-new-id" {
			os.Stdout.WriteString("not-a-thread\n")
			os.Exit(0)
		}
		os.Stdout.WriteString("\x1b[32mT-fake-thread\x1b[0m\n")
	case "list":
		if mode == "bad-list-json" {
			os.Stdout.WriteString("{")
			os.Exit(0)
		}
		os.Stdout.WriteString(`[{"id":"T-fake-thread","title":"Fake","updated":"now","tree":"file:///tmp/project","messageCount":2}]` + "\n")
	case "export":
		if mode == "export-fail" {
			os.Stderr.WriteString("export failed\n")
			os.Exit(1)
		}
		if mode == "probe-export-absent" {
			os.Stderr.WriteString("error: unknown command 'export'\n")
			os.Exit(1)
		}
		if mode == "probe-export-missing" && args[len(args)-1] == "T-00000000-0000-0000-0000-000000000000" {
			os.Stderr.WriteString("Thread not found\n")
			os.Exit(1)
		}
		if mode == "bad-export-json" {
			os.Stdout.WriteString("{")
			os.Exit(0)
		}
		os.Stdout.WriteString(`{"thread":"` + args[len(args)-1] + `"}` + "\n")
	case "delete":
		if mode == "delete-fail" {
			os.Stderr.WriteString("delete failed\n")
			os.Exit(1)
		}
		if args[len(args)-1] == "T-missing" {
			os.Stderr.WriteString("Thread does not exist\n")
			os.Exit(1)
		}
		os.Stdout.WriteString("deleted\n")
	case "continue":
		if mode == "probe-continue-success" {
			os.Exit(0)
		}
		stdin, _ := io.ReadAll(os.Stdin)
		recordHelperJSON(state, "stdin.jsonl", strings.TrimSpace(string(stdin)))
		helperContinue(mode, state)
	default:
		os.Stderr.WriteString("unknown threads subcommand\n")
		os.Exit(2)
	}
	os.Exit(0)
}

func TestClientCommandsUseGlobalArgsAndParseOutput(t *testing.T) {
	path, state := fakeAmpPath(t, "")
	client := NewClient(nil, Options{
		CLIPath:       path,
		Cwd:           t.TempDir(),
		SettingsFile:  filepath.Join(t.TempDir(), "settings.json"),
		Env:           map[string]string{"AMP_API_KEY": "fake"},
		Mode:          "strange-mode",
		MCPConfigPath: filepath.Join(t.TempDir(), "mcp.json"),
	})
	ctx := context.Background()

	if version, err := client.Version(ctx); err != nil || version != "0.0.1783155105-gfake" {
		t.Fatalf("Version = %q, %v", version, err)
	}
	if err := client.StartupProbe(ctx); err != nil {
		t.Fatalf("StartupProbe: %v", err)
	}
	records := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	var startupContinue []string
	for _, args := range records {
		if slices.Contains(args, startupProbeThreadID) && slices.Contains(args, ampThreadContinue) {
			startupContinue = args
			break
		}
	}
	if len(startupContinue) == 0 {
		t.Fatalf("startup continue probe not recorded: %#v", records)
	}
	for _, want := range []string{"--settings-file", "--mcp-config", "-m", "medium", "--stream-json", "--stream-json-input", "-x"} {
		if !slices.Contains(startupContinue, want) {
			t.Fatalf("startup continue probe missing %q: %#v", want, startupContinue)
		}
	}
	for _, global := range []string{ampArgNoIDE, ampArgNoColor, ampArgNoNotifications, "--settings-file", "--mcp-config", "-m"} {
		if count := countArg(startupContinue, global); count != 1 {
			t.Fatalf("startup continue probe has %d copies of %q: %#v", count, global, startupContinue)
		}
	}
	if slices.Contains(startupContinue, "--effort") {
		t.Fatalf("startup continue probe used removed --effort flag: %#v", startupContinue)
	}
	if err := client.StartupProbe(ctx); err != nil {
		t.Fatalf("StartupProbe cached: %v", err)
	}
	if id, err := client.NewThread(ctx); err != nil || id != "T-fake-thread" {
		t.Fatalf("NewThread = %q, %v", id, err)
	}
	threads, err := client.ListThreads(ctx)
	if err != nil {
		t.Fatalf("ListThreads: %v", err)
	}
	if len(threads) != 1 || threads[0].ID != "T-fake-thread" || threads[0].Tree == "" {
		t.Fatalf("threads = %#v", threads)
	}
	raw, err := client.ExportThread(ctx, "T-fake-thread")
	if err != nil || string(raw) != `{"thread":"T-fake-thread"}` {
		t.Fatalf("ExportThread = %s, %v", raw, err)
	}
	if err := client.DeleteThread(ctx, "T-missing"); err != nil {
		t.Fatalf("DeleteThread missing should be idempotent: %v", err)
	}

	records = readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	if len(records) < 5 {
		t.Fatalf("recorded args = %#v", records)
	}
	last := records[len(records)-1]
	for _, want := range []string{ampArgNoIDE, ampArgNoColor, ampArgNoNotifications, "--settings-file", "--mcp-config", "-m", "strange-mode", "threads", "delete", "T-missing"} {
		if !slices.Contains(last, want) {
			t.Fatalf("last args missing %q: %#v", want, last)
		}
	}
}

func countArg(args []string, want string) int {
	count := 0
	for _, arg := range args {
		if arg == want {
			count++
		}
	}

	return count
}

func TestStartupProbeAndVersionBranches(t *testing.T) {
	ctx := context.Background()
	oldLookPath := lookPath
	lookPath = func(string) (string, error) { return "", errors.New("lookpath failed") }
	if err := NewClient(nil, Options{}).StartupProbe(ctx); err == nil {
		t.Fatal("StartupProbe discover error ignored")
	}
	lookPath = oldLookPath

	if err := NewClient(nil, Options{CLIPath: "/does/not/exist"}).StartupProbe(ctx); err == nil {
		t.Fatal("StartupProbe version command error ignored")
	}
	badVersion, _ := fakeAmpPath(t, "bad-version")
	if err := NewClient(nil, Options{CLIPath: badVersion, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "below required") {
		t.Fatalf("bad version probe = %v", err)
	}
	badList, _ := fakeAmpPath(t, "bad-list-json")
	if err := NewClient(nil, Options{CLIPath: badList, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "threads list --json probe failed") {
		t.Fatalf("list probe = %v", err)
	}
	exportAbsent, _ := fakeAmpPath(t, "probe-export-absent")
	if err := NewClient(nil, Options{CLIPath: exportAbsent, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "threads export probe failed") {
		t.Fatalf("export method-present probe = %v", err)
	}
	exportMissing, _ := fakeAmpPath(t, "probe-export-missing")
	if err := NewClient(nil, Options{CLIPath: exportMissing, Cwd: t.TempDir()}).StartupProbe(ctx); err != nil {
		t.Fatalf("export missing-thread domain error should count as present: %v", err)
	}
	continueSuccess, _ := fakeAmpPath(t, "probe-continue-success")
	if err := NewClient(nil, Options{CLIPath: continueSuccess, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "unexpectedly succeeded") {
		t.Fatalf("continue missing-thread success gate = %v", err)
	}
	if err := methodProbeError("threads continue", errors.New("usage"), true); err == nil || !strings.Contains(err.Error(), "did not return missing-thread") {
		t.Fatalf("continue missing-thread usage gate = %v", err)
	}
	func() {
		oldMkdirTemp := mkdirTemp
		defer func() { mkdirTemp = oldMkdirTemp }()
		mkdirTemp = func(string, string) (string, error) { return "", errors.New("mkdir temp failed") }
		tempFail, _ := fakeAmpPath(t, "startup-temp-fail")
		if err := NewClient(nil, Options{CLIPath: tempFail, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "create amp startup settings dir") {
			t.Fatalf("startup temp dir failure = %v", err)
		}
	}()
	func() {
		oldWriteFile := writeFile
		defer func() { writeFile = oldWriteFile }()
		writeFile = func(string, []byte, os.FileMode) error { return errors.New("write settings failed") }
		writeFail, _ := fakeAmpPath(t, "startup-write-fail")
		if err := NewClient(nil, Options{CLIPath: writeFail, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "write amp startup settings file") {
			t.Fatalf("startup settings write failure = %v", err)
		}
	}()
	func() {
		oldWriteFile := writeFile
		defer func() { writeFile = oldWriteFile }()
		writeFile = func(path string, data []byte, mode os.FileMode) error {
			if filepath.Base(path) == "mcp.json" {
				return errors.New("write MCP failed")
			}

			return oldWriteFile(path, data, mode)
		}
		writeFail, _ := fakeAmpPath(t, "startup-mcp-write-fail")
		if err := NewClient(nil, Options{CLIPath: writeFail, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "write amp startup MCP config") {
			t.Fatalf("startup MCP write failure = %v", err)
		}
	}()

	if !versionAtLeast("0.0.1783155106-gx", MinimumVersion) {
		t.Fatal("newer version rejected")
	}
	if versionAtLeast("0.0.1", MinimumVersion) {
		t.Fatal("older version accepted")
	}
	if !versionAtLeast("1", "1.0.0.0") {
		t.Fatal("short equal version rejected")
	}
	if !versionAtLeast("1.0.0.1", "1") {
		t.Fatal("longer newer version rejected")
	}
	if parts := versionParts(""); parts != nil {
		t.Fatalf("empty version parts = %#v", parts)
	}
	if parts := versionParts("not-a-version"); parts != nil {
		t.Fatalf("invalid version parts = %#v", parts)
	}
}

func TestContinueFramesMalformedLinesAndStderr(t *testing.T) {
	path, state := fakeAmpPath(t, "stream")
	client := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir(), ThreadID: "T-fake-thread", MaxLineBytes: 1024})

	turn, err := client.Continue(context.Background(), "T-fake-thread", map[string]any{"type": "user", "text": "hello"})
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	var (
		types []string
		errs  []error
	)
	for messages, errorsCh := turn.Messages(), turn.Errors(); messages != nil || errorsCh != nil; {
		select {
		case msg, ok := <-messages:
			if !ok {
				messages = nil
				continue
			}
			types = append(types, msg.AmpType())
		case err, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			errs = append(errs, err)
		}
	}
	if !slices.Equal(types, []string{TypeSystem, TypeAssistant, TypeResult}) {
		t.Fatalf("message types = %#v", types)
	}
	if len(errs) == 0 || !strings.Contains(errs[0].Error(), "decode amp json line") {
		t.Fatalf("expected malformed-line error, got %#v", errs)
	}
	stdin := readHelperJSON[string](t, filepath.Join(state, "stdin.jsonl"))
	if len(stdin) != 1 || !strings.Contains(stdin[0], `"hello"`) {
		t.Fatalf("stdin records = %#v", stdin)
	}
}

func TestContinueMissingThreadCarriesStderr(t *testing.T) {
	path, _ := fakeAmpPath(t, "missing")
	client := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir(), ThreadID: "T-deleted"})

	turn, err := client.Continue(context.Background(), "T-deleted", map[string]any{"type": "user"})
	if err != nil {
		t.Fatalf("Continue start: %v", err)
	}
	var got error
	for messages, errorsCh := turn.Messages(), turn.Errors(); messages != nil || errorsCh != nil; {
		select {
		case _, ok := <-messages:
			if !ok {
				messages = nil
			}
		case err, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			got = err
		}
	}
	if got == nil || !strings.Contains(got.Error(), "Thread not found") {
		t.Fatalf("missing-thread error = %v", got)
	}
}

func TestInterruptSIGINTAndKillFallback(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode string
	}{
		{name: "clean", mode: "sigint-clean"},
		{name: "kill", mode: "sigint-ignore"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, state := fakeAmpPath(t, tc.mode)
			client := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir(), ThreadID: "T-fake-thread"})
			turn, err := client.Continue(context.Background(), "T-fake-thread", map[string]any{"type": "user"})
			if err != nil {
				t.Fatalf("Continue: %v", err)
			}
			waitForFile(t, filepath.Join(state, "stdin.jsonl"))
			if err := turn.Interrupt(context.Background(), 100*time.Millisecond); err != nil {
				t.Fatalf("Interrupt: %v", err)
			}
			waitForFile(t, filepath.Join(state, "signal"))
		})
	}
}

func TestInterruptWaitsAfterKillFallback(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sleep process test uses POSIX process handling")
	}

	t.Run("waits for post-kill wait", func(t *testing.T) {
		cmd := exec.Command("sleep", "10")
		configureCommand(cmd)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		waitStarted := make(chan struct{})
		releaseWait := make(chan struct{})
		turn := &Turn{cmd: cmd, waitFunc: func() error {
			close(waitStarted)
			<-releaseWait
			return nil
		}}
		done := make(chan error, 1)
		go func() { done <- turn.Interrupt(context.Background(), 20*time.Millisecond) }()
		<-waitStarted
		select {
		case err := <-done:
			t.Fatalf("interrupt returned before post-kill wait completed: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		close(releaseWait)
		if err := <-done; err != nil {
			t.Fatalf("Interrupt: %v", err)
		}
		_, _ = cmd.Process.Wait()
	})

	t.Run("post-kill wait obeys context", func(t *testing.T) {
		cmd := exec.Command("sleep", "10")
		configureCommand(cmd)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		waitStarted := make(chan struct{})
		releaseWait := make(chan struct{})
		turn := &Turn{cmd: cmd, waitFunc: func() error {
			close(waitStarted)
			<-releaseWait
			return nil
		}}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		err := turn.Interrupt(ctx, 20*time.Millisecond)
		close(releaseWait)
		<-waitStarted
		_, _ = cmd.Process.Wait()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Interrupt context error = %v", err)
		}
	})
}

func TestClientErrorBranches(t *testing.T) {
	if _, err := Discover(cancelledContext(), ""); err == nil {
		t.Fatal("expected canceled Discover error")
	}
	if got, err := Discover(context.Background(), "/tmp/custom-amp"); err != nil || got != "/tmp/custom-amp" {
		t.Fatalf("explicit Discover = %q, %v", got, err)
	}
	env := BuildEnv(map[string]string{"Z": "1", "A": "2", "": "ignored"}, "/tmp/cwd")
	if !slices.Contains(env, "A=2") || !slices.Contains(env, "Z=1") || !slices.Contains(env, "PWD=/tmp/cwd") {
		t.Fatalf("env missing overrides: %#v", env)
	}

	path, _ := fakeAmpPath(t, "bad-new-id")
	client := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir()})
	if _, err := client.NewThread(context.Background()); err == nil {
		t.Fatal("expected bad thread id error")
	}
	path, _ = fakeAmpPath(t, "bad-list-json")
	if _, err := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir()}).ListThreads(context.Background()); err == nil {
		t.Fatal("expected list decode error")
	}
	path, _ = fakeAmpPath(t, "export-fail")
	if _, err := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir()}).ExportThread(context.Background(), "T-1"); err == nil {
		t.Fatal("expected export error")
	}
	path, _ = fakeAmpPath(t, "delete-fail")
	if err := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir()}).DeleteThread(context.Background(), "T-1"); err == nil {
		t.Fatal("expected delete error")
	}
	if _, err := NewClient(nil, Options{CLIPath: "/does/not/exist"}).Version(context.Background()); err == nil {
		t.Fatal("expected version discover error")
	}
	path, _ = fakeAmpPath(t, "")
	if _, err := NewClient(nil, Options{CLIPath: path, Cwd: filepath.Join(t.TempDir(), "missing")}).Continue(context.Background(), "T-1", map[string]any{"type": "user"}); err == nil {
		t.Fatal("expected continue start error")
	}
	pathDir := t.TempDir()
	path, _ = fakeAmpPath(t, "")
	link := filepath.Join(pathDir, "amp")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathDir)
	if got, err := Discover(context.Background(), ""); err != nil || got != link {
		t.Fatalf("PATH Discover = %q, %v", got, err)
	}

	turn := &Turn{stdin: failingWriteCloser{}, maxLineBytes: 8}
	if err := turn.Send(context.Background(), map[string]string{"too": "large"}); err == nil {
		t.Fatal("expected max line error")
	}
	if err := (&Turn{stdin: failingWriteCloser{}, maxLineBytes: 1024}).Send(context.Background(), make(chan int)); err == nil {
		t.Fatal("expected marshal error")
	}
	if err := (&Turn{stdin: failingWriteCloser{}, maxLineBytes: 1024}).Send(context.Background(), map[string]string{"ok": "yes"}); err == nil {
		t.Fatal("expected write error")
	}
	if err := (&Turn{stdin: failingWriteCloser{}, maxLineBytes: 1024}).Send(cancelledContext(), map[string]string{"ok": "yes"}); err == nil {
		t.Fatal("expected canceled send")
	}
	blocking := &blockingWriteCloser{started: make(chan struct{}), release: make(chan struct{})}
	blockCtx, blockCancel := context.WithCancel(context.Background())
	blockErr := make(chan error, 1)
	go func() {
		blockErr <- (&Turn{stdin: blocking, maxLineBytes: 1024}).Send(blockCtx, map[string]string{"ok": "yes"})
	}()
	<-blocking.started
	blockCancel()
	if err := <-blockErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked send cancel = %v", err)
	}
	close(blocking.release)
	if err := (&Turn{}).Interrupt(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("nil interrupt: %v", err)
	}
	expectedCmd := exec.Command("/bin/sleep", "10")
	configureCommand(expectedCmd)
	if err := expectedCmd.Start(); err != nil {
		t.Fatal(err)
	}
	expectedTurn := &Turn{cmd: expectedCmd, waitFunc: func() error { return nil }}
	if err := expectedTurn.Interrupt(context.Background(), time.Second); err != nil {
		t.Fatalf("expected-exit interrupt: %v", err)
	}
	_ = killProcess(expectedCmd)
	_ = expectedCmd.Wait()
	if err := (&Turn{}).Close(); err != nil {
		t.Fatalf("nil close: %v", err)
	}
	if err := interruptProcess(nil); err != nil {
		t.Fatalf("nil interruptProcess: %v", err)
	}
	if err := killProcess(nil); err != nil {
		t.Fatalf("nil killProcess: %v", err)
	}
	drop := &Turn{errs: make(chan error, 1)}
	drop.errs <- errors.New("full")
	drop.sendErr(errors.New("dropped"))
	drop.sendErr(nil)
	tail := &Turn{}
	tail.captureStderr(strings.Repeat("x", maxCapturedStderrBytes+10))
	if len(tail.stderrText()) > maxCapturedStderrBytes {
		t.Fatal("stderr tail was not bounded")
	}
	closeTurn := &Turn{stdin: failingWriteCloser{}, stdout: failingReadCloser{}, stderr: failingReadCloser{}}
	if err := closeTurn.Close(); err == nil {
		t.Fatal("expected close errors")
	}
	if !expectedExit(nil) || !expectedExit(context.Canceled) || !expectedExit(&exec.ExitError{}) {
		t.Fatal("expected exit classification failed")
	}
	if expectedExit(errors.New("boom")) {
		t.Fatal("unexpected expectedExit success")
	}
	if stripANSI("\x1b[31mred\x1b[0m") != "red" {
		t.Fatal("stripANSI failed")
	}
}

func TestClientProcessSeamsReaderAndInterruptEdges(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAmpPath(t, "")

	if _, err := NewClient(nil, Options{CLIPath: "/does/not/exist", Cwd: t.TempDir()}).NewThread(ctx); err == nil {
		t.Fatal("NewThread output error ignored")
	}
	if _, err := NewClient(nil, Options{CLIPath: "/does/not/exist", Cwd: t.TempDir()}).ListThreads(ctx); err == nil {
		t.Fatal("ListThreads output error ignored")
	}

	oldLookPath := lookPath
	lookPath = func(string) (string, error) { return "", errors.New("lookpath failed") }
	if _, err := NewClient(nil, Options{}).Version(ctx); err == nil {
		t.Fatal("Version discover error ignored")
	}
	if _, err := NewClient(nil, Options{Cwd: t.TempDir()}).Continue(ctx, "T-1", map[string]any{"type": "user"}); err == nil {
		t.Fatal("Continue discover error ignored")
	}
	if _, err := Discover(ctx, ""); err == nil {
		t.Fatal("Discover lookpath error ignored")
	}
	lookPath = oldLookPath

	oldGetwd := getwd
	getwd = func() (string, error) { return "", errors.New("getwd failed") }
	if _, err := NewClient(nil, Options{CLIPath: path}).Continue(ctx, "T-1", map[string]any{"type": "user"}); err == nil {
		t.Fatal("Continue getwd error ignored")
	}
	getwd = oldGetwd

	oldCommandContext := commandContext
	for _, tc := range []struct {
		name  string
		shape func(*exec.Cmd)
		want  string
	}{
		{name: "stdin", shape: func(cmd *exec.Cmd) { cmd.Stdin = strings.NewReader("taken") }, want: "create amp stdin"},
		{name: "stdout", shape: func(cmd *exec.Cmd) { cmd.Stdout = io.Discard }, want: "create amp stdout"},
		{name: "stderr", shape: func(cmd *exec.Cmd) { cmd.Stderr = io.Discard }, want: "create amp stderr"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, name, args...)
				tc.shape(cmd)
				return cmd
			}
			_, err := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir()}).Continue(ctx, "T-1", map[string]any{"type": "user"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Continue pipe error = %v, want %q", err, tc.want)
			}
		})
	}
	commandContext = oldCommandContext

	if _, err := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir()}).Continue(ctx, "T-1", make(chan int)); err == nil {
		t.Fatal("Continue send error ignored")
	}

	oldCloseWriteCloser := closeWriteCloser
	closeWriteCloser = func(io.Closer) error { return errors.New("close stdin failed") }
	if _, err := NewClient(nil, Options{CLIPath: path, Cwd: t.TempDir()}).Continue(ctx, "T-1", map[string]any{"type": "user"}); err == nil || !strings.Contains(err.Error(), "close amp stdin") {
		t.Fatalf("Continue stdin close error = %v", err)
	}
	closeWriteCloser = oldCloseWriteCloser

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	cancelRead := &Turn{
		stdout:       io.NopCloser(strings.NewReader(`{"type":"assistant","message":{"content":[{"type":"text","text":"x"}]}}` + "\n")),
		messages:     make(chan Message),
		errs:         make(chan error, 2),
		maxLineBytes: 1024,
	}
	cancelRead.readStdout(cancelled)
	if err := <-cancelRead.errs; !errors.Is(err, context.Canceled) {
		t.Fatalf("readStdout canceled error = %v", err)
	}

	readErr := &Turn{stdout: failingReadCloser{}, messages: make(chan Message), errs: make(chan error, 2), maxLineBytes: 1024}
	readErr.readStdout(ctx)
	if err := <-readErr.errs; err == nil || !strings.Contains(err.Error(), "read amp stdout") {
		t.Fatalf("readStdout scanner error = %v", err)
	}

	doneCmd := exec.Command("sh", "-c", "exit 0")
	configureCommand(doneCmd)
	if err := doneCmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = doneCmd.Wait()
	if err := (&Turn{cmd: doneCmd}).Interrupt(ctx, time.Second); err == nil {
		t.Fatal("interrupt of exited process did not report signal error")
	}

	zeroPath, zeroState := fakeAmpPath(t, "sigint-ignore")
	zeroTurn, err := NewClient(nil, Options{CLIPath: zeroPath, Cwd: t.TempDir()}).Continue(ctx, "T-1", map[string]any{"type": "user"})
	if err != nil {
		t.Fatalf("Continue zero interrupt: %v", err)
	}
	waitForFile(t, filepath.Join(zeroState, "stdin.jsonl"))
	if interruptErr := zeroTurn.Interrupt(ctx, 0); interruptErr != nil {
		t.Fatalf("zero-timeout interrupt: %v", interruptErr)
	}
	_ = zeroTurn.Close()

	ctxPath, ctxState := fakeAmpPath(t, "sigint-ignore")
	ctxTurn, err := NewClient(nil, Options{CLIPath: ctxPath, Cwd: t.TempDir()}).Continue(ctx, "T-1", map[string]any{"type": "user"})
	if err != nil {
		t.Fatalf("Continue ctx interrupt: %v", err)
	}
	waitForFile(t, filepath.Join(ctxState, "stdin.jsonl"))
	interruptCtx, interruptCancel := context.WithCancel(ctx)
	interruptCancel()
	if interruptErr := ctxTurn.Interrupt(interruptCtx, time.Second); !errors.Is(interruptErr, context.Canceled) {
		t.Fatalf("interrupt ctx error = %v", interruptErr)
	}
	_ = ctxTurn.Close()

	waitPath, waitState := fakeAmpPath(t, "sigint-ignore")
	waitTurn, err := NewClient(nil, Options{CLIPath: waitPath, Cwd: t.TempDir()}).Continue(ctx, "T-1", map[string]any{"type": "user"})
	if err != nil {
		t.Fatalf("Continue wait interrupt: %v", err)
	}
	waitForFile(t, filepath.Join(waitState, "stdin.jsonl"))
	waitBoom := errors.New("wait boom")
	waitTurn.waitFunc = func() error { return waitBoom }
	if err := waitTurn.Interrupt(ctx, time.Second); !errors.Is(err, waitBoom) {
		t.Fatalf("interrupt wait error = %v", err)
	}
	_ = killProcess(waitTurn.cmd)
	_, _ = waitTurn.cmd.Process.Wait()

	drop := &Turn{log: slog.Default(), errs: make(chan error, 1)}
	drop.errs <- errors.New("full")
	drop.sendErr(errors.New("dropped with logger"))

	tail := &Turn{}
	tail.captureStderr("first")
	tail.captureStderr("second")
	if got := tail.stderrText(); !strings.Contains(got, "first\nsecond") {
		t.Fatalf("stderr newline capture = %q", got)
	}
}

type failingWriteCloser struct{}

func (failingWriteCloser) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func (failingWriteCloser) Close() error              { return nil }

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (failingReadCloser) Close() error             { return errors.New("close failed") }

type blockingWriteCloser struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingWriteCloser) Write(p []byte) (int, error) {
	close(b.started)
	<-b.release
	return len(p), nil
}

func (b *blockingWriteCloser) Close() error { return nil }

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func fakeAmpPath(t *testing.T, mode string) (string, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake shell executable uses POSIX sh")
	}
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "amp")
	script := "#!/bin/sh\nGO_WANT_FAKE_AMP=1 FAKE_AMP_MODE=" + shellQuote(mode) + " FAKE_AMP_STATE=" + shellQuote(state) + " exec " + shellQuote(os.Args[0]) + " -test.run=TestFakeAmpHelper -- \"$@\"\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path, state
}

func helperArgs() []string {
	for i, arg := range os.Args {
		if arg == "--" {
			return append([]string(nil), os.Args[i+1:]...)
		}
	}
	return nil
}

func helperContinue(mode string, state string) {
	switch mode {
	case "missing":
		os.Stderr.WriteString("Thread not found\n")
		os.Exit(1)
	case "sigint-clean":
		waitForSignal(state, true)
	case "sigint-ignore":
		waitForSignal(state, false)
	case "stream":
		os.Stderr.WriteString("native stderr noise\n")
		os.Stdout.WriteString("native stdout noise\n")
		os.Stdout.WriteString("{bad json\n")
		os.Stdout.WriteString(`{"type":"system","subtype":"init","cwd":"/tmp/project","session_id":"T-fake-thread","tools":["Read"],"mcp_servers":[{"name":"svc","status":"connected"}],"agent_mode":"medium","reasoning_effort":"high"}` + "\n")
		os.Stdout.WriteString(`{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}],"usage":{"input_tokens":1,"output_tokens":2,"max_tokens":99,"service_tier":"standard"}},"session_id":"T-fake-thread"}` + "\n")
		os.Stdout.WriteString(`{"type":"result","subtype":"success","duration_ms":1,"is_error":false,"num_turns":1,"result":"done","session_id":"T-fake-thread","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":99}}` + "\n")
	default:
		os.Stdout.WriteString(`{"type":"result","subtype":"success","duration_ms":1,"is_error":false,"num_turns":1,"result":"done","session_id":"T-fake-thread"}` + "\n")
	}
}

func waitForSignal(state string, exitOnSignal bool) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	<-signals
	_ = os.WriteFile(filepath.Join(state, "signal"), []byte("sigint\n"), 0o600)
	if exitOnSignal {
		os.Exit(0)
	}
	select {}
}

func recordHelperJSON(state string, name string, value any) {
	if state == "" {
		return
	}
	file, err := os.OpenFile(filepath.Join(state, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		os.Exit(2)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(value); err != nil {
		os.Exit(2)
	}
}

func readHelperJSON[T any](t *testing.T, path string) []T {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	out := make([]T, 0, len(lines))
	for _, line := range lines {
		var value T
		if err := json.Unmarshal([]byte(line), &value); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		out = append(out, value)
	}
	return out
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s was not created", path)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func TestTurnRecoverGoroutine(t *testing.T) {
	// Without a handler the panic propagates unchanged.
	bare := &Turn{}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic swallowed without handler")
			}
		}()
		func() {
			defer bare.recoverGoroutine(context.Background(), "no handler")
			panic("boom")
		}()
	}()

	// With a handler the panic is recovered and reported exactly once.
	var gotName string
	var gotValue any
	handled := &Turn{onPanic: func(_ context.Context, name string, recovered any) {
		gotName = name
		gotValue = recovered
	}}
	func() {
		defer handled.recoverGoroutine(context.Background(), "handled")
		panic("boom2")
	}()
	if gotName != "handled" || gotValue != "boom2" {
		t.Fatalf("recovered = %q %v", gotName, gotValue)
	}

	// A clean return never invokes the handler.
	gotName, gotValue = "", nil
	func() {
		defer handled.recoverGoroutine(context.Background(), "clean")
	}()
	if gotName != "" || gotValue != nil {
		t.Fatal("handler invoked without panic")
	}
}

func TestFinishProcessTreeObservationUsesProofBoundary(t *testing.T) {
	var quiescent, unproven int
	observer := ProcessSnapshotObserver{
		Quiescent: func(context.Context) { quiescent++ },
		Unproven:  func() { unproven++ },
	}

	finishProcessTreeObservation(t.Context(), observer, nil)
	if quiescent != 1 || unproven != 0 {
		t.Fatalf("proven lifecycle = quiescent %d, unproven %d", quiescent, unproven)
	}

	finishProcessTreeObservation(t.Context(), observer, ErrProcessTreeNotQuiescent)
	if quiescent != 1 || unproven != 1 {
		t.Fatalf("unproven lifecycle = quiescent %d, unproven %d", quiescent, unproven)
	}
}

func TestProcessTreeSnapshotAvailabilityBoundary(t *testing.T) {
	if observer := (*Client)(nil).newProcessSnapshotObserver(t.Context(), nil); observer.Refresh != nil || observer.Quiescent != nil || observer.Unproven != nil {
		t.Fatal("nil client created a process observer")
	}
	if observer := (&Client{}).newProcessSnapshotObserver(t.Context(), nil); observer.Refresh != nil || observer.Quiescent != nil || observer.Unproven != nil {
		t.Fatal("client without a factory created a process observer")
	}

	created := false
	var inventory ProcessInventory
	client := &Client{options: Options{NewProcessSnapshotObserver: func(_ context.Context, got ProcessInventory) ProcessSnapshotObserver {
		created = true
		inventory = got
		return ProcessSnapshotObserver{}
	}}}
	_ = client.newProcessSnapshotObserver(t.Context(), &processTree{})
	if !created {
		t.Fatal("process observer factory was not called")
	}
	if count, available := inventory(); available || count != 0 {
		t.Fatalf("Unix inventory = (%d, %t), want unavailable", count, available)
	}

	original := processTreeDescendantCount
	t.Cleanup(func() { processTreeDescendantCount = original })
	processTreeDescendantCount = func(*processTree) (int, bool) { return 7, true }
	observer := client.newProcessSnapshotObserver(t.Context(), &processTree{})
	if count, available := inventory(); !available || count != 7 {
		t.Fatalf("available inventory = (%d, %t), want (7, true)", count, available)
	}

	observeProcessTreeSnapshot(t.Context(), observer)
	refreshed := false
	observeProcessTreeSnapshot(t.Context(), ProcessSnapshotObserver{Refresh: func(context.Context) { refreshed = true }})
	if !refreshed {
		t.Fatal("snapshot refresh callback was not called")
	}
}

func TestTurnClosePreservesSnapshotOnUnprovenTree(t *testing.T) {
	original := processTreeTerminateAndWait
	t.Cleanup(func() { processTreeTerminateAndWait = original })
	processTreeTerminateAndWait = func(*processTree, time.Duration) error {
		return ErrProcessTreeNotQuiescent
	}

	var quiescent, unproven int
	turn := &Turn{
		tree: &processTree{},
		processObserver: ProcessSnapshotObserver{
			Quiescent: func(context.Context) { quiescent++ },
			Unproven:  func() { unproven++ },
		},
	}
	err := turn.Close()
	if !errors.Is(err, ErrProcessTreeNotQuiescent) {
		t.Fatalf("Close error = %v, want process-tree proof sentinel", err)
	}
	if quiescent != 0 || unproven != 1 {
		t.Fatalf("Close lifecycle = quiescent %d, unproven %d", quiescent, unproven)
	}
}
