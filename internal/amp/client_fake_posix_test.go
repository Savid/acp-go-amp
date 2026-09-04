//go:build !windows

// The fake amp harness is a POSIX shell script that re-enters this test binary,
// so every case that drives a fake child lives here. Windows resolves
// executables through PATHEXT and runs no shebang, which leaves no spelling of
// this harness a Windows host could launch; a build tag, not a runtime skip, is
// what keeps those cases off that platform.

//nolint:gocyclo,nlreturn // Fake executable harness keeps process cases in one place.
package amp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"path/filepath"
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

	if len(args) > 0 && args[len(args)-1] == authLoginSubcommand {
		helperLogin(mode, state)
		os.Exit(0)
	}
	if slices.Contains(args, "version") {
		if mode == "bad-version" {
			os.Stdout.WriteString("0.0.1\n")
			os.Exit(0)
		}
		os.Stdout.WriteString(MinimumVersion + "-gfake\n")
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
	if threads < 0 && slices.Contains(args, ampArgExecute) {
		stdin, _ := io.ReadAll(os.Stdin)
		recordHelperJSON(state, "stdin.jsonl", strings.TrimSpace(string(stdin)))
		helperContinue(mode, state)
		os.Exit(0)
	}
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
	case "list":
		if mode == "hang-list" {
			for {
				time.Sleep(time.Hour)
			}
		}
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

func TestRetainedExecutableLaunchesWithoutResolving(t *testing.T) {
	path, _ := fakeAmpPath(t, "")
	unresolvable := t.TempDir()

	client := newTestClient(t, nil, Options{
		CLIPath:            ampExecutableName,
		Cwd:                t.TempDir(),
		ResolutionEnv:      map[string]string{"PATH": unresolvable},
		Env:                map[string]string{"PATH": unresolvable, "AMP_API_KEY": "fake"},
		ResolvedExecutable: path,
	})

	version, err := client.Version(context.Background())
	if err != nil || version != MinimumVersion+"-gfake" {
		t.Fatalf("Version through the retained harness = %q, %v", version, err)
	}

	// A retained path removes lookup, not the caller's cancellation: no child
	// starts once the context is done.
	if _, err := client.Version(cancelledContext()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled retained launch = %v, want context.Canceled", err)
	}

	// Skipping resolution does not skip child-environment validation.
	invalidEnv := newTestClient(t, nil, Options{
		Cwd:                t.TempDir(),
		Env:                map[string]string{"": "x"},
		ResolvedExecutable: path,
	})
	if _, err := invalidEnv.Version(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid environment key") {
		t.Fatalf("retained launch with an unusable child key = %v, want a refusal", err)
	}

	relative := newTestClient(t, nil, Options{Cwd: t.TempDir(), ResolvedExecutable: "amp"})
	if _, err := relative.Version(context.Background()); err == nil || !strings.Contains(err.Error(), "not an absolute path") {
		t.Fatalf("relative retained harness = %v, want a refusal", err)
	}
}

func TestClientCommandsUseGlobalArgsAndParseOutput(t *testing.T) {
	path, state := fakeAmpPath(t, "")
	client := newTestProbeClient(t, nil, Options{
		CLIPath: path,
		Cwd:     t.TempDir(),
		Env:     map[string]string{"AMP_API_KEY": "fake"},
		Mode:    "strange-mode",
	})
	ctx := context.Background()

	if version, err := client.Version(ctx); err != nil || version != MinimumVersion+"-gfake" {
		t.Fatalf("Version = %q, %v", version, err)
	}
	if _, err := client.DiscoveryProbe(ctx); err != nil {
		t.Fatalf("DiscoveryProbe: %v", err)
	}
	if _, err := client.StartupProbe(ctx); err != nil {
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
	for _, want := range []string{"--settings-file", "--mcp-config", "-m", "medium", "--stream-json", "--stream-json-input", "-x", ampArgNoArchiveAfterExecute} {
		if !slices.Contains(startupContinue, want) {
			t.Fatalf("startup continue probe missing %q: %#v", want, startupContinue)
		}
	}
	for _, global := range []string{ampArgNoIDE, ampArgNoColor, ampArgNoNotifications, "--settings-file", "--mcp-config", "-m"} {
		if count := countArg(startupContinue, global); count != 1 {
			t.Fatalf("startup continue probe has %d copies of %q: %#v", count, global, startupContinue)
		}
	}
	if _, err := client.StartupProbe(ctx); err != nil {
		t.Fatalf("StartupProbe cached: %v", err)
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
	missing := Options{OrdinaryEnvironment: map[string]string{"PATH": t.TempDir()}}
	if _, _, err := NewClient(nil, missing).discoverVersion(ctx); err == nil {
		t.Fatal("version discovery PATH error ignored")
	}
	if _, err := NewClient(nil, missing).StartupProbe(ctx); err == nil {
		t.Fatal("StartupProbe PATH error ignored")
	}

	if _, err := newTestProbeClient(t, nil, Options{CLIPath: "/does/not/exist"}).StartupProbe(ctx); err == nil {
		t.Fatal("StartupProbe version command error ignored")
	}
	if _, err := newTestProbeClient(t, nil, Options{CLIPath: "/does/not/exist"}).DiscoveryProbe(ctx); err == nil {
		t.Fatal("DiscoveryProbe version command error ignored")
	}
	badVersion, _ := fakeAmpPath(t, "bad-version")
	if _, err := newTestProbeClient(t, nil, Options{CLIPath: badVersion, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "below required") {
		t.Fatalf("bad version probe = %v", err)
	}
	badList, _ := fakeAmpPath(t, "bad-list-json")
	if _, err := newTestProbeClient(t, nil, Options{CLIPath: badList, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "threads list --json probe failed") {
		t.Fatalf("list probe = %v", err)
	}
	exportAbsent, _ := fakeAmpPath(t, "probe-export-absent")
	if _, err := newTestProbeClient(t, nil, Options{CLIPath: exportAbsent, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "threads export probe failed") {
		t.Fatalf("export method-present probe = %v", err)
	}
	exportMissing, _ := fakeAmpPath(t, "probe-export-missing")
	if _, err := newTestProbeClient(t, nil, Options{CLIPath: exportMissing, Cwd: t.TempDir()}).StartupProbe(ctx); err != nil {
		t.Fatalf("export missing-thread domain error should count as present: %v", err)
	}
	continueSuccess, _ := fakeAmpPath(t, "probe-continue-success")
	if _, err := newTestProbeClient(t, nil, Options{CLIPath: continueSuccess, Cwd: t.TempDir()}).StartupProbe(ctx); err == nil || !strings.Contains(err.Error(), "unexpectedly succeeded") {
		t.Fatalf("continue missing-thread success gate = %v", err)
	}
	if err := methodProbeError("threads continue", errors.New("usage"), true); err == nil || !strings.Contains(err.Error(), "did not return missing-thread") {
		t.Fatalf("continue missing-thread usage gate = %v", err)
	}
	if !versionAtLeast("0.0.1784765893-gx", MinimumVersion) {
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
	client := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir(), MaxLineBytes: 1024})

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
	client := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()})

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

func TestClientErrorBranches(t *testing.T) {
	if _, err := Discover(cancelledContext(), ""); err == nil {
		t.Fatal("expected canceled Discover error")
	}
	if got, err := Discover(context.Background(), "/usr/bin/true", []string{"PATH=/usr/bin"}); err != nil || got != "/usr/bin/true" {
		t.Fatalf("explicit Discover = %q, %v", got, err)
	}
	privateA := adapterPrivateEnvPrefix + "A"
	privateB := adapterPrivateEnvPrefix + "B"
	t.Setenv(privateA, "secret")
	env := BuildEnv(map[string]string{
		"Z":      "1",
		"A":      "2",
		privateB: "secret",
	}, "/tmp/cwd")
	if !slices.Contains(env, "A=2") || !slices.Contains(env, "Z=1") || !slices.Contains(env, "PWD=/tmp/cwd") {
		t.Fatalf("env missing overrides: %#v", env)
	}
	if refused := BuildEnv(map[string]string{"": "ignored"}, "/tmp/cwd"); refused != nil {
		t.Fatalf("empty override key accepted: %#v", refused)
	}
	for _, privateKey := range []string{privateA, privateB} {
		for _, item := range env {
			if strings.HasPrefix(item, privateKey+"=") {
				t.Fatalf("private adapter env leaked to native Amp: %q", item)
			}
		}
	}

	for _, test := range []struct {
		name string
		id   string
		ok   bool
	}{
		{name: "missing prefix", id: "thread"},
		{name: "exact maximum", id: "T-" + strings.Repeat("x", MaxThreadIDBytes-2), ok: true},
		{name: "over maximum", id: "T-" + strings.Repeat("x", MaxThreadIDBytes-1)},
	} {
		t.Run("thread id "+test.name, func(t *testing.T) {
			err := ValidateThreadID(test.id)
			if test.ok && err != nil {
				t.Fatalf("ValidateThreadID(%q): %v", test.name, err)
			}
			if !test.ok && err == nil {
				t.Fatalf("ValidateThreadID(%q) succeeded", test.name)
			}
		})
	}
	path, _ := fakeAmpPath(t, "bad-list-json")
	if _, err := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()}).ListThreads(context.Background()); err == nil {
		t.Fatal("expected list decode error")
	}
	path, _ = fakeAmpPath(t, "export-fail")
	if _, err := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()}).ExportThread(context.Background(), "T-1"); err == nil {
		t.Fatal("expected export error")
	}
	path, _ = fakeAmpPath(t, "delete-fail")
	if err := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()}).DeleteThread(context.Background(), "T-1"); err == nil {
		t.Fatal("expected delete error")
	}
	if _, err := newTestClient(t, nil, Options{CLIPath: "/does/not/exist"}).Version(context.Background()); err == nil {
		t.Fatal("expected version discover error")
	}
	path, _ = fakeAmpPath(t, "")
	if _, err := newTestClient(t, nil, Options{CLIPath: path, Cwd: filepath.Join(t.TempDir(), "missing")}).Continue(context.Background(), "T-1", map[string]any{"type": "user"}); err == nil {
		t.Fatal("expected continue start error")
	}
	pathDir := t.TempDir()
	path, _ = fakeAmpPath(t, "")
	link := filepath.Join(pathDir, "amp")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathDir)
	if got, err := Discover(context.Background(), "", []string{"PATH=" + pathDir}); err != nil || got != link {
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
	if err := (&Turn{}).Interrupt(context.Background()); err != nil {
		t.Fatalf("nil interrupt: %v", err)
	}
	if err := (&Turn{}).Close(); err != nil {
		t.Fatalf("nil close: %v", err)
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
	if stripANSI("\x1b[31mred\x1b[0m") != "red" {
		t.Fatal("stripANSI failed")
	}
}

func fakeAmpPath(t *testing.T, mode string) (string, string) {
	t.Helper()
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

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
