package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	ampacp "github.com/savid/acp-go-amp"
)

func TestRunPassesContractFlags(t *testing.T) {
	originalServe := serve
	originalAgentVersion := agentVersion
	t.Cleanup(func() {
		serve = originalServe
		agentVersion = originalAgentVersion
	})

	var got ampacp.Options
	serve = func(_ context.Context, _ io.Reader, _ io.Writer, opts ...ampacp.Option) error {
		for _, opt := range opts {
			opt(&got)
		}

		return nil
	}
	agentVersion = func() string { return "v1.2.3" }

	code := run(context.Background(), []string{
		"-path", "/bin/amp",
		"-home", "/tmp/amp",
		"-model", "ignored",
		"-scratch-dir", "/tmp/scratch",
		"-debug",
	}, bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil))

	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if got.AgentVersion != "v1.2.3" {
		t.Fatalf("AgentVersion = %q", got.AgentVersion)
	}
	if got.ExecutablePath != "/bin/amp" {
		t.Fatalf("ExecutablePath = %q", got.ExecutablePath)
	}
	if got.Home != "/tmp/amp" {
		t.Fatalf("Home = %q", got.Home)
	}
	if got.DefaultModel != "ignored" {
		t.Fatalf("DefaultModel = %q", got.DefaultModel)
	}
	if got.ScratchDir != "/tmp/scratch" {
		t.Fatalf("ScratchDir = %q", got.ScratchDir)
	}
	if got.Logger == nil {
		t.Fatal("Logger is nil")
	}
	if got.TextMapPropagator == nil {
		t.Fatal("TextMapPropagator is nil")
	}
}

func TestRunDarwinBestEffortFlag(t *testing.T) {
	var got ampacp.Options
	originalServe := serve
	t.Cleanup(func() { serve = originalServe })
	serve = func(_ context.Context, _ io.Reader, _ io.Writer, opts ...ampacp.Option) error {
		for _, option := range opts {
			option(&got)
		}

		return nil
	}
	var stderr bytes.Buffer
	code := run(context.Background(), []string{"-darwin-best-effort-containment"}, bytes.NewBuffer(nil), bytes.NewBuffer(nil), &stderr)
	if runtime.GOOS == "darwin" {
		if code != 0 || !got.DarwinBestEffortContainment || !strings.Contains(stderr.String(), "containment=best_effort") {
			t.Fatalf("code=%d options=%#v stderr=%q", code, got, stderr.String())
		}

		return
	}
	if code != 2 || got.DarwinBestEffortContainment {
		t.Fatalf("off-Darwin code=%d options=%#v", code, got)
	}
}

func TestRunSimulatedDarwinBestEffortFlag(t *testing.T) {
	originalGOOS := runtimeGOOS
	originalServe := serve
	t.Cleanup(func() {
		runtimeGOOS = originalGOOS
		serve = originalServe
	})
	runtimeGOOS = platformDarwin
	selected := false
	serve = func(_ context.Context, _ io.Reader, _ io.Writer, options ...ampacp.Option) error {
		var configured ampacp.Options
		for _, option := range options {
			option(&configured)
		}
		selected = configured.DarwinBestEffortContainment

		return nil
	}
	var stderr bytes.Buffer
	if code := run(context.Background(), []string{"-darwin-best-effort-containment"}, strings.NewReader(""), io.Discard, &stderr); code != 0 || !selected || !strings.Contains(stderr.String(), "containment=best_effort") {
		t.Fatalf("Darwin flag = %d, selected=%v stderr=%q", code, selected, stderr.String())
	}
}

func TestRunSeedFiles(t *testing.T) {
	originalServe := serve
	originalAgentVersion := agentVersion
	t.Cleanup(func() {
		serve = originalServe
		agentVersion = originalAgentVersion
	})

	var got ampacp.Options
	serve = func(_ context.Context, _ io.Reader, _ io.Writer, opts ...ampacp.Option) error {
		for _, opt := range opts {
			opt(&got)
		}

		return nil
	}
	agentVersion = func() string { return "v1.2.3" }

	hostFile := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(hostFile, []byte(`{"seed":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	code := run(context.Background(), []string{
		"-seed-file", "custom/settings.json=" + hostFile,
		"-seed-file", "custom/settings.json=" + hostFile,
	}, bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil))

	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if got.SeedFiles["custom/settings.json"] != `{"seed":true}` {
		t.Fatalf("SeedFiles = %#v", got.SeedFiles)
	}
}

func TestRunSeedFilesErrors(t *testing.T) {
	if code := run(context.Background(), []string{"-seed-file", "noequalsign"}, bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil)); code != 2 {
		t.Fatalf("bad seed-file format code = %d, want 2", code)
	}

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"-seed-file", "rel=" + filepath.Join(t.TempDir(), "missing")}, bytes.NewBuffer(nil), bytes.NewBuffer(nil), &stderr)
	if code != 2 {
		t.Fatalf("missing host file code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "read -seed-file") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSeedFileFlagString(t *testing.T) {
	f := seedFileFlag{}
	if err := f.Set("a=host1"); err != nil {
		t.Fatal(err)
	}
	if err := f.Set("b=host2"); err != nil {
		t.Fatal(err)
	}
	if f.String() != "a,b" {
		t.Fatalf("String = %q, want \"a,b\"", f.String())
	}
}

func TestRunVersion(t *testing.T) {
	originalAgentVersion := agentVersion
	t.Cleanup(func() { agentVersion = originalAgentVersion })
	agentVersion = func() string { return "v9.9.9" }

	var stdout bytes.Buffer
	code := run(context.Background(), []string{"-version"}, bytes.NewBuffer(nil), &stdout, bytes.NewBuffer(nil))

	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if stdout.String() != "v9.9.9\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunErrorBranches(t *testing.T) {
	originalServe := serve
	originalShutdown := shutdownOpenTelemetry
	originalAgentVersion := agentVersion
	t.Cleanup(func() {
		serve = originalServe
		shutdownOpenTelemetry = originalShutdown
		agentVersion = originalAgentVersion
	})
	agentVersion = func() string { return "v1.2.3" }

	if code := run(context.Background(), []string{"-bad"}, bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil)); code != 2 {
		t.Fatalf("bad flag code = %d, want 2", code)
	}

	serve = func(context.Context, io.Reader, io.Writer, ...ampacp.Option) error {
		return errors.New("serve failed")
	}
	shutdownOpenTelemetry = func(context.Context, func(context.Context) error) error { return nil }
	var stderr bytes.Buffer
	code := run(context.Background(), nil, bytes.NewBuffer(nil), bytes.NewBuffer(nil), &stderr)
	if code != 1 {
		t.Fatalf("serve error code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "serve failed") {
		t.Fatalf("stderr = %q", stderr.String())
	}

	serve = func(ctx context.Context, _ io.Reader, _ io.Writer, _ ...ampacp.Option) error {
		return ctx.Err()
	}
	shutdownOpenTelemetry = func(context.Context, func(context.Context) error) error { return errors.New("shutdown failed") }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stderr.Reset()
	code = run(ctx, nil, bytes.NewBuffer(nil), bytes.NewBuffer(nil), &stderr)
	if code != 1 {
		t.Fatalf("shutdown error code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "shutdown OpenTelemetry") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestPendingSignalAndSignalCode(t *testing.T) {
	signals := make(chan os.Signal, 1)
	if pendingSignal(signals) != nil {
		t.Fatal("empty channel returned a signal")
	}
	if got := signalCode(fakeSignal("fake")); got != 1 {
		t.Fatalf("signalCode(fake) = %d, want 1", got)
	}
}

func TestMainExitBranch(t *testing.T) {
	originalServe := serve
	originalExit := exit
	originalArgs := os.Args
	t.Cleanup(func() {
		serve = originalServe
		exit = originalExit
		os.Args = originalArgs
	})

	serve = func(context.Context, io.Reader, io.Writer, ...ampacp.Option) error {
		return errors.New("serve failed")
	}
	os.Args = []string{"acp-go-amp"}
	exitCode := -1
	exit = func(code int) { exitCode = code }

	main()
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

type fakeSignal string

func (s fakeSignal) String() string { return string(s) }

func (s fakeSignal) Signal() {}
