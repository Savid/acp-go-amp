package ampacp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func TestAgentContainmentModeAndObservation(t *testing.T) {
	if got := (*Agent)(nil).ContainmentMode(); got != RuntimeContainmentUnavailable {
		t.Fatalf("nil agent mode = %q", got)
	}
	var observed []RuntimeContainmentMode
	defaultAgent := NewAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		ObserveContainment: func(_ context.Context, mode RuntimeContainmentMode) {
			observed = append(observed, mode)
		},
	}))
	var want RuntimeContainmentMode
	switch runtime.GOOS {
	case "linux", "windows":
		want = RuntimeContainmentAuthoritative
	default:
		want = RuntimeContainmentUnavailable
	}
	if got := defaultAgent.ContainmentMode(); got != want {
		t.Fatalf("default mode = %q, want %q", got, want)
	}
	if len(observed) != 1 || observed[0] != want {
		t.Fatalf("containment observations = %v", observed)
	}

	var logs bytes.Buffer
	var snapshots int
	opted := NewAgent(
		WithDarwinBestEffortContainment(),
		WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ObserveProcessSnapshot: func(context.Context, RuntimeProcessKind, int) { snapshots++ },
		}),
	)
	if runtime.GOOS == "darwin" {
		if opted.ContainmentMode() != RuntimeContainmentBestEffort {
			t.Fatalf("opted mode = %q", opted.ContainmentMode())
		}
		if !strings.Contains(logs.String(), `"containment":"best_effort"`) || !strings.Contains(logs.String(), "escaped descendants may survive") {
			t.Fatalf("structured best-effort warning = %q", logs.String())
		}
		observer := opted.newProcessSnapshotObserver(t.Context(), func() (int, bool) { return 7, true })
		observer.Refresh(t.Context())
		observer.Complete(t.Context())
		observer.Incomplete()
		if snapshots != 0 {
			t.Fatalf("best-effort provider snapshots = %d", snapshots)
		}

		return
	}
	if opted.ContainmentMode() != RuntimeContainmentUnavailable {
		t.Fatalf("off-Darwin opted mode = %q", opted.ContainmentMode())
	}
	if _, err := opted.Initialize(t.Context(), acp.InitializeRequest{}); err == nil || !strings.Contains(err.Error(), "supported only on darwin") {
		t.Fatalf("off-Darwin opt-in initialization error = %v", err)
	}
}

func TestConfigureNativeClientDarwinGenerationResources(t *testing.T) {
	originalMkdir := mkdirTemp
	originalRemove := removeSessionDir
	t.Cleanup(func() {
		mkdirTemp = originalMkdir
		removeSessionDir = originalRemove
	})

	want := errors.New("resource")
	reserved := 0
	released := 0
	newConfigured := func(scratch string, reserve func(context.Context, RuntimeResourceKind) (func(), error)) nativeamp.Options {
		agent := NewAgent(
			WithScratchDir(scratch),
			WithDarwinBestEffortContainment(),
			WithRuntimeResourceHooks(RuntimeResourceHooks{ReserveScratchRoot: reserve}),
		)
		var options nativeamp.Options
		agent.configureNativeClient(&options, RuntimeResourcePrompt)

		return options
	}

	options := newConfigured(t.TempDir(), func(context.Context, RuntimeResourceKind) (func(), error) { return nil, want })
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) {
		t.Fatalf("reserve error = %v", err)
	}

	fileParent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileParent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	reserve := func(context.Context, RuntimeResourceKind) (func(), error) {
		reserved++

		return func() { released++ }, nil
	}
	options = newConfigured(fileParent, reserve)
	if _, err := options.NewDarwinGeneration(t.Context()); err == nil || reserved != 1 || released != 1 {
		t.Fatalf("scratch-parent error=%v reserved=%d released=%d", err, reserved, released)
	}

	parent := t.TempDir()
	mkdirTemp = func(string, string) (string, error) { return "", want }
	options = newConfigured(parent, reserve)
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) || released != 2 {
		t.Fatalf("mkdir error=%v released=%d", err, released)
	}
	mkdirTemp = originalMkdir

	registry := filepath.Join(parent, "acp-go-amp-containment")
	if err := os.WriteFile(registry, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	options = newConfigured(parent, reserve)
	if _, err := options.NewDarwinGeneration(t.Context()); err == nil || released != 3 {
		t.Fatalf("record error=%v released=%d", err, released)
	}
	removeSessionDir = func(string) error { return want }
	if _, err := options.NewDarwinGeneration(t.Context()); !errors.Is(err, want) || released != 3 {
		t.Fatalf("record/remove error=%v released=%d", err, released)
	}
	removeSessionDir = originalRemove
	if err := os.Remove(registry); err != nil {
		t.Fatal(err)
	}

	generation, err := options.NewDarwinGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	beforeRelease := released
	if releaseErr := generation.Release(false); releaseErr != nil || released != beforeRelease {
		t.Fatalf("incomplete release error=%v releases=%d", releaseErr, released)
	}
	removeSessionDir = func(string) error { return want }
	if releaseErr := generation.Release(true); !errors.Is(releaseErr, want) || released != beforeRelease {
		t.Fatalf("failed complete release error=%v releases=%d", releaseErr, released)
	}
	removeSessionDir = originalRemove

	generation, err = options.NewDarwinGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if releaseErr := generation.Release(true); releaseErr != nil || released != beforeRelease+1 {
		t.Fatalf("complete release error=%v releases=%d", releaseErr, released)
	}
}
