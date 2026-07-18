package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func TestRunContainmentUsage(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"unknown"},
		{"diagnose"},
		{"diagnose", "-scratch-dir", " \t"},
		{"diagnose", "-scratch-dir", "bad\x00path"},
		{"diagnose", "-bad"},
		{"diagnose", "-scratch-dir", t.TempDir(), "extra"},
		{"cleanup", "-scratch-dir", t.TempDir(), "-runtime-id", strings.Repeat("0", 32)},
		{"cleanup", "-scratch-dir", t.TempDir(), "-runtime-id", "BAD", "-force"},
		{"cleanup", "-bad"},
		{"cleanup", "-scratch-dir", t.TempDir(), "-runtime-id", strings.Repeat("0", 32), "-force", "extra"},
	} {
		var stderr bytes.Buffer
		if code := runContainment(args, &bytes.Buffer{}, &stderr); code != 2 {
			t.Fatalf("runContainment(%v) = %d, stderr=%q", args, code, stderr.String())
		}
	}
	if !validContainmentRuntimeID(strings.Repeat("a", 32)) || validContainmentRuntimeID(strings.Repeat("A", 32)) {
		t.Fatal("runtime id validation mismatch")
	}
	if validContainmentScratchDir(" \t") || validContainmentScratchDir("bad\x00path") || !validContainmentScratchDir("relative") {
		t.Fatal("scratch directory validation mismatch")
	}
}

func TestRunContainmentDispatchAndOffDarwinFlag(t *testing.T) {
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"containment"}, strings.NewReader(""), &bytes.Buffer{}, &stderr); code != 2 {
		t.Fatalf("containment dispatch = %d, stderr=%q", code, stderr.String())
	}

	originalGOOS := runtimeGOOS
	t.Cleanup(func() { runtimeGOOS = originalGOOS })
	runtimeGOOS = "linux"
	stderr.Reset()
	if code := run(t.Context(), []string{"-darwin-best-effort-containment"}, strings.NewReader(""), &bytes.Buffer{}, &stderr); code != 2 || !strings.Contains(stderr.String(), "only on darwin") {
		t.Fatalf("off-Darwin flag = %d, stderr=%q", code, stderr.String())
	}
}

func TestRunContainmentDarwinGoldenJSON(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin operational command")
	}
	parent := t.TempDir()
	var stdout, stderr bytes.Buffer
	if code := runContainment([]string{"diagnose", "-scratch-dir", parent}, &stdout, &stderr); code != 0 {
		t.Fatalf("diagnose = %d, stderr=%q", code, stderr.String())
	}
	warning := "PID-by-PID cleanup has a PID-reuse time-of-check/time-of-use race and can signal an unrelated reused PID; correlation is not ownership or proof of absence; inherited markers can be scrubbed"
	wantDiagnose := fmt.Sprintf(`{"vendor":"amp","containment":"best_effort","scratch_parent":%q,"warning":%q,"records":[]}`+"\n", parent, warning)
	if stdout.String() != wantDiagnose || stderr.Len() != 0 {
		t.Fatalf("diagnose stdout=%q stderr=%q, want %q", stdout.String(), stderr.String(), wantDiagnose)
	}

	root := filepath.Join(parent, "acp-go-amp-command-partial")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := nativeamp.NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	if err := generation.RecordStarted(os.Getpid(), os.Getpid()); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	code := runContainment([]string{
		"cleanup", "-scratch-dir", parent, "-runtime-id", generation.RuntimeID, "-force",
	}, &stdout, &stderr)
	if code != 1 || !strings.HasPrefix(stderr.String(), "acp-go-amp: correlated or ambiguous processes remained") {
		t.Fatalf("partial cleanup = %d, stderr=%q", code, stderr.String())
	}
	wantCleanup := fmt.Sprintf(`{"vendor":"amp","containment":"best_effort","scratch_parent":%q,"warning":%q,"runtime_id":%q,"generation_root":%q,"term_signaled_pids":[],"kill_signaled_pids":[],"remaining_correlated_pids":[],"ambiguous_pids":[%d],"root_removed":false}`+"\n",
		parent, warning, generation.RuntimeID, root, os.Getpid())
	if stdout.String() != wantCleanup {
		t.Fatalf("cleanup stdout=%q, want %q", stdout.String(), wantCleanup)
	}
}

func TestRunContainmentDiagnose(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runContainment([]string{"diagnose", "-scratch-dir", t.TempDir()}, &stdout, &stderr)
	if runtime.GOOS == "darwin" {
		if code != 0 || !strings.Contains(stdout.String(), `"containment":"best_effort"`) || stderr.Len() != 0 {
			t.Fatalf("diagnose = %d, stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}

		return
	}
	if code != 1 || !strings.Contains(stderr.String(), "only on darwin") {
		t.Fatalf("diagnose = %d, stderr=%q", code, stderr.String())
	}
}

func TestRunContainmentCleanupOperationalFailure(t *testing.T) {
	var stderr bytes.Buffer
	code := runContainment([]string{
		"cleanup", "-scratch-dir", t.TempDir(), "-runtime-id", strings.Repeat("0", 32), "-force",
	}, &bytes.Buffer{}, &stderr)
	if code != 1 || !strings.HasPrefix(stderr.String(), "acp-go-amp: ") || strings.Count(strings.TrimSpace(stderr.String()), "\n") != 0 {
		t.Fatalf("cleanup = %d, stderr=%q", code, stderr.String())
	}
}

func TestRunContainmentOperationalSuccess(t *testing.T) {
	originalDiagnose := diagnoseContainment
	originalCleanup := cleanupContainment
	t.Cleanup(func() {
		diagnoseContainment = originalDiagnose
		cleanupContainment = originalCleanup
	})
	diagnoseContainment = func(string, io.Writer) error { return nil }
	cleanupContainment = func(string, string, bool, io.Writer) error { return nil }

	if code := runContainment([]string{"diagnose", "-scratch-dir", "scratch"}, io.Discard, io.Discard); code != 0 {
		t.Fatalf("diagnose = %d", code)
	}
	if code := runContainment([]string{
		"cleanup", "-scratch-dir", "scratch", "-runtime-id", strings.Repeat("a", 32), "-force",
	}, io.Discard, io.Discard); code != 0 {
		t.Fatalf("cleanup = %d", code)
	}
}
