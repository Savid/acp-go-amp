package amp

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOrdinaryProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_ORDINARY_PROCESS_HELPER") != "1" {
		return
	}

	_, _ = os.Stdout.WriteString("ordinary stdout")
	_, _ = os.Stderr.WriteString("ordinary stderr")
	if os.Getenv("GO_WANT_ORDINARY_PROCESS_BLOCK") == "1" {
		time.Sleep(time.Hour)
	}

	os.Exit(0)
}

func TestOrdinaryProcessResultAndPipes(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	process, err := startOrdinaryNative(t.Context(), NativeRequest{
		Executable: executable,
		Arguments:  []string{"-test.run=^TestOrdinaryProcessHelper$"},
		Environment: []string{
			"GO_WANT_ORDINARY_PROCESS_HELPER=1",
			"GOCOVERDIR=" + os.Getenv("GOCOVERDIR"),
		},
		WorkingDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = process.Stdin().Close()

	// Wait runs before either stream is drained on purpose: what the child
	// wrote belongs to whoever holds the pipe, not to whoever happened to be
	// scheduled before the exit. A backend that hands its parent ends to
	// exec.Cmd.Wait loses both payloads here every time.
	result, err := process.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result != (NativeResult{ExitCode: 0}) {
		t.Fatalf("result = %#v", result)
	}

	stdout, err := io.ReadAll(process.Stdout())
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := io.ReadAll(process.Stderr())
	if err != nil {
		t.Fatal(err)
	}
	if got := string(stdout); got != "ordinary stdout" {
		t.Fatalf("stdout = %q", got)
	}
	if got := string(stderr); got != "ordinary stderr" {
		t.Fatalf("stderr = %q", got)
	}
}

// TestOrdinaryProcessPipeExhaustionReleasesClaimedDescriptors covers each pipe
// this backend claims: a host that cannot hand out the next one refuses the
// start naming that stream, and every descriptor already claimed is released
// rather than leaked into the refusal.
func TestOrdinaryProcessPipeExhaustionReleasesClaimedDescriptors(t *testing.T) {
	original := newProcessPipe
	t.Cleanup(func() { newProcessPipe = original })

	want := errors.New("no descriptors left")
	for _, tc := range []struct {
		stream string
		allow  int
	}{
		{stream: "stdin", allow: 0},
		{stream: "stdout", allow: 1},
		{stream: "stderr", allow: 2},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			remaining := tc.allow
			newProcessPipe = func() (*os.File, *os.File, error) {
				if remaining == 0 {
					return nil, nil, want
				}

				remaining--

				return original()
			}

			_, err := startOrdinaryNative(t.Context(), NativeRequest{Executable: "ignored"})
			if !errors.Is(err, want) {
				t.Fatalf("error = %v, want %v", err, want)
			}
			if got := err.Error(); !strings.Contains(got, "create native "+tc.stream) {
				t.Fatalf("error = %q, want it to name create native %s", got, tc.stream)
			}
		})
	}
}

func TestOrdinaryProcessCanceledWaitCanSettleAfterRevoke(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	process, err := startOrdinaryNative(t.Context(), NativeRequest{
		Executable: executable,
		Arguments:  []string{"-test.run=^TestOrdinaryProcessHelper$"},
		Environment: []string{
			"GO_WANT_ORDINARY_PROCESS_HELPER=1",
			"GO_WANT_ORDINARY_PROCESS_BLOCK=1",
			"GOCOVERDIR=" + os.Getenv("GOCOVERDIR"),
		},
		WorkingDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = process.Stdin().Close()
	go func() { _, _ = io.Copy(io.Discard, process.Stdout()) }()
	go func() { _, _ = io.Copy(io.Discard, process.Stderr()) }()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, waitErr := process.Wait(canceled); waitErr == nil {
		t.Fatal("canceled Wait succeeded")
	}
	if revokeErr := process.Revoke(t.Context()); revokeErr != nil {
		t.Fatal(revokeErr)
	}
	result, err := process.Wait(t.Context())
	if err == nil {
		t.Fatal("revoked process returned no exit error")
	}
	if !result.Revoked {
		t.Fatalf("result = %#v, want revoked", result)
	}
}
