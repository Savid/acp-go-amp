package amp

import (
	"context"
	"io"
	"os"
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
		},
		WorkingDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

	_ = process.Stdin().Close()
	stdoutDone := make(chan []byte, 1)
	stderrDone := make(chan []byte, 1)
	go func() { data, _ := io.ReadAll(process.Stdout()); stdoutDone <- data }()
	go func() { data, _ := io.ReadAll(process.Stderr()); stderrDone <- data }()

	result, err := process.Wait(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result != (NativeResult{ExitCode: 0}) {
		t.Fatalf("result = %#v", result)
	}
	if got := string(<-stdoutDone); got != "ordinary stdout" {
		t.Fatalf("stdout = %q", got)
	}
	if got := string(<-stderrDone); got != "ordinary stderr" {
		t.Fatalf("stderr = %q", got)
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
