//go:build unix

package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"syscall"
	"testing"

	ampacp "github.com/savid/acp-go-amp"
)

func TestPendingSignalReturnsSignal(t *testing.T) {
	signals := make(chan os.Signal, 1)
	signals <- syscall.SIGTERM
	if got := pendingSignal(signals); got != syscall.SIGTERM {
		t.Fatalf("pendingSignal = %v, want SIGTERM", got)
	}
	if got := signalCode(syscall.SIGTERM); got != 128+int(syscall.SIGTERM) {
		t.Fatalf("signalCode(SIGTERM) = %d", got)
	}
}

func TestRunReturnsSignalCode(t *testing.T) {
	originalServe := serve
	originalShutdown := shutdownOpenTelemetry
	t.Cleanup(func() {
		serve = originalServe
		shutdownOpenTelemetry = originalShutdown
	})

	serve = func(ctx context.Context, _ io.Reader, _ io.Writer, _ ...ampacp.Option) error {
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			t.Errorf("kill: %v", err)
		}
		<-ctx.Done()

		return ctx.Err()
	}
	shutdownOpenTelemetry = func(context.Context, func(context.Context) error) error { return nil }

	code := run(context.Background(), nil, bytes.NewBuffer(nil), bytes.NewBuffer(nil), bytes.NewBuffer(nil))
	if code != 128+int(syscall.SIGTERM) {
		t.Fatalf("code = %d, want %d", code, 128+int(syscall.SIGTERM))
	}
}
