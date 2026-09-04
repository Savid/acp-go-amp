package amp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

type failingWriteCloser struct{}

func (failingWriteCloser) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func (failingWriteCloser) Close() error              { return nil }

type interruptProcess struct {
	result    NativeResult
	waitErr   error
	revokeErr error
}

func (p *interruptProcess) Stdin() io.WriteCloser { return failingWriteCloser{} }
func (p *interruptProcess) Stdout() io.ReadCloser { return failingReadCloser{} }
func (p *interruptProcess) Stderr() io.ReadCloser { return failingReadCloser{} }
func (p *interruptProcess) Wait(context.Context) (NativeResult, error) {
	return p.result, p.waitErr
}
func (p *interruptProcess) Revoke(context.Context) error { return p.revokeErr }

func TestInterruptPreservesContainmentFailureAfterRevocation(t *testing.T) {
	process := &interruptProcess{
		result:  NativeResult{Revoked: true},
		waitErr: fmt.Errorf("authority could not prove vacancy: %w", ErrContainmentIncomplete),
	}
	turn := &Turn{process: process, stdin: failingWriteCloser{}}

	if err := turn.Interrupt(t.Context()); !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("Interrupt = %v, want containment failure", err)
	}
}

func TestInterruptIgnoresDetachedRevokeContextAfterTerminalWait(t *testing.T) {
	process := &interruptProcess{
		result:    NativeResult{Revoked: true},
		revokeErr: context.Canceled,
	}
	turn := &Turn{process: process, stdin: failingWriteCloser{}}

	if err := turn.Interrupt(t.Context()); err != nil {
		t.Fatalf("Interrupt = %v, want terminal Wait result", err)
	}
}

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
