package main

import (
	"context"
	"io"
	"strings"
	"testing"

	ampacp "github.com/savid/acp-go-amp"
)

func TestMainCompiles(t *testing.T) {}

func TestMainCallsServeWithStore(t *testing.T) {
	if err := serve(context.Background(), strings.NewReader(""), io.Discard, ampacp.WithSessionStore(ampacp.NewInMemorySessionStore())); err != nil {
		t.Fatalf("serve EOF: %v", err)
	}
	orig := serve
	t.Cleanup(func() { serve = orig })
	called := false
	serve = func(ctx context.Context, input io.Reader, output io.Writer, opts ...ampacp.Option) error {
		called = ctx != nil && input != nil && output != nil && len(opts) == 1

		return nil
	}
	main()
	if !called {
		t.Fatal("serve was not called")
	}
}
