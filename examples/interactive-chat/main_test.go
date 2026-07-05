package main

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestMainCompiles(t *testing.T) {}

func TestMainCallsServe(t *testing.T) {
	if err := serve(context.Background(), strings.NewReader(""), io.Discard); err != nil {
		t.Fatalf("serve EOF: %v", err)
	}
	orig := serve
	t.Cleanup(func() { serve = orig })
	called := false
	serve = func(ctx context.Context, input io.Reader, output io.Writer) error {
		called = ctx != nil && input != nil && output != nil

		return nil
	}
	main()
	if !called {
		t.Fatal("serve was not called")
	}
}
