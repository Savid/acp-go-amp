package ampacp

import (
	"context"
	"io"
	"runtime"
)

func testContainmentOptions(options []Option) []Option {
	if runtime.GOOS == "darwin" {
		return append(options, WithDarwinBestEffortContainment())
	}

	return options
}

func newTestAgent(options ...Option) *Agent {
	return NewAgent(testContainmentOptions(options)...)
}

func serveTest(ctx context.Context, input io.Reader, output io.Writer, options ...Option) error {
	return Serve(ctx, input, output, testContainmentOptions(options)...)
}
