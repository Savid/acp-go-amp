package ampacp

import (
	"context"
	"io"
	"os"
	"runtime"
)

func testContainmentOptions(options []Option) []Option {
	baseEnvironment := map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")}
	for _, key := range []string{"AMP_API_KEY", "AMP_URL"} {
		if value, ok := os.LookupEnv(key); ok {
			baseEnvironment[key] = value
		}
	}
	options = append(options, WithProcessIsolation(ProcessIsolation{
		UID: uint32(os.Geteuid()), GID: uint32(os.Getegid()),
		BaseEnvironment: baseEnvironment,
	}))
	options = append(options, func(options *Options) { options.testOnlyNoCredential = true })
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
