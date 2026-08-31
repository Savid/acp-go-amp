package ampacp

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// testHarnessPath stands in for the absolute harness a real probe resolves and
// validates. The agent retains whatever a probe answers, so a stubbed probe
// must answer an absolute path; the tests that use this one never launch a
// child from it.
func testHarnessPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(t.TempDir(), "amp")
}

// testScratchDir is a scratch parent the isolated identity can enter. Trees
// generated under it are handed to that identity, and the handoff walks the
// whole ancestry: a t.TempDir leaf sits under a 0700 directory no other
// identity may traverse, so every such handoff is refused.
func testScratchDir(t *testing.T) string {
	t.Helper()
	scratch, err := os.MkdirTemp("", "acp-go-amp-scratch-")
	if err != nil {
		t.Fatalf("create traversable scratch dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	if err = os.Chmod(scratch, 0o711); err != nil {
		t.Fatalf("make scratch dir traversable: %v", err)
	}

	return scratch
}

func testContainmentOptions(options []Option) []Option {
	options = append(options, func(options *Options) {
		options.testOnlyNoCredential = true
		if options.testOnlyAuthLoginPlatform == "" {
			options.testOnlyAuthLoginPlatform = platformLinux
		}
	})

	return options
}

func newTestAgent(options ...Option) *Agent {
	return NewAgent(testContainmentOptions(options)...)
}

func serveTest(ctx context.Context, input io.Reader, output io.Writer, options ...Option) error {
	return Serve(ctx, input, output, testContainmentOptions(options)...)
}

func requireInvalidParamsData(t *testing.T, err error, want map[string]any) {
	t.Helper()
	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, -32602, reqErr.Code)
	require.Equal(t, want, reqErr.Data)
}

func requireInternalErrorData(t *testing.T, err error, want map[string]any) {
	t.Helper()
	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, -32603, reqErr.Code)
	require.Equal(t, want, reqErr.Data)
}
