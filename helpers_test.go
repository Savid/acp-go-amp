package ampacp

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

// absTestPath builds a host-absolute path from POSIX-looking segments, so a
// test states "an absolute working directory" rather than a spelling only
// one platform accepts.
func absTestPath(segments ...string) string {
	root := "/"
	if runtime.GOOS == "windows" {
		root = `C:\`
	}

	return filepath.Join(append([]string{root}, segments...)...)
}

// hostFilePerm maps a POSIX file mode onto the permission bits the host
// actually records. Windows keeps no POSIX mode: os.Chmod there sets only the
// read-only attribute, so a writable file reports 0666 and a read-only one
// 0444 whatever mode created it. Restriction on Windows is the inherited ACL,
// which these bits do not describe.
func hostFilePerm(perm os.FileMode) os.FileMode {
	if runtime.GOOS != "windows" {
		return perm
	}

	if perm&0o200 == 0 {
		return 0o444
	}

	return 0o666
}

// hostDirPerm is hostFilePerm for a directory, which Windows always reports as
// 0777 because the read-only attribute does not apply to one.
func hostDirPerm(perm os.FileMode) os.FileMode {
	if runtime.GOOS != "windows" {
		return perm
	}

	return 0o777
}

// testExecutableName spells a harness file name the way the host resolves
// executables. Windows honours PATHEXT, so a name with no extension is not an
// executable there however it is written to disk.
func testExecutableName(base string) string {
	if runtime.GOOS == "windows" {
		return base + ".exe"
	}

	return base
}

// testHarnessPath stands in for the absolute harness a real probe resolves and
// validates. The agent retains whatever a probe answers, so a stubbed probe
// must answer an absolute path; the tests that use this one never launch a
// child from it.
func testHarnessPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(t.TempDir(), testExecutableName("amp"))
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

func residualCancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return ctx
}
