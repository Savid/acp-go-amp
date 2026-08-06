package ampacp

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

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

// testIsolationIdentity is the identity every adapter-level fixture isolates
// to. The effective identity cannot serve as-is: the policy forbids UID or GID
// zero, so a root test runner is rejected before reaching anything under test.
// The substitute matches the one the native package already uses.
func testIsolationIdentity() (uint32, uint32) {
	uid, gid := os.Geteuid(), os.Getegid()
	if uid == 0 || gid == 0 {
		uid, gid = 65534, 65534
	}

	return uint32(uid), uint32(gid)
}

func testContainmentOptions(options []Option) []Option {
	baseEnvironment := map[string]string{"PATH": os.Getenv("PATH"), "HOME": os.Getenv("HOME")}
	for _, key := range []string{"AMP_API_KEY", "AMP_URL"} {
		if value, ok := os.LookupEnv(key); ok {
			baseEnvironment[key] = value
		}
	}
	uid, gid := testIsolationIdentity()
	options = append(options, WithProcessIsolation(ProcessIsolation{
		UID: uid, GID: gid,
		BaseEnvironment:   baseEnvironment,
		StandaloneOwnerID: "acp-go-amp-tests", StandaloneStateRoot: os.TempDir(),
	}))
	options = append(options, func(options *Options) {
		options.testOnlyNoCredential = true
		options.testOnlyIdentityLockRoot = testIdentityLockRoot()
		if options.testOnlyAuthLoginPlatform == "" {
			options.testOnlyAuthLoginPlatform = platformLinux
		}
	})
	if runtime.GOOS == "darwin" {
		return append(options, WithDarwinBestEffortContainment())
	}

	return options
}

func testIdentityLockRoot() string {
	root := filepath.Join(os.TempDir(), "acp-go-amp-agent-identities-"+strconv.Itoa(os.Getpid()))
	if err := os.Mkdir(root, 0o700); err != nil && !os.IsExist(err) {
		panic(err)
	}

	return root
}

func newTestAgent(options ...Option) *Agent {
	return NewAgent(testContainmentOptions(options)...)
}

func serveTest(ctx context.Context, input io.Reader, output io.Writer, options ...Option) error {
	return Serve(ctx, input, output, testContainmentOptions(options)...)
}
