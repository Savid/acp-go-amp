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

func testIsolationClaimsStandaloneAuthority(uid uint32) bool {
	return uid != uint32(os.Geteuid())
}

func testContainmentOptions(options []Option) []Option {
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

// testStandaloneStateRoot is the standalone state root every adapter-level
// fixture claims. The authority binds it as the claimed identity's own storage,
// so the leaf has to be a UID:GID-owned mode-0700 directory under a root-owned
// ancestry. The ambient temp root is only the ancestry: it is root-owned and
// world-readable, so naming it directly can never satisfy the claim.
func testStandaloneStateRoot(uid, gid uint32) string {
	root := filepath.Join(os.TempDir(), "acp-go-amp-standalone-state-"+strconv.Itoa(os.Getpid()))
	if err := os.Mkdir(root, 0o700); err != nil && !os.IsExist(err) {
		panic(err)
	}
	if err := os.Chown(root, int(uid), int(gid)); err != nil {
		panic(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		panic(err)
	}

	return root
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
