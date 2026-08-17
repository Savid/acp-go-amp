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

// testStandaloneStateRoot creates the state root an adapter-level fixture
// claims. A root Linux runner uses /var/lib so every ancestor is protected
// root-owned storage; ordinary temporary directories are intentionally
// world-writable and must be refused by the production authority walk.
func testStandaloneStateRoot(t *testing.T, uid, gid uint32) string {
	t.Helper()

	parent := os.TempDir()
	if runtime.GOOS == platformLinux && os.Geteuid() == 0 {
		parent = "/var/lib"
	}
	base, err := os.MkdirTemp(parent, "acp-go-amp-standalone-state-")
	if err != nil {
		t.Fatalf("create standalone state parent: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(base) })
	if err := os.Chmod(base, 0o700); err != nil {
		t.Fatalf("protect standalone state parent: %v", err)
	}

	root := filepath.Join(base, "state")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("create standalone state root: %v", err)
	}
	if err := os.Chown(root, int(uid), int(gid)); err != nil {
		t.Fatalf("own standalone state root: %v", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("protect standalone state root: %v", err)
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
