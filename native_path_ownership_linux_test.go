//go:build linux

package ampacp

import (
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"golang.org/x/sys/unix"
)

func TestGeneratedNativeTreeDistinctIdentityTraversal(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	parent, err := os.MkdirTemp("/tmp", "acp-go-amp-ownership-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })

	if chmodErr := os.Chmod(parent, 0o711); chmodErr != nil {
		t.Fatal(chmodErr)
	}

	control := filepath.Join(parent, "control")
	native := filepath.Join(parent, "native")
	if mkdirErr := os.Mkdir(control, 0o700); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	if writeErr := os.WriteFile(filepath.Join(control, "secret"), []byte("root"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if mkdirErr := os.Mkdir(native, 0o700); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	if writeErr := os.WriteFile(filepath.Join(native, "input"), []byte("ok"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
	if handoffErr := handoffGeneratedNativeTree(native, isolation); handoffErr != nil {
		t.Fatal(handoffErr)
	}

	command := exec.Command(
		"/bin/sh",
		"-c",
		`set -eu
test "$(cat "$1/input")" = ok
printf native >"$1/output"
if cat "$2/secret" >/dev/null 2>&1; then exit 42; fi`,
		"sh",
		native,
		control,
	)
	command.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: isolation.UID, Gid: isolation.GID, Groups: []uint32{}},
	}
	if output, combinedErr := command.CombinedOutput(); combinedErr != nil {
		t.Fatalf("dropped-identity proof: %v: %s", combinedErr, output)
	}

	contents, err := os.ReadFile(filepath.Join(native, "output"))
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "native" {
		t.Fatalf("native output = %q", contents)
	}
	if contents, err := os.ReadFile(filepath.Join(control, "secret")); err != nil || string(contents) != "root" {
		t.Fatalf("trusted control changed: %q, %v", contents, err)
	}
}

func TestGeneratedNativeTreeRejectsUntraversableCallerRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	parent, err := os.MkdirTemp("/tmp", "acp-go-amp-caller-root-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(parent) })

	native := filepath.Join(parent, "native")
	if err := os.Mkdir(native, 0o700); err != nil {
		t.Fatal(err)
	}

	isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
	if err := handoffGeneratedNativeTree(native, isolation); err == nil {
		t.Fatal("0700 caller root accepted")
	}
	if err := os.Chmod(parent, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := handoffGeneratedNativeTree(native, isolation); err != nil {
		t.Fatalf("0711 protected caller root: %v", err)
	}
}

func TestGeneratedNativeTreeRejectsUnsafeEntries(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}

	for _, testCase := range []struct {
		name string
		seed func(string) error
	}{
		{name: "symlink", seed: func(root string) error {
			return os.Symlink("/etc/passwd", filepath.Join(root, "entry"))
		}},
		{name: "hardlink", seed: func(root string) error {
			first := filepath.Join(root, "first")
			if err := os.WriteFile(first, []byte("x"), 0o600); err != nil {
				return err
			}

			return os.Link(first, filepath.Join(root, "second"))
		}},
		{name: "broad mode", seed: func(root string) error {
			return os.WriteFile(filepath.Join(root, "entry"), []byte("x"), 0o644)
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			parent, err := os.MkdirTemp("/tmp", "acp-go-amp-unsafe-*")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(parent) })
			if err := os.Chmod(parent, 0o711); err != nil {
				t.Fatal(err)
			}

			native := filepath.Join(parent, "native")
			if err := os.Mkdir(native, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := testCase.seed(native); err != nil {
				t.Fatal(err)
			}

			isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
			if err := handoffGeneratedNativeTree(native, isolation); err == nil || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsafe tree result = %v", err)
			}
		})
	}
}

// nativeOwnershipTestRoot builds a trusted 0700 native root under a 0711
// caller root, which is the shape the handoff accepts.
func nativeOwnershipTestRoot(t *testing.T) string {
	t.Helper()
	requireNativeOwnershipRoot(t)

	parent, err := os.MkdirTemp("/tmp", "acp-go-amp-refusal-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o711))

	native := filepath.Join(parent, "native")
	require.NoError(t, os.Mkdir(native, 0o700))

	return native
}

func requireNativeOwnershipRoot(t *testing.T) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
}

func nativeOwnershipTestIdentity() *ProcessIsolation {
	return &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
}

func nativeOwnershipOwner(t *testing.T, path string) (uint32, uint32) {
	t.Helper()

	var stat unix.Stat_t
	require.NoError(t, unix.Stat(path, &stat))

	return stat.Uid, stat.Gid
}

// openNativeOwnershipPathDescriptor returns an O_PATH descriptor. O_PATH
// descriptors answer fstat but reject every operation that reads or writes the
// inode, which is how these tests make an already-validated descriptor stop
// answering without racing the filesystem.
func openNativeOwnershipPathDescriptor(t *testing.T, path string, directory bool) *os.File {
	t.Helper()

	flags := unix.O_PATH | unix.O_CLOEXEC
	if directory {
		flags |= unix.O_DIRECTORY
	}

	fd, err := unix.Open(path, flags, 0)
	require.NoError(t, err)

	file := os.NewFile(uintptr(fd), path)
	t.Cleanup(func() { _ = file.Close() })

	return file
}

// TestNativeOwnershipTraversalRejectsRelativeRoot proves the traversal refuses
// a relative root outright: a relative walk would resolve against the working
// directory the agent controls, not the trusted tree the caller named.
func TestNativeOwnershipTraversalRejectsRelativeRoot(t *testing.T) {
	requireNativeOwnershipRoot(t)

	err := handoffGeneratedNativeTree("relative/native", nativeOwnershipTestIdentity())
	require.ErrorContains(t, err, "native path must be absolute")
}

// TestNativeOwnershipTraversalValidatesFilesystemRootBeforeComponents proves
// the filesystem root itself is validated before any component is opened, so a
// compromised "/" cannot be walked through on the way to a trusted leaf.
func TestNativeOwnershipTraversalValidatesFilesystemRootBeforeComponents(t *testing.T) {
	requireNativeOwnershipRoot(t)

	var seen []bool

	wantErr := errors.New("root ancestry refused")
	directory, err := openNativeOwnershipDirectory("/etc/hosts", func(_ unix.Stat_t, final bool) error {
		seen = append(seen, final)

		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, directory)
	require.Equal(t, []bool{false}, seen, "traversal continued past a refused filesystem root")
}

// TestNativeOwnershipTraversalOpensFilesystemRootItself proves "/" is a valid
// traversal target and is presented to the validator as the final component
// rather than as an ancestor.
func TestNativeOwnershipTraversalOpensFilesystemRootItself(t *testing.T) {
	requireNativeOwnershipRoot(t)

	var seen []bool

	directory, err := openNativeOwnershipDirectory("/", func(_ unix.Stat_t, final bool) error {
		seen = append(seen, final)

		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = directory.Close() })
	require.Equal(t, []bool{true}, seen)

	var opened, root unix.Stat_t
	require.NoError(t, unix.Fstat(int(directory.Fd()), &opened))
	require.NoError(t, unix.Stat("/", &root))
	require.Equal(t, root.Ino, opened.Ino)
	require.Equal(t, root.Dev, opened.Dev)
}

// TestNativeOwnershipTraversalPropagatesMissingComponent proves a missing
// component surfaces the kernel's own error rather than being treated as an
// empty tree that needs no handoff.
func TestNativeOwnershipTraversalPropagatesMissingComponent(t *testing.T) {
	requireNativeOwnershipRoot(t)

	native := nativeOwnershipTestRoot(t)

	err := handoffGeneratedNativeTree(
		filepath.Join(filepath.Dir(native), "absent"), nativeOwnershipTestIdentity(),
	)
	require.ErrorIs(t, err, unix.ENOENT)
}

// TestNativeOwnershipTraversalFailsClosedOnKernelFaults proves each descriptor
// syscall the traversal depends on aborts the walk when it fails. A traversal
// that swallowed any of these would return a descriptor whose ancestry it never
// actually proved.
func TestNativeOwnershipTraversalFailsClosedOnKernelFaults(t *testing.T) {
	requireNativeOwnershipRoot(t)

	accept := func(unix.Stat_t, bool) error { return nil }

	t.Run("filesystem root unopenable", func(t *testing.T) {
		previous := nativeOwnershipOpenFilesystemRoot
		nativeOwnershipOpenFilesystemRoot = func() (int, error) { return -1, unix.EMFILE }

		t.Cleanup(func() { nativeOwnershipOpenFilesystemRoot = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EMFILE)
		require.Nil(t, directory)
	})

	t.Run("filesystem root unstattable", func(t *testing.T) {
		previous := nativeOwnershipFstat
		nativeOwnershipFstat = func(int, *unix.Stat_t) error { return unix.EIO }

		t.Cleanup(func() { nativeOwnershipFstat = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})

	t.Run("component unstattable", func(t *testing.T) {
		previous := nativeOwnershipFstat
		calls := 0
		nativeOwnershipFstat = func(fd int, stat *unix.Stat_t) error {
			calls++
			if calls == 1 {
				return previous(fd, stat)
			}

			return unix.EIO
		}

		t.Cleanup(func() { nativeOwnershipFstat = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
		require.Equal(t, 2, calls, "traversal statted past the faulted component")
	})

	t.Run("parent descriptor unreleasable", func(t *testing.T) {
		previous := nativeOwnershipClose
		nativeOwnershipClose = func(fd int) error {
			_ = previous(fd)

			return unix.EIO
		}

		t.Cleanup(func() { nativeOwnershipClose = previous })

		directory, err := openNativeOwnershipDirectory("/etc", accept)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})
}

// TestGeneratedNativeAncestorStatesEachRefusal pins the exact reason the
// generated-tree ancestry validator refuses each unsafe shape. These reasons
// are the containment contract: an ancestor that is not trusted-owned, a leaf
// that is not exactly 0700, a non-sticky writable ancestor, or an ancestor the
// dropped identity cannot traverse.
func TestGeneratedNativeAncestorStatesEachRefusal(t *testing.T) {
	const (
		trustedUID = uint32(0)
		trustedGID = uint32(0)
		targetUID  = uint32(65534)
		targetGID  = uint32(65534)
	)

	directory := func(mode uint32, uid, gid uint32) unix.Stat_t {
		return unix.Stat_t{Mode: unix.S_IFDIR | mode, Uid: uid, Gid: gid}
	}

	for _, testCase := range []struct {
		name  string
		stat  unix.Stat_t
		final bool
		want  string
	}{
		{
			name: "not a directory",
			stat: unix.Stat_t{Mode: unix.S_IFREG | 0o700},
			want: "not a trusted directory",
		},
		{
			name: "ancestor owned by another identity",
			stat: directory(0o755, targetUID, targetGID),
			want: "not a trusted directory",
		},
		{
			name:  "leaf is not exactly 0700",
			stat:  directory(0o750, trustedUID, trustedGID),
			final: true,
			want:  "generated native root mode 0750 is unsafe",
		},
		{
			name: "group-writable ancestor without sticky bit",
			stat: directory(0o771, trustedUID, trustedGID),
			want: "0771 is writable without sticky protection",
		},
		{
			name: "world-writable ancestor without sticky bit",
			stat: directory(0o717, trustedUID, trustedGID),
			want: "0717 is writable without sticky protection",
		},
		{
			name: "ancestor the target identity cannot traverse",
			stat: directory(0o700, trustedUID, trustedGID),
			want: "not traversable by the target identity",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateGeneratedNativeAncestor(
				testCase.stat, testCase.final, trustedUID, trustedGID, targetUID, targetGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o711, trustedUID, trustedGID), false, trustedUID, trustedGID, targetUID, targetGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o1777, trustedUID, trustedGID), false, trustedUID, trustedGID, targetUID, targetGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o700, trustedUID, trustedGID), true, trustedUID, trustedGID, targetUID, targetGID,
	))
}

// TestGeneratedNativeAncestorUnderASharedIdentityAcceptsOnlyRootAncestors
// proves how far the generated-tree ancestry rule relaxes when the trusted
// identity is also the target identity. Nothing separates the wrapper from the
// dropped identity in that shape, so the root-owned directories every path is
// reached through are acceptable ancestors — and nothing else is: a third
// identity's ancestor, an ancestor root left writable without sticky
// protection, and a generated root root still owns are all refused.
func TestGeneratedNativeAncestorUnderASharedIdentityAcceptsOnlyRootAncestors(t *testing.T) {
	const (
		sharedUID = uint32(1000)
		sharedGID = uint32(1000)
	)

	directory := func(mode uint32, uid, gid uint32) unix.Stat_t {
		return unix.Stat_t{Mode: unix.S_IFDIR | mode, Uid: uid, Gid: gid}
	}

	for _, testCase := range []struct {
		name  string
		stat  unix.Stat_t
		final bool
		want  string
	}{
		{
			name: "ancestor owned by a third identity",
			stat: directory(0o755, 4242, 4242),
			want: "generated native path ancestor is uid=4242 gid=4242; " +
				"run the supervisor as root to isolate the agent identity, " +
				"or place the native directory under a path the agent identity owns",
		},
		{
			name: "ancestor owned by root with a foreign group",
			stat: directory(0o755, 0, 4242),
			want: "generated native path ancestor is uid=0 gid=4242",
		},
		{
			name: "world-writable root-owned ancestor without sticky protection",
			stat: directory(0o777, 0, 0),
			want: "0777 is writable without sticky protection",
		},
		{
			name: "root-owned ancestor the target identity cannot traverse",
			stat: directory(0o700, 0, 0),
			want: "not traversable by the target identity",
		},
		{
			name:  "generated root still owned by root",
			stat:  directory(0o700, 0, 0),
			final: true,
			want:  "generated native path ancestry is not a trusted directory",
		},
		{
			name: "not a directory",
			stat: unix.Stat_t{Mode: unix.S_IFREG | 0o700},
			want: "generated native path ancestry is not a trusted directory",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateGeneratedNativeAncestor(
				testCase.stat, testCase.final, sharedUID, sharedGID, sharedUID, sharedGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o755, 0, 0), false, sharedUID, sharedGID, sharedUID, sharedGID,
	), "the root-owned ancestry every home directory is reached through was refused")
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o1777, 0, 0), false, sharedUID, sharedGID, sharedUID, sharedGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o711, sharedUID, sharedGID), false, sharedUID, sharedGID, sharedUID, sharedGID,
	))
	require.NoError(t, validateGeneratedNativeAncestor(
		directory(0o700, sharedUID, sharedGID), true, sharedUID, sharedGID, sharedUID, sharedGID,
	))
}

// TestNativeIdentityTraversalUsesTheApplicableModeClass proves traversability
// is decided by the single mode class the kernel would apply — owner, then
// group, then other — and never by a union of them. Reading the wrong class
// would let the handoff accept a path the dropped identity cannot enter, or
// refuse one it can.
func TestNativeIdentityTraversalUsesTheApplicableModeClass(t *testing.T) {
	const (
		uid = uint32(65534)
		gid = uint32(65535)
	)

	for _, testCase := range []struct {
		name string
		stat unix.Stat_t
		want bool
	}{
		{name: "owner execute", stat: unix.Stat_t{Uid: uid, Gid: 0, Mode: 0o100}, want: true},
		{name: "owner without execute ignores group", stat: unix.Stat_t{Uid: uid, Gid: gid, Mode: 0o011}},
		{name: "group execute", stat: unix.Stat_t{Uid: 0, Gid: gid, Mode: 0o010}, want: true},
		{name: "group without execute ignores other", stat: unix.Stat_t{Uid: 0, Gid: gid, Mode: 0o101}},
		{name: "other execute", stat: unix.Stat_t{Uid: 0, Gid: 0, Mode: 0o001}, want: true},
		{name: "other without execute", stat: unix.Stat_t{Uid: 0, Gid: 0, Mode: 0o110}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, nativeIdentityCanTraverse(testCase.stat, uid, gid))
		})
	}
}

// TestNativeOwnershipHandoffRefusesUnsafeRootMode proves a native root that is
// not exactly 0700 is refused before any inode is handed to the dropped
// identity.
func TestNativeOwnershipHandoffRefusesUnsafeRootMode(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	seed := filepath.Join(native, "input")
	require.NoError(t, os.WriteFile(seed, []byte("x"), 0o600))

	directory := openNativeOwnershipPathDescriptor(t, native, true)
	require.NoError(t, os.Chmod(native, 0o750))

	err := handoffNativeOwnershipDirectory(directory, 0, 0, 65534, 65534)
	require.ErrorContains(t, err, "generated native directory mode 0750 is unsafe")

	uid, gid := nativeOwnershipOwner(t, seed)
	require.Equal(t, uint32(0), uid)
	require.Equal(t, uint32(0), gid)
}

// TestNativeOwnershipHandoffRefusesUnenumerableDirectory proves a directory
// whose contents cannot be listed is refused rather than chowned blind. Handing
// the root over without enumerating it would transfer whatever it contains.
func TestNativeOwnershipHandoffRefusesUnenumerableDirectory(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	seed := filepath.Join(native, "input")
	require.NoError(t, os.WriteFile(seed, []byte("x"), 0o600))

	directory := openNativeOwnershipPathDescriptor(t, native, true)

	err := handoffNativeOwnershipDirectory(directory, 0, 0, 65534, 65534)
	require.Error(t, err)

	uid, gid := nativeOwnershipOwner(t, native)
	require.Equal(t, uint32(0), uid, "unenumerable root was handed over anyway")
	require.Equal(t, uint32(0), gid)
}

// nativeOwnershipTestEntry is a directory entry whose name the kernel would
// never produce through ReadDir.
type nativeOwnershipTestEntry struct {
	name string
}

func (entry nativeOwnershipTestEntry) Name() string         { return entry.name }
func (nativeOwnershipTestEntry) IsDir() bool                { return false }
func (nativeOwnershipTestEntry) Type() os.FileMode          { return 0 }
func (nativeOwnershipTestEntry) Info() (os.FileInfo, error) { return nil, unix.EBADF }

// TestNativeOwnershipHandoffRefusesEscapingEntryName proves the handoff never
// resolves an entry name that could leave the directory it is walking. Every
// name reached from here is fed straight back to openat against the directory
// descriptor, so "..", "." and any name carrying a separator would hand the
// dropped identity an inode outside the generated tree.
func TestNativeOwnershipHandoffRefusesEscapingEntryName(t *testing.T) {
	native := nativeOwnershipTestRoot(t)

	directory, err := os.Open(native)
	require.NoError(t, err)
	t.Cleanup(func() { _ = directory.Close() })

	for _, name := range []string{".", "..", "nested/leaf"} {
		t.Run(name, func(t *testing.T) {
			previous := nativeOwnershipReadDir
			nativeOwnershipReadDir = func(*os.File) ([]os.DirEntry, error) {
				return []os.DirEntry{nativeOwnershipTestEntry{name: name}}, nil
			}

			t.Cleanup(func() { nativeOwnershipReadDir = previous })

			err := handoffNativeOwnershipDirectory(directory, 0, 0, 65534, 65534)
			require.ErrorContains(t, err, "invalid generated native entry")

			uid, gid := nativeOwnershipOwner(t, native)
			require.Equal(t, uint32(0), uid, "escaping entry name still reached the handoff")
			require.Equal(t, uint32(0), gid)
		})
	}
}

// TestNativeOwnershipHandoffDescendsSubdirectories proves the handoff is
// recursive: a nested directory and its contents are transferred with their
// modes intact, so the dropped identity owns the whole generated tree and
// nothing outside it.
func TestNativeOwnershipHandoffDescendsSubdirectories(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	nested := filepath.Join(native, "nested")
	require.NoError(t, os.Mkdir(nested, 0o700))

	leaf := filepath.Join(nested, "leaf")
	require.NoError(t, os.WriteFile(leaf, []byte("x"), 0o600))

	isolation := nativeOwnershipTestIdentity()
	require.NoError(t, handoffGeneratedNativeTree(native, isolation))

	for path, mode := range map[string]os.FileMode{
		native: 0o700,
		nested: 0o700,
		leaf:   0o600,
	} {
		uid, gid := nativeOwnershipOwner(t, path)
		require.Equal(t, isolation.UID, uid, path)
		require.Equal(t, isolation.GID, gid, path)

		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, mode, info.Mode().Perm(), path)
	}
}

// TestNativeOwnershipHandoffRefusesNonRegularEntry proves the handoff refuses
// any inode that is neither a directory nor a regular file. Chowning a FIFO or
// device node to the dropped identity would hand it a channel the trusted
// process still holds open.
func TestNativeOwnershipHandoffRefusesNonRegularEntry(t *testing.T) {
	native := nativeOwnershipTestRoot(t)
	fifo := filepath.Join(native, "channel")
	require.NoError(t, unix.Mkfifo(fifo, 0o600))

	err := handoffGeneratedNativeTree(native, nativeOwnershipTestIdentity())
	require.ErrorContains(t, err, "unsupported type")

	uid, gid := nativeOwnershipOwner(t, fifo)
	require.Equal(t, uint32(0), uid)
	require.Equal(t, uint32(0), gid)
}

// TestNativeOwnershipEntryRejectsUnusableDescriptor proves an entry descriptor
// the kernel no longer answers for is refused instead of being classified by a
// zero-valued stat, which would look like an unsupported type at best and a
// directory at worst.
func TestNativeOwnershipEntryRejectsUnusableDescriptor(t *testing.T) {
	requireNativeOwnershipRoot(t)

	entry, err := os.Open(os.DevNull)
	require.NoError(t, err)
	require.NoError(t, entry.Close())

	require.ErrorIs(t, handoffNativeOwnershipEntry(entry, 0, 0, 65534, 65534), unix.EBADF)
}

// TestValidateHandoffNativeInodeRefusesDriftedInodes proves the pre-chown
// revalidation catches every way the inode behind an accepted descriptor can
// stop being the trusted inode the traversal approved.
func TestValidateHandoffNativeInodeRefusesDriftedInodes(t *testing.T) {
	requireNativeOwnershipRoot(t)

	native := nativeOwnershipTestRoot(t)
	regular := filepath.Join(native, "file")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))

	file, err := os.Open(regular)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	t.Run("unusable descriptor", func(t *testing.T) {
		require.ErrorIs(
			t,
			validateHandoffNativeInode(-1, unix.S_IFREG, 0, 0, 65534, 65534, true),
			unix.EBADF,
		)
	})

	t.Run("inode type changed", func(t *testing.T) {
		err := validateHandoffNativeInode(int(file.Fd()), unix.S_IFDIR, 0, 0, 65534, 65534, false)
		require.ErrorContains(t, err, "inode type 0100000 changed")
	})

	t.Run("inode owner changed", func(t *testing.T) {
		require.NoError(t, unix.Fchown(int(file.Fd()), 65534, 65534))
		t.Cleanup(func() { require.NoError(t, unix.Fchown(int(file.Fd()), 0, 0)) })

		err := validateHandoffNativeInode(int(file.Fd()), unix.S_IFREG, 0, 0, 65534, 65534, true)
		require.ErrorContains(t, err, "owner changed to uid=65534 gid=65534")
	})
}

// TestChownAndVerifyNativeInodeProvesTheTransfer proves the handoff never
// reports success on an unproven transfer: the chown must succeed, the re-read
// must confirm the new owner and the expected inode type, and a file must still
// have exactly one link afterwards.
func TestChownAndVerifyNativeInodeProvesTheTransfer(t *testing.T) {
	requireNativeOwnershipRoot(t)

	native := nativeOwnershipTestRoot(t)
	regular := filepath.Join(native, "file")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0o600))

	t.Run("descriptor cannot be chowned", func(t *testing.T) {
		descriptor := openNativeOwnershipPathDescriptor(t, regular, false)
		require.ErrorIs(
			t,
			chownAndVerifyNativeInode(int(descriptor.Fd()), unix.S_IFREG, 65534, 65534, true),
			unix.EBADF,
		)

		uid, gid := nativeOwnershipOwner(t, regular)
		require.Equal(t, uint32(0), uid)
		require.Equal(t, uint32(0), gid)
	})

	t.Run("transferred inode is not the expected type", func(t *testing.T) {
		file, err := os.Open(regular)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })
		t.Cleanup(func() { require.NoError(t, unix.Fchown(int(file.Fd()), 0, 0)) })

		err = chownAndVerifyNativeInode(int(file.Fd()), unix.S_IFDIR, 65534, 65534, false)
		require.ErrorContains(t, err, "ownership handoff could not be proven")
	})

	t.Run("transferred inode cannot be re-read", func(t *testing.T) {
		file, err := os.Open(regular)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })
		t.Cleanup(func() { require.NoError(t, unix.Fchown(int(file.Fd()), 0, 0)) })

		previous := nativeOwnershipFstat
		nativeOwnershipFstat = func(int, *unix.Stat_t) error { return unix.EIO }

		t.Cleanup(func() { nativeOwnershipFstat = previous })

		err = chownAndVerifyNativeInode(int(file.Fd()), unix.S_IFREG, 65534, 65534, true)
		require.ErrorIs(t, err, unix.EIO)
	})

	t.Run("transferred file gained a link", func(t *testing.T) {
		linked := filepath.Join(native, "linked")
		require.NoError(t, os.WriteFile(linked, []byte("x"), 0o600))

		file, err := os.Open(linked)
		require.NoError(t, err)
		t.Cleanup(func() { _ = file.Close() })

		require.NoError(t, os.Link(linked, filepath.Join(native, "alias")))

		err = chownAndVerifyNativeInode(int(file.Fd()), unix.S_IFREG, 65534, 65534, true)
		require.ErrorContains(t, err, "has 2 links after handoff")
	})
}

const (
	nativeOwnedCovUID = uint32(65534)
	nativeOwnedCovGID = uint32(65534)
)

func nativeOwnedCovIsolation() *ProcessIsolation {
	return &ProcessIsolation{
		UID: nativeOwnedCovUID, GID: nativeOwnedCovGID,
		BaseEnvironment: map[string]string{},
	}
}

// nativeOwnedCovParent builds the durable-file parent the writer accepts: a
// 0700 directory the dropped identity owns outright, under a 0711 trusted
// ancestry that identity can traverse but not write. It returns the trusted
// ancestor as well, so a case can also stage a parent the writer must refuse.
func nativeOwnedCovParent(t *testing.T) (string, string) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("native-owned file writes require root")
	}
	ancestor, err := os.MkdirTemp("/tmp", "acp-go-amp-owned-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(ancestor) })
	require.NoError(t, os.Chmod(ancestor, 0o711))

	parent := filepath.Join(ancestor, "state")
	require.NoError(t, os.Mkdir(parent, 0o700))
	require.NoError(t, os.Chown(parent, int(nativeOwnedCovUID), int(nativeOwnedCovGID)))

	return parent, ancestor
}

func nativeOwnedCovStat(t *testing.T, path string) unix.Stat_t {
	t.Helper()

	var stat unix.Stat_t
	require.NoError(t, unix.Lstat(path, &stat))

	return stat
}

// TestNativeOwnedFileSkipsAnUnisolatedLaunch proves both native-ownership
// entry points treat "no isolation policy" as "nothing to hand over" rather
// than as a reason to chown to whatever identity happens to be zero. A launch
// that never asked for containment must not have its generated tree or its
// durable files re-owned.
func TestNativeOwnedFileSkipsAnUnisolatedLaunch(t *testing.T) {
	require.NoError(t, handoffGeneratedNativeTree("relative/native", nil))

	path := filepath.Join(t.TempDir(), "absent")
	require.NoError(t, writeNativeOwnedFile(path, []byte("ignored"), nil))
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist,
		"an unisolated launch must not have created the file at all",
	)
}

// TestDurableNativeAncestorStatesEachRefusal pins the exact reason the durable
// ancestry validator refuses each unsafe shape. A durable native file is
// written by the trusted process and then read and rewritten by the isolated
// one, so its parent ancestry has to be provably neither a foreign directory,
// nor writable by anyone outside the two identities involved, nor a leaf the
// target does not fully own, nor untraversable.
func TestDurableNativeAncestorStatesEachRefusal(t *testing.T) {
	const (
		trustedUID = uint32(1000)
		trustedGID = uint32(1001)
	)

	for name, testCase := range map[string]struct {
		stat  unix.Stat_t
		final bool
		want  string
	}{
		"not a directory": {
			stat: unix.Stat_t{Mode: unix.S_IFREG | 0o711, Uid: trustedUID, Gid: trustedGID},
			want: "native-owned path ancestry is not a directory",
		},
		"third-party owner": {
			stat: unix.Stat_t{Mode: unix.S_IFDIR | 0o711, Uid: trustedUID + 7, Gid: trustedGID + 7},
			want: "native-owned path ancestor is uid=1007 gid=1008",
		},
		"writable without sticky": {
			stat: unix.Stat_t{Mode: unix.S_IFDIR | 0o777, Uid: trustedUID, Gid: trustedGID},
			want: "native-owned path ancestor mode 0777 is writable",
		},
		"target-owned and writable is still refused": {
			stat: unix.Stat_t{Mode: unix.S_IFDIR | 0o1777, Uid: nativeOwnedCovUID, Gid: nativeOwnedCovGID},
			want: "native-owned path ancestor mode 01777 is writable",
		},
		"leaf not owned by the target": {
			stat:  unix.Stat_t{Mode: unix.S_IFDIR | 0o711, Uid: trustedUID, Gid: trustedGID},
			final: true,
			want:  "native-owned directory is not safely owned by the target identity",
		},
		"leaf owned by the target but not fully accessible": {
			stat:  unix.Stat_t{Mode: unix.S_IFDIR | 0o500, Uid: nativeOwnedCovUID, Gid: nativeOwnedCovGID},
			final: true,
			want:  "native-owned directory is not safely owned by the target identity",
		},
		"untraversable ancestor": {
			stat: unix.Stat_t{Mode: unix.S_IFDIR | 0o700, Uid: trustedUID, Gid: trustedGID},
			want: "native-owned path ancestry is not traversable by the target identity",
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, validateDurableNativeAncestor(
				testCase.stat, testCase.final, trustedUID, trustedGID,
				nativeOwnedCovUID, nativeOwnedCovGID,
			), testCase.want)
		})
	}

	require.NoError(t, validateDurableNativeAncestor(
		unix.Stat_t{Mode: unix.S_IFDIR | 0o1777, Uid: trustedUID, Gid: trustedGID}, false,
		trustedUID, trustedGID, nativeOwnedCovUID, nativeOwnedCovGID,
	), "a sticky world-writable trusted ancestor is the one writable shape the walk accepts")
	require.NoError(t, validateDurableNativeAncestor(
		unix.Stat_t{Mode: unix.S_IFDIR | 0o700, Uid: nativeOwnedCovUID, Gid: nativeOwnedCovGID}, true,
		trustedUID, trustedGID, nativeOwnedCovUID, nativeOwnedCovGID,
	))
}

// TestDurableNativeAncestorUnderASharedIdentityAcceptsOnlyRootAncestors proves
// the same bound on the native-owned ancestry rule. The leaf contract is
// untouched: it still has to belong to the identity outright with full owner
// rights, and only its ancestry admits the root-owned components a home
// directory hangs from.
func TestDurableNativeAncestorUnderASharedIdentityAcceptsOnlyRootAncestors(t *testing.T) {
	const (
		sharedUID = uint32(1000)
		sharedGID = uint32(1000)
	)

	directory := func(mode uint32, uid, gid uint32) unix.Stat_t {
		return unix.Stat_t{Mode: unix.S_IFDIR | mode, Uid: uid, Gid: gid}
	}

	for name, testCase := range map[string]struct {
		stat  unix.Stat_t
		final bool
		want  string
	}{
		"ancestor owned by a third identity": {
			stat: directory(0o755, 4242, 4242),
			want: "native-owned path ancestor is uid=4242 gid=4242; " +
				"run the supervisor as root to isolate the agent identity, " +
				"or place the native directory under a path the agent identity owns",
		},
		"ancestor owned by root with a foreign group": {
			stat: directory(0o755, 0, 4242),
			want: "native-owned path ancestor is uid=0 gid=4242",
		},
		"world-writable root-owned ancestor without sticky protection": {
			stat: directory(0o777, 0, 0),
			want: "native-owned path ancestor mode 0777 is writable",
		},
		"root-owned ancestor the target identity cannot traverse": {
			stat: directory(0o700, 0, 0),
			want: "not traversable by the target identity",
		},
		"leaf still owned by root": {
			stat:  directory(0o700, 0, 0),
			final: true,
			want:  "native-owned path ancestor is uid=0 gid=0",
		},
		"not a directory": {
			stat: unix.Stat_t{Mode: unix.S_IFREG | 0o700},
			want: "native-owned path ancestry is not a directory",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateDurableNativeAncestor(
				testCase.stat, testCase.final, sharedUID, sharedGID, sharedUID, sharedGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	require.NoError(t, validateDurableNativeAncestor(
		directory(0o755, 0, 0), false, sharedUID, sharedGID, sharedUID, sharedGID,
	), "the root-owned ancestry every home directory is reached through was refused")
	require.NoError(t, validateDurableNativeAncestor(
		directory(0o1777, 0, 0), false, sharedUID, sharedGID, sharedUID, sharedGID,
	))
	require.NoError(t, validateDurableNativeAncestor(
		directory(0o700, sharedUID, sharedGID), true, sharedUID, sharedGID, sharedUID, sharedGID,
	))
}

// TestNativeOwnershipWalksARootOwnedAncestryUnderASharedIdentity proves both
// native-path users accept the shape a wrapper that never dropped privilege
// presents: its own identity is the isolated identity, and the tree it was
// handed hangs from root-owned directories it will never own. The effective
// identity is staged through its seams so the proof does not depend on which
// identity runs the tests; the chowns behind both users still need the root the
// rest of this file requires, so the durable file is seeded as the shared
// identity's own and the write reopens the inode it finds.
func TestNativeOwnershipWalksARootOwnedAncestryUnderASharedIdentity(t *testing.T) {
	requireNativeOwnershipRoot(t)

	const (
		sharedUID = uint32(65534)
		sharedGID = uint32(65534)
	)

	parent, err := os.MkdirTemp("/tmp", "acp-go-amp-shared-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o711))

	native := filepath.Join(parent, "native")
	require.NoError(t, os.Mkdir(native, 0o700))

	seed := filepath.Join(native, "seed")
	require.NoError(t, os.WriteFile(seed, []byte("x"), 0o600))

	durable := filepath.Join(native, "mcp.json")
	require.NoError(t, os.WriteFile(durable, []byte("{}\n"), 0o600))

	for _, path := range []string{seed, durable, native} {
		require.NoError(t, os.Chown(path, int(sharedUID), int(sharedGID)))
	}

	realUID, realGID := effectiveUIDSource, effectiveGIDSource
	t.Cleanup(func() { effectiveUIDSource, effectiveGIDSource = realUID, realGID })

	effectiveUIDSource = func() int { return int(sharedUID) }
	effectiveGIDSource = func() int { return int(sharedGID) }

	isolation := &ProcessIsolation{UID: sharedUID, GID: sharedGID, BaseEnvironment: map[string]string{}}
	require.NoError(t, handoffGeneratedNativeTree(native, isolation))
	require.NoError(t, writeNativeOwnedFile(durable, []byte("{\"a\":1}\n"), isolation))

	uid, gid := nativeOwnershipOwner(t, seed)
	require.Equal(t, sharedUID, uid)
	require.Equal(t, sharedGID, gid)

	written := nativeOwnedCovStat(t, durable)
	require.Equal(t, sharedUID, written.Uid, "the durable write never reached its ownership handoff")
	require.Equal(t, sharedGID, written.Gid)
	require.Equal(t, uint32(0o600), written.Mode&0o7777)
	payload, err := os.ReadFile(durable)
	require.NoError(t, err)
	require.Equal(t, "{\"a\":1}\n", string(payload),
		"the durable write must have truncated and republished behind the relaxed walk",
	)

	require.NoError(t, os.Chown(parent, 4242, 4242))

	handoffErr := handoffGeneratedNativeTree(native, isolation)
	require.ErrorContains(t, handoffErr, "generated native path ancestor is uid=4242 gid=4242")
	require.ErrorContains(t, handoffErr, "run the supervisor as root to isolate the agent identity")

	writeErr := writeNativeOwnedFile(durable, []byte("{}\n"), isolation)
	require.ErrorContains(t, writeErr, "native-owned path ancestor is uid=4242 gid=4242")
	require.ErrorContains(t, writeErr, "place the native directory under a path the agent identity owns")
}

// TestNativeOwnedFileWriteTransfersOwnershipOnCreation proves a first write
// creates the file 0600 under the trusted identity, publishes the contents, and
// hands the inode it just wrote to the dropped identity — the same inode, with
// one link. The isolated agent reads this file, so an ownership handoff that
// landed on a different inode would leave the agent reading someone else's
// bytes.
func TestNativeOwnedFileWriteTransfersOwnershipOnCreation(t *testing.T) {
	parent, _ := nativeOwnedCovParent(t)
	path := filepath.Join(parent, "mcp.json")
	require.NoError(t, writeNativeOwnedFile(path, []byte("{\"a\":1}\n"), nativeOwnedCovIsolation()))

	created := nativeOwnedCovStat(t, path)
	require.Equal(t, nativeOwnedCovUID, created.Uid)
	require.Equal(t, nativeOwnedCovGID, created.Gid)
	require.Equal(t, uint32(0o600), created.Mode&0o7777)
	require.Equal(t, uint64(1), uint64(created.Nlink))
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{\"a\":1}\n", string(payload))

	// The rewrite path takes the other arm: the file already belongs to the
	// target, so it is truncated and rewritten in place rather than re-created.
	require.NoError(t, writeNativeOwnedFile(path, []byte("{}\n"), nativeOwnedCovIsolation()))
	rewritten := nativeOwnedCovStat(t, path)
	require.Equal(t, created.Ino, rewritten.Ino, "the rewrite must land on the same inode")
	require.Equal(t, nativeOwnedCovUID, rewritten.Uid)
	payload, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{}\n", string(payload),
		"the rewrite must truncate rather than leave the longer previous contents behind",
	)
}

// TestNativeOwnedFileWriteFailsClosed proves the durable write refuses at every
// point where it cannot prove what it is about to publish: an ancestry it does
// not accept, a name it cannot open for writing, a descriptor that stops
// answering, an existing file whose shape it did not create, and each of the
// four kernel operations that actually publish the contents. A write that
// swallowed any of these would leave the isolated agent a file the trusted
// process never proved it owned.
func TestNativeOwnedFileWriteFailsClosed(t *testing.T) {
	parent, ancestor := nativeOwnedCovParent(t)

	t.Run("ancestry is refused", func(t *testing.T) {
		closed := filepath.Join(ancestor, "closed")
		require.NoError(t, os.Mkdir(closed, 0o700))

		require.ErrorContains(t, writeNativeOwnedFile(
			filepath.Join(closed, "mcp.json"), []byte("{}\n"), nativeOwnedCovIsolation(),
		), "native-owned directory is not safely owned by the target identity")
	})

	t.Run("name cannot be opened for writing", func(t *testing.T) {
		require.NoError(t, os.Mkdir(filepath.Join(parent, "directory-name"), 0o700))

		require.ErrorIs(t, writeNativeOwnedFile(
			filepath.Join(parent, "directory-name"), []byte("{}\n"), nativeOwnedCovIsolation(),
		), unix.EISDIR)
	})

	t.Run("existing file has an unsafe shape", func(t *testing.T) {
		loose := filepath.Join(parent, "loose.json")
		require.NoError(t, os.WriteFile(loose, []byte("{}\n"), 0o644))

		require.ErrorContains(t, writeNativeOwnedFile(
			loose, []byte("{}\n"), nativeOwnedCovIsolation(),
		), "native-owned file is unsafe")
		payload, err := os.ReadFile(loose)
		require.NoError(t, err)
		require.Equal(t, "{}\n", string(payload), "a refused write must not have published anything")
	})

	unpublished := func(t *testing.T, path string) {
		t.Helper()
		stat := nativeOwnedCovStat(t, path)
		require.Zero(t, stat.Size, "a write that refused before publishing must leave no contents")
		require.Equal(t, uint32(os.Geteuid()), stat.Uid,
			"a write that refused before publishing must not have handed the inode over",
		)
	}

	for name, testCase := range map[string]struct {
		file    string
		arrange func(t *testing.T)
		want    error
		verify  func(t *testing.T, path string)
	}{
		"descriptor stops answering": {
			file: "faulted-fstat.json",
			arrange: func(t *testing.T) {
				t.Helper()
				previous := nativeOwnershipFstat
				// Fault only the file's own descriptor, so the ancestry walk
				// that runs first still completes on the real kernel and the
				// case lands on the read-back the writer owes.
				nativeOwnershipFstat = func(fd int, stat *unix.Stat_t) error {
					if err := previous(fd, stat); err != nil {
						return err
					}
					if stat.Mode&unix.S_IFMT == unix.S_IFREG {
						return unix.EIO
					}

					return nil
				}
				t.Cleanup(func() { nativeOwnershipFstat = previous })
			},
			want:   unix.EIO,
			verify: unpublished,
		},
		"file cannot be truncated": {
			file: "faulted-truncate.json",
			arrange: func(t *testing.T) {
				t.Helper()
				previous := nativeOwnershipFtruncate
				nativeOwnershipFtruncate = func(int, int64) error { return unix.EIO }
				t.Cleanup(func() { nativeOwnershipFtruncate = previous })
			},
			want:   unix.EIO,
			verify: unpublished,
		},
		"contents cannot be written": {
			file: "faulted-write.json",
			arrange: func(t *testing.T) {
				t.Helper()
				previous := nativeOwnershipWrite
				nativeOwnershipWrite = func(*os.File, []byte) (int, error) { return 0, unix.ENOSPC }
				t.Cleanup(func() { nativeOwnershipWrite = previous })
			},
			want:   unix.ENOSPC,
			verify: unpublished,
		},
		"ownership cannot be transferred": {
			file: "faulted-chown.json",
			arrange: func(t *testing.T) {
				t.Helper()
				previous := nativeOwnershipFchown
				nativeOwnershipFchown = func(int, int, int) error { return unix.EPERM }
				t.Cleanup(func() { nativeOwnershipFchown = previous })
			},
			want: unix.EPERM,
			verify: func(t *testing.T, path string) {
				t.Helper()
				require.Equal(t, uint32(os.Geteuid()), nativeOwnedCovStat(t, path).Uid,
					"a refused transfer must leave the inode with the trusted identity",
				)
			},
		},
		"contents cannot be durably flushed": {
			file: "faulted-sync.json",
			arrange: func(t *testing.T) {
				t.Helper()
				previous := nativeOwnershipSync
				nativeOwnershipSync = func(*os.File) error { return unix.EIO }
				t.Cleanup(func() { nativeOwnershipSync = previous })
			},
			want: unix.EIO,
			verify: func(t *testing.T, path string) {
				t.Helper()
				// The transfer itself already happened here, so the property
				// this case pins is that an unflushed publication is still
				// reported as a failure rather than as a durable write.
				require.Equal(t, nativeOwnedCovUID, nativeOwnedCovStat(t, path).Uid)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			testCase.arrange(t)
			path := filepath.Join(parent, testCase.file)

			require.ErrorIs(t,
				writeNativeOwnedFile(path, []byte("{}\n"), nativeOwnedCovIsolation()),
				testCase.want,
			)
			testCase.verify(t, path)
		})
	}
}

// TestEffectiveIdentityFailsClosedOnAnUnrepresentableKernelAnswer proves the
// effective-id helpers refuse rather than narrow. Every caller compares their
// result against an inode's 32-bit owner, so an answer outside that width must
// not be truncated into an id a real inode could carry — the truncation of an
// answer one past the 32-bit range is 0, which is root. Linux stores its ids in
// 32 bits and cannot produce such an answer, so it is staged through the seams
// the helpers read.
func TestEffectiveIdentityFailsClosedOnAnUnrepresentableKernelAnswer(t *testing.T) {
	realUID, realGID := effectiveUIDSource, effectiveGIDSource
	t.Cleanup(func() { effectiveUIDSource, effectiveGIDSource = realUID, realGID })

	effectiveUIDSource = func() int { return -1 }
	effectiveGIDSource = func() int { return -1 }
	require.Equal(t, uint32(math.MaxUint32), effectiveUID())
	require.Equal(t, uint32(math.MaxUint32), effectiveGID())

	effectiveUIDSource = func() int { return math.MaxUint32 + 1 }
	effectiveGIDSource = func() int { return math.MaxUint32 + 1 }

	uid, gid := effectiveUID(), effectiveGID()
	require.Equal(t, uint32(math.MaxUint32), uid)
	require.Equal(t, uint32(math.MaxUint32), gid)
	require.NotZero(t, uid, "narrowing this answer would have claimed root")
	require.NotZero(t, gid, "narrowing this answer would have claimed the root group")

	effectiveUIDSource = func() int { return 65534 }
	effectiveGIDSource = func() int { return 65533 }
	require.Equal(t, uint32(65534), effectiveUID())
	require.Equal(t, uint32(65533), effectiveGID())
}
