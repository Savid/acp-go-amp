//go:build linux

package ampacp

import (
	"errors"
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

	if err := os.Chmod(parent, 0o711); err != nil {
		t.Fatal(err)
	}

	control := filepath.Join(parent, "control")
	native := filepath.Join(parent, "native")
	if err := os.Mkdir(control, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(control, "secret"), []byte("root"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(native, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(native, "input"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}

	isolation := &ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}}
	if err := handoffGeneratedNativeTree(native, isolation); err != nil {
		t.Fatal(err)
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
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("dropped-identity proof: %v: %s", err, output)
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
