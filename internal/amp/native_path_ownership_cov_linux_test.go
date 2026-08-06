//go:build linux

package amp

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

const (
	generatedNativeCovUID = uint32(65534)
	generatedNativeCovGID = uint32(65534)
)

func generatedNativeCovRequireRoot(t *testing.T) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("generated native ownership handoff requires root")
	}
}

func generatedNativeCovIsolation() *ProcessIsolation {
	return &ProcessIsolation{
		UID: generatedNativeCovUID, GID: generatedNativeCovGID,
		BaseEnvironment: map[string]string{},
	}
}

// generatedNativeCovRoot builds the exact shape the handoff accepts: a trusted
// 0700 native root under a 0711 caller root on /tmp, whose whole ancestry is
// trusted-owned, not group- or world-writable without the sticky bit, and
// traversable by the identity the tree is being handed to.
func generatedNativeCovRoot(t *testing.T) string {
	t.Helper()
	generatedNativeCovRequireRoot(t)

	parent, err := os.MkdirTemp("/tmp", "acp-go-amp-gen-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o711))

	native := filepath.Join(parent, "native")
	require.NoError(t, os.Mkdir(native, 0o700))

	return native
}

func generatedNativeCovStat(t *testing.T, path string) unix.Stat_t {
	t.Helper()

	var stat unix.Stat_t
	require.NoError(t, unix.Lstat(path, &stat))

	return stat
}

func generatedNativeCovFaultFstat(t *testing.T, after int, verdict error) *int {
	t.Helper()
	previous := generatedNativeFstat
	calls := 0
	generatedNativeFstat = func(fd int, stat *unix.Stat_t) error {
		calls++
		if calls <= after {
			return previous(fd, stat)
		}

		return verdict
	}
	t.Cleanup(func() { generatedNativeFstat = previous })

	return &calls
}

// TestGeneratedNativeCovTreeHandoffRefusesBeforeItWalks proves the browser-shim
// ownership handoff decides the two things it can decide without touching the
// filesystem: a launch with no isolation policy hands nothing over, and a
// relative root is refused outright. A relative walk would resolve against the
// working directory the agent controls rather than the trusted tree the caller
// named.
func TestGeneratedNativeCovTreeHandoffRefusesBeforeItWalks(t *testing.T) {
	require.NoError(t, handoffGeneratedNativeTree("relative/native", nil),
		"a launch with no isolation policy has no identity to hand the tree to",
	)
	require.ErrorContains(t,
		handoffGeneratedNativeTree("relative/native", generatedNativeCovIsolation()),
		"generated native path must be absolute",
	)
}

// TestGeneratedNativeCovTreeHandoffTransfersEveryInode proves the handoff walks
// the whole generated tree and leaves every inode in it owned by the dropped
// identity, on the same inodes it validated. The browser shim is written by the
// trusted process and then executed by the isolated one, so an entry the walk
// skipped would be an entry the agent cannot read, and an inode swapped for
// another would be an ownership grant the walk never proved.
func TestGeneratedNativeCovTreeHandoffTransfersEveryInode(t *testing.T) {
	root := generatedNativeCovRoot(t)
	nested := filepath.Join(root, "nested")
	require.NoError(t, os.Mkdir(nested, 0o700))
	shim := filepath.Join(root, "shim")
	require.NoError(t, os.WriteFile(shim, []byte("#!/bin/sh\n"), 0o700))
	leaf := filepath.Join(nested, "config")
	require.NoError(t, os.WriteFile(leaf, []byte("{}\n"), 0o600))

	before := map[string]unix.Stat_t{}
	for _, path := range []string{root, nested, shim, leaf} {
		before[path] = generatedNativeCovStat(t, path)
	}

	require.NoError(t, handoffGeneratedNativeTree(root, generatedNativeCovIsolation()))

	for _, path := range []string{root, nested, shim, leaf} {
		after := generatedNativeCovStat(t, path)
		require.Equal(t, before[path].Ino, after.Ino, "%s was replaced rather than handed over", path)
		require.Equal(t, before[path].Dev, after.Dev, "%s moved device during the handoff", path)
		require.Equal(t, generatedNativeCovUID, after.Uid, "%s was not handed to the target uid", path)
		require.Equal(t, generatedNativeCovGID, after.Gid, "%s was not handed to the target gid", path)
		require.Equal(t, before[path].Mode, after.Mode, "%s changed mode during the handoff", path)
	}
}

// TestGeneratedNativeCovTraversalFailsClosedOnKernelFaults proves every
// descriptor syscall the ancestry walk depends on aborts the walk. A walk that
// swallowed any of them would return a directory descriptor whose ancestry it
// never actually proved, and the handoff would then chown an unvetted tree to
// the dropped identity.
func TestGeneratedNativeCovTraversalFailsClosedOnKernelFaults(t *testing.T) {
	generatedNativeCovRequireRoot(t)

	t.Run("filesystem root unopenable", func(t *testing.T) {
		previous := generatedNativeOpenFilesystemRoot
		generatedNativeOpenFilesystemRoot = func() (int, error) { return -1, unix.EMFILE }
		t.Cleanup(func() { generatedNativeOpenFilesystemRoot = previous })

		directory, err := openGeneratedNativeDirectory("/etc", 0, 0, generatedNativeCovUID, generatedNativeCovGID)
		require.ErrorIs(t, err, unix.EMFILE)
		require.Nil(t, directory)
	})

	t.Run("filesystem root unstattable", func(t *testing.T) {
		generatedNativeCovFaultFstat(t, 0, unix.EIO)

		directory, err := openGeneratedNativeDirectory("/etc", 0, 0, generatedNativeCovUID, generatedNativeCovGID)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})

	t.Run("component unstattable", func(t *testing.T) {
		calls := generatedNativeCovFaultFstat(t, 1, unix.EIO)

		directory, err := openGeneratedNativeDirectory("/etc", 0, 0, generatedNativeCovUID, generatedNativeCovGID)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
		require.Equal(t, 2, *calls, "the walk statted past the faulted component")
	})

	t.Run("filesystem root is not trusted-owned", func(t *testing.T) {
		// The trusted identity is whatever euid the login is running as. A
		// non-root one does not own /, so the walk must refuse at the very
		// first ancestor rather than descend into a tree it cannot vouch for.
		directory, err := openGeneratedNativeDirectory(
			"/etc", 1000, 1000, generatedNativeCovUID, generatedNativeCovGID,
		)
		require.ErrorContains(t, err, "generated native path ancestry is not a trusted directory")
		require.Nil(t, directory)
	})

	t.Run("filesystem root named as the tree itself", func(t *testing.T) {
		// "/" splits into a single empty component, so the walk has to accept
		// the root descriptor it already validated instead of trying to open a
		// child called "". The seam only supplies the 0700 trusted shape the
		// leaf rule demands, which no real / has.
		previous := generatedNativeFstat
		generatedNativeFstat = func(fd int, stat *unix.Stat_t) error {
			if err := previous(fd, stat); err != nil {
				return err
			}
			stat.Mode = unix.S_IFDIR | 0o700
			stat.Uid, stat.Gid = 0, 0

			return nil
		}
		t.Cleanup(func() { generatedNativeFstat = previous })

		directory, err := openGeneratedNativeDirectory("/", 0, 0, generatedNativeCovUID, generatedNativeCovGID)
		require.NoError(t, err)
		require.NotNil(t, directory)
		t.Cleanup(func() { _ = directory.Close() })
		require.Equal(t, "/", directory.Name())

		var opened, root unix.Stat_t
		require.NoError(t, previous(int(directory.Fd()), &opened))
		require.NoError(t, unix.Stat("/", &root))
		require.Equal(t, root.Ino, opened.Ino, "the walk must hand back the filesystem root itself")
	})

	t.Run("component absent", func(t *testing.T) {
		directory, err := openGeneratedNativeDirectory(
			"/etc/acp-go-amp-absent", 0, 0, generatedNativeCovUID, generatedNativeCovGID,
		)
		require.ErrorIs(t, err, unix.ENOENT)
		require.Nil(t, directory)
	})

	t.Run("parent descriptor unreleasable", func(t *testing.T) {
		// A multi-component root, so the walk reaches the release of a parent
		// it has already accepted rather than refusing at the leaf first.
		root := generatedNativeCovRoot(t)
		previous := generatedNativeClose
		generatedNativeClose = func(fd int) error {
			_ = previous(fd)

			return unix.EIO
		}
		t.Cleanup(func() { generatedNativeClose = previous })

		directory, err := openGeneratedNativeDirectory(root, 0, 0, generatedNativeCovUID, generatedNativeCovGID)
		require.ErrorIs(t, err, unix.EIO)
		require.Nil(t, directory)
	})
}

// TestGeneratedNativeCovAncestorStatesEachRefusal pins the exact reason the
// ancestry validator refuses each unsafe shape. These reasons are the
// containment contract: an ancestor that is not trusted-owned, a leaf that is
// not exactly 0700, a writable ancestor with no sticky protection, and an
// ancestor the dropped identity cannot traverse.
func TestGeneratedNativeCovAncestorStatesEachRefusal(t *testing.T) {
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
			stat: unix.Stat_t{Mode: unix.S_IFREG | 0o700, Uid: trustedUID, Gid: trustedGID},
			want: "generated native path ancestry is not a trusted directory",
		},
		"foreign owner": {
			stat: unix.Stat_t{Mode: unix.S_IFDIR | 0o700, Uid: trustedUID + 1, Gid: trustedGID},
			want: "generated native path ancestry is not a trusted directory",
		},
		"foreign group": {
			stat: unix.Stat_t{Mode: unix.S_IFDIR | 0o700, Uid: trustedUID, Gid: trustedGID + 1},
			want: "generated native path ancestry is not a trusted directory",
		},
		"leaf is not exactly 0700": {
			stat:  unix.Stat_t{Mode: unix.S_IFDIR | 0o750, Uid: trustedUID, Gid: trustedGID},
			final: true,
			want:  "generated native root mode 0750 is unsafe",
		},
		"writable ancestor without sticky": {
			stat: unix.Stat_t{Mode: unix.S_IFDIR | 0o777, Uid: trustedUID, Gid: trustedGID},
			want: "generated native ancestor mode 0777 is writable without sticky protection",
		},
		"untraversable ancestor": {
			stat: unix.Stat_t{Mode: unix.S_IFDIR | 0o700, Uid: trustedUID, Gid: trustedGID},
			want: "generated native path ancestry is not traversable by the target identity",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateGeneratedNativeAncestor(
				testCase.stat, testCase.final, trustedUID, trustedGID,
				generatedNativeCovUID, generatedNativeCovGID,
			)
			require.ErrorContains(t, err, testCase.want)
		})
	}

	require.NoError(t, validateGeneratedNativeAncestor(
		unix.Stat_t{Mode: unix.S_IFDIR | 0o1777, Uid: trustedUID, Gid: trustedGID}, false,
		trustedUID, trustedGID, generatedNativeCovUID, generatedNativeCovGID,
	), "a sticky world-writable ancestor is the one writable shape the walk accepts")
	require.NoError(t, validateGeneratedNativeAncestor(
		unix.Stat_t{Mode: unix.S_IFDIR | 0o700, Uid: trustedUID, Gid: trustedGID}, true,
		trustedUID, trustedGID, generatedNativeCovUID, generatedNativeCovGID,
	), "the leaf is not required to be traversable by the target before the chown")
}

// TestGeneratedNativeCovTraversabilityUsesTheApplicableModeClass proves
// traversability is decided by the one permission class that applies to the
// target identity, not by any bit that happens to be set. Reading the wrong
// class would let a directory the dropped identity cannot enter pass as one it
// can, and the agent would then fail to reach its own shim.
func TestGeneratedNativeCovTraversabilityUsesTheApplicableModeClass(t *testing.T) {
	const (
		uid = uint32(1000)
		gid = uint32(1001)
	)

	require.True(t, nativeIdentityCanTraverse(unix.Stat_t{Uid: uid, Mode: 0o100}, uid, gid))
	require.False(t, nativeIdentityCanTraverse(unix.Stat_t{Uid: uid, Mode: 0o011}, uid, gid),
		"an owner-matched directory must be judged on its owner bit alone",
	)
	require.True(t, nativeIdentityCanTraverse(unix.Stat_t{Uid: uid + 1, Gid: gid, Mode: 0o010}, uid, gid))
	require.False(t, nativeIdentityCanTraverse(unix.Stat_t{Uid: uid + 1, Gid: gid, Mode: 0o101}, uid, gid),
		"a group-matched directory must be judged on its group bit alone",
	)
	require.True(t, nativeIdentityCanTraverse(unix.Stat_t{Uid: uid + 1, Gid: gid + 1, Mode: 0o001}, uid, gid))
	require.False(t, nativeIdentityCanTraverse(unix.Stat_t{Uid: uid + 1, Gid: gid + 1, Mode: 0o110}, uid, gid),
		"an unmatched directory must be judged on its other bit alone",
	)
}

// TestGeneratedNativeCovDirectoryHandoffFailsClosed proves the per-directory
// stage refuses rather than partially transferring: it refuses a directory
// whose own inode no longer validates, refuses when the directory cannot be
// enumerated, and refuses an entry it cannot open. A walk that continued past
// any of these would report a handoff it never completed.
func TestGeneratedNativeCovDirectoryHandoffFailsClosed(t *testing.T) {
	root := generatedNativeCovRoot(t)

	t.Run("directory inode no longer validates", func(t *testing.T) {
		require.NoError(t, os.Chmod(root, 0o750))
		t.Cleanup(func() { require.NoError(t, os.Chmod(root, 0o700)) })
		directory, err := os.Open(root)
		require.NoError(t, err)
		defer directory.Close()

		require.ErrorContains(t,
			handoffGeneratedNativeDirectory(
				directory, 0, 0, generatedNativeCovUID, generatedNativeCovGID,
			),
			"generated native directory mode 0750 is unsafe",
		)
	})

	t.Run("directory cannot be enumerated", func(t *testing.T) {
		previous := generatedNativeReadDir
		generatedNativeReadDir = func(*os.File) ([]os.DirEntry, error) { return nil, unix.EIO }
		t.Cleanup(func() { generatedNativeReadDir = previous })

		directory, err := os.Open(root)
		require.NoError(t, err)
		defer directory.Close()

		require.ErrorIs(t,
			handoffGeneratedNativeDirectory(
				directory, 0, 0, generatedNativeCovUID, generatedNativeCovGID,
			),
			unix.EIO,
		)
	})

	t.Run("entry cannot be opened", func(t *testing.T) {
		previous := generatedNativeReadDir
		generatedNativeReadDir = func(*os.File) ([]os.DirEntry, error) {
			return []os.DirEntry{generatedNativeCovEntry{name: "absent"}}, nil
		}
		t.Cleanup(func() { generatedNativeReadDir = previous })

		directory, err := os.Open(root)
		require.NoError(t, err)
		defer directory.Close()

		err = handoffGeneratedNativeDirectory(
			directory, 0, 0, generatedNativeCovUID, generatedNativeCovGID,
		)
		require.ErrorContains(t, err, `open generated native entry "absent"`)
		require.ErrorIs(t, err, unix.ENOENT)
	})

	t.Run("an entry refuses mid-walk", func(t *testing.T) {
		enclosing := generatedNativeCovRoot(t)
		loose := filepath.Join(enclosing, "loose")
		require.NoError(t, os.WriteFile(loose, []byte("#!/bin/sh\n"), 0o644))
		directory, err := os.Open(enclosing)
		require.NoError(t, err)
		defer directory.Close()

		require.ErrorContains(t,
			handoffGeneratedNativeDirectory(
				directory, 0, 0, generatedNativeCovUID, generatedNativeCovGID,
			),
			"generated native file mode 0644 is unsafe",
		)
		require.Equal(t, uint32(0), generatedNativeCovStat(t, enclosing).Uid,
			"a directory whose entry refused must not have been handed over itself",
		)
		require.Equal(t, uint32(0), generatedNativeCovStat(t, loose).Uid,
			"the refused entry must keep the trusted identity",
		)
	})
}

type generatedNativeCovEntry struct{ name string }

func (entry generatedNativeCovEntry) Name() string         { return entry.name }
func (generatedNativeCovEntry) IsDir() bool                { return false }
func (generatedNativeCovEntry) Type() os.FileMode          { return 0 }
func (generatedNativeCovEntry) Info() (os.FileInfo, error) { return nil, unix.EBADF }

// TestGeneratedNativeCovEntryHandoffRefusesUnsupportedInodes proves the entry
// stage only ever hands over the two inode kinds a generated tree may contain,
// and aborts when the kernel stops answering for an entry descriptor it already
// opened. A symlink, socket or device that was chowned to the dropped identity
// would be a capability the trusted process never intended to grant.
func TestGeneratedNativeCovEntryHandoffRefusesUnsupportedInodes(t *testing.T) {
	root := generatedNativeCovRoot(t)

	t.Run("entry descriptor stops answering", func(t *testing.T) {
		shim := filepath.Join(root, "shim")
		require.NoError(t, os.WriteFile(shim, []byte("#!/bin/sh\n"), 0o700))
		file, err := os.Open(shim)
		require.NoError(t, err)
		defer file.Close()
		generatedNativeCovFaultFstat(t, 0, unix.EIO)

		require.ErrorIs(t,
			handoffGeneratedNativeEntry(file, 0, 0, generatedNativeCovUID, generatedNativeCovGID),
			unix.EIO,
		)
	})

	t.Run("unsupported inode type", func(t *testing.T) {
		fifo := filepath.Join(root, "fifo")
		require.NoError(t, unix.Mkfifo(fifo, 0o600))
		fd, err := unix.Open(fifo, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		require.NoError(t, err)
		file := os.NewFile(uintptr(fd), fifo)
		defer file.Close()

		require.ErrorContains(t,
			handoffGeneratedNativeEntry(file, 0, 0, generatedNativeCovUID, generatedNativeCovGID),
			"generated native inode has unsupported type",
		)
	})

	t.Run("regular entry that no longer validates", func(t *testing.T) {
		shim := filepath.Join(root, "loose")
		require.NoError(t, os.WriteFile(shim, []byte("#!/bin/sh\n"), 0o644))
		file, err := os.Open(shim)
		require.NoError(t, err)
		defer file.Close()

		require.ErrorContains(t,
			handoffGeneratedNativeEntry(file, 0, 0, generatedNativeCovUID, generatedNativeCovGID),
			"generated native file mode 0644 is unsafe",
		)
	})
}

// TestGeneratedNativeCovInodeValidationStatesEachRefusal pins the exact reason
// the pre-chown inode check refuses. It runs on a descriptor the walk already
// opened, so it is the last thing standing between a swapped, shared or
// loosely-permissioned inode and an ownership grant to the dropped identity.
func TestGeneratedNativeCovInodeValidationStatesEachRefusal(t *testing.T) {
	root := generatedNativeCovRoot(t)
	directory, err := os.Open(root)
	require.NoError(t, err)
	defer directory.Close()

	t.Run("descriptor stops answering", func(t *testing.T) {
		generatedNativeCovFaultFstat(t, 0, unix.EIO)
		require.ErrorIs(t, validateGeneratedNativeInode(
			int(directory.Fd()), unix.S_IFDIR, 0, 0,
			generatedNativeCovUID, generatedNativeCovGID, false,
		), unix.EIO)
	})

	t.Run("inode type changed", func(t *testing.T) {
		require.ErrorContains(t, validateGeneratedNativeInode(
			int(directory.Fd()), unix.S_IFREG, 0, 0,
			generatedNativeCovUID, generatedNativeCovGID, false,
		), "generated native inode type changed")
	})

	t.Run("inode owner changed", func(t *testing.T) {
		require.ErrorContains(t, validateGeneratedNativeInode(
			int(directory.Fd()), unix.S_IFDIR, generatedNativeCovUID, generatedNativeCovGID,
			generatedNativeCovUID, generatedNativeCovGID, false,
		), "generated native inode owner changed to uid=0 gid=0")
	})

	t.Run("file has extra links", func(t *testing.T) {
		shim := filepath.Join(root, "linked")
		require.NoError(t, os.WriteFile(shim, []byte("#!/bin/sh\n"), 0o600))
		require.NoError(t, os.Link(shim, filepath.Join(root, "linked-again")))
		file, openErr := os.Open(shim)
		require.NoError(t, openErr)
		defer file.Close()

		require.ErrorContains(t, validateGeneratedNativeInode(
			int(file.Fd()), unix.S_IFREG, 0, 0,
			generatedNativeCovUID, generatedNativeCovGID, true,
		), "generated native file has 2 links")
	})
}

// TestGeneratedNativeCovChownProvesTheTransferItPerformed proves the chown
// stage refuses when the kernel will not perform the transfer, when it stops
// answering for the descriptor afterwards, and when the inode it reads back is
// not the one it just handed over. Reporting an unproven handoff would leave
// the isolated agent with a tree it cannot use and the caller believing it can.
func TestGeneratedNativeCovChownProvesTheTransferItPerformed(t *testing.T) {
	root := generatedNativeCovRoot(t)
	directory, err := os.Open(root)
	require.NoError(t, err)
	defer directory.Close()

	t.Run("kernel refuses the transfer", func(t *testing.T) {
		previous := generatedNativeFchown
		generatedNativeFchown = func(int, int, int) error { return unix.EPERM }
		t.Cleanup(func() { generatedNativeFchown = previous })

		require.ErrorIs(t, chownGeneratedNativeInode(
			int(directory.Fd()), unix.S_IFDIR, generatedNativeCovUID, generatedNativeCovGID, false,
		), unix.EPERM)
	})

	t.Run("descriptor stops answering after the transfer", func(t *testing.T) {
		generatedNativeCovFaultFstat(t, 0, unix.EIO)
		require.ErrorIs(t, chownGeneratedNativeInode(
			int(directory.Fd()), unix.S_IFDIR, generatedNativeCovUID, generatedNativeCovGID, false,
		), unix.EIO)
		require.Equal(t, generatedNativeCovUID, generatedNativeCovStat(t, root).Uid,
			"the transfer itself must still have happened",
		)
	})

	t.Run("read-back disagrees with the transfer", func(t *testing.T) {
		require.NoError(t, os.Chown(root, 0, 0))
		previous := generatedNativeFchown
		generatedNativeFchown = func(int, int, int) error { return nil }
		t.Cleanup(func() { generatedNativeFchown = previous })

		require.ErrorContains(t, chownGeneratedNativeInode(
			int(directory.Fd()), unix.S_IFDIR, generatedNativeCovUID, generatedNativeCovGID, false,
		), "generated native inode ownership handoff could not be proven")
	})
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
