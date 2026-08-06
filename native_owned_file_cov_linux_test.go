//go:build linux

package ampacp

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, uint64(1), created.Nlink)
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
