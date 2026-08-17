//go:build linux

package amp

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneCovRestoreSyscallSeams restores every syscall seam this file
// faults, so one case can never leak a fault into the next.
func agentStandaloneCovRestoreSyscallSeams(t *testing.T) {
	t.Helper()
	randRead := agentStandaloneRandRead
	openat := agentStandaloneOpenat
	fstat := agentStandaloneFstat
	fstatat := agentStandaloneFstatat
	fchown := agentStandaloneFchown
	fchmod := agentStandaloneFchmod
	flock := agentStandaloneFlock
	renameat := agentStandaloneRenameat
	unlinkat := agentStandaloneUnlinkat
	readAll := agentStandaloneReadAll
	write := agentStandaloneFileWrite
	sync := agentStandaloneFileSync
	closeFile := agentStandaloneFileClose
	t.Cleanup(func() {
		agentStandaloneRandRead = randRead
		agentStandaloneOpenat = openat
		agentStandaloneFstat = fstat
		agentStandaloneFstatat = fstatat
		agentStandaloneFchown = fchown
		agentStandaloneFchmod = fchmod
		agentStandaloneFlock = flock
		agentStandaloneRenameat = renameat
		agentStandaloneUnlinkat = unlinkat
		agentStandaloneReadAll = readAll
		agentStandaloneFileWrite = write
		agentStandaloneFileSync = sync
		agentStandaloneFileClose = closeFile
	})
}

// agentStandaloneCovNthCall reports true exactly once, on the nth call.
func agentStandaloneCovNthCall(n int) func() bool {
	calls := 0

	return func() bool {
		calls++

		return calls == n
	}
}

// agentStandaloneCovFaultSyscall makes the nth call to the named syscall seam
// fail. Every guard it reaches this way is an `if err != nil` over a syscall
// the code has already validated its inputs for, so faulting the seam is the
// only way to prove the operation aborts instead of proceeding on an answer
// the kernel never gave.
func agentStandaloneCovFaultSyscall(t *testing.T, target string, call int, verdict error) {
	t.Helper()
	agentStandaloneCovRestoreSyscallSeams(t)
	hit := agentStandaloneCovNthCall(call)
	switch target {
	case "rand":
		previous := agentStandaloneRandRead
		agentStandaloneRandRead = func(payload []byte) (int, error) {
			if hit() {
				return 0, verdict
			}

			return previous(payload)
		}
	case "openat":
		previous := agentStandaloneOpenat
		agentStandaloneOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
			if hit() {
				return -1, verdict
			}

			return previous(dirfd, path, flags, mode)
		}
	case "fstat":
		previous := agentStandaloneFstat
		agentStandaloneFstat = func(fd int, stat *unix.Stat_t) error {
			if hit() {
				return verdict
			}

			return previous(fd, stat)
		}
	case "fstatat":
		previous := agentStandaloneFstatat
		agentStandaloneFstatat = func(dirfd int, path string, stat *unix.Stat_t, flags int) error {
			if hit() {
				return verdict
			}

			return previous(dirfd, path, stat, flags)
		}
	case "fchown":
		previous := agentStandaloneFchown
		agentStandaloneFchown = func(fd, uid, gid int) error {
			if hit() {
				return verdict
			}

			return previous(fd, uid, gid)
		}
	case "fchmod":
		previous := agentStandaloneFchmod
		agentStandaloneFchmod = func(fd int, mode uint32) error {
			if hit() {
				return verdict
			}

			return previous(fd, mode)
		}
	case "flock":
		previous := agentStandaloneFlock
		agentStandaloneFlock = func(fd, how int) error {
			if hit() {
				return verdict
			}

			return previous(fd, how)
		}
	case "renameat":
		previous := agentStandaloneRenameat
		agentStandaloneRenameat = func(oldDir int, oldPath string, newDir int, newPath string) error {
			if hit() {
				return verdict
			}

			return previous(oldDir, oldPath, newDir, newPath)
		}
	case "unlinkat":
		previous := agentStandaloneUnlinkat
		agentStandaloneUnlinkat = func(dirfd int, path string, flags int) error {
			if hit() {
				return verdict
			}

			return previous(dirfd, path, flags)
		}
	case "write":
		previous := agentStandaloneFileWrite
		agentStandaloneFileWrite = func(file *os.File, payload []byte) (int, error) {
			if hit() {
				return 0, verdict
			}

			return previous(file, payload)
		}
	case "sync":
		previous := agentStandaloneFileSync
		agentStandaloneFileSync = func(file *os.File) error {
			if hit() {
				return verdict
			}

			return previous(file)
		}
	case "readall":
		previous := agentStandaloneReadAll
		agentStandaloneReadAll = func(reader io.Reader) ([]byte, error) {
			if hit() {
				return nil, verdict
			}

			return previous(reader)
		}
	default:
		t.Fatalf("unknown syscall seam %q", target)
	}
}

// agentStandaloneCovFaultClose makes the nth close of the named descriptor
// fail, so a case can prove a claim that could not release a lease refuses
// rather than carrying on believing it did.
func agentStandaloneCovFaultClose(t *testing.T, name string, call int, verdict error) {
	t.Helper()
	agentStandaloneCovRestoreSyscallSeams(t)
	hit := agentStandaloneCovNthCall(call)
	previous := agentStandaloneFileClose
	agentStandaloneFileClose = func(file *os.File) error {
		if file.Name() == name && hit() {
			require.NoError(t, previous(file))

			return verdict
		}

		return previous(file)
	}
}

// TestAgentStandaloneCovPublicationAbortsOnAnyFaultedStep proves each atomic
// publication — the owner binding, the authority record and the durable marker
// — abandons its temporary and publishes nothing when any single step of the
// write-fsync-rename-verify sequence does not complete. A publication that
// carried on past a faulted step would leave a registry entry nobody had
// proved was durable, correctly owned, or the inode it just wrote.
func TestAgentStandaloneCovPublicationAbortsOnAnyFaultedStep(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneCovOwner(62991, 62992, "publication", "/srv/amp/publication", 91, 92)
	for _, testCase := range []struct {
		name       string
		target     string
		call       int
		postRename bool
		publish    func(t *testing.T, directory *os.File) error
		final      string
	}{
		{name: "owner temporary name", target: "rand", call: 1},
		{name: "owner temporary creation", target: "openat", call: 1},
		{name: "owner temporary ownership", target: "fchown", call: 1},
		{name: "owner temporary mode", target: "fchmod", call: 1},
		{name: "owner payload write", target: "write", call: 1},
		{name: "owner payload sync", target: "sync", call: 1},
		{name: "owner temporary identity", target: "fstat", call: 1},
		{name: "owner published inode", target: "fstatat", call: 3, postRename: true},
		{name: "record temporary name", target: "rand", call: 1},
		{name: "record temporary creation", target: "openat", call: 1},
		{name: "record temporary ownership", target: "fchown", call: 1},
		{name: "record temporary mode", target: "fchmod", call: 1},
		{name: "record payload write", target: "write", call: 1},
		{name: "record payload sync", target: "sync", call: 1},
		{name: "record temporary identity", target: "fstat", call: 1},
		{name: "record publication rename", target: "renameat", call: 1},
		{name: "record published inode", target: "fstatat", call: 1, postRename: true},
		{name: "marker temporary name", target: "rand", call: 1},
		{name: "marker temporary creation", target: "openat", call: 1},
		{name: "marker temporary ownership", target: "fchown", call: 1},
		{name: "marker temporary mode", target: "fchmod", call: 1},
		{name: "marker payload write", target: "write", call: 1},
		{name: "marker payload sync", target: "sync", call: 1},
		{name: "marker temporary identity", target: "fstat", call: 1},
		{name: "marker publication rename", target: "renameat", call: 1},
		{name: "marker published inode", target: "fstatat", call: 3, postRename: true},
	} {
		publish := testCase.publish
		final := testCase.final
		switch {
		case publish != nil:
		case len(testCase.name) > 5 && testCase.name[:5] == "owner":
			final = "62991.owner"
			publish = func(t *testing.T, directory *os.File) error {
				t.Helper()

				return createAgentStandaloneOwner(directory, owner, ownerUID, ownerGID)
			}
		case len(testCase.name) > 6 && testCase.name[:6] == "record":
			final = "domain.json"
			publish = func(t *testing.T, directory *os.File) error {
				t.Helper()
				record, err := currentAgentAuthorityDomain(directory)
				require.NoError(t, err)
				record.AuthorityID = "0123456789abcdef0123456789abcdef"

				return replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record)
			}
		default:
			final = "62993.quarantine"
			publish = func(t *testing.T, directory *os.File) error {
				t.Helper()

				return replaceAgentStandaloneFile(
					directory, "62993.quarantine", []byte(`{"version":2}`), ownerUID, ownerGID,
					time.Now().Add(time.Second), nil, nil,
				)
			}
		}
		t.Run(testCase.name, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			wantErr := errors.New("injected " + testCase.name + " failure")
			agentStandaloneCovFaultSyscall(t, testCase.target, testCase.call, wantErr)

			require.ErrorIs(t, publish(t, directory), wantErr)
			agentStandaloneCovRestoreSyscallSeams(t)
			entries, err := os.ReadDir(directory.Name())
			require.NoError(t, err)
			for _, entry := range entries {
				require.NotContains(t, entry.Name(), ".next-",
					"no temporary may survive an aborted publication",
				)
			}
			if testCase.postRename {
				require.FileExists(t, filepath.Join(directory.Name(), final),
					"a post-rename refusal still reports failure to its caller",
				)

				return
			}
			require.NoFileExists(t, filepath.Join(directory.Name(), final))
			require.Empty(t, entries)
		})
	}
}

// TestAgentStandaloneCovPublicationReportsAFailedTemporaryCleanup proves that
// when the temporary a publication wrote cannot be unlinked afterwards, the
// publication reports it instead of returning success. A silently retained
// temporary is registry state the next audit would refuse.
func TestAgentStandaloneCovPublicationReportsAFailedTemporaryCleanup(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("owner", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected owner temporary unlink failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		require.ErrorIs(t, createAgentStandaloneOwner(
			directory, agentStandaloneCovOwner(62995, 62996, "unlink", "/srv/amp/unlink", 93, 94),
			ownerUID, ownerGID,
		), wantErr)
	})

	t.Run("record", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "0123456789abcdef0123456789abcdef"
		wantErr := errors.New("injected record temporary unlink failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		require.ErrorIs(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record), wantErr)
	})

	t.Run("marker", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected marker temporary unlink failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		require.ErrorIs(t, replaceAgentStandaloneFile(
			directory, "62997.quarantine", []byte(`{"version":2}`), ownerUID, ownerGID,
			time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})
}

// TestAgentStandaloneCovDurabilityProbeAbortsOnAnyFaultedStep proves the
// authority durability probe refuses when any step of its
// create-own-lock-contend-write-sync-rename-verify sequence does not complete,
// including the case where a second open of the same file can take the
// exclusive lock the first open already holds. A filesystem without that
// exclusion cannot carry agent authority at all.
func TestAgentStandaloneCovDurabilityProbeAbortsOnAnyFaultedStep(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		target string
		call   int
	}{
		{name: "probe name", target: "rand", call: 1},
		{name: "probe ownership", target: "fchown", call: 1},
		{name: "probe mode", target: "fchmod", call: 1},
		{name: "probe exclusive lock", target: "flock", call: 1},
		{name: "probe contender open", target: "openat", call: 2},
		{name: "probe payload write", target: "write", call: 1},
		{name: "probe payload sync", target: "sync", call: 1},
		{name: "probe inode identity", target: "fstat", call: 1},
		{name: "probe rename", target: "renameat", call: 1},
		{name: "probe renamed inode", target: "fstatat", call: 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			wantErr := errors.New("injected " + testCase.name + " failure")
			agentStandaloneCovFaultSyscall(t, testCase.target, testCase.call, wantErr)

			require.ErrorIs(t, probeAgentStandaloneFilesystem(directory, true), wantErr)
		})
	}

	t.Run("filesystem without separate-open exclusion", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovRestoreSyscallSeams(t)
		previous := agentStandaloneFlock
		hit := agentStandaloneCovNthCall(2)
		agentStandaloneFlock = func(fd, how int) error {
			if hit() {
				return nil
			}

			return previous(fd, how)
		}

		require.ErrorContains(t, probeAgentStandaloneFilesystem(directory, true),
			"lacks separate-open flock exclusion",
		)
	})
}

// TestAgentStandaloneCovRegistryReadRefusesAnIncompleteOrShiftingFile proves a
// registry record is only accepted when its metadata could be read, its bytes
// could be read, and the name still points at the same inode with the same
// metadata afterwards. Without the last check a file swapped underneath the
// read would be accepted with the identity of the file that was there before.
func TestAgentStandaloneCovRegistryReadRefusesAnIncompleteOrShiftingFile(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owner := agentStandaloneCovOwner(63001, 63002, "shifting", "/srv/amp/shifting", 95, 96)

	t.Run("metadata unavailable", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, owner)
		wantErr := errors.New("injected registry fstat failure")
		agentStandaloneCovFaultSyscall(t, "fstat", 1, wantErr)

		_, err := loadAgentStandaloneOwner(directory, owner.UID, ownerUID, ownerGID)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("payload unreadable", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, owner)
		wantErr := errors.New("injected registry read failure")
		agentStandaloneCovFaultSyscall(t, "readall", 1, wantErr)

		_, err := loadAgentStandaloneOwner(directory, owner.UID, ownerUID, ownerGID)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("file replaced while it was read", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, owner)
		agentStandaloneCovRestoreSyscallSeams(t)
		previous := agentStandaloneReadAll
		swapped := false
		agentStandaloneReadAll = func(reader io.Reader) ([]byte, error) {
			payload, err := previous(reader)
			if !swapped {
				swapped = true
				require.NoError(t, os.Remove(filepath.Join(directory.Name(), "63001.owner")))
				agentStandaloneCovWriteOwner(t, directory, owner)
			}

			return payload, err
		}

		_, err := loadAgentStandaloneOwner(directory, owner.UID, ownerUID, ownerGID)
		require.ErrorContains(t, err, "changed while its payload was read")
		require.True(t, swapped)
	})
}

// TestAgentStandaloneCovLockAcquisitionAbortsOnAFaultedFlock proves the three
// lock helpers treat an unexpected flock failure as a refusal rather than as
// "the lock is busy", and release the descriptor they opened. Reading a real
// failure as contention would make a claim retry forever against a lock it can
// never take.
func TestAgentStandaloneCovLockAcquisitionAbortsOnAFaultedFlock(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("blocking acquisition", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		wantErr := errors.New("injected blocking flock failure")
		agentStandaloneCovFaultSyscall(t, "flock", 1, wantErr)

		lock, err := acquireAgentStandaloneNamedLock(
			directory, "owners.lock", unix.LOCK_EX, false,
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lock)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("non-blocking acquisition", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		wantErr := errors.New("injected try-flock failure")
		agentStandaloneCovFaultSyscall(t, "flock", 1, wantErr)

		lock, acquired, err := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
		require.Nil(t, lock)
		require.False(t, acquired)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("probe temporary cleanup", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := ".authority-probe-" + agentStandaloneCovSuffix
		agentStandaloneCovPermanentLock(t, directory, name)
		wantErr := errors.New("injected probe cleanup flock failure")
		agentStandaloneCovFaultSyscall(t, "flock", 1, wantErr)

		require.ErrorIs(t, cleanupAgentStandaloneProbeTemporary(directory, name, ownerUID, ownerGID), wantErr)
		agentStandaloneCovRestoreSyscallSeams(t)
		require.FileExists(t, filepath.Join(directory.Name(), name))
	})

	t.Run("shared lease normalization", func(t *testing.T) {
		directory, ownerUID, ownerGID, _ := createAgentStandaloneMatchingDomainFixture(t)
		record, err := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
		require.NoError(t, err)
		lease, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
		require.NoError(t, err)
		defer lease.Close()
		wantErr := errors.New("injected lease demotion failure")
		agentStandaloneCovFaultSyscall(t, "flock", 1, wantErr)

		err = normalizeAgentStandaloneSharedDomainLease(directory, lease, ownerUID, ownerGID, record)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "normalize agent authority domain shared lease")
	})
}

// TestAgentStandaloneCovTemporaryCleanupReportsAFailedUnlink proves each
// registry temporary cleanup reports a failed unlink instead of returning
// success, so a caller never proceeds believing a temporary it must not race
// with is gone.
func TestAgentStandaloneCovTemporaryCleanupReportsAFailedUnlink(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	suffix := agentStandaloneCovSuffix

	t.Run("owner temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := "63011.owner.next-" + suffix
		agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		wantErr := errors.New("injected owner temporary unlink failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		require.ErrorIs(t, cleanupAgentStandaloneOwnerTemporary(directory, name, ownerUID, ownerGID), wantErr)
	})

	t.Run("domain temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := "domain.json.next-" + suffix
		agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		wantErr := errors.New("injected domain temporary unlink failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		require.ErrorIs(t, cleanupAgentStandaloneDomainTemporary(directory, name, ownerUID, ownerGID), wantErr)
	})

	t.Run("probe temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		name := ".authority-probe-" + suffix
		agentStandaloneCovPermanentLock(t, directory, name)
		wantErr := errors.New("injected probe temporary unlink failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		require.ErrorIs(t, cleanupAgentStandaloneProbeTemporary(directory, name, ownerUID, ownerGID), wantErr)
	})

	t.Run("marker temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "63013.lock")
		name := "63013.quarantine.next-" + suffix
		agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		wantErr := errors.New("injected marker temporary unlink failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		require.ErrorIs(t,
			cleanupAgentStandaloneMarkerTemporary(directory, 63013, name, ownerUID, ownerGID),
			wantErr,
		)
	})

	t.Run("target marker temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		uidLock := createAgentStandaloneTestLock(t, directory, "63015.lock", ownerUID, ownerGID)
		name := "63015.quarantine.next-" + suffix
		agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		wantErr := errors.New("injected target marker unlink failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		require.ErrorIs(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, 63015, uidLock, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})
}

// TestAgentStandaloneCovDescriptorIdentityChecksFailClosed proves the three
// places that re-identify a descriptor against its name abort when the kernel
// will not answer for it: the permanent lock inode check, the held UID lock
// inode check, and the pre-creation UID lock probe.
func TestAgentStandaloneCovDescriptorIdentityChecksFailClosed(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("permanent lock identity", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		wantErr := errors.New("injected lock fstat failure")
		agentStandaloneCovFaultSyscall(t, "fstat", 1, wantErr)

		lock, err := openAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
		require.Nil(t, lock)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("held uid lock identity", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		uidLock := createAgentStandaloneTestLock(t, directory, "63021.lock", ownerUID, ownerGID)
		wantErr := errors.New("injected held lock fstat failure")
		agentStandaloneCovFaultSyscall(t, "fstat", 1, wantErr)

		require.ErrorIs(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, 63021, uidLock, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})

	t.Run("uid lock pre-creation probe", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected uid lock fstatat failure")
		agentStandaloneCovFaultSyscall(t, "fstatat", 1, wantErr)

		require.ErrorIs(t, validateAgentStandaloneUIDLockMayBeCreated(directory, 63023), wantErr)
	})
}

// TestAgentStandaloneCovAuthorityRandomnessFailuresAbortTheClaim proves the
// two places that mint identifiers from the kernel CSPRNG — the authority id
// of a first-ever domain record and the lease id of a published ACTIVE marker
// — refuse rather than publishing a predictable or zero identifier.
func TestAgentStandaloneCovAuthorityRandomnessFailuresAbortTheClaim(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("authority id", func(t *testing.T) {
		directory, registryUID, registryGID := agentStandaloneCovPristineDomainFixture(t)
		wantErr := errors.New("injected authority id randomness failure")
		agentStandaloneCovRestoreSyscallSeams(t)
		previous := agentStandaloneRandRead
		agentStandaloneRandRead = func(payload []byte) (int, error) {
			if len(payload) == 16 {
				return 0, wantErr
			}

			return previous(payload)
		}

		lease, err := acquireAgentStandaloneDomain(
			directory, agentStandaloneCovStaticOwner(63031, 63032, "randomness"),
			registryUID, registryGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
		agentStandaloneCovRestoreSyscallSeams(t)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})

	t.Run("marker lease id", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected lease id randomness failure")
		agentStandaloneCovFaultSyscall(t, "rand", 1, wantErr)

		require.ErrorIs(t, publishAgentStandaloneActive(
			directory, 63033, 63034, ownerUID, ownerGID, "standalone:key",
			time.Now().Add(time.Second), nil, nil,
		), wantErr)
		agentStandaloneCovRestoreSyscallSeams(t)
		require.NoFileExists(t, filepath.Join(directory.Name(), "63033.quarantine"))
	})
}

// TestAgentStandaloneCovClaimRefusesWhenALeaseCannotBeReleased proves a claim
// that cannot release the registry-wide owners.lock, the domain lease it is
// stepping out of, or a lock the audit opened, refuses instead of carrying on
// as though the lease were gone.
func TestAgentStandaloneCovClaimRefusesWhenALeaseCannotBeReleased(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("owners lock after a fresh claim", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "63041.lock")
		want := agentStandaloneCovOwner(63041, 63042, "close", "/acp-go-standalone-cov-absent/state", 1, 2)
		wantErr := errors.New("injected owners.lock close failure")
		agentStandaloneCovFaultClose(t, "owners.lock", 1, wantErr)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("owners lock inside the same-boot rebind", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovOwner(63043, 63044, "rebind-close", "/srv/amp/rebind-close", 97, 98)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "63043.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteActiveMarker(t, directory, want)
		agentStandaloneCovNoVacancy(t, nil)
		wantErr := errors.New("injected rebind owners.lock close failure")
		agentStandaloneCovFaultClose(t, "owners.lock", 1, wantErr)

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("audited lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		wantErr := errors.New("injected audited lock close failure")
		agentStandaloneCovFaultClose(t, "owners.lock", 1, wantErr)

		require.ErrorIs(t, auditAgentStandaloneAuthorityRoot(
			directory, ownerUID, ownerGID, false, false, false, time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})

	t.Run("audited uid lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "63045.lock")
		wantErr := errors.New("injected audited uid lock close failure")
		agentStandaloneCovFaultClose(t, "63045.lock", 1, wantErr)

		require.ErrorIs(t, auditAgentStandaloneAuthorityRoot(
			directory, ownerUID, ownerGID, false, false, false, time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})

	t.Run("domain lease before escalation", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial")
		wantErr := errors.New("injected domain lease close failure")
		agentStandaloneCovFaultClose(t, "domain.lock", 1, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("domain lease before a rebind", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
		wantErr := errors.New("injected divergent domain lease close failure")
		agentStandaloneCovFaultClose(t, "domain.lock", 1, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, agentStandaloneCovStaticOwner(63047, 63048, "divergent-close"),
			ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
	})
}
