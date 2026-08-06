//go:build linux

package amp

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneResFaultCurrentDomain makes the nth live-domain measurement
// fail. Every caller of that measurement compares it against the published
// authority record, so a claim that carried on without one would be comparing
// the record against nothing.
func agentStandaloneResFaultCurrentDomain(t *testing.T, call int, verdict error) {
	t.Helper()
	previous := agentStandaloneCurrentDomain
	t.Cleanup(func() { agentStandaloneCurrentDomain = previous })
	hit := agentStandaloneCovNthCall(call)
	agentStandaloneCurrentDomain = func(directory *os.File) (agentAuthorityDomainRecord, error) {
		if hit() {
			return agentAuthorityDomainRecord{}, verdict
		}

		return previous(directory)
	}
}

// agentStandaloneResFaultReadlink makes the named symlink resolution fail and
// leaves every other resolution alone.
func agentStandaloneResFaultReadlink(t *testing.T, target string, verdict error) {
	t.Helper()
	previous := agentStandaloneReadlink
	t.Cleanup(func() { agentStandaloneReadlink = previous })
	agentStandaloneReadlink = func(path string) (string, error) {
		if path == target {
			return "", verdict
		}

		return previous(path)
	}
}

// agentStandaloneResNamespaceInode rewrites the inode this process appears to
// have in its PID namespace, for both the "self" anchors and the numeric
// anchor the binder resolves. The binder's verdict turns on that inode, and a
// test cannot enter or leave a PID namespace without CAP_SYS_ADMIN, so the
// kernel answer is the only thing worth substituting.
func agentStandaloneResNamespaceInode(t *testing.T, ino uint64) {
	t.Helper()
	previous := agentAuthorityDomainStat
	t.Cleanup(func() { agentAuthorityDomainStat = previous })
	numeric := filepath.Join("/proc", strconv.Itoa(os.Getpid()), "ns", "pid")
	agentAuthorityDomainStat = func(path string, stat *unix.Stat_t) error {
		if err := previous(path, stat); err != nil {
			return err
		}
		if path == "/proc/self/ns/pid" || path == "/proc/self/ns/pid_for_children" || path == numeric {
			stat.Ino = ino
		}

		return nil
	}
}

// TestAgentStandaloneResStateRootBindingRefusesWithoutAFilesystemRoot proves
// the state-root binding refuses when the kernel will not hand back a
// descriptor for "/". That descriptor is the anchor the whole walk hangs off:
// without it the only way left to reach the state root would be to resolve it
// by name, which is exactly the symlink-followable lookup the walk exists to
// avoid.
func TestAgentStandaloneResStateRootBindingRefusesWithoutAFilesystemRoot(t *testing.T) {
	wantErr := errors.New("injected filesystem root open failure")
	previous := agentStandaloneStateRootOpen
	agentStandaloneStateRootOpen = func(string, int, uint32) (int, error) { return -1, wantErr }
	t.Cleanup(func() { agentStandaloneStateRootOpen = previous })

	bound, err := bindAgentStandaloneStateRoot("/var/lib/acp-go-amp-state", 62991, 62992)
	require.Equal(t, agentStandaloneStateRoot{}, bound)
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "open filesystem root for standalone state root")
}

// TestAgentStandaloneResDomainClaimRefusesAnUnmeasurableLiveDomain proves that
// every point at which the domain claim measures the live PID/user namespace
// domain aborts when that measurement fails: the first shared-lease read, the
// re-read taken after the claim upgrades to the exclusive lease, the read that
// follows a divergent record, and the read a first-ever claim mints its record
// from. A claim that proceeded on an unmeasured domain would compare the
// published authority record against a zero value and adopt or replace it on
// that basis.
func TestAgentStandaloneResDomainClaimRefusesAnUnmeasurableLiveDomain(t *testing.T) {
	t.Run("first shared-lease read", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		before, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, err)
		wantErr := errors.New("injected shared-lease domain measurement failure")
		agentStandaloneResFaultCurrentDomain(t, 1, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
		after, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, err)
		require.Equal(t, before, after, "an unmeasured domain must never rewrite the record")
		contender, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
		require.NoError(t, err)
		require.NoError(t, unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB),
			"the refused claim must release the domain lease",
		)
		require.NoError(t, contender.Close())
	})

	t.Run("re-read under the upgraded exclusive lease", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial",
		)
		wantErr := errors.New("injected exclusive-lease domain measurement failure")
		agentStandaloneResFaultCurrentDomain(t, 2, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
		require.FileExists(t, temporary, "the temporary is only cleaned once the re-read agrees")
	})

	t.Run("read that follows a divergent record", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
		before, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, err)
		wantErr := errors.New("injected divergent-record domain measurement failure")
		agentStandaloneResFaultCurrentDomain(t, 2, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, agentStandaloneCovStaticOwner(62993, 62994, "unmeasurable"),
			ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
		after, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, err)
		require.Equal(t, before, after, "a rebind must not begin before the live domain is known")
	})

	t.Run("read a first-ever record is minted from", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		wantErr := errors.New("injected pristine domain measurement failure")
		agentStandaloneResFaultCurrentDomain(t, 1, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, agentStandaloneCovStaticOwner(62995, 62996, "unmeasurable-pristine"),
			ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})
}

// TestAgentStandaloneResAuditRefusesAnAuthorityRootItCannotName proves the
// registry audit refuses when it cannot resolve the path its own descriptor
// refers to, and that the same registry is otherwise refused for the reason
// that path exists to detect: an owner that claims the authority registry as
// its state root. Auditing without the path would silently admit that owner.
func TestAgentStandaloneResAuditRefusesAnAuthorityRootItCannotName(t *testing.T) {
	stage := func(t *testing.T) (*os.File, uint32, uint32) {
		t.Helper()
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62997.lock")
		agentStandaloneCovWriteOwner(t, directory,
			agentStandaloneCovOwner(62997, 62998, "registry-as-state-root", directory.Name(), 31, 32),
		)

		return directory, ownerUID, ownerGID
	}

	t.Run("path resolves", func(t *testing.T) {
		directory, ownerUID, ownerGID := stage(t)

		err := auditAgentStandaloneAuthorityRoot(
			directory, ownerUID, ownerGID, false, false, true, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorContains(t, err, "uses the authority registry as its state root")
	})

	t.Run("path cannot be resolved", func(t *testing.T) {
		directory, ownerUID, ownerGID := stage(t)
		wantErr := errors.New("injected authority root resolution failure")
		agentStandaloneResFaultReadlink(t, fmt.Sprintf("/proc/self/fd/%d", directory.Fd()), wantErr)

		err := auditAgentStandaloneAuthorityRoot(
			directory, ownerUID, ownerGID, false, false, true, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "resolve agent authority root path")
	})
}

// TestAgentStandaloneResProbeRefusesWhenItCannotReleaseItsContender proves the
// durability probe refuses when the second descriptor it opened to prove
// separate-open flock exclusion cannot be closed, and that it still removes
// the probe file it created. Reporting success there would certify the
// filesystem on the strength of a descriptor the probe no longer controls.
func TestAgentStandaloneResProbeRefusesWhenItCannotReleaseItsContender(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	wantErr := errors.New("injected probe contender close failure")
	previous := agentStandaloneProbeCloseFD
	agentStandaloneProbeCloseFD = func(fd int) error {
		require.NoError(t, previous(fd))

		return wantErr
	}
	t.Cleanup(func() { agentStandaloneProbeCloseFD = previous })

	require.ErrorIs(t, probeAgentStandaloneFilesystem(directory, true), wantErr)
	entries, err := os.ReadDir(directory.Name())
	require.NoError(t, err)
	require.Empty(t, entries, "the refused probe must not leave its probe file behind")
}

// TestAgentStandaloneResPublishedDomainRecordMustBeTheBytesWeWrote proves that
// a peer which rewrites domain.json in place, in the window between the
// publishing rename and the read-back, is caught by the byte comparison rather
// than passed off as our own publication. The rewrite keeps the inode, so the
// inode identity check that follows would have accepted it; the assertion
// below pins that the payload comparison is what refuses.
func TestAgentStandaloneResPublishedDomainRecordMustBeTheBytesWeWrote(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	peer := record
	peer.AuthorityID = "fedcba9876543210fedcba9876543210"
	peerPayload, err := json.Marshal(peer)
	require.NoError(t, err)
	peerPayload = append(peerPayload, '\n')
	recordPath := filepath.Join(directory.Name(), "domain.json")

	agentStandaloneCovRestoreSyscallSeams(t)
	previous := agentStandaloneRenameat
	var temporaryInode uint64
	agentStandaloneRenameat = func(oldDir int, oldPath string, newDir int, newPath string) error {
		var staged unix.Stat_t
		require.NoError(t, unix.Fstatat(oldDir, oldPath, &staged, unix.AT_SYMLINK_NOFOLLOW))
		if renameErr := previous(oldDir, oldPath, newDir, newPath); renameErr != nil {
			return renameErr
		}
		temporaryInode = staged.Ino
		require.NoError(t, os.WriteFile(recordPath, peerPayload, 0o600))

		return nil
	}

	err = replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record)
	require.ErrorContains(t, err, "published agent authority record payload changed")
	published, err := os.ReadFile(recordPath)
	require.NoError(t, err)
	require.Equal(t, peerPayload, published, "the refusal must not overwrite what it found")
	var named unix.Stat_t
	require.NoError(t, unix.Stat(recordPath, &named))
	require.Equal(t, temporaryInode, named.Ino,
		"the rewrite kept our inode, so only the payload comparison could have caught it",
	)
	entries, err := os.ReadDir(directory.Name())
	require.NoError(t, err)
	require.Len(t, entries, 1, "no publication temporary may survive the refusal")
	require.Equal(t, "domain.json", entries[0].Name())
}

// TestAgentStandaloneResOwnerClaimRefusesAnInterruptionDuringFinalRevalidation
// proves that a signal which arrives while the last state-root revalidation is
// in flight still stops the claim before the ACTIVE marker is published. The
// interruption check on either side of that revalidation is the last chance to
// abandon a claim, and publishing ACTIVE for a claim the operator interrupted
// would leave a disposition nobody is holding.
func TestAgentStandaloneResOwnerClaimRefusesAnInterruptionDuringFinalRevalidation(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("protected state-root interruption test requires root")
	}
	const uid, gid = uint32(62961), uint32(62962)
	stateRoot := createAgentStandaloneProtectedStateRoot(t, uid, gid)
	bound, err := bindAgentStandaloneStateRoot(stateRoot, uid, gid)
	require.NoError(t, err)
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	owners := createAgentStandaloneTestLock(t, directory, "owners.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(owners.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	uidLock := createAgentStandaloneTestLock(t, directory, "62961.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(uidLock.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	want := agentStandaloneOwner{
		Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "interrupted-revalidation", StateRoot: bound,
	}
	agentStandaloneCovNoVacancy(t, nil)
	signals := make(chan os.Signal, 1)
	binds := 0
	previous := agentStandaloneStateRootOpen
	agentStandaloneStateRootOpen = func(path string, flags int, mode uint32) (int, error) {
		binds++
		if binds == 2 {
			signals <- unix.SIGTERM
		}

		return previous(path, flags, mode)
	}
	t.Cleanup(func() { agentStandaloneStateRootOpen = previous })

	err = completeAgentStandaloneOwnerClaim(
		directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, signals,
	)
	require.ErrorContains(t, err, "interrupted by terminated")
	require.Equal(t, 2, binds, "the interruption must land on the second revalidation, not the first")
	require.FileExists(t, filepath.Join(directory.Name(), "62961.owner"))
	require.NoFileExists(t, filepath.Join(directory.Name(), "62961.quarantine"),
		"an interrupted claim must never publish ACTIVE",
	)
}

// TestAgentStandaloneResBinderRefusesEveryProcfsDisagreement proves the
// standalone authority binder refuses when the PID namespace cannot be
// established, when procfs will not name this process, when it names another
// process, when the namespace behind that name cannot be read, when it is not
// the namespace this process reported for itself, and when PID 1 is not
// visible. It then proves the rule those checks exist to enforce, in both
// directions: the initial PID namespace may establish authority, and any other
// namespace may do so only from its own PID 1.
//
// The namespace inode is substituted rather than entered, because creating a
// PID namespace needs CAP_SYS_ADMIN which the coverage container does not
// grant. Everything else here is the real /proc of the running process.
func TestAgentStandaloneResBinderRefusesEveryProcfsDisagreement(t *testing.T) {
	const initialPIDNamespaceInode = 0xeffffffc
	selfPID := strconv.Itoa(os.Getpid())

	t.Run("PID namespace cannot be established", func(t *testing.T) {
		wantErr := errors.New("injected PID namespace stat failure")
		previous := agentAuthorityDomainStat
		t.Cleanup(func() { agentAuthorityDomainStat = previous })
		agentAuthorityDomainStat = func(path string, stat *unix.Stat_t) error {
			if path == "/proc/self/ns/pid" {
				return wantErr
			}

			return previous(path, stat)
		}

		require.ErrorIs(t, validateAgentStandaloneBinder(), wantErr)
	})

	t.Run("procfs will not name this process", func(t *testing.T) {
		wantErr := errors.New("injected proc self readlink failure")
		agentStandaloneResFaultReadlink(t, "/proc/self", wantErr)

		err := validateAgentStandaloneBinder()
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "resolve procfs self PID anchor")
	})

	t.Run("procfs names another process", func(t *testing.T) {
		other := strconv.Itoa(os.Getpid() + 1)
		previous := agentStandaloneReadlink
		t.Cleanup(func() { agentStandaloneReadlink = previous })
		agentStandaloneReadlink = func(path string) (string, error) {
			if path == "/proc/self" {
				return other, nil
			}

			return previous(path)
		}

		require.ErrorContains(t, validateAgentStandaloneBinder(),
			fmt.Sprintf("procfs self PID anchor is %q, want %q", other, selfPID),
		)
	})

	t.Run("named PID namespace cannot be read", func(t *testing.T) {
		wantErr := errors.New("injected numeric PID namespace stat failure")
		numeric := filepath.Join("/proc", selfPID, "ns", "pid")
		previous := agentAuthorityDomainStat
		t.Cleanup(func() { agentAuthorityDomainStat = previous })
		agentAuthorityDomainStat = func(path string, stat *unix.Stat_t) error {
			if path == numeric {
				return wantErr
			}

			return previous(path, stat)
		}

		err := validateAgentStandaloneBinder()
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "inspect procfs self PID namespace anchor")
	})

	t.Run("named PID namespace is not the one we reported", func(t *testing.T) {
		numeric := filepath.Join("/proc", selfPID, "ns", "pid")
		previous := agentAuthorityDomainStat
		t.Cleanup(func() { agentAuthorityDomainStat = previous })
		agentAuthorityDomainStat = func(path string, stat *unix.Stat_t) error {
			if err := previous(path, stat); err != nil {
				return err
			}
			if path == numeric {
				stat.Ino++
			}

			return nil
		}

		require.ErrorContains(t, validateAgentStandaloneBinder(),
			"requires self and procfs PID namespaces to match",
		)
	})

	t.Run("PID 1 is not visible", func(t *testing.T) {
		wantErr := errors.New("injected init status read failure")
		previous := agentStandaloneReadFile
		t.Cleanup(func() { agentStandaloneReadFile = previous })
		agentStandaloneReadFile = func(path string) ([]byte, error) {
			if path == "/proc/1/status" {
				return nil, wantErr
			}

			return previous(path)
		}

		err := validateAgentStandaloneBinder()
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "prove unrestricted root procfs visibility")
	})

	t.Run("initial PID namespace establishes authority", func(t *testing.T) {
		agentStandaloneResNamespaceInode(t, initialPIDNamespaceInode)

		require.NoError(t, validateAgentStandaloneBinder())
	})

	t.Run("non-initial PID namespace below its own PID 1", func(t *testing.T) {
		if os.Getpid() == 1 {
			t.Skip("this process is namespace PID 1, which may establish authority")
		}
		agentStandaloneResNamespaceInode(t, initialPIDNamespaceInode-1)

		require.ErrorContains(t, validateAgentStandaloneBinder(),
			"non-initial PID namespace may establish agent authority only from namespace PID 1",
		)
	})
}

// agentStandaloneResStageMarkerTemporaryRace stages the one registry state that
// makes the pristine-registry audit report a live UID holder: a marker
// temporary with no permanent UID lock beside it, plus a peer that creates and
// holds that UID lock in the window between the audit listing the registry and
// the audit reaching the temporary. That window is real — the listing is a
// snapshot taken through (*os.File).ReadDir, and a concurrent
// acquireAgentStandaloneOwnerIdentity creates its UID lock exactly there — and
// the lock-open seam is the only way to place a peer inside it.
func agentStandaloneResStageMarkerTemporaryRace(
	t *testing.T,
	directory *os.File,
	ownerUID uint32,
	ownerGID uint32,
	arrive func(),
) {
	t.Helper()
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "62997.quarantine.next-"+agentStandaloneCovSuffix, "{}\n",
	)
	restoreAgentStandalonePermanentLockSeams(t)
	original := agentStandaloneLockOpenat
	raced := false
	agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		if path == "62997.lock" && !raced {
			raced = true
			held, err := openAgentStandaloneNamedLock(directory, "62997.lock", true, ownerUID, ownerGID)
			require.NoError(t, err)
			require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
			t.Cleanup(func() { _ = held.Close() })
			require.NoError(t, os.Remove(temporary))
			arrive()
		}

		return original(dirfd, path, flags, mode)
	}
	t.Cleanup(func() { require.True(t, raced, "the peer never took the UID lock") })
}

// agentStandaloneResDomainLockIsFree asserts the claim released the domain lock
// it took, so a refusal never leaves the registry wedged for the next claim.
func agentStandaloneResDomainLockIsFree(t *testing.T, directory *os.File, ownerUID, ownerGID uint32) {
	t.Helper()
	contender, acquired, err := tryAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	require.True(t, acquired, "the refused claim must release the domain lock")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneResDomainClaimRetriesAroundALiveUIDHolder proves the
// first-ever domain claim treats "a peer holds that identity's UID lock" as a
// reason to wait and look again, not as a reason to refuse the registry, and
// that once the peer's lock is the only prior state left the claim refuses by
// naming it. The audit reads the registry before it adjudicates it, so a UID
// lock created inside that window must not be mistaken for an unaccountable
// registry — and a claim that gave up there would refuse a registry that is
// merely busy.
func TestAgentStandaloneResDomainClaimRetriesAroundALiveUIDHolder(t *testing.T) {
	want := agentStandaloneCovOwner(62997, 62998, "res-busy", "/srv/amp/res-busy", 41, 42)

	t.Run("waits and looks again", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		agentStandaloneResStageMarkerTemporaryRace(t, directory, ownerUID, ownerGID, func() {})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(2*time.Second), nil, nil,
		)
		require.Nil(t, authority)
		require.ErrorContains(t, err, `root contains prior lock "62997.lock"`)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		agentStandaloneResDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})

	t.Run("gives up when cancellation arrives first", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		canceled := make(chan struct{})
		agentStandaloneResStageMarkerTemporaryRace(t, directory, ownerUID, ownerGID, func() {
			close(canceled)
		})

		authority, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(2*time.Second), canceled, nil,
		)
		require.Nil(t, authority)
		require.ErrorIs(t, err, errAgentStandaloneCanceled)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		agentStandaloneResDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}
