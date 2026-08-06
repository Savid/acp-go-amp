//go:build linux

package amp

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneCovPlantOnLockOpen runs plant the first time the named
// permanent lock is opened, which is how these cases stage a peer's write
// inside a window the claim cannot otherwise be interrupted in.
func agentStandaloneCovPlantOnLockOpen(t *testing.T, name string, plant func()) {
	t.Helper()
	restoreAgentStandalonePermanentLockSeams(t)
	original := agentStandaloneLockOpenat
	planted := false
	agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		if !planted && path == name {
			planted = true
			plant()
		}

		return original(dirfd, path, flags, mode)
	}
}

// TestAgentStandaloneCovDurabilityProbeRefusesUnusableStorage proves the
// authority durability probe refuses storage it cannot interrogate, refuses a
// read-only mount, refuses a registry root that has been removed, accepts a
// filesystem type on the durable allowlist, and reports a failed cleanup of
// its own probe file rather than swallowing it. The probe is what decides
// whether this filesystem may hold agent authority at all.
func TestAgentStandaloneCovDurabilityProbeRefusesUnusableStorage(t *testing.T) {
	t.Run("filesystem cannot be interrogated", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected statfs failure")
		previous := agentStandaloneProbeFstatfs
		agentStandaloneProbeFstatfs = func(int, *unix.Statfs_t) error { return wantErr }
		t.Cleanup(func() { agentStandaloneProbeFstatfs = previous })

		require.ErrorIs(t, probeAgentStandaloneFilesystem(directory, true), wantErr)
	})

	t.Run("read-only mount", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		previous := agentStandaloneProbeFstatfs
		agentStandaloneProbeFstatfs = func(fd int, filesystem *unix.Statfs_t) error {
			if err := previous(fd, filesystem); err != nil {
				return err
			}
			filesystem.Flags |= unix.ST_RDONLY

			return nil
		}
		t.Cleanup(func() { agentStandaloneProbeFstatfs = previous })

		require.ErrorContains(t, probeAgentStandaloneFilesystem(directory, true), "filesystem is read-only")
	})

	t.Run("allowlisted filesystem type", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		previous := agentStandaloneProbeFstatfs
		agentStandaloneProbeFstatfs = func(fd int, filesystem *unix.Statfs_t) error {
			if err := previous(fd, filesystem); err != nil {
				return err
			}
			filesystem.Type = 0xef53

			return nil
		}
		t.Cleanup(func() { agentStandaloneProbeFstatfs = previous })

		require.NoError(t, probeAgentStandaloneFilesystem(directory, false))
		entries, err := os.ReadDir(directory.Name())
		require.NoError(t, err)
		require.Empty(t, entries, "the probe must leave nothing behind")
	})

	t.Run("registry root removed", func(t *testing.T) {
		require.ErrorIs(t, probeAgentStandaloneFilesystem(agentStandaloneCovRemovedDirectory(t), true), unix.ENOENT)
	})

	t.Run("probe file cannot be removed", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		wantErr := errors.New("injected probe unlink failure")
		previous := agentStandaloneProbeUnlinkat
		agentStandaloneProbeUnlinkat = func(int, string, int) error { return wantErr }
		t.Cleanup(func() { agentStandaloneProbeUnlinkat = previous })

		require.ErrorIs(t, probeAgentStandaloneFilesystem(directory, true), wantErr)
	})
}

// TestAgentStandaloneCovProbeDescriptorMustBeCloseOnExec proves the probe
// refuses when the descriptor flags cannot be read, cannot be set, or cannot
// be read back. A probe descriptor that survived an exec would hand a spawned
// agent an open write descriptor inside the authority registry.
func TestAgentStandaloneCovProbeDescriptorMustBeCloseOnExec(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		fault int
		want  string
	}{
		{name: "read flags", fault: 1, want: "read authority probe descriptor flags"},
		{name: "set close-on-exec", fault: 2, want: "set authority probe close-on-exec"},
		{name: "re-read flags", fault: 3, want: "re-read authority probe descriptor flags"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			wantErr := errors.New("injected fcntl failure")
			previous := agentStandaloneProbeFcntl
			calls := 0
			agentStandaloneProbeFcntl = func(fd uintptr, cmd, arg int) (int, error) {
				calls++
				if calls == testCase.fault {
					return 0, wantErr
				}

				return previous(fd, cmd, arg)
			}
			t.Cleanup(func() { agentStandaloneProbeFcntl = previous })

			err := probeAgentStandaloneFilesystem(directory, true)
			require.ErrorIs(t, err, wantErr)
			require.ErrorContains(t, err, testCase.want)
		})
	}
}

// TestAgentStandaloneCovStandaloneIdentityRefusesBeforeItTakesAnything proves
// the top-level standalone acquisition refuses an unusable state root, a
// test-only disposition with no test root, a state root that lives inside the
// agent identity authority root, and a run root it cannot bootstrap — each
// before it has taken any lease.
func TestAgentStandaloneCovStandaloneIdentityRefusesBeforeItTakesAnything(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone identity acquisition requires root to build a protected state root")
	}
	const uid, gid = uint32(62971), uint32(62972)

	t.Run("state root is not a clean absolute path", func(t *testing.T) {
		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "owner", "srv/amp/state", true, t.TempDir(), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "must be a clean absolute path")
	})

	t.Run("test-only disposition without a test root", func(t *testing.T) {
		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "owner", createAgentStandaloneProtectedStateRoot(t, uid, gid), true, "", nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "test agent identity lock root is required")
	})

	t.Run("state root inside the authority root", func(t *testing.T) {
		testRoot := t.TempDir()
		require.NoError(t, os.Chmod(testRoot, 0o700))
		require.NoError(t, os.Mkdir(filepath.Join(testRoot, "acp-go"), 0o700))
		stateRoot := filepath.Join(testRoot, "acp-go", "agent-identities")
		require.NoError(t, os.Mkdir(stateRoot, 0o700))
		require.NoError(t, os.Chown(stateRoot, int(uid), int(gid)))

		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "owner", stateRoot, true, testRoot, nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "must be separate from the agent identity authority root")
	})

	t.Run("run root that cannot be bootstrapped", func(t *testing.T) {
		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "owner", createAgentStandaloneProtectedStateRoot(t, uid, gid), true,
			filepath.Join(t.TempDir(), "absent"), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "open agent identity runtime root")
	})
}

// TestAgentStandaloneCovStandaloneIdentityReleasesTheAuthorityOnLaterFailure
// proves the top-level acquisition surfaces a refused authority claim, and
// that when the authority is taken but the owner identity is then refused, the
// authority lease is released rather than leaked. A leaked domain lease would
// keep every later claim on the host waiting behind a process that already
// gave up.
func TestAgentStandaloneCovStandaloneIdentityReleasesTheAuthorityOnLaterFailure(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone identity acquisition requires root to build a protected state root")
	}
	const uid, gid = uint32(62973), uint32(62974)

	t.Run("authority claim refused", func(t *testing.T) {
		testRoot := t.TempDir()
		require.NoError(t, os.Chmod(testRoot, 0o700))
		wantErr := errors.New("injected authority probe failure")
		agentStandaloneCovFailingProbe(t, wantErr)

		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "owner", createAgentStandaloneProtectedStateRoot(t, uid, gid), true, testRoot, nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("owner identity refused after the authority is taken", func(t *testing.T) {
		testRoot := t.TempDir()
		require.NoError(t, os.Chmod(testRoot, 0o700))
		canceled := make(chan struct{})
		previous := agentStandaloneReplaceDomain
		agentStandaloneReplaceDomain = func(
			directory *os.File, ownerUID, ownerGID uint32, record agentAuthorityDomainRecord,
		) error {
			err := previous(directory, ownerUID, ownerGID, record)
			close(canceled)

			return err
		}
		t.Cleanup(func() { agentStandaloneReplaceDomain = previous })

		identity, err := acquireAgentStandaloneIdentity(
			uid, gid, "owner", createAgentStandaloneProtectedStateRoot(t, uid, gid), true, testRoot,
			canceled, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, errAgentStandaloneCanceled)
		authority, err := openAgentIdentityLockDirectory(testRoot, uint32(os.Geteuid()), uint32(os.Getegid()))
		require.NoError(t, err)
		defer authority.Close()
		lease, taken, lockErr := tryAgentStandaloneNamedLock(
			authority, "domain.lock", false, uint32(os.Geteuid()), uint32(os.Getegid()),
		)
		require.NoError(t, lockErr)
		require.True(t, taken, "the refused claim must release the authority domain lease")
		require.NoError(t, lease.Close())
	})
}

// TestAgentStandaloneCovDomainClaimActsOnWhatTheExclusiveLeaseReveals proves
// two more windows the exclusive lease exists to close: a peer that writes an
// unparsable record while a pristine claim queues, and a peer that makes a
// domain-record temporary untrusted while a matching-domain claim queues for
// the exclusive lease it needs to clean that temporary.
func TestAgentStandaloneCovDomainClaimActsOnWhatTheExclusiveLeaseReveals(t *testing.T) {
	t.Run("peer writes an unparsable record while a pristine claim queues", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		held := agentStandaloneCovHoldDomainShared(t, directory)
		corrupted := make(chan struct{})
		go func() {
			time.Sleep(60 * time.Millisecond)
			writeErr := os.WriteFile(filepath.Join(directory.Name(), "domain.json"), []byte("not json\n"), 0o600)
			closeErr := held.Close()
			if writeErr != nil || closeErr != nil {
				panic(errors.Join(writeErr, closeErr))
			}
			close(corrupted)
		}()

		lease, err := acquireAgentStandaloneDomain(
			directory, agentStandaloneCovStaticOwner(62975, 62976, "pristine-race"),
			ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		<-corrupted
		require.Nil(t, lease)
		require.ErrorContains(t, err, "invalid character")
	})

	t.Run("peer makes the domain temporary untrusted while we queue", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial",
		)
		held := agentStandaloneCovHoldDomainShared(t, directory)
		tampered := make(chan struct{})
		go func() {
			time.Sleep(60 * time.Millisecond)
			chmodErr := os.Chmod(temporary, 0o644)
			closeErr := held.Close()
			if chmodErr != nil || closeErr != nil {
				panic(errors.Join(chmodErr, closeErr))
			}
			close(tampered)
		}()

		lease, err := acquireAgentStandaloneDomain(
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		<-tampered
		require.Nil(t, lease)
		require.ErrorContains(t, err, "not a trusted bounded regular file")
		require.FileExists(t, temporary)
	})
}

// TestAgentStandaloneCovOwnerIdentityRestartsOnRegistryStateItDidNotSee
// proves the claim restarts rather than proceeding when a peer's owner
// temporary appears after the claim has already checked for temporaries: an
// accountable one is drained on the next pass, and one whose UID lock is still
// held keeps the claim waiting instead of being removed.
func TestAgentStandaloneCovOwnerIdentityRestartsOnRegistryStateItDidNotSee(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	suffix := agentStandaloneCovSuffix

	t.Run("peer temporary seen only by the registry audit", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovOwner(62961, 62962, "existing-temp", "/acp-go-standalone-cov-absent/state", 1, 2)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62961.lock")
		agentStandaloneCovPermanentLock(t, directory, "62963.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		temporary := filepath.Join(directory.Name(), "62963.owner.next-"+suffix)
		agentStandaloneCovPlantOnLockOpen(t, "62961.lock", func() {
			require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(5*time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT, "the restarted claim reaches its own state root refusal")
		require.NoFileExists(t, temporary, "the accountable peer temporary is drained on the next pass")
	})

	t.Run("peer temporary with a live uid holder keeps the claim waiting", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		held := createAgentStandaloneTestLock(t, directory, "62965.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		temporary := filepath.Join(directory.Name(), "62965.owner.next-"+suffix)
		agentStandaloneCovPlantOnLockOpen(t, "owners.lock", func() {
			require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62967, 62968, "waiting"),
			ownerUID, ownerGID, time.Now().Add(150*time.Millisecond), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.FileExists(t, temporary, "a live peer temporary is never removed")
	})
}
