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

// agentStandaloneCovDivergentDomainFixture publishes a domain record that
// belongs to another PID namespace, which is what a claim sees when the host
// rebooted into a different namespace domain since the registry was written.
func agentStandaloneCovDivergentDomainFixture(t *testing.T) (*os.File, uint32, uint32) {
	t.Helper()
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "domain.lock")
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	record.PIDNamespace.Ino++
	require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))

	return directory, ownerUID, ownerGID
}

// agentStandaloneCovPristineDomainFixture stages a registry that has its
// permanent domain lock but has never published an authority record.
func agentStandaloneCovPristineDomainFixture(t *testing.T) (*os.File, uint32, uint32) {
	t.Helper()
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "domain.lock")

	return directory, ownerUID, ownerGID
}

// agentStandaloneCovHoldDomainShared takes a shared lease on the permanent
// domain lock, which lets other shared readers in but blocks any contender
// that needs the exclusive lease.
func agentStandaloneCovHoldDomainShared(t *testing.T, directory *os.File) *os.File {
	t.Helper()
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	held, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_SH|unix.LOCK_NB))

	return held
}

// agentStandaloneCovFailingProbe makes the durability probe refuse and reports
// how many times the claim consulted it, so a case can pin where in the domain
// transition the probe is reached and prove that an earlier refusal never
// reached it at all.
func agentStandaloneCovFailingProbe(t *testing.T, verdict error) *int {
	t.Helper()
	previous := agentStandaloneFilesystemProbe
	probes := 0
	agentStandaloneFilesystemProbe = func(*os.File, bool) error {
		probes++

		return verdict
	}
	t.Cleanup(func() { agentStandaloneFilesystemProbe = previous })

	return &probes
}

// agentStandaloneCovBinderVerdicts are the two answers the process binder can
// give a claim, each forced through an existing seam rather than inherited from
// whichever PID namespace the container happens to give the test. CI runs this
// package in the initial namespace, where the binder always agrees, and the
// fast iteration container runs it in a nested one, where the binder always
// refuses; without substituting the verdict each environment would leave the
// other's branch unproven.
func agentStandaloneCovBinderVerdicts(refusal error) []struct {
	name    string
	arrange func(t *testing.T)
	wantErr func(probeErr error) error
	probes  int
} {
	const initialPIDNamespaceInode = 0xeffffffc

	return []struct {
		name    string
		arrange func(t *testing.T)
		wantErr func(probeErr error) error
		probes  int
	}{
		{
			name: "binder refuses",
			arrange: func(t *testing.T) {
				t.Helper()
				agentStandaloneResFaultReadlink(t, "/proc/self", refusal)
			},
			wantErr: func(error) error { return refusal },
		},
		{
			name: "binder agrees",
			arrange: func(t *testing.T) {
				t.Helper()
				agentStandaloneResNamespaceInode(t, initialPIDNamespaceInode)
			},
			wantErr: func(probeErr error) error { return probeErr },
			probes:  1,
		},
	}
}

// TestAgentStandaloneCovDomainAcquisitionRefusesAnUnusableRegistry proves the
// authority domain acquisition refuses a domain lock that is not the trusted
// permanent inode, refuses a record it cannot parse, and refuses when the
// exclusive lease it needs is not available within the claim budget. Any of
// these proceeding would let a claim publish an authority record beside a peer
// that still believes it holds the domain.
func TestAgentStandaloneCovDomainAcquisitionRefusesAnUnusableRegistry(t *testing.T) {
	want := agentStandaloneCovStaticOwner(62901, 62902, "domain-claim")

	t.Run("domain lock with wrong mode", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "domain.lock"), 0o644))

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorContains(t, err, "mode")
	})

	t.Run("unreadable domain record", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
		agentStandaloneCovPermanentLock(t, directory, "domain.lock")
		agentStandaloneCovWriteRegistryFile(t, directory, "domain.json", "not json\n")

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorContains(t, err, "invalid character")
	})

	t.Run("exclusive lease unavailable for a pristine registry", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		held := agentStandaloneCovHoldDomainShared(t, directory)
		defer held.Close()

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(120*time.Millisecond), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})

	t.Run("exclusive lease unavailable for a matching-domain cleanup", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial",
		)
		held := agentStandaloneCovHoldDomainShared(t, directory)
		defer held.Close()

		lease, err := acquireAgentStandaloneDomain(
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(120*time.Millisecond), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.FileExists(t, temporary, "the temporary is only cleaned under the exclusive lease")
	})
}

// TestAgentStandaloneCovDomainAcquisitionRereadsUnderTheExclusiveLease proves
// the claim re-reads the authority record after it takes the exclusive lease
// and acts on what it finds there. A peer that published, corrupted or
// replaced the record while we queued for the lease must decide the outcome,
// not the record we read before queueing.
func TestAgentStandaloneCovDomainAcquisitionRereadsUnderTheExclusiveLease(t *testing.T) {
	// The adopting branch and the shared-lease-window refusal it re-opens are
	// proven in agent_standalone_identity_adoption_linux_test.go, which owns the
	// defect that branch carried.

	t.Run("peer corrupts the record while we queue", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial")
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
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		<-corrupted
		require.Nil(t, lease)
		require.ErrorContains(t, err, "invalid character")
	})

	t.Run("peer replaces the record with another domain while we queue", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial",
		)
		divergent, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		divergent.AuthorityID = "fedcba9876543210fedcba9876543210"
		divergent.PIDNamespace.Ino++
		held := agentStandaloneCovHoldDomainShared(t, directory)
		replaced := make(chan struct{})
		go func() {
			time.Sleep(60 * time.Millisecond)
			replaceErr := replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, divergent)
			closeErr := held.Close()
			if replaceErr != nil || closeErr != nil {
				panic(errors.Join(replaceErr, closeErr))
			}
			close(replaced)
		}()

		lease, err := acquireAgentStandaloneDomain(
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		<-replaced
		require.Nil(t, lease)
		require.ErrorIs(t, err, unix.ENOENT, "the restarted claim rebinds and stops at the missing owners.lock")
		require.NoFileExists(t, temporary, "the restarted claim cleans the accountable temporary")
	})
}

// TestAgentStandaloneCovDomainRebindRefusesAnUnaccountableRegistry proves a
// rebind onto a foreign authority record refuses while owner temporaries are
// unaccounted for, while the registry audit rejects an entry, and while a
// marker temporary still has a live UID holder. Rebinding past any of these
// would adopt an authority whose registry state nobody has accounted for.
func TestAgentStandaloneCovDomainRebindRefusesAnUnaccountableRegistry(t *testing.T) {
	want := agentStandaloneCovStaticOwner(62905, 62906, "rebind")
	suffix := agentStandaloneCovSuffix

	t.Run("owner temporary without its uid lock", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		temporary := agentStandaloneCovWriteRegistryFile(t, directory, "62907.owner.next-"+suffix, "partial")

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, unix.ENOENT)
		require.FileExists(t, temporary)
	})

	t.Run("registry entry that belongs to nothing", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "leftover", "x")

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorContains(t, err, `unknown entry "leftover"`)
	})

	t.Run("marker temporary with a live uid holder", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		held := createAgentStandaloneTestLock(t, directory, "62909.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		temporary := agentStandaloneCovWriteRegistryFile(t, directory, "62909.quarantine.next-"+suffix, "partial")

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(120*time.Millisecond), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.FileExists(t, temporary, "a live marker temporary is never removed")
	})

	t.Run("marker temporary released mid-claim is cleaned and the rebind continues", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		held := createAgentStandaloneTestLock(t, directory, "62911.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		temporary := agentStandaloneCovWriteRegistryFile(t, directory, "62911.quarantine.next-"+suffix, "partial")
		released := make(chan struct{})
		go func() {
			time.Sleep(60 * time.Millisecond)
			if err := held.Close(); err != nil {
				panic(err)
			}
			close(released)
		}()

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		<-released
		require.Nil(t, lease)
		require.ErrorContains(t, err, "requires exactly one standalone owner binding")
		require.NoFileExists(t, temporary, "the released marker temporary is cleaned once its holder is gone")
	})
}

// TestAgentStandaloneCovSameBootRebindRefusesAnythingButItsOwnExactState
// proves the same-boot rebind refuses without the permanent owners.lock,
// refuses on an exhausted budget, refuses an owner name that is not a uid,
// refuses when the registry holds anything other than exactly this one owner,
// refuses when the owner's UID lock still has a holder, refuses an owner
// record that is not the exact claimed tuple and refuses a missing or
// mismatched retained marker. Same-boot rebinding is the one path that adopts
// an authority without a reboot in between, so it may only proceed on state it
// has proved is its own.
func TestAgentStandaloneCovSameBootRebindRefusesAnythingButItsOwnExactState(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	want := agentStandaloneCovStaticOwner(62921, 62922, "same-boot")

	t.Run("no permanent owners lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
	})

	t.Run("expired budget", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(-time.Millisecond), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "exceeded 30 seconds")
	})

	t.Run("owner name is not a uid", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovWriteRegistryFile(t, directory, "bad.owner", "{}\n")

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "invalid uid")
	})

	t.Run("no standalone owner at all", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "requires exactly one standalone owner binding")
	})

	t.Run("uid lock still has a holder", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		held := createAgentStandaloneTestLock(t, directory, "62921.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "still has a live UID lock holder")
	})

	t.Run("owner record is another tuple", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62921.lock")
		agentStandaloneCovWriteOwner(t, directory,
			agentStandaloneCovStaticOwner(62921, 62922, "somebody-else"),
		)

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "requires the exact standalone owner binding")
	})

	t.Run("no retained marker", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62921.lock")
		agentStandaloneCovWriteOwner(t, directory, want)

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "requires the retained standalone ACTIVE marker")
	})

	t.Run("retained marker is not this session", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62921.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteCleanMarker(t, directory, want.UID, want.GID, "another-session")

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "requires the exact retained standalone ACTIVE marker")
	})
}

// TestAgentStandaloneCovSameBootRebindReleasesItsIdentityOnLaterFailure proves
// the UID lock the same-boot rebind acquired is released when the durability
// probe or the record publication that follows it refuses, and that the old
// authority record survives untouched. Leaking that lock would leave the UID
// permanently unclaimable by any later process.
func TestAgentStandaloneCovSameBootRebindReleasesItsIdentityOnLaterFailure(t *testing.T) {
	const uid, gid = uint32(62931), uint32(62932)
	for _, fault := range []string{"probe", "publication"} {
		t.Run(fault, func(t *testing.T) {
			directory := openAgentStandaloneTestDirectory(t)
			ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
			agentStandaloneCovPermanentLock(t, directory, "domain.lock")
			record, err := currentAgentAuthorityDomain(directory)
			require.NoError(t, err)
			record.AuthorityID = "0123456789abcdef0123456789abcdef"
			record.PIDNamespace.Ino++
			require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record))
			before, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
			require.NoError(t, err)
			owner := agentStandaloneCovOwner(uid, gid, "rebind-fault", "/srv/amp/rebind-fault", 31, 32)
			agentStandaloneCovPermanentLock(t, directory, "owners.lock")
			agentStandaloneCovPermanentLock(t, directory, "62931.lock")
			agentStandaloneCovWriteOwner(t, directory, owner)
			agentStandaloneCovWriteActiveMarker(t, directory, owner)
			agentStandaloneCovNoVacancy(t, nil)
			wantErr := errors.New("injected " + fault + " failure")
			if fault == "probe" {
				agentStandaloneCovFailingProbe(t, wantErr)
			} else {
				previous := agentStandaloneReplaceDomain
				agentStandaloneReplaceDomain = func(
					*os.File, uint32, uint32, agentAuthorityDomainRecord,
				) error {
					return wantErr
				}
				t.Cleanup(func() { agentStandaloneReplaceDomain = previous })
			}

			lease, err := acquireAgentStandaloneDomain(
				directory, owner, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
			)
			require.Nil(t, lease)
			require.ErrorIs(t, err, wantErr)
			after, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
			require.NoError(t, err)
			require.Equal(t, before, after, "the old authority record must survive a refused rebind")
			contender, taken, lockErr := tryAgentStandaloneNamedLock(directory, "62931.lock", false, ownerUID, ownerGID)
			require.NoError(t, lockErr)
			require.True(t, taken, "the rebind must release the UID lock it acquired")
			require.NoError(t, contender.Close())
		})
	}
}

// TestAgentStandaloneCovPristineDomainClaimRefusesUnaccountableState proves a
// first-ever authority claim refuses while owner temporaries are unaccounted
// for, refuses a registry that is not actually pristine, and refuses when the
// durability probe or the record publication fails — publishing no record in
// any of those cases.
func TestAgentStandaloneCovPristineDomainClaimRefusesUnaccountableState(t *testing.T) {
	want := agentStandaloneCovStaticOwner(62941, 62942, "pristine")
	suffix := agentStandaloneCovSuffix

	t.Run("owner temporary without its uid lock", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		temporary := agentStandaloneCovWriteRegistryFile(t, directory, "62943.owner.next-"+suffix, "partial")

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, unix.ENOENT)
		require.FileExists(t, temporary)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})

	t.Run("owner temporary with a live uid holder", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		held := createAgentStandaloneTestLock(t, directory, "62945.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		temporary := agentStandaloneCovWriteRegistryFile(t, directory, "62945.owner.next-"+suffix, "partial")

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(120*time.Millisecond), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.FileExists(t, temporary)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})

	t.Run("owner temporary released mid-claim exposes the non-pristine registry", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		held := createAgentStandaloneTestLock(t, directory, "62947.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		temporary := agentStandaloneCovWriteRegistryFile(t, directory, "62947.owner.next-"+suffix, "partial")
		released := make(chan struct{})
		go func() {
			time.Sleep(60 * time.Millisecond)
			if err := held.Close(); err != nil {
				panic(err)
			}
			close(released)
		}()

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		<-released
		require.Nil(t, lease)
		require.ErrorContains(t, err, "record is missing but root contains prior lock")
		require.NoFileExists(t, temporary, "the released owner temporary is drained")
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})

	t.Run("durability probe refuses", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		wantErr := errors.New("injected pristine probe failure")
		agentStandaloneCovFailingProbe(t, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})

	t.Run("record publication refuses", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		wantErr := errors.New("injected pristine publication failure")
		previous := agentStandaloneReplaceDomain
		agentStandaloneReplaceDomain = func(*os.File, uint32, uint32, agentAuthorityDomainRecord) error {
			return wantErr
		}
		t.Cleanup(func() { agentStandaloneReplaceDomain = previous })

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})
}

// TestAgentStandaloneCovAuthorityBinderMatchesTheProcessPIDNamespace proves
// the binder agrees with what procfs says about this process: its self anchor,
// its namespace identity and its visibility of PID 1. It then proves the one
// rule that decides whether a non-initial PID namespace may establish
// authority at all — only namespace PID 1 may — by asserting the exact verdict
// for the namespace this test is actually running in.
func TestAgentStandaloneCovAuthorityBinderMatchesTheProcessPIDNamespace(t *testing.T) {
	const initialPIDNamespaceInode = 0xeffffffc
	var namespace unix.Stat_t
	require.NoError(t, unix.Stat("/proc/self/ns/pid", &namespace))

	err := validateAgentStandaloneBinder()
	if namespace.Ino == initialPIDNamespaceInode || os.Getpid() == 1 {
		require.NoError(t, err)

		return
	}
	require.ErrorContains(t, err,
		"non-initial PID namespace may establish agent authority only from namespace PID 1",
	)
}

// TestAgentStandaloneCovDomainClaimConsultsTheBinderBeforeMutating proves that
// a claim which is about to rebind a foreign authority record, and a claim
// which is about to mint the first one, both consult the process binder before
// they touch the registry: a refusing binder ends the claim with its own
// refusal, the durability probe that follows it is never consulted, and the
// published record is left exactly as it was found. Each registry is otherwise
// staged so the claim would run all the way to the probe, which is what pins
// the refusal to the binder rather than to an earlier guard that happens to
// fire first.
//
// The binder is what stops a container in its own PID namespace from minting
// authority the host would honour, so both of its verdicts are substituted
// here instead of inherited from the container the test runs in.
func TestAgentStandaloneCovDomainClaimConsultsTheBinderBeforeMutating(t *testing.T) {
	binderErr := errors.New("injected binder self-anchor failure")
	probeErr := errors.New("injected post-binder probe failure")

	for _, verdict := range agentStandaloneCovBinderVerdicts(binderErr) {
		t.Run("rebind/"+verdict.name, func(t *testing.T) {
			verdict.arrange(t)
			directory, ownerUID, ownerGID, rebinding := agentStandaloneCovRebindableFixture(t, 62951, 62952, "binder")
			before, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
			require.NoError(t, err)
			probes := agentStandaloneCovFailingProbe(t, probeErr)

			lease, err := acquireAgentStandaloneDomain(
				directory, rebinding, ownerUID, ownerGID, false, time.Now().Add(5*time.Second), nil, nil,
			)
			require.Nil(t, lease)
			require.ErrorIs(t, err, verdict.wantErr(probeErr))
			require.Equal(t, verdict.probes, *probes, "the probe follows the binder, never precedes it")
			after, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
			require.NoError(t, err)
			require.Equal(t, before, after, "a refused rebind must leave the foreign record intact")
			contender, taken, lockErr := tryAgentStandaloneNamedLock(directory, "62951.lock", false, ownerUID, ownerGID)
			require.NoError(t, lockErr)
			require.True(t, taken, "a refused rebind must hold no UID lock")
			require.NoError(t, contender.Close())
		})

		t.Run("pristine/"+verdict.name, func(t *testing.T) {
			verdict.arrange(t)
			directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
			want := agentStandaloneCovStaticOwner(62953, 62954, "binder-pristine")
			probes := agentStandaloneCovFailingProbe(t, probeErr)

			lease, err := acquireAgentStandaloneDomain(
				directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
			)
			require.Nil(t, lease)
			require.ErrorIs(t, err, verdict.wantErr(probeErr))
			require.Equal(t, verdict.probes, *probes, "the probe follows the binder, never precedes it")
			require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
		})
	}
}

// TestAgentStandaloneCovDomainRebindRefusesABusyMarkerItCannotWaitOut proves
// the one rebind arm that finds a busy marker temporary and then cannot wait to
// re-observe the domain: it surfaces the wait refusal, reports no retry,
// releases the exclusive lease it was handed and leaves the busy temporary for
// its live holder. Reporting retry there would send the caller back around the
// claim loop with a budget it has already spent, and unlinking the temporary
// would delete a file another claim is still working on.
//
// The concurrent case above only reaches this arm when its budget happens to
// expire on the same pass that finds the marker busy — the claim loop's own
// budget check wins otherwise — so this case drives the arm directly.
//
// A zero deadline is the only refusal signal that cannot race here. Everything
// between the rebind entry and the audit's busy verdict (the owner-temporary
// drain, the owner collection pass and the classifying pass) calls
// checkAgentStandaloneAcquisition once per registry entry, and that reads a
// closed cancel channel or an expired deadline as a refusal — either would stop
// the audit before the marker is ever adjudicated. It reads a zero deadline as
// no budget at all and passes. waitAgentStandaloneRetry reads that same zero
// deadline as remaining <= 0 and refuses immediately, before it builds a timer
// or evaluates any select, so the refusal is a straight-line consequence of the
// input rather than a scheduling outcome.
func TestAgentStandaloneCovDomainRebindRefusesABusyMarkerItCannotWaitOut(t *testing.T) {
	directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	held := createAgentStandaloneTestLock(t, directory, "62961.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	temporary := agentStandaloneCovWriteRegistryFile(
		t, directory, "62961.quarantine.next-"+agentStandaloneCovSuffix, "partial",
	)

	// The registry carries no owner temporaries, so the drain that precedes the
	// audit stays clean and the busy verdict the rebind acts on can only be the
	// marker temporary's.
	ownerTempsBusy, drainErr := drainAgentStandaloneDomainOwnerTemporaries(
		directory, ownerUID, ownerGID, time.Time{}, nil, nil,
	)
	require.NoError(t, drainErr)
	require.False(t, ownerTempsBusy, "the busy arm under test must be the audit's, not the drain's")
	require.ErrorIs(t, auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, true, false, time.Time{}, nil, nil,
	), errAgentStandaloneMarkerTempBusy)

	// The record names another PID namespace, so the rebind cannot downgrade to
	// a shared lease and has to settle the registry first.
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	record.PIDNamespace.Ino++

	exclusive, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	t.Cleanup(func() {
		if exclusive.Fd() != ^uintptr(0) {
			require.NoError(t, exclusive.Close())
		}
	})
	require.NoError(t, unix.Flock(int(exclusive.Fd()), unix.LOCK_EX|unix.LOCK_NB))

	lease, retry, err := rebindAgentStandaloneDomain(
		directory, exclusive, agentStandaloneCovStaticOwner(62961, 62962, "rebind-busy"),
		ownerUID, ownerGID, true, record, time.Time{}, nil, nil,
	)
	require.Nil(t, lease)
	require.False(t, retry, "a rebind that cannot wait must refuse, never ask for another pass")
	require.EqualError(t, err, "standalone agent identity acquisition exceeded 30 seconds")
	require.NotErrorIs(t, err, errAgentStandaloneMarkerTempBusy,
		"the wait refusal replaces the audit verdict it was reached through")
	require.ErrorIs(t, exclusive.Close(), os.ErrClosed,
		"the refused rebind releases the exclusive lease it was handed")
	require.FileExists(t, temporary, "a busy marker temporary is never unlinked")

	contender, taken, lockErr := tryAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, taken, "the released lease leaves the permanent domain lock free")
	require.NoError(t, contender.Close())
}
