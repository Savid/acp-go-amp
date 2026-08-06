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

// agentStandaloneCovClosedDirectory hands back a registry descriptor the
// kernel has already stopped answering for, so a case can prove an operation
// aborts instead of proceeding on a descriptor it once accepted.
func agentStandaloneCovClosedDirectory(t *testing.T) *os.File {
	t.Helper()
	directory, err := os.Open(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, directory.Close())

	return directory
}

// agentStandaloneCovStaticOwner is the tuple most owner-identity cases claim.
// Its state root is a plausible bound inode rather than a real directory,
// because every case here refuses before the state root is revalidated.
func agentStandaloneCovStaticOwner(uid, gid uint32, ownerID string) agentStandaloneOwner {
	return agentStandaloneCovOwner(uid, gid, ownerID, "/srv/amp/"+ownerID, 101, 102)
}

// agentStandaloneCovNoVacancy replaces the /proc vacancy sweep with a
// deterministic verdict, so a case about registry state is never decided by
// whichever processes happen to be running in the test container.
func agentStandaloneCovNoVacancy(t *testing.T, verdict error) *int {
	t.Helper()
	previous := agentStandaloneVacancyScan
	scans := 0
	agentStandaloneVacancyScan = func(uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal) error {
		scans++

		return verdict
	}
	t.Cleanup(func() { agentStandaloneVacancyScan = previous })

	return &scans
}

// TestAgentStandaloneCovClosedRegistryDescriptorFailsClosed proves that when
// the kernel stops answering for the registry descriptor a claim already
// accepted, the listing and the uniqueness sweep abort rather than concluding
// the registry is empty. Concluding "empty" would let a claim mint a second
// authority beside the real one.
func TestAgentStandaloneCovClosedRegistryDescriptorFailsClosed(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	entries, err := agentStandaloneAuthorityEntries(agentStandaloneCovClosedDirectory(t))
	require.ErrorIs(t, err, unix.EBADF)
	require.Nil(t, entries)

	require.ErrorIs(t, validateAgentStandaloneOwnerUniqueness(
		agentStandaloneCovClosedDirectory(t), agentStandaloneCovStaticOwner(62801, 62802, "closed-registry"),
		ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	), unix.EBADF)

	require.ErrorIs(t, validateAgentStandaloneOwnerUniqueness(
		agentStandaloneCovRemovedDirectory(t), agentStandaloneCovStaticOwner(62801, 62802, "removed-registry"),
		ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
	), unix.ENOENT)
}

// TestAgentStandaloneCovOwnerIdentityRefusesEveryUnsafeRegistry proves the
// owner-identity acquisition refuses — without taking or leaving a UID lock —
// whenever the registry is not in a state it can account for. This is the
// gate that decides whether this process may own an agent UID at all.
func TestAgentStandaloneCovOwnerIdentityRefusesEveryUnsafeRegistry(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	suffix := agentStandaloneCovSuffix

	t.Run("expired budget", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovStaticOwner(62803, 62804, "expired")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(-time.Millisecond), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62803.lock"))
	})

	t.Run("registry root removed", func(t *testing.T) {
		identity, err := acquireAgentStandaloneOwnerIdentity(
			agentStandaloneCovRemovedDirectory(t), agentStandaloneCovStaticOwner(62805, 62806, "removed"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
	})

	t.Run("owner temporary without its uid lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		temporary := agentStandaloneCovWriteRegistryFile(t, directory, "62807.owner.next-"+suffix, "partial")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62807, 62808, "orphan-temp"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
		require.FileExists(t, temporary, "an unaccountable temporary is never unlinked")
	})

	t.Run("uid bound to another owner", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteOwner(t, directory, agentStandaloneCovStaticOwner(62809, 62810, "incumbent"))

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62809, 62810, "challenger"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "permanently bound to another standalone owner")
	})

	t.Run("unreadable owner binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovWriteRegistryFile(t, directory, "62811.owner", "not json\n")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62811, 62812, "unreadable"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "invalid character")
	})

	t.Run("non-pristine registry without owners lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "62813.lock")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62815, 62816, "no-owners-lock"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "missing from non-pristine registry")
	})

	t.Run("durable marker without its uid lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovWriteCleanMarker(t, directory, 62817, 62818, "orphan-marker")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62817, 62818, "orphan-marker"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "permanent lock is missing while its registry state")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62817.lock"))
	})

	t.Run("uid lock with wrong mode", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62819.lock")
		require.NoError(t, os.Chmod(filepath.Join(directory.Name(), "62819.lock"), 0o644))

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62819, 62820, "bad-mode"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "mode")
	})

	t.Run("uid lock held by a live peer", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		held := createAgentStandaloneTestLock(t, directory, "62821.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62821, 62822, "contended"),
			ownerUID, ownerGID, time.Now().Add(150*time.Millisecond), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		contender, taken, lockErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
		require.NoError(t, lockErr)
		require.True(t, taken, "each retry must release owners.lock")
		require.NoError(t, contender.Close())
	})

	t.Run("malformed target marker temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62823.lock")
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "62823.quarantine.next-0123456789ABCDEF01234567", "partial",
		)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62823, 62824, "bad-target-temp"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "is invalid")
		require.FileExists(t, temporary)
	})

	t.Run("registry entry that belongs to nothing", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62825.lock")
		agentStandaloneCovWriteRegistryFile(t, directory, "leftover", "x")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62825, 62826, "leftover"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, `unknown entry "leftover"`)
		require.NoFileExists(t, filepath.Join(directory.Name(), "62825.owner"))
	})

	t.Run("state root that no longer resolves", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62827.lock")
		want := agentStandaloneCovOwner(62827, 62828, "gone", "/acp-go-standalone-cov-absent/state", 1, 2)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
		require.NoFileExists(t, filepath.Join(directory.Name(), "62827.owner"))
	})
}

// TestAgentStandaloneCovOwnerIdentityRefusesAnUnsafeExistingBinding proves a
// returning owner is put through the same registry accounting as a fresh one:
// it needs its permanent UID lock and the permanent owners.lock, its target
// marker temporaries must be well formed, the registry must audit clean and
// its state root must still resolve. Skipping any of these would let a
// previously bound UID be re-entered on a registry that has since drifted.
func TestAgentStandaloneCovOwnerIdentityRefusesAnUnsafeExistingBinding(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("no permanent uid lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovStaticOwner(62831, 62832, "no-uid-lock")
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovWriteOwner(t, directory, want)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
	})

	t.Run("no permanent owners lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovStaticOwner(62833, 62834, "no-owners-lock")
		agentStandaloneCovPermanentLock(t, directory, "62833.lock")
		agentStandaloneCovWriteOwner(t, directory, want)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
	})

	t.Run("malformed target marker temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovStaticOwner(62835, 62836, "bad-temp")
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62835.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "62835.quarantine.next-0123456789ABCDEF01234567", "partial",
		)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "is invalid")
		require.FileExists(t, temporary)
	})

	t.Run("registry entry that belongs to nothing", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovStaticOwner(62837, 62838, "leftover")
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62837.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteRegistryFile(t, directory, "leftover", "x")

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, `unknown entry "leftover"`)
	})

	t.Run("state root that no longer resolves", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovOwner(62839, 62840, "gone", "/acp-go-standalone-cov-absent/state", 1, 2)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62839.lock")
		agentStandaloneCovWriteOwner(t, directory, want)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
	})
}

// TestAgentStandaloneCovOwnerIdentityAdmitsAndRepublishesAnExistingBinding
// proves a returning owner on a coherent registry is admitted, retains the
// UID lock it acquired, proves vacancy twice before it is trusted, and
// republishes its ACTIVE marker.
func TestAgentStandaloneCovOwnerIdentityAdmitsAndRepublishesAnExistingBinding(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone owner admission requires root to build a protected state root")
	}
	const uid, gid = uint32(62841), uint32(62842)
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	bound, err := bindAgentStandaloneStateRoot(createAgentStandaloneProtectedStateRoot(t, uid, gid), uid, gid)
	require.NoError(t, err)
	want := agentStandaloneOwner{
		Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: "returning", StateRoot: bound,
	}
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, "62841.lock")
	agentStandaloneCovWriteOwner(t, directory, want)
	agentStandaloneCovWriteActiveMarker(t, directory, want)
	scans := agentStandaloneCovNoVacancy(t, nil)

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, want, ownerUID, ownerGID, time.Now().Add(5*time.Second), nil, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, identity)
	require.Equal(t, 2, *scans, "a returning owner must prove vacancy twice")
	contender, taken, lockErr := tryAgentStandaloneNamedLock(directory, "62841.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.False(t, taken, "the admitted owner must still hold its UID lock")
	require.Nil(t, contender)
	marker, err := loadAgentStandaloneMarker(directory, uid, ownerUID, ownerGID)
	require.NoError(t, err)
	require.Equal(t, "active", marker.State)
	require.NoError(t, identity.Close())
}

// TestAgentStandaloneCovOwnerIdentityHandlesRegistryChangeUnderOwnersLock
// proves the claim re-reads the registry after it takes owners.lock and acts
// on what it finds there rather than on what it saw before: a peer's owner
// temporary is drained or refused, and a peer's owner binding either ends the
// claim or restarts it. Acting on the pre-lock view would let two claims
// publish over one another.
func TestAgentStandaloneCovOwnerIdentityHandlesRegistryChangeUnderOwnersLock(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	suffix := agentStandaloneCovSuffix
	plantUnderOwnersLock := func(t *testing.T, plant func()) {
		t.Helper()
		restoreAgentStandalonePermanentLockSeams(t)
		original := agentStandaloneLockOpenat
		planted := false
		agentStandaloneLockOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
			if !planted && path == "owners.lock" {
				planted = true
				plant()
			}

			return original(dirfd, path, flags, mode)
		}
	}

	t.Run("peer owner temporary without its uid lock", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		temporary := filepath.Join(directory.Name(), "62851.owner.next-"+suffix)
		plantUnderOwnersLock(t, func() {
			require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62853, 62854, "peer-temp"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT)
		require.FileExists(t, temporary)
	})

	t.Run("peer owner temporary is drained and the claim restarts", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62855.lock")
		temporary := filepath.Join(directory.Name(), "62855.owner.next-"+suffix)
		plantUnderOwnersLock(t, func() {
			require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovOwner(62857, 62858, "restarted", "/acp-go-standalone-cov-absent/state", 1, 2),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT, "the restarted claim reaches its own state root refusal")
		require.NoFileExists(t, temporary, "an accountable peer temporary is drained")
	})

	t.Run("peer binds the same uid to another tuple", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		plantUnderOwnersLock(t, func() {
			agentStandaloneCovWriteOwner(t, directory, agentStandaloneCovStaticOwner(62859, 62860, "peer"))
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62859, 62860, "loser"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "permanently bound to another standalone owner")
	})

	t.Run("peer writes an unreadable binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		plantUnderOwnersLock(t, func() {
			agentStandaloneCovWriteRegistryFile(t, directory, "62861.owner", "not json\n")
		})

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(62861, 62862, "unreadable-peer"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "invalid character")
	})

	t.Run("peer publishes the identical binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "62863.lock")
		want := agentStandaloneCovOwner(62863, 62864, "same", "/acp-go-standalone-cov-absent/state", 1, 2)
		plantUnderOwnersLock(t, func() { agentStandaloneCovWriteOwner(t, directory, want) })

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, unix.ENOENT, "the restarted claim adopts the peer binding and then refuses its state root")
	})
}

// TestAgentStandaloneCovOwnerClaimCompletionOrdersItsProofs proves the claim
// completion refuses on an exhausted budget before it touches anything,
// refuses a state root that no longer resolves, propagates a registry refusal
// from the binding step, and refuses when either the pre-publication vacancy
// proof or the post-proof budget check says no — always without publishing an
// ACTIVE marker.
func TestAgentStandaloneCovOwnerClaimCompletionOrdersItsProofs(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone claim completion requires root to build a protected state root")
	}
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	const uid, gid = uint32(62871), uint32(62872)
	newClaim := func(t *testing.T) (*os.File, agentStandaloneOwner) {
		t.Helper()
		directory := openAgentStandaloneTestDirectory(t)
		bound, err := bindAgentStandaloneStateRoot(createAgentStandaloneProtectedStateRoot(t, uid, gid), uid, gid)
		require.NoError(t, err)

		return directory, agentStandaloneOwner{
			Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
			Provider: agentStandaloneOwnerID, OwnerID: "completion", StateRoot: bound,
		}
	}

	t.Run("expired budget", func(t *testing.T) {
		directory, want := newClaim(t)

		require.ErrorContains(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(-time.Millisecond), nil, nil,
		), "exceeded 30 seconds")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62871.owner"))
	})

	t.Run("state root that no longer resolves", func(t *testing.T) {
		directory, want := newClaim(t)
		want.StateRoot.Path = "/acp-go-standalone-cov-absent/state"

		require.ErrorIs(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		), unix.ENOENT)
		require.NoFileExists(t, filepath.Join(directory.Name(), "62871.owner"))
	})

	t.Run("binding refused by the registry", func(t *testing.T) {
		directory, want := newClaim(t)
		agentStandaloneCovWriteOwner(t, directory,
			agentStandaloneCovOwner(62873, gid, "gid-holder", "/srv/amp/gid-holder", 201, 202),
		)

		require.ErrorContains(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		), "already bound to uid 62873")
		require.NoFileExists(t, filepath.Join(directory.Name(), "62871.quarantine"))
	})

	t.Run("fresh binding with a live task", func(t *testing.T) {
		directory, want := newClaim(t)
		wantErr := errors.New("a task still holds the identity")
		previous := agentStandaloneVacancyScan
		scans := 0
		agentStandaloneVacancyScan = func(uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal) error {
			scans++
			if scans == 1 {
				return nil
			}

			return wantErr
		}
		t.Cleanup(func() { agentStandaloneVacancyScan = previous })

		err := completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), nil, nil,
		)
		require.ErrorIs(t, err, wantErr)
		require.ErrorContains(t, err, "post-owner standalone task vacancy proof")
		require.FileExists(t, filepath.Join(directory.Name(), "62871.owner"))
		require.NoFileExists(t, filepath.Join(directory.Name(), "62871.quarantine"))
	})

	t.Run("returning binding with a live task", func(t *testing.T) {
		directory, want := newClaim(t)
		agentStandaloneCovWriteOwner(t, directory, want)
		agentStandaloneCovWriteActiveMarker(t, directory, want)
		wantErr := errors.New("a task re-entered the identity")
		previous := agentStandaloneVacancyScan
		agentStandaloneVacancyScan = func(uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal) error {
			return wantErr
		}
		t.Cleanup(func() { agentStandaloneVacancyScan = previous })

		require.ErrorIs(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})

	t.Run("canceled after the post-owner vacancy proof", func(t *testing.T) {
		directory, want := newClaim(t)
		canceled := make(chan struct{})
		previous := agentStandaloneVacancyScan
		scans := 0
		agentStandaloneVacancyScan = func(uint32, uint32, time.Time, <-chan struct{}, <-chan os.Signal) error {
			scans++
			if scans == 2 {
				close(canceled)
			}

			return nil
		}
		t.Cleanup(func() { agentStandaloneVacancyScan = previous })

		require.ErrorIs(t, completeAgentStandaloneOwnerClaim(
			directory, want, ownerUID, ownerGID, false, time.Now().Add(time.Second), canceled, nil,
		), errAgentStandaloneCanceled)
		require.NoFileExists(t, filepath.Join(directory.Name(), "62871.quarantine"))
	})
}

// TestAgentStandaloneCovOwnerPublicationRefusesWhatItCannotReadBack proves the
// two publication paths verify what landed on disk instead of trusting the
// rename: an owner name that already exists is never replaced, an owner whose
// payload does not read back as the tuple it published is refused, and a
// marker too large to be read back under its own bound is refused.
func TestAgentStandaloneCovOwnerPublicationRefusesWhatItCannotReadBack(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("owner name already taken", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		incumbent := agentStandaloneCovStaticOwner(62881, 62882, "incumbent")
		agentStandaloneCovWriteOwner(t, directory, incumbent)
		challenger := agentStandaloneCovStaticOwner(62881, 62882, "challenger")

		require.ErrorContains(t,
			createAgentStandaloneOwner(directory, challenger, ownerUID, ownerGID),
			"publish immutable standalone owner without replacement",
		)
		loaded, err := loadAgentStandaloneOwner(directory, 62881, ownerUID, ownerGID)
		require.NoError(t, err)
		require.Equal(t, incumbent, loaded, "the incumbent binding must survive")
	})

	t.Run("owner that cannot be read back", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		unloadable := agentStandaloneCovStaticOwner(62883, 62884, "unloadable")
		unloadable.GID = 0

		require.ErrorContains(t,
			createAgentStandaloneOwner(directory, unloadable, ownerUID, ownerGID),
			"published standalone owner payload changed",
		)
	})

	t.Run("marker too large to read back", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		payload := make([]byte, agentStandaloneMarkerMax+1)
		for index := range payload {
			payload[index] = 'a'
		}

		require.ErrorContains(t, replaceAgentStandaloneFile(
			directory, "62885.quarantine", payload, ownerUID, ownerGID,
			time.Now().Add(time.Second), nil, nil,
		), "published agent identity marker payload changed")
	})

	t.Run("domain record that cannot be read back", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "not-thirty-two-hex-characters"

		require.ErrorContains(t,
			replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record),
			"record is incomplete",
		)
	})
}
