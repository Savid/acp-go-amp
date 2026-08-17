//go:build linux

package amp

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// agentStandaloneCovFaultSharedDemotion makes the nth exclusive-to-shared
// lease demotion fail. Only the demotions pass a bare LOCK_SH, so this cannot
// be confused with an acquisition, which always adds LOCK_NB.
func agentStandaloneCovFaultSharedDemotion(t *testing.T, call int, verdict error) {
	t.Helper()
	agentStandaloneCovRestoreSyscallSeams(t)
	hit := agentStandaloneCovNthCall(call)
	previous := agentStandaloneFlock
	agentStandaloneFlock = func(fd, how int) error {
		if how == unix.LOCK_SH && hit() {
			return verdict
		}

		return previous(fd, how)
	}
}

// agentStandaloneCovRebindableFixture stages a registry whose published
// authority record belongs to another PID namespace but whose single
// standalone owner, UID lock and retained ACTIVE marker satisfy the same-boot
// rebind, so a case can drive the rebind all the way to its publication.
func agentStandaloneCovRebindableFixture(
	t *testing.T,
	uid, gid uint32,
	ownerID string,
) (*os.File, uint32, uint32, agentStandaloneOwner) {
	t.Helper()
	directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
	owner := agentStandaloneCovOwner(uid, gid, ownerID, "/srv/amp/"+ownerID, 41, 42)
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, strconv.FormatUint(uint64(uid), 10)+".lock")
	agentStandaloneCovWriteOwner(t, directory, owner)
	agentStandaloneCovWriteActiveMarker(t, directory, owner)
	agentStandaloneCovNoVacancy(t, nil)

	return directory, ownerUID, ownerGID, owner
}

// TestAgentStandaloneCovDomainLeaseDemotionFailuresAbortTheClaim proves that
// when the exclusive-to-shared demotion of the authority domain lease cannot
// be performed, the claim refuses and releases the lease rather than returning
// a lease that is still exclusive. A returned exclusive lease would block
// every other standalone claim on the host for as long as this process lives.
func TestAgentStandaloneCovDomainLeaseDemotionFailuresAbortTheClaim(t *testing.T) {
	want := agentStandaloneCovStaticOwner(63101, 63102, "demotion")

	t.Run("matching domain under a shared lease", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		wantErr := errors.New("injected shared demotion failure")
		agentStandaloneCovFaultSharedDemotion(t, 1, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("matching domain after an exclusive cleanup", func(t *testing.T) {
		directory, ownerUID, ownerGID, matching := createAgentStandaloneMatchingDomainFixture(t)
		temporary := agentStandaloneCovWriteRegistryFile(
			t, directory, "domain.json.next-"+agentStandaloneCovSuffix, "partial",
		)
		wantErr := errors.New("injected exclusive demotion failure")
		agentStandaloneCovFaultSharedDemotion(t, 1, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, matching, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
		agentStandaloneCovRestoreSyscallSeams(t)
		require.NoFileExists(t, temporary)
	})

	t.Run("pristine claim", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		wantErr := errors.New("injected pristine demotion failure")
		agentStandaloneCovFaultSharedDemotion(t, 1, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("rebind", func(t *testing.T) {
		directory, ownerUID, ownerGID, owner := agentStandaloneCovRebindableFixture(t, 63107, 63108, "demotion")
		wantErr := errors.New("injected rebind demotion failure")
		agentStandaloneCovFaultSharedDemotion(t, 1, wantErr)

		lease, err := acquireAgentStandaloneDomain(
			directory, owner, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, wantErr)
	})
}

// TestAgentStandaloneCovDomainClaimRevalidatesWhatItPublished proves the claim
// reads the authority record back after publishing it and refuses when what is
// on disk is not what it just wrote — on the first-ever claim and on a rebind
// alike. Without that read-back a publication that silently did nothing would
// hand back a lease for an authority the registry never recorded.
func TestAgentStandaloneCovDomainClaimRevalidatesWhatItPublished(t *testing.T) {
	silentPublication := func(t *testing.T) {
		t.Helper()
		previous := agentStandaloneReplaceDomain
		agentStandaloneReplaceDomain = func(*os.File, uint32, uint32, agentAuthorityDomainRecord) error {
			return nil
		}
		t.Cleanup(func() { agentStandaloneReplaceDomain = previous })
	}

	t.Run("pristine claim", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
		silentPublication(t)

		lease, err := acquireAgentStandaloneDomain(
			directory, agentStandaloneCovStaticOwner(63103, 63104, "silent-pristine"),
			ownerUID, ownerGID, true, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorIs(t, err, unix.ENOENT)
		require.NoFileExists(t, filepath.Join(directory.Name(), "domain.json"))
	})

	t.Run("rebind", func(t *testing.T) {
		directory, ownerUID, ownerGID, owner := agentStandaloneCovRebindableFixture(t, 63105, 63106, "silent-rebind")
		before, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, err)
		silentPublication(t)

		lease, err := acquireAgentStandaloneDomain(
			directory, owner, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		require.Nil(t, lease)
		require.ErrorContains(t, err, "changed during shared-lease transition")
		after, err := os.ReadFile(filepath.Join(directory.Name(), "domain.json"))
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}

// TestAgentStandaloneCovRebindRefusesWhenItCannotReleaseTheIdentityLock proves
// a same-boot rebind that publishes its new authority record but then cannot
// release the UID lock it took refuses, rather than returning a lease while
// still holding a lock the newly authorised owner will need.
func TestAgentStandaloneCovRebindRefusesWhenItCannotReleaseTheIdentityLock(t *testing.T) {
	directory, ownerUID, ownerGID := agentStandaloneCovDivergentDomainFixture(t)
	owner := agentStandaloneCovOwner(63111, 63112, "rebind-close", "/srv/amp/rebind-close", 41, 42)
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	agentStandaloneCovPermanentLock(t, directory, "63111.lock")
	agentStandaloneCovWriteOwner(t, directory, owner)
	agentStandaloneCovWriteActiveMarker(t, directory, owner)
	agentStandaloneCovNoVacancy(t, nil)
	wantErr := errors.New("injected rebind identity lock close failure")
	agentStandaloneCovFaultClose(t, "63111.lock", 2, wantErr)

	lease, err := acquireAgentStandaloneDomain(
		directory, owner, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
	)
	require.Nil(t, lease)
	require.ErrorIs(t, err, wantErr)
}

// TestAgentStandaloneCovOwnerIdentityRefusesWhenItCannotReleaseOwnersLock
// proves every place the owner-identity claim steps out of the registry-wide
// owners.lock treats a failed release as a refusal: after draining a peer
// temporary, after adopting a peer's identical binding, after losing the UID
// lock race, and after completing its own claim.
func TestAgentStandaloneCovOwnerIdentityRefusesWhenItCannotReleaseOwnersLock(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	suffix := agentStandaloneCovSuffix

	t.Run("after draining a peer temporary", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "63121.lock")
		temporary := filepath.Join(directory.Name(), "63121.owner.next-"+suffix)
		agentStandaloneCovPlantOnLockOpen(t, "owners.lock", func() {
			require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
		})
		wantErr := errors.New("injected drain owners.lock close failure")
		previousClose := agentStandaloneFileClose
		agentStandaloneFileClose = func(file *os.File) error {
			if file.Name() == "owners.lock" {
				require.NoError(t, previousClose(file))

				return wantErr
			}

			return previousClose(file)
		}
		t.Cleanup(func() { agentStandaloneFileClose = previousClose })

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(63123, 63124, "drained"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("after completing a fresh claim", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("completing a fresh claim requires root to build a protected state root")
		}
		const uid, gid = uint32(63135), uint32(63136)
		directory := openAgentStandaloneTestDirectory(t)
		bound, err := bindAgentStandaloneStateRoot(createAgentStandaloneProtectedStateRoot(t, uid, gid), uid, gid)
		require.NoError(t, err)
		want := agentStandaloneOwner{
			Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
			Provider: agentStandaloneOwnerID, OwnerID: "fresh-close", StateRoot: bound,
		}
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovNoVacancy(t, nil)
		wantErr := errors.New("injected completed owners.lock close failure")
		agentStandaloneCovFaultClose(t, "owners.lock", 2, wantErr)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(5*time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
		agentStandaloneCovRestoreSyscallSeams(t)
		require.FileExists(t, filepath.Join(directory.Name(), "63135.quarantine"),
			"the claim had already published before the release failed",
		)
	})

	t.Run("after adopting a peer binding", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "63125.lock")
		want := agentStandaloneCovOwner(63125, 63126, "adopted", "/acp-go-standalone-cov-absent/state", 1, 2)
		agentStandaloneCovPlantOnLockOpen(t, "owners.lock", func() {
			agentStandaloneCovWriteOwner(t, directory, want)
		})
		previousClose := agentStandaloneFileClose
		wantErr := errors.New("injected adoption owners.lock close failure")
		agentStandaloneFileClose = func(file *os.File) error {
			if file.Name() == "owners.lock" {
				require.NoError(t, previousClose(file))

				return wantErr
			}

			return previousClose(file)
		}
		t.Cleanup(func() { agentStandaloneFileClose = previousClose })

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("after losing the uid lock race", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		held := createAgentStandaloneTestLock(t, directory, "63127.lock", ownerUID, ownerGID)
		require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		wantErr := errors.New("injected contended owners.lock close failure")
		agentStandaloneCovFaultClose(t, "owners.lock", 1, wantErr)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, agentStandaloneCovStaticOwner(63127, 63128, "contended"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("after a returning owner is admitted", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		want := agentStandaloneCovOwner(63129, 63130, "returning-close", "/acp-go-standalone-cov-absent/state", 1, 2)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovPermanentLock(t, directory, "63129.lock")
		agentStandaloneCovWriteOwner(t, directory, want)
		wantErr := errors.New("injected returning owners.lock close failure")
		agentStandaloneCovFaultClose(t, "owners.lock", 1, wantErr)

		identity, err := acquireAgentStandaloneOwnerIdentity(
			directory, want, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})
}

// TestAgentStandaloneCovBusyPeerTemporaryExhaustsTheClaimBudget proves the
// claim that finds a peer owner temporary with a live UID holder only after it
// has taken owners.lock waits under its own budget and then refuses, rather
// than removing a temporary whose owner is still running.
func TestAgentStandaloneCovBusyPeerTemporaryExhaustsTheClaimBudget(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	held := createAgentStandaloneTestLock(t, directory, "63131.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	temporary := filepath.Join(directory.Name(), "63131.owner.next-"+agentStandaloneCovSuffix)
	agentStandaloneCovPlantOnLockOpen(t, "owners.lock", func() {
		require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
	})

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, agentStandaloneCovStaticOwner(63133, 63134, "busy-peer"),
		ownerUID, ownerGID, time.Now().Add(15*time.Millisecond), nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorContains(t, err, "exceeded 30 seconds")
	require.FileExists(t, temporary)
}

// TestAgentStandaloneCovRegistryListingFailuresAbortTheirCallers proves the
// two remaining registry walks — the same-boot rebind owner census and the
// held-lock target marker cleanup — abort when the registry cannot be listed,
// and that the census also honours the claim budget while it walks.
func TestAgentStandaloneCovRegistryListingFailuresAbortTheirCallers(t *testing.T) {
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()

	t.Run("same-boot census listing", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		wantErr := errors.New("injected census listing failure")
		agentStandaloneCovFaultSyscall(t, "openat", 1, wantErr)

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, agentStandaloneCovStaticOwner(63141, 63142, "census"),
			ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("same-boot census budget", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		agentStandaloneCovPermanentLock(t, directory, "owners.lock")
		agentStandaloneCovRestoreSyscallSeams(t)
		previous := agentStandaloneOpenat
		slept := false
		agentStandaloneOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
			if !slept {
				slept = true
				time.Sleep(60 * time.Millisecond)
			}

			return previous(dirfd, path, flags, mode)
		}

		identity, err := validateAgentStandaloneSameBootRebind(
			directory, agentStandaloneCovStaticOwner(63143, 63144, "census-budget"),
			ownerUID, ownerGID, time.Now().Add(30*time.Millisecond), nil, nil,
		)
		require.Nil(t, identity)
		require.ErrorContains(t, err, "exceeded 30 seconds")
		require.True(t, slept)
	})

	t.Run("target marker cleanup listing", func(t *testing.T) {
		directory := openAgentStandaloneTestDirectory(t)
		uidLock := createAgentStandaloneTestLock(t, directory, "63145.lock", ownerUID, ownerGID)
		wantErr := errors.New("injected target cleanup listing failure")
		agentStandaloneCovFaultSyscall(t, "openat", 1, wantErr)

		require.ErrorIs(t, cleanupAgentStandaloneTargetMarkerTemporaries(
			directory, 63145, uidLock, ownerUID, ownerGID, time.Now().Add(time.Second), nil, nil,
		), wantErr)
	})

	t.Run("matching domain temporary cleanup", func(t *testing.T) {
		directory, ownerUID, ownerGID, _ := createAgentStandaloneMatchingDomainFixture(t)
		name := "domain.json.next-" + agentStandaloneCovSuffix
		agentStandaloneCovWriteRegistryFile(t, directory, name, "partial")
		wantErr := errors.New("injected matching domain cleanup failure")
		agentStandaloneCovFaultSyscall(t, "unlinkat", 1, wantErr)

		requiresExclusive, err := adjudicateAgentStandaloneMatchingDomainTemporaries(
			directory, ownerUID, ownerGID, true,
		)
		require.ErrorIs(t, err, wantErr)
		require.False(t, requiresExclusive)
	})
}

// TestAgentStandaloneCovAuditHonoursTheBudgetBetweenItsTwoPasses proves the
// registry audit rechecks the claim budget when it starts its second pass over
// the registry, so a claim whose budget expired while the first pass was
// loading owner bindings never goes on to open and account for locks.
func TestAgentStandaloneCovAuditHonoursTheBudgetBetweenItsTwoPasses(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovWriteOwner(t, directory,
		agentStandaloneCovOwner(63151, 63152, "two-pass", "/srv/amp/two-pass", 51, 52),
	)
	canceled := make(chan struct{})
	agentStandaloneCovRestoreSyscallSeams(t)
	previous := agentStandaloneReadAll
	reads := 0
	agentStandaloneReadAll = func(reader io.Reader) ([]byte, error) {
		payload, err := previous(reader)
		reads++
		if reads == 1 {
			close(canceled)
		}

		return payload, err
	}

	require.ErrorIs(t, auditAgentStandaloneAuthorityRoot(
		directory, ownerUID, ownerGID, false, false, false, time.Now().Add(time.Second), canceled, nil,
	), errAgentStandaloneCanceled)
	require.Equal(t, 1, reads, "the second pass must not start after cancellation")
}

// TestAgentStandaloneCovAdoptedMatchingDomainDemotionFailureAbortsTheClaim
// proves that when a peer publishes the authority record while we queue for
// the exclusive lease, and the lease we then hold cannot be demoted to shared,
// the claim refuses and releases it instead of returning an exclusive lease
// that would lock every other claim on the host out.
func TestAgentStandaloneCovAdoptedMatchingDomainDemotionFailureAbortsTheClaim(t *testing.T) {
	directory, ownerUID, ownerGID := agentStandaloneCovPristineDomainFixture(t)
	record, err := currentAgentAuthorityDomain(directory)
	require.NoError(t, err)
	record.AuthorityID = "0123456789abcdef0123456789abcdef"
	held := agentStandaloneCovHoldDomainShared(t, directory)
	published := make(chan struct{})
	go func() {
		time.Sleep(60 * time.Millisecond)
		publishErr := replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record)
		closeErr := held.Close()
		if publishErr != nil || closeErr != nil {
			panic(errors.Join(publishErr, closeErr))
		}
		close(published)
	}()
	wantErr := errors.New("injected adopted lease demotion failure")
	agentStandaloneCovFaultSharedDemotion(t, 1, wantErr)

	lease, err := acquireAgentStandaloneDomain(
		directory, agentStandaloneCovStaticOwner(63161, 63162, "adopted-demotion"),
		ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
	)
	<-published
	require.Nil(t, lease)
	require.ErrorIs(t, err, wantErr)
	agentStandaloneCovRestoreSyscallSeams(t)
	contender, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	require.NoError(t, unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB),
		"the refused claim must release the lease it could not demote",
	)
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneCovStateRootWalkAbortsWhenAncestorMetadataIsUnavailable
// proves the state root walk refuses when the kernel will not describe an
// ancestor it has just opened, at both the ancestor and the final component.
// Proceeding on an unanswered stat would accept a state root whose ownership
// and mode nobody checked.
func TestAgentStandaloneCovStateRootWalkAbortsWhenAncestorMetadataIsUnavailable(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("state root walk requires root to build a protected state root")
	}
	const uid, gid = uint32(63171), uint32(63172)
	stateRoot := createAgentStandaloneProtectedStateRoot(t, uid, gid)
	depth := len(strings.Split(strings.TrimPrefix(stateRoot, "/"), "/"))

	for _, testCase := range []struct {
		name string
		call int
	}{
		{name: "ancestor", call: 1},
		{name: "final component", call: depth + 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			wantErr := errors.New("injected " + testCase.name + " stat failure")
			agentStandaloneCovFaultSyscall(t, "fstat", testCase.call, wantErr)

			bound, err := bindAgentStandaloneStateRoot(stateRoot, uid, gid)
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, agentStandaloneStateRoot{}, bound)
		})
	}
}

// TestAgentStandaloneCovContendedPeerTemporaryWaitHonoursTheRemainingBudget
// proves that when a peer owner temporary with a live UID holder appears only
// after owners.lock is taken, the claim releases owners.lock, waits no longer
// than its remaining budget, and then refuses — never removing a temporary
// whose holder is still running.
func TestAgentStandaloneCovContendedPeerTemporaryWaitHonoursTheRemainingBudget(t *testing.T) {
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	agentStandaloneCovPermanentLock(t, directory, "owners.lock")
	held := createAgentStandaloneTestLock(t, directory, "63181.lock", ownerUID, ownerGID)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_EX|unix.LOCK_NB))
	temporary := filepath.Join(directory.Name(), "63181.owner.next-"+agentStandaloneCovSuffix)
	agentStandaloneCovPlantOnLockOpen(t, "owners.lock", func() {
		require.NoError(t, os.WriteFile(temporary, []byte("partial"), 0o600))
	})

	identity, err := acquireAgentStandaloneOwnerIdentity(
		directory, agentStandaloneCovStaticOwner(63183, 63184, "contended-wait"),
		ownerUID, ownerGID, time.Now().Add(6*time.Millisecond), nil, nil,
	)
	require.Nil(t, identity)
	require.ErrorContains(t, err, "exceeded 30 seconds")
	require.FileExists(t, temporary)
	contender, taken, lockErr := tryAgentStandaloneNamedLock(directory, "owners.lock", false, ownerUID, ownerGID)
	require.NoError(t, lockErr)
	require.True(t, taken, "the waiting claim must not hold owners.lock")
	require.NoError(t, contender.Close())
}
