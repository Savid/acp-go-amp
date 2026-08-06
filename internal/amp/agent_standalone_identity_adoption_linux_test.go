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

// agentStandaloneAdoptionFixture stages a registry that owns its permanent
// domain lock but has never published an authority record, which is the state a
// first claim finds and the state a peer racing it is about to leave behind.
func agentStandaloneAdoptionFixture(t *testing.T) (*os.File, uint32, uint32) {
	t.Helper()
	directory := openAgentStandaloneTestDirectory(t)
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	lock := createAgentStandaloneTestLock(t, directory, "domain.lock", ownerUID, ownerGID)
	require.NoError(t, lock.Close())

	return directory, ownerUID, ownerGID
}

// agentStandaloneAdoptionOwner is the owner tuple a domain claim carries. Its
// state root is a plausible bound inode rather than a real directory, because
// every case here settles the domain before any owner binding is revalidated.
func agentStandaloneAdoptionOwner(uid, gid uint32, ownerID string) agentStandaloneOwner {
	return agentStandaloneOwner{
		Version: 1, UID: uid, GID: gid, Kind: agentStandaloneOwnerKind,
		Provider: agentStandaloneOwnerID, OwnerID: ownerID,
		StateRoot: agentStandaloneStateRoot{Path: "/srv/amp/" + ownerID, Dev: 101, Ino: 102},
	}
}

// agentStandaloneAdoptionHoldShared takes a shared lease on the permanent
// domain lock. Other shared readers still get in, so a claim can read the
// registry, but any contender that needs the exclusive lease has to queue —
// which is what puts a claim in the window a peer publishes into.
func agentStandaloneAdoptionHoldShared(t *testing.T, directory *os.File) *os.File {
	t.Helper()
	ownerUID, ownerGID := agentStandaloneTestAuthorityIDs()
	held, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	require.NoError(t, unix.Flock(int(held.Fd()), unix.LOCK_SH|unix.LOCK_NB))

	return held
}

// agentStandaloneAdoptionRecordImage is the exact on-disk identity of the
// published authority record: which inode holds it and what bytes are in it.
// Comparing the whole image rather than a decoded field proves an adopting
// claim left the peer's record alone instead of republishing an equal one onto
// a fresh inode.
type agentStandaloneAdoptionRecordImage struct {
	dev     uint64
	ino     uint64
	payload []byte
}

// agentStandaloneAdoptionCaptureRecord returns its error rather than failing
// the test, because one caller runs on the peer goroutine, where a t.FailNow
// would only park that goroutine and hang the claim it is racing.
func agentStandaloneAdoptionCaptureRecord(directory *os.File) (agentStandaloneAdoptionRecordImage, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(
		int(directory.Fd()), agentAuthorityDomainRecordName, &stat, unix.AT_SYMLINK_NOFOLLOW,
	); err != nil {
		return agentStandaloneAdoptionRecordImage{}, err
	}
	payload, err := os.ReadFile(filepath.Join(directory.Name(), agentAuthorityDomainRecordName))
	if err != nil {
		return agentStandaloneAdoptionRecordImage{}, err
	}

	return agentStandaloneAdoptionRecordImage{dev: stat.Dev, ino: stat.Ino, payload: payload}, nil
}

// agentStandaloneAdoptionDomainLockIsFree asserts the claim released the domain
// lock it took, so a refusal never leaves the registry wedged for the next
// claim.
func agentStandaloneAdoptionDomainLockIsFree(t *testing.T, directory *os.File, ownerUID, ownerGID uint32) {
	t.Helper()
	contender, acquired, err := tryAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
	require.NoError(t, err)
	require.True(t, acquired, "the refused claim must release the domain lock")
	require.NoError(t, contender.Close())
}

// TestAgentStandaloneDomainClaimAdoptsAPeerAuthorityPublishedWhileItQueued
// proves what a claim does when it took the exclusive domain lease, re-read the
// authority record and found a peer had published one for this very domain
// while it queued. That peer minted the authority this claim was about to mint
// itself, so the claim must adopt the peer's record rather than replace it, and
// must hand back a lease downgraded to shared the way every other adopting
// branch does. Two agents starting together is the ordinary way to reach this,
// so a claim that refused here would be a spurious refusal on the common path;
// a claim that republished would hand out an authority id the peer never saw.
//
// The second case pins the guard that adoption still owes: the downgrade to
// shared happens before the record is read back, so a peer holding the same
// shared lease can still replace the record inside that window, and the
// read-back is the only thing between that peer and a lease handed out for an
// authority this claim never inspected.
func TestAgentStandaloneDomainClaimAdoptsAPeerAuthorityPublishedWhileItQueued(t *testing.T) {
	t.Run("peer publishes a matching record while we queue", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneAdoptionFixture(t)
		want := agentStandaloneAdoptionOwner(62903, 62904, "queued")
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "0123456789abcdef0123456789abcdef"
		held := agentStandaloneAdoptionHoldShared(t, directory)
		published := make(chan struct{})
		var peerImage agentStandaloneAdoptionRecordImage
		go func() {
			// The claim cannot leave the exclusive-lease queue until this
			// goroutine drops its shared lease, so the record is captured at
			// an instant when only the peer has ever written it.
			time.Sleep(60 * time.Millisecond)
			publishErr := replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, record)
			if publishErr != nil {
				panic(publishErr)
			}
			captured, captureErr := agentStandaloneAdoptionCaptureRecord(directory)
			peerImage = captured
			closeErr := held.Close()
			if captureErr != nil || closeErr != nil {
				panic(errors.Join(captureErr, closeErr))
			}
			close(published)
		}()

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		<-published
		require.NoError(t, err)
		require.NotNil(t, lease)
		defer lease.Close()
		reread, err := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
		require.NoError(t, err)
		require.Equal(t, record.AuthorityID, reread.AuthorityID,
			"the adopting claim must take the peer's authority id, not mint its own",
		)
		adopted, err := agentStandaloneAdoptionCaptureRecord(directory)
		require.NoError(t, err)
		require.Equal(t, peerImage.dev, adopted.dev,
			"the adopting claim must leave the peer's record on the device it published to",
		)
		require.Equal(t, peerImage.ino, adopted.ino,
			"the adopting claim must leave the peer's record on its own inode, not republish onto a fresh one",
		)
		require.Equal(t, peerImage.payload, adopted.payload,
			"the adopting claim must leave the peer's record byte-identical",
		)
		contender, err := openAgentStandaloneNamedLock(directory, "domain.lock", false, ownerUID, ownerGID)
		require.NoError(t, err)
		require.NoError(t, unix.Flock(int(contender.Fd()), unix.LOCK_SH|unix.LOCK_NB),
			"the adopted lease must be shared, so peers on the same authority may hold it too",
		)
		require.ErrorIs(t, unix.Flock(int(contender.Fd()), unix.LOCK_EX|unix.LOCK_NB), unix.EWOULDBLOCK,
			"the adopted lease must still exclude a contender that wants the domain to itself",
		)
		require.NoError(t, contender.Close())
	})

	t.Run("peer replaces the adopted record in the shared-lease window", func(t *testing.T) {
		directory, ownerUID, ownerGID := agentStandaloneAdoptionFixture(t)
		want := agentStandaloneAdoptionOwner(62913, 62914, "adopted")
		record, err := currentAgentAuthorityDomain(directory)
		require.NoError(t, err)
		record.AuthorityID = "0123456789abcdef0123456789abcdef"
		successor := record
		successor.AuthorityID = "fedcba9876543210fedcba9876543210"
		held := agentStandaloneAdoptionHoldShared(t, directory)
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
		// Only the lease downgrade flocks bare LOCK_SH; every acquisition adds
		// LOCK_NB, so this lands the peer in the downgrade window and nowhere
		// else.
		previous := agentStandaloneFlock
		t.Cleanup(func() { agentStandaloneFlock = previous })
		replaced := false
		agentStandaloneFlock = func(fd, how int) error {
			if how == unix.LOCK_SH && !replaced {
				replaced = true
				require.NoError(t, replaceAgentStandaloneDomainRecord(directory, ownerUID, ownerGID, successor))
			}

			return previous(fd, how)
		}

		lease, err := acquireAgentStandaloneDomain(
			directory, want, ownerUID, ownerGID, true, time.Now().Add(5*time.Second), nil, nil,
		)
		<-published
		require.Nil(t, lease)
		require.ErrorContains(t, err, "changed during shared-lease transition")
		require.True(t, replaced, "the peer never reached the shared-lease window")
		reread, err := loadAgentAuthorityDomainRecord(directory, ownerUID, ownerGID)
		require.NoError(t, err)
		require.Equal(t, successor.AuthorityID, reread.AuthorityID,
			"the refusal must leave the peer's replacement in place",
		)
		agentStandaloneAdoptionDomainLockIsFree(t, directory, ownerUID, ownerGID)
	})
}
