//go:build linux

package amp

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/stretchr/testify/require"
)

// identityLockResRewriteMarkerOnNthRead replaces the named registry entry in
// place, keeping its inode and metadata, immediately before its nth read. It
// models a peer that rewrites a disposition inside the window between two
// reads of it.
func identityLockResRewriteMarkerOnNthRead(t *testing.T, name, payload string, read int) *int {
	t.Helper()
	agentStandaloneCovRestoreSyscallSeams(t)
	reads := 0
	previous := agentStandaloneOpenat
	agentStandaloneOpenat = func(dirfd int, path string, flags int, mode uint32) (int, error) {
		if path == name {
			reads++
			if reads == read {
				resolved, err := os.Readlink("/proc/self/fd/" + strconv.Itoa(dirfd))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(resolved, name), []byte(payload), 0o600))
			}
		}

		return previous(dirfd, path, flags, mode)
	}

	return &reads
}

// identityLockResCleanMarker renders the CLEAN disposition a peer would leave
// behind after releasing an identity: well formed, loadable, and not ACTIVE.
func identityLockResCleanMarker(uid, gid uint32, key string) string {
	return `{"version":2,"uid":` + strconv.FormatUint(uint64(uid), 10) +
		`,"gid":` + strconv.FormatUint(uint64(gid), 10) +
		`,"sessionKey":"` + key + `","state":"clean-ready"}` + "\n"
}

// TestIdentityLockResStandaloneDispositionCatchesAMarkerRewrittenAfterTheAudit
// proves the standalone disposition check re-reads the ACTIVE marker for
// itself and refuses when what it finds is no longer the disposition the
// registry audit accepted moments earlier. The audit applies the same
// predicate, so this second read only ever matters when a peer rewrote the
// marker in between — and that is exactly the window in which re-admitting a
// standalone identity on a disposition it no longer holds would hand the agent
// its uid back without a live ACTIVE lease.
func TestIdentityLockResStandaloneDispositionCatchesAMarkerRewrittenAfterTheAudit(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("standalone disposition rewrite requires a root-owned authority root")
	}
	const uid, gid = uint32(62981), uint32(62982)
	identityLockCovSeams(t)
	root := configureAgentIdentityLockTestRoot(t)
	stateRoot := createAgentStandaloneProtectedStateRoot(t, uid, gid)
	standalone, err := acquireAgentStandaloneIdentity(
		uid, gid, "disposition-rewrite", stateRoot, false, root, make(chan struct{}), make(chan os.Signal),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = standalone.Close() })
	markerName := strconv.FormatUint(uint64(uid), 10) + ".quarantine"
	markerPath := filepath.Join(root, "acp-go", "agent-identities", markerName)
	sealed, err := os.ReadFile(markerPath)
	require.NoError(t, err)

	require.NoError(t, validateStandaloneAgentIdentityDisposition(standalone.owner, true, root),
		"the sealed disposition must be accepted before anything rewrites it",
	)

	var before unix.Stat_t
	require.NoError(t, unix.Stat(markerPath, &before))
	reads := identityLockResRewriteMarkerOnNthRead(t, markerName,
		identityLockResCleanMarker(uid, gid, agentStandaloneSessionKey(standalone.owner)), 2,
	)

	err = validateStandaloneAgentIdentityDisposition(standalone.owner, true, root)
	require.ErrorContains(t, err, "does not retain its exact ACTIVE disposition")
	require.NotContains(t, err.Error(), "audit standalone agent identity authority",
		"the audit must have accepted the marker it read, so only the second read can refuse",
	)
	require.Equal(t, 2, *reads, "the rewrite must land between the audit's read and the disposition read")
	var after unix.Stat_t
	require.NoError(t, unix.Stat(markerPath, &after))
	require.Equal(t, before.Ino, after.Ino,
		"the rewrite kept the trusted inode, so only the payload could have refused it",
	)
	rewritten, err := os.ReadFile(markerPath)
	require.NoError(t, err)
	require.NotEqual(t, string(sealed), string(rewritten),
		"the case is only meaningful if the marker actually changed",
	)
}
