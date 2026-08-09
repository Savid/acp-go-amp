//go:build unix

package amp

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func restoreSharedIdentitySeams(t *testing.T) {
	t.Helper()

	goos, geteuid, getegid := processIsolationGOOS, processIsolationGeteuid, processIsolationGetegid
	t.Cleanup(func() {
		processIsolationGOOS = goos
		processIsolationGeteuid, processIsolationGetegid = geteuid, getegid
	})
}

// TestSharedProcessIdentityNamesOnlyTheSupervisorsOwnLinuxIdentity proves the
// arm is selected by one fact — the native identity is the identity this
// process runs as — that a trusted root supervisor can never reach it, because
// a zero effective uid disqualifies the process before the comparison is made,
// and that only the Linux backend recognises the shape at all.
func TestSharedProcessIdentityNamesOnlyTheSupervisorsOwnLinuxIdentity(t *testing.T) {
	restoreSharedIdentitySeams(t)
	processIsolationGOOS = processIsolationLinux

	require.False(t, sharedProcessIdentity(nil))

	processIsolationGeteuid = func() int { return 1000 }
	require.True(t, sharedProcessIdentity(&ProcessIsolation{UID: 1000, GID: 1000}))
	require.True(t, sharedProcessIdentity(&ProcessIsolation{UID: 1000, GID: 1001}))
	require.False(t, sharedProcessIdentity(&ProcessIsolation{UID: 1001, GID: 1000}))

	processIsolationGeteuid = func() int { return 0 }
	require.False(t, sharedProcessIdentity(&ProcessIsolation{UID: 0, GID: 0}))
	require.False(t, sharedProcessIdentity(&ProcessIsolation{UID: 1000, GID: 1000}))

	processIsolationGOOS = "darwin"
	processIsolationGeteuid = func() int { return 1000 }
	require.False(t, sharedProcessIdentity(&ProcessIsolation{UID: 1000, GID: 1000}))
}

// TestSharedIdentityCredentialRequestsNoIdentityChange proves the launch asks
// for no credential change at all under a shared identity — not an empty group
// set, which an unprivileged process cannot install — while a native group the
// supervisor is not in is still refused, and the isolated arm still emits the
// full credential it always has.
func TestSharedIdentityCredentialRequestsNoIdentityChange(t *testing.T) {
	restoreSharedIdentitySeams(t)
	processIsolationGOOS = processIsolationLinux
	processIsolationGeteuid = func() int { return 1000 }
	processIsolationGetegid = func() int { return 1000 }

	isolation := &ProcessIsolation{UID: 1000, GID: 1000, BaseEnvironment: map[string]string{}}

	command := exec.Command("/bin/true")
	require.NoError(t, applyProcessIsolation(command, isolation))
	require.Nil(t, command.SysProcAttr)

	foreignGroup := &ProcessIsolation{UID: 1000, GID: 1001, BaseEnvironment: map[string]string{}}
	require.ErrorContains(t, applyProcessIsolation(exec.Command("/bin/true"), foreignGroup),
		"native group 1001 cannot be entered from group 1000; "+sharedIdentitySupervisorRemedy)

	processIsolationGetegid = func() int { return -1 }
	require.ErrorContains(t, applyProcessIsolation(exec.Command("/bin/true"), isolation),
		"native group 1000 cannot be entered from group -1")

	processIsolationGeteuid = func() int { return 0 }
	processIsolationGetegid = func() int { return 0 }

	command = exec.Command("/bin/true")
	require.NoError(t, applyProcessIsolation(command, &ProcessIsolation{
		UID: 1000, GID: 1000, BaseEnvironment: map[string]string{},
		StandaloneOwnerID: "acp-go-amp-isolated", StandaloneStateRoot: "/var/tmp/acp-go-amp-isolated",
	}))
	require.NotNil(t, command.SysProcAttr)
	require.NotNil(t, command.SysProcAttr.Credential)
	require.Equal(t, uint32(1000), command.SysProcAttr.Credential.Uid)
	require.Equal(t, uint32(1000), command.SysProcAttr.Credential.Gid)
	require.Empty(t, command.SysProcAttr.Credential.Groups)
	require.False(t, command.SysProcAttr.Credential.NoSetGroups)
}
