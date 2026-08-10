//go:build unix

package amp

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

var (
	processIsolationGeteuid   = os.Geteuid
	processIsolationGetegid   = os.Getegid
	processIsolationGetgroups = os.Getgroups
)

func validateProcessIsolationPlatform(isolation *ProcessIsolation) error {
	// An implicit policy has exactly one valid shape: the identity the process
	// already runs as. Anything else means the capture and the process have
	// diverged, and a launch built on that capture would misdescribe itself.
	if isolation.Implicit {
		uid, gid := processIsolationGeteuid(), processIsolationGetegid()
		if int64(isolation.UID) != int64(uid) || int64(isolation.GID) != int64(gid) {
			return fmt.Errorf(
				"implicit current-identity policy names uid=%d gid=%d, process runs as uid=%d gid=%d",
				isolation.UID, isolation.GID, uid, gid,
			)
		}

		return nil
	}

	return validateStandaloneIdentityDispositionPlatform(isolation)
}

// sharedProcessIdentity reports whether the native identity is the identity the
// supervisor already runs as. Nothing separates the two ends of the launch in
// that shape, so every step that exists to cross the boundary has nothing to
// cross. A zero effective uid never qualifies: the supervisor holds the trusted
// identity there, and a nonzero native uid is required everywhere, so the two
// can never name the same identity. Only the Linux backend recognises the
// shape; the Darwin backend states its own boundary and is left as it is.
func sharedProcessIdentity(isolation *ProcessIsolation) bool {
	if isolation == nil || processIsolationGOOS != processIsolationLinux {
		return false
	}

	// The implicit policy is the current identity by construction, root
	// included: omission launches native work as whoever already runs the
	// supervisor, so the shared shape is the only truthful description.
	if isolation.Implicit {
		return true
	}

	effectiveUID := processIsolationGeteuid()

	return effectiveUID > 0 && uint64(isolation.UID) == uint64(effectiveUID)
}

func applyProcessIsolation(cmd *exec.Cmd, isolation *ProcessIsolation) error {
	if err := validateProcessIsolation(isolation); err != nil {
		return err
	}

	if cmd == nil {
		return fmt.Errorf("process isolation command is nil")
	}

	if isolation.TestOnlyNoCredential {
		return nil
	}

	// The implicit policy already validated as the running identity, so there
	// is no credential to apply and the ambient supplementary groups — which
	// belong to that identity — stay untouched.
	if isolation.Implicit {
		return nil
	}

	// Requesting no credential change at all is the only honest instruction when
	// the native identity is already the running one. The supplementary groups
	// belong to the account the supervisor was started under, and an
	// unprivileged process can neither shed them nor re-enter them, so the
	// emptiness the isolated arm asserts is not a property this arm can hold.
	if sharedProcessIdentity(isolation) {
		effectiveGID := int64(processIsolationGetegid())
		if effectiveGID < 0 || int64(isolation.GID) != effectiveGID {
			return fmt.Errorf(
				"native group %d cannot be entered from group %d; %s",
				isolation.GID, effectiveGID, sharedIdentitySupervisorRemedy,
			)
		}

		return nil
	}

	uid, gid := int64(processIsolationGeteuid()), int64(processIsolationGetegid())
	if uid == int64(isolation.UID) && gid == int64(isolation.GID) {
		return verifyProcessIsolation(isolation)
	}

	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}

	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: isolation.UID, Gid: isolation.GID, Groups: []uint32{}}

	return nil
}

func verifyProcessIsolation(isolation *ProcessIsolation) error {
	if err := validateProcessIsolation(isolation); err != nil {
		return err
	}

	uid, gid := int64(processIsolationGeteuid()), int64(processIsolationGetegid())
	if uid != int64(isolation.UID) || gid != int64(isolation.GID) {
		return fmt.Errorf("process identity is uid=%d gid=%d, want uid=%d gid=%d", uid, gid, isolation.UID, isolation.GID)
	}

	groups, err := processIsolationGetgroups()
	if err != nil {
		return fmt.Errorf("read supplementary groups: %w", err)
	}

	if len(groups) != 0 {
		return fmt.Errorf("unexpected supplementary groups %v", groups)
	}

	return nil
}
