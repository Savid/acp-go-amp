//go:build unix

package amp

import (
	"errors"
	"os/exec"
	"syscall"
)

var (
	syscallGetpgid = syscall.Getpgid
	syscallKill    = syscall.Kill
)

func interruptProcess(cmd *exec.Cmd) error {
	return signalProcessGroup(cmd, syscall.SIGINT)
}

func killProcess(cmd *exec.Cmd) error {
	return signalProcessGroup(cmd, syscall.SIGKILL)
}

// signalProcessGroup signals the child's process group, treating an
// already-exited child as success. The Getpgid probe doubles as the liveness
// check: darwin returns EPERM (not ESRCH) when signalling a group whose only
// member is an unreaped zombie, so kill errors alone can't distinguish "gone"
// from "not permitted".
func signalProcessGroup(cmd *exec.Cmd, signal syscall.Signal) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	pgid, err := syscallGetpgid(cmd.Process.Pid)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		return err
	}

	if err := syscallKill(-pgid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	return nil
}
