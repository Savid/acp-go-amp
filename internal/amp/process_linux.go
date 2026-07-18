//go:build linux

package amp

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

func configureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}
}

func (t *processTree) terminateAndWait(timeout time.Duration) error {
	return t.terminateAndWaitAuthoritative(timeout)
}

func (t *processTree) terminateAndWaitAuthoritative(timeout time.Duration) error {
	if t == nil {
		return nil
	}

	t.cleanupOnce.Do(func() {
		t.cleanupErr = t.runAuthoritativeCleanup(timeout)
	})

	return t.cleanupErr
}

func (t *processTree) runAuthoritativeCleanup(timeout time.Duration) error {
	if t == nil || t.pgid <= 0 {
		return nil
	}

	if err := t.kill(); err != nil {
		return t.finish(fmt.Errorf("%w: terminate process group %d: %w", ErrProcessContainmentIncomplete, t.pgid, err))
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		err := syscallKill(-t.pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return t.finish(nil)
		}

		if err != nil && !errors.Is(err, syscall.EPERM) {
			return t.finish(fmt.Errorf("%w: inspect process group %d: %w", ErrProcessContainmentIncomplete, t.pgid, err))
		}

		select {
		case <-deadline.C:
			return t.finish(fmt.Errorf("%w: process group %d remained live", ErrProcessContainmentIncomplete, t.pgid))
		case <-ticker.C:
		}
	}
}

func validateBestEffortLaunch(*processTreeCommand, *processTree, func()) error { return nil }
