//go:build linux || darwin || freebsd || openbsd

package amp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

var (
	syscallGetpgid = syscall.Getpgid
	syscallKill    = syscall.Kill
)

type processTree struct {
	mu         sync.Mutex
	pgid       int
	control    *os.File
	supervised bool
}

func (*processTree) descendantCount() (int, bool) { return 0, false }

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	if err := launch.cmd.Start(); err != nil {
		launch.close()

		return nil, err
	}

	launch.releaseInherited()
	tree := &processTree{
		pgid:       launch.cmd.Process.Pid,
		control:    launch.control,
		supervised: launch.control != nil,
	}
	launch.control = nil

	if err := awaitProcessTreeReady(launch); err != nil {
		launch.close()
		waiter := newCommandWait(launch.cmd.Wait)
		quiescenceErr := processTreeTerminateAndWait(tree, commandWaitTimeout)
		waitCtx, cancelWait := context.WithTimeout(context.Background(), commandWaitTimeout)
		waitErr, completed := waiter.await(waitCtx)

		cancelWait()

		if !completed {
			waitErr = fmt.Errorf("%w: wait for failed Amp containment launch: %v", ErrProcessTreeNotQuiescent, waitErr)
		}

		return nil, errors.Join(err, waitErr, quiescenceErr)
	}

	return tree, nil
}

func (t *processTree) interrupt() error {
	return signalProcessGroupID(t.pgid, syscall.SIGINT)
}

func (t *processTree) kill() error {
	t.mu.Lock()
	if t.supervised {
		var err error
		if t.control != nil {
			err = t.control.Close()
			t.control = nil
		}
		t.mu.Unlock()

		return err
	}
	t.mu.Unlock()

	return signalProcessGroupID(t.pgid, syscall.SIGKILL)
}

func (t *processTree) terminateAndWait(timeout time.Duration) error {
	if t == nil || t.pgid <= 0 {
		return nil
	}

	if err := t.kill(); err != nil {
		return fmt.Errorf("%w: terminate process group %d: %w", ErrProcessTreeNotQuiescent, t.pgid, err)
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		err := syscallKill(-t.pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		if err != nil && !errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("%w: inspect process group %d: %w", ErrProcessTreeNotQuiescent, t.pgid, err)
		}

		select {
		case <-deadline.C:
			return fmt.Errorf("%w: process group %d remained live", ErrProcessTreeNotQuiescent, t.pgid)
		case <-ticker.C:
		}
	}
}

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

	return signalProcessGroupID(pgid, signal)
}

func signalProcessGroupID(pgid int, signal syscall.Signal) error {
	if pgid <= 0 {
		return nil
	}

	if err := syscallKill(-pgid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	return nil
}
