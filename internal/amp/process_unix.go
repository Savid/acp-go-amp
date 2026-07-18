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
)

var (
	syscallGetpgid       = syscall.Getpgid
	syscallKill          = syscall.Kill
	processTreeReadyWait = awaitProcessTreeReady
)

type processTree struct {
	mu            sync.Mutex
	pgid          int
	process       *os.Process
	control       *os.File
	supervised    bool
	waiter        *commandWait
	releaseWaiter func()
	generation    *DarwinGeneration
	releaseNative func()
	finishOnce    sync.Once
	finishErr     error
	cleanupOnce   sync.Once
	cleanupErr    error
}

func (*processTree) descendantCount() (int, bool) { return 0, false }

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	releaseNative, err := launch.acquire()
	if err != nil {
		finishErr := launch.close()

		return nil, errors.Join(err, finishErr)
	}

	launch.releaseNative = releaseNative
	if err := launch.cmd.Start(); err != nil {
		releaseNative()

		closeErr := launch.close()

		return nil, errors.Join(err, closeErr)
	}

	launch.started = true

	launch.releaseInherited()
	waiter, beginWait := startPausedCommandWait(launch.cmd.Wait)
	tree := &processTree{
		pgid:          launch.cmd.Process.Pid,
		process:       launch.cmd.Process,
		control:       launch.control,
		supervised:    launch.control != nil,
		waiter:        waiter,
		releaseWaiter: beginWait,
		generation:    launch.generation,
		releaseNative: launch.releaseNative,
	}

	launch.control = nil
	if launch.bestEffort {
		pgid, pgidErr := syscallGetpgid(launch.cmd.Process.Pid)
		if errors.Is(pgidErr, syscall.ESRCH) {
			return nil, errors.Join(
				errors.New("amp launch exited before Darwin process-group identity validation"),
				handleDarwinFastExit(launch, tree, beginWait),
			)
		}

		if pgidErr != nil || pgid != launch.cmd.Process.Pid {
			containmentErr := fmt.Errorf("%w: validate Darwin process-group leader pid=%d pgid=%d: %v", ErrProcessContainmentIncomplete, launch.cmd.Process.Pid, pgid, pgidErr)
			launch.abortStartGate()

			if tree.process != nil {
				_ = tree.process.Signal(syscall.SIGTERM)
			}

			beginWait()

			return nil, errors.Join(containmentErr, abortUnvalidatedProcessTree(tree))
		}

		tree.pgid = pgid
		if recordErr := tree.generation.started(launch.cmd.Process.Pid, pgid); recordErr != nil {
			launch.abortStartGate()

			return nil, errors.Join(recordErr, tree.terminateAndWait(defaultCloseWait))
		}

		if gateErr := launch.releaseStartGate(); gateErr != nil {
			return nil, errors.Join(
				fmt.Errorf("%w: release validated Darwin native launch: %v", ErrProcessContainmentIncomplete, gateErr),
				tree.terminateAndWait(defaultCloseWait),
			)
		}
	}

	beginWait()

	if err := processTreeReadyWait(launch); err != nil {
		_ = launch.close()
		containmentErr := processTreeTerminateAndWait(tree, commandWaitTimeout)
		waitCtx, cancelWait := context.WithTimeout(context.Background(), commandWaitTimeout)
		waitErr, completed := tree.waiter.await(waitCtx)

		cancelWait()

		if !completed {
			waitErr = fmt.Errorf("%w: wait for failed Amp containment launch: %v", ErrProcessContainmentIncomplete, waitErr)
		}

		return nil, errors.Join(err, waitErr, containmentErr)
	}

	return tree, nil
}

func (t *processTree) commandWait() *commandWait {
	if t == nil {
		return nil
	}

	return t.waiter
}

func (t *processTree) finish(err error) error {
	if t == nil {
		return err
	}

	t.finishOnce.Do(func() {
		complete := ProcessContainmentComplete(err)

		t.finishErr = t.generation.finish(complete)
		if complete && ProcessContainmentComplete(t.finishErr) && t.releaseNative != nil {
			t.releaseNative()
		}
	})

	return errors.Join(err, t.finishErr)
}

func (t *processTree) interrupt() error {
	if t.generation != nil {
		return t.terminateAndWait(defaultCloseWait)
	}

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

	if t.generation != nil {
		return t.terminateAndWait(defaultCloseWait)
	}

	return signalProcessGroupID(t.pgid, syscall.SIGKILL)
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
