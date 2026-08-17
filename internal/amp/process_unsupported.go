//go:build !unix

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// killProcessHandle hard-terminates a portable child. It is the single place
// this backend touches a live process handle, kept as a seam so the suite can
// prove no termination is attempted after the direct-child waiter settles.
//
// A process that finished between the liveness check and the call completed
// the termination this backend asked for. Go reports that released handle as
// os.ErrProcessDone or EINVAL, so neither is retained as the operation result.
var killProcessHandle = func(process *os.Process) error {
	if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.EINVAL) {
		return err
	}

	return nil
}

type processTree struct {
	process       *os.Process
	waiter        *commandWait
	releaseNative func()
	finishOnce    sync.Once
	finishErr     error
	cleanupOnce   sync.Once
	cleanupErr    error
}

func (*processTree) descendantCount() (int, bool) { return 0, false }

func configureCommand(*exec.Cmd) {}

func startProcessTree(launch *processTreeCommand) (*processTree, error) {
	releaseNative, err := launch.acquire()
	if err != nil {
		return nil, errors.Join(err, launch.close())
	}

	launch.releaseNative = releaseNative
	if err := launch.cmd.Start(); err != nil {
		releaseNative()
		return nil, errors.Join(err, launch.close())
	}

	launch.started = true
	launch.releaseInherited()

	tree := &processTree{
		process:       launch.cmd.Process,
		waiter:        startCommandWait(launch.cmd.Wait),
		releaseNative: releaseNative,
	}

	canceled := make(chan error, 1)
	stopCancellation := func() bool { return true }
	if launch.onStartCancel != nil {
		stopCancellation = launch.onStartCancel(func() { canceled <- tree.kill() })
	}
	if !stopCancellation() {
		_ = <-canceled
	}
	if launch.startError != nil {
		if startErr := launch.startError(); startErr != nil {
			return nil, errors.Join(startErr, tree.terminateAndWait(commandWaitTimeout))
		}
	}

	return tree, nil
}

func (t *processTree) commandWait() *commandWait {
	if t == nil {
		return nil
	}
	return t.waiter
}

// settled reports whether the memoized direct-child waiter has already
// published its result. The portable backend addresses its child through a
// process handle whose numeric ID the host may reuse once the child is reaped,
// so no termination is delivered after the waiter settles.
func (t *processTree) settled() bool {
	if t == nil {
		return false
	}

	return t.waiter.settled()
}

// interrupt cancels a live portable child.
//
// Windows has no interrupt this process can deliver to another process. A
// portable cancellation therefore uses the bounded hard termination the
// platform can actually provide and never touches an already-settled handle.
func (t *processTree) interrupt() error {
	if t == nil || t.process == nil || t.settled() {
		return nil
	}

	return t.terminateAndWait(commandWaitTimeout)
}

func (t *processTree) kill() error {
	if t == nil || t.process == nil || t.settled() {
		return nil
	}

	return killProcessHandle(t.process)
}

func (t *processTree) terminateAndWait(timeout time.Duration) error {
	if t == nil {
		return nil
	}
	t.cleanupOnce.Do(func() {
		_ = t.kill()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if _, completed := t.waiter.await(ctx); !completed {
			t.cleanupErr = t.finish(ErrProcessContainmentIncomplete)
			return
		}
		t.cleanupErr = t.finish(nil)
	})
	return t.cleanupErr
}

func (t *processTree) finish(err error) error {
	if t == nil {
		return err
	}
	t.finishOnce.Do(func() {
		if err == nil && t.releaseNative != nil {
			t.releaseNative()
		}
		t.finishErr = err
	})
	return errors.Join(err, t.finishErr)
}

// interruptProcess cancels a portable command that owns no process tree. The
// platform has no deliverable interrupt, so bounded hard termination is the
// whole cancellation contract here.
func interruptProcess(cmd *exec.Cmd) error {
	return killProcess(cmd)
}

func killProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return killProcessHandle(cmd.Process)
}
