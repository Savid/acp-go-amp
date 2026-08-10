//go:build !linux && !darwin && !freebsd && !openbsd

package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"time"
)

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

func (t *processTree) interrupt() error {
	if t == nil || t.process == nil {
		return nil
	}
	return t.process.Signal(os.Interrupt)
}

func (t *processTree) kill() error {
	if t == nil || t.process == nil {
		return nil
	}
	return t.process.Kill()
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

func interruptProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Signal(os.Interrupt)
}

func killProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
