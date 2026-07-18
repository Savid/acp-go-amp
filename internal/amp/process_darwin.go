//go:build darwin

package amp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

var (
	darwinContainmentNow   = time.Now
	darwinContainmentSleep = time.Sleep
	darwinAbortKillAfter   = defaultCloseKillAfter
	darwinAbortWait        = defaultCloseWait
	darwinFastExitWait     = defaultCloseWait
)

// Darwin has no Pdeathsig equivalent; parent-death cleanup is best-effort via
// process-group signalling.
func configureCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func (t *processTree) terminateAndWait(_ time.Duration) error {
	if t == nil {
		return nil
	}

	t.cleanupOnce.Do(func() {
		t.cleanupErr = t.runDarwinCleanup()
	})

	return t.cleanupErr
}

func (t *processTree) runDarwinCleanup() error {
	if t == nil || t.pgid <= 0 {
		return nil
	}

	deadline := darwinContainmentNow().Add(defaultCloseWait)
	termDeadline := darwinContainmentNow().Add(defaultCloseKillAfter)

	termErr := signalDarwinProcessGroupID(t.pgid, syscall.SIGTERM)
	if t.releaseWaiter != nil {
		t.releaseWaiter()
	}

	return t.runDarwinCleanupAfterTerm(deadline, termDeadline, termErr)
}

func (t *processTree) runDarwinCleanupAfterTerm(deadline, termDeadline time.Time, termErr error) error {
	groupAbsent := errors.Is(termErr, syscall.ESRCH)
	if termErr != nil && !groupAbsent && !errors.Is(termErr, syscall.EPERM) {
		return t.abortDarwinCleanup(deadline, fmt.Errorf("%w: terminate original process group %d: %v", ErrProcessContainmentIncomplete, t.pgid, termErr))
	}

	killed := false

	for darwinContainmentNow().Before(deadline) {
		if !groupAbsent {
			probeErr := syscallKill(-t.pgid, 0)
			switch {
			case errors.Is(probeErr, syscall.ESRCH):
				groupAbsent = true
			case probeErr != nil && !errors.Is(probeErr, syscall.EPERM):
				return t.abortDarwinCleanup(deadline, fmt.Errorf("%w: inspect original process group %d: %w", ErrProcessContainmentIncomplete, t.pgid, probeErr))
			}
		}

		reaped := false

		if t.waiter != nil {
			select {
			case <-t.waiter.done:
				reaped = true
			default:
			}
		}

		if groupAbsent && reaped {
			return t.finish(nil)
		}

		if !groupAbsent && !killed && !darwinContainmentNow().Before(termDeadline) {
			killErr := signalDarwinProcessGroupID(t.pgid, syscall.SIGKILL)
			if errors.Is(killErr, syscall.ESRCH) {
				groupAbsent = true
			} else if killErr != nil && !errors.Is(killErr, syscall.EPERM) {
				return t.abortDarwinCleanup(deadline, fmt.Errorf("%w: kill original process group %d: %v", ErrProcessContainmentIncomplete, t.pgid, killErr))
			}

			killed = true
		}

		darwinContainmentSleep(10 * time.Millisecond)
	}

	return t.finish(fmt.Errorf("%w: direct child or original process group %d remained observable", ErrProcessContainmentIncomplete, t.pgid))
}

func (t *processTree) abortDarwinCleanup(deadline time.Time, cause error) error {
	var killErr error

	if t.process != nil {
		killErr = t.process.Kill()
		if errors.Is(killErr, os.ErrProcessDone) || errors.Is(killErr, syscall.ESRCH) {
			killErr = nil
		}
	}

	if t.releaseWaiter != nil {
		t.releaseWaiter()
	}

	var reapErr error

	if t.waiter != nil {
		remaining := deadline.Sub(darwinContainmentNow())
		if remaining <= 0 {
			reapErr = errors.New("direct child remained unreaped at the containment deadline")
		} else if !awaitDarwinCommandWait(t.waiter, remaining) {
			reapErr = errors.New("direct child remained unreaped at the containment deadline")
		}
	}

	return t.finish(errors.Join(cause, killErr, reapErr))
}

func awaitDarwinCommandWait(waiter *commandWait, timeout time.Duration) bool {
	waitContext, cancelWait := context.WithTimeout(context.Background(), timeout)
	defer cancelWait()

	_, completed := waiter.await(waitContext)

	return completed
}

func handleDarwinFastExit(launch *processTreeCommand, tree *processTree, beginWait func()) error {
	launch.abortStartGate()

	probeErr := syscallKill(-tree.pgid, 0)
	switch {
	case errors.Is(probeErr, syscall.ESRCH):
		beginWait()

		waitContext, cancelWait := context.WithTimeout(context.Background(), darwinFastExitWait)
		defer cancelWait()

		waitErr, completed := tree.waiter.await(waitContext)
		if !completed {
			return tree.finish(fmt.Errorf("%w: reap fast-exit Darwin child: %v", ErrProcessContainmentIncomplete, waitErr))
		}

		return errors.Join(waitErr, tree.finish(nil))
	case probeErr == nil || errors.Is(probeErr, syscall.EPERM):
		tree.cleanupOnce.Do(func() {
			deadline := darwinContainmentNow().Add(defaultCloseWait)
			termDeadline := darwinContainmentNow().Add(defaultCloseKillAfter)
			termErr := signalDarwinProcessGroupID(tree.pgid, syscall.SIGTERM)

			beginWait()

			tree.cleanupErr = tree.runDarwinCleanupAfterTerm(deadline, termDeadline, termErr)
		})

		return tree.cleanupErr
	default:
		if tree.process != nil {
			_ = tree.process.Signal(syscall.SIGTERM)
		}

		beginWait()

		return errors.Join(
			fmt.Errorf("%w: probe expected Darwin process group %d: %v", ErrProcessContainmentIncomplete, tree.pgid, probeErr),
			abortUnvalidatedProcessTree(tree),
		)
	}
}

func signalDarwinProcessGroupID(pgid int, signal syscall.Signal) error {
	if pgid <= 0 {
		return nil
	}

	return syscallKill(-pgid, signal)
}

func abortUnvalidatedProcessTree(t *processTree) error {
	if t == nil {
		return ErrProcessContainmentIncomplete
	}

	term := time.NewTimer(darwinAbortKillAfter)
	defer term.Stop()

	deadline := time.NewTimer(darwinAbortWait)
	defer deadline.Stop()

	termC := term.C

	for {
		select {
		case <-t.waiter.done:
			return t.finish(ErrProcessContainmentIncomplete)
		case <-termC:
			if t.process != nil {
				_ = t.process.Kill()
			}

			termC = nil
		case <-deadline.C:
			return t.finish(ErrProcessContainmentIncomplete)
		}
	}
}
