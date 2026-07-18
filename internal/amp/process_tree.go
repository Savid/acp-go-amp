package amp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
)

// ErrProcessContainmentIncomplete means the selected native containment
// boundary did not complete.
var ErrProcessContainmentIncomplete = errors.New("amp process containment incomplete")

// processTreeCommand owns the platform launch wrapper and any parent-side
// descriptors that establish its containment boundary. Linux uses an embedded
// subreaper supervisor, Windows uses a Job Object, and platforms without an
// unescapable native-command boundary reject the launch.
type processTreeCommand struct {
	cmd           *exec.Cmd
	inherited     []*os.File
	startGate     *os.File
	control       *os.File
	ready         *os.File
	bestEffort    bool
	generation    *DarwinGeneration
	releaseNative func()
	acquireNative func() (func(), error)
	started       bool
}

type processLaunchOptions struct {
	DarwinBestEffort bool
	Generation       *DarwinGeneration
	ReleaseNative    func()
}

func (c *processTreeCommand) acquire() (func(), error) {
	if c == nil || c.acquireNative == nil {
		return func() {}, nil
	}

	return c.acquireNative()
}

// commandWait runs exec.Cmd.Wait exactly once without forcing a bounded
// containment caller to block forever. The result is published before
// done closes, so every waiter observes the same memoized error.
type commandWait struct {
	done chan struct{}
	err  error
}

func startCommandWait(wait func() error) *commandWait {
	state, begin := startPausedCommandWait(wait)
	begin()

	return state
}

func startPausedCommandWait(wait func() error) (*commandWait, func()) {
	state := &commandWait{done: make(chan struct{})}
	start := make(chan struct{})

	var once sync.Once

	go func() {
		<-start

		if wait != nil {
			state.err = wait()
		}

		close(state.done)
	}()

	return state, func() { once.Do(func() { close(start) }) }
}

func (w *commandWait) await(ctx context.Context) (error, bool) {
	if w == nil {
		return nil, true
	}

	select {
	case <-w.done:
		return w.err, true
	case <-ctx.Done():
		return ctx.Err(), false
	}
}

func (c *processTreeCommand) releaseInherited() {
	for _, file := range c.inherited {
		_ = file.Close()
	}

	c.inherited = nil
}

func (c *processTreeCommand) releaseStartGate() error {
	if c == nil || c.startGate == nil {
		return nil
	}

	gate := c.startGate
	c.startGate = nil
	_, writeErr := gate.Write([]byte{1})
	closeErr := gate.Close()

	return errors.Join(writeErr, closeErr)
}

func (c *processTreeCommand) abortStartGate() {
	if c == nil || c.startGate == nil {
		return
	}

	_ = c.startGate.Close()
	c.startGate = nil
}

func (c *processTreeCommand) close() error {
	if c == nil {
		return nil
	}

	var finishErr error
	if !c.started {
		finishErr = c.generation.finish(true)
	}

	c.releaseInherited()
	c.abortStartGate()

	if c.control != nil {
		_ = c.control.Close()
		c.control = nil
	}

	if c.ready != nil {
		_ = c.ready.Close()
		c.ready = nil
	}

	return finishErr
}

// ProcessContainmentComplete reports whether the selected boundary completed.
func ProcessContainmentComplete(err error) bool {
	return !errors.Is(err, ErrProcessContainmentIncomplete)
}
