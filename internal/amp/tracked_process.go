package amp

import (
	"context"
	"io"
	"sync"
)

type trackedProcess struct {
	process NativeProcess
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser

	waitOnce sync.Once
	waitDone chan struct{}
	result   NativeResult
	waitErr  error
}

func trackProcess(process NativeProcess) *trackedProcess {
	return &trackedProcess{
		process: process,
		stdin:   process.Stdin(), stdout: process.Stdout(), stderr: process.Stderr(),
		waitDone: make(chan struct{}),
	}
}

func (p *trackedProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *trackedProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *trackedProcess) Stderr() io.ReadCloser { return p.stderr }

func (p *trackedProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.waitOnce.Do(func() {
		go func() {
			p.result, p.waitErr = p.process.Wait(context.Background())
			close(p.waitDone)
		}()
	})

	select {
	case <-ctx.Done():
		return NativeResult{}, ctx.Err()
	case <-p.waitDone:
		return p.result, p.waitErr
	}
}

func (p *trackedProcess) Revoke(ctx context.Context) error { return p.process.Revoke(ctx) }
