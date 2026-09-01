package amp

import (
	"context"
	"io"
)

type trackedProcess struct {
	process NativeProcess
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser
}

func trackProcess(process NativeProcess) *trackedProcess {
	return &trackedProcess{
		process: process,
		stdin:   process.Stdin(), stdout: process.Stdout(), stderr: process.Stderr(),
	}
}

func (p *trackedProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *trackedProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *trackedProcess) Stderr() io.ReadCloser { return p.stderr }

func (p *trackedProcess) Wait(ctx context.Context) (NativeResult, error) {
	return p.process.Wait(ctx)
}

func (p *trackedProcess) Revoke(ctx context.Context) error { return p.process.Revoke(ctx) }
