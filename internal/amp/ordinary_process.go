package amp

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
)

type ordinaryProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	waitOnce sync.Once
	waitDone chan struct{}
	result   NativeResult
	waitErr  error

	revokeOnce sync.Once
	revokeErr  error
	revoked    atomic.Bool
}

func startOrdinaryNative(ctx context.Context, request NativeRequest) (NativeProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cmd := exec.Command(request.Executable, request.Arguments...) //nolint:gosec // Ordinary mode executes the configured native harness.
	cmd.Dir = request.WorkingDirectory
	cmd.Env = append([]string(nil), request.Environment...)

	return startOrdinaryCommand(cmd)
}

func startOrdinaryCommand(cmd *exec.Cmd) (NativeProcess, error) {
	stdin, stdout, stderr, err := ordinaryProcessPipes(cmd)
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()

		return nil, err
	}

	return &ordinaryProcess{
		cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr,
		waitDone: make(chan struct{}),
	}, nil
}

func ordinaryProcessPipes(cmd *exec.Cmd) (io.WriteCloser, io.ReadCloser, io.ReadCloser, error) {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create native stdin: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()

		return nil, nil, nil, fmt.Errorf("create native stdout: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()

		return nil, nil, nil, fmt.Errorf("create native stderr: %w", err)
	}

	return stdin, stdout, stderr, nil
}

func (p *ordinaryProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *ordinaryProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *ordinaryProcess) Stderr() io.ReadCloser { return p.stderr }

func (p *ordinaryProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.waitOnce.Do(func() {
		go func() {
			err := p.cmd.Wait()

			p.result.ExitCode = p.cmd.ProcessState.ExitCode()
			if status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				p.result.Signal = int(status.Signal())
			}

			p.result.Revoked = p.revoked.Load()
			p.waitErr = err
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

func (p *ordinaryProcess) Revoke(ctx context.Context) error {
	p.revokeOnce.Do(func() {
		p.revoked.Store(true)

		if p.cmd.Process != nil {
			p.revokeErr = p.cmd.Process.Kill()
			if processAlreadyGone(p.revokeErr) {
				p.revokeErr = nil
			}
		}
	})

	if p.revokeErr != nil {
		return p.revokeErr
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.waitDone:
		return nil
	default:
		return nil
	}
}
