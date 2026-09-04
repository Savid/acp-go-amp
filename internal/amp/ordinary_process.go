package amp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
)

// newProcessPipe is the seam the pipe-exhaustion branches are proven through.
var newProcessPipe = os.Pipe

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
	pipes, err := ordinaryProcessPipes(cmd)
	if err != nil {
		return nil, err
	}

	startErr := cmd.Start()

	// From Start onward the child holds its own ends of all three pipes. This
	// process must drop its copies or the child never reads EOF on stdin and
	// its stdout and stderr never reach one either.
	pipes.closeChildEnds()

	if startErr != nil {
		pipes.closeParentEnds()

		return nil, startErr
	}

	return &ordinaryProcess{
		cmd: cmd, stdin: pipes.stdin, stdout: pipes.stdout, stderr: pipes.stderr,
		waitDone: make(chan struct{}),
	}, nil
}

// ordinaryPipes holds both ends of the child's three standard streams while the
// process is being started, so a failure at any point releases every descriptor
// it already claimed.
type ordinaryPipes struct {
	stdin  *os.File
	stdout *os.File
	stderr *os.File

	childStdin  *os.File
	childStdout *os.File
	childStderr *os.File
}

func (p *ordinaryPipes) closeChildEnds() {
	_ = p.childStdin.Close()
	_ = p.childStdout.Close()
	_ = p.childStderr.Close()
}

func (p *ordinaryPipes) closeParentEnds() {
	_ = p.stdin.Close()
	_ = p.stdout.Close()
	_ = p.stderr.Close()
}

// ordinaryProcessPipes wires the child's three standard streams as ordinary OS
// pipes this process owns outright.
//
// exec.Cmd's own StdinPipe/StdoutPipe/StderrPipe hand their parent ends to
// Cmd.Wait, which closes them the moment the child exits — a close that races
// whoever is still draining what the child already wrote. Every amp process is
// short-lived and writes its whole answer just before exiting, so on a busy
// machine that race is lost routinely: a version probe reads an empty version,
// a thread listing decodes as truncated JSON, and a failing turn loses the
// stderr line its classification depends on. Owning the pipes here keeps each
// parent end open until its reader sees EOF, so the child's bytes survive
// whatever the scheduler does with the exit.
func ordinaryProcessPipes(cmd *exec.Cmd) (_ *ordinaryPipes, err error) {
	pipes := &ordinaryPipes{}

	defer func() {
		if err != nil {
			pipes.closeChildEnds()
			pipes.closeParentEnds()
		}
	}()

	if cmd.Stdin != nil {
		return nil, errors.New("create native stdin: stdin already set")
	}

	pipes.childStdin, pipes.stdin, err = newProcessPipe()
	if err != nil {
		return nil, fmt.Errorf("create native stdin: %w", err)
	}

	if cmd.Stdout != nil {
		return nil, errors.New("create native stdout: stdout already set")
	}

	pipes.stdout, pipes.childStdout, err = newProcessPipe()
	if err != nil {
		return nil, fmt.Errorf("create native stdout: %w", err)
	}

	if cmd.Stderr != nil {
		return nil, errors.New("create native stderr: stderr already set")
	}

	pipes.stderr, pipes.childStderr, err = newProcessPipe()
	if err != nil {
		return nil, fmt.Errorf("create native stderr: %w", err)
	}

	cmd.Stdin = pipes.childStdin
	cmd.Stdout = pipes.childStdout
	cmd.Stderr = pipes.childStderr

	return pipes, nil
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
