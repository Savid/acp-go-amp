package amp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type coverageWriteCloser struct {
	bytes.Buffer
	writeErr error
	closeErr error
}

func (w *coverageWriteCloser) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}

	return w.Buffer.Write(p)
}

func (w *coverageWriteCloser) Close() error { return w.closeErr }

type coverageNativeProcess struct {
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	stderr    io.ReadCloser
	wait      func(context.Context) (NativeResult, error)
	revokeErr error
}

func newCoverageNativeProcess() *coverageNativeProcess {
	return &coverageNativeProcess{
		stdin:  &coverageWriteCloser{},
		stdout: io.NopCloser(bytes.NewReader(nil)),
		stderr: io.NopCloser(bytes.NewReader(nil)),
		wait:   func(context.Context) (NativeResult, error) { return NativeResult{}, nil },
	}
}

func (p *coverageNativeProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *coverageNativeProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *coverageNativeProcess) Stderr() io.ReadCloser { return p.stderr }
func (p *coverageNativeProcess) Wait(ctx context.Context) (NativeResult, error) {
	return p.wait(ctx)
}
func (p *coverageNativeProcess) Revoke(context.Context) error { return p.revokeErr }

func TestClientDiscoveryAndProbeResidualErrors(t *testing.T) {
	_, err := NewClient(nil, Options{}).DiscoveryProbe(t.Context())
	require.ErrorContains(t, err, "isolated writable root")

	probe := newTestProbeClient(t, nil, Options{})
	probe.options.Env["bad=key"] = "x"
	require.ErrorContains(t, probe.validateProbeResidence(), "invalid environment key")

	containment := errors.Join(errors.New("wait failed"), ErrContainmentIncomplete)
	require.ErrorIs(t, methodProbeError("threads export", containment, false), ErrContainmentIncomplete)
	require.NoError(t, methodProbeError("threads export", nil, false))

	_, err = newTestClient(t, nil, Options{ResolvedExecutable: "/missing"}).ListThreads(cancelledContext())
	require.ErrorIs(t, err, context.Canceled)

	_, err = Discover(t.Context(), "amp")
	require.ErrorContains(t, err, "complete process environment")
	_, err = Discover(t.Context(), "amp", []string{"PATH=" + t.TempDir()})
	require.ErrorContains(t, err, "find amp in PATH")
}

func TestClientStartTurnResidualErrors(t *testing.T) {
	original := getwd
	t.Cleanup(func() { getwd = original })
	wantGetwd := errors.New("getwd failed")
	getwd = func() (string, error) { return "", wantGetwd }

	client := NewClient(nil, Options{StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return newCoverageNativeProcess(), nil
	}})
	_, err := client.Execute(t.Context(), map[string]string{"text": "x"})
	require.ErrorIs(t, err, wantGetwd)

	getwd = func() (string, error) { return t.TempDir(), nil }
	client.options.Env = map[string]string{"bad=key": "x"}
	_, err = client.Execute(t.Context(), map[string]string{"text": "x"})
	require.ErrorContains(t, err, "invalid environment key")

	client = NewClient(nil, Options{OrdinaryEnvironment: map[string]string{"PATH": t.TempDir()}, Cwd: t.TempDir()})
	_, err = client.Execute(t.Context(), map[string]string{"text": "x"})
	require.ErrorContains(t, err, "find amp in PATH")

	wantStart := errors.New("start refused")
	client = NewClient(nil, Options{Cwd: t.TempDir(), CLIPath: "amp", StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, wantStart
	}})
	_, err = client.Execute(t.Context(), map[string]string{"text": "x"})
	require.ErrorIs(t, err, wantStart)

	client = NewClient(nil, Options{Cwd: t.TempDir(), CLIPath: "amp", StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		process := newCoverageNativeProcess()
		process.stdin = &coverageWriteCloser{writeErr: errors.New("write refused")}

		return process, nil
	}})
	_, err = client.Execute(t.Context(), map[string]string{"text": "x"})
	require.ErrorContains(t, err, "write amp stdin")

	client = NewClient(nil, Options{Cwd: t.TempDir(), CLIPath: "amp", StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		process := newCoverageNativeProcess()
		process.stdin = &coverageWriteCloser{closeErr: errors.New("close refused")}

		return process, nil
	}})
	_, err = client.Execute(t.Context(), map[string]string{"text": "x"})
	require.ErrorContains(t, err, "close amp stdin")
}

func TestClientOutputResidualBranches(t *testing.T) {
	client := NewClient(nil, Options{OrdinaryEnvironment: map[string]string{"PATH": t.TempDir()}})
	_, err := client.outputWithArgs(t.Context(), "version")
	require.Error(t, err)

	original := getwd
	t.Cleanup(func() { getwd = original })
	getwd = func() (string, error) { return "/chosen", nil }
	require.Equal(t, "/chosen", NewClient(nil, Options{}).commandCwd())

	_, err = client.outputAtPath(cancelledContext(), "/unused", "version")
	require.ErrorIs(t, err, context.Canceled)

	wantProbe := errors.New("probe refused")
	client = NewClient(nil, Options{NewProbeClient: func(context.Context) (*Client, func() error, error) {
		return nil, nil, wantProbe
	}})
	_, err = client.outputAtPath(t.Context(), "/unused", "version")
	require.ErrorIs(t, err, wantProbe)

	wantCleanup := errors.New("cleanup refused")
	client = NewClient(nil, Options{NewProbeClient: func(context.Context) (*Client, func() error, error) {
		return NewClient(nil, Options{}), func() error { return wantCleanup }, nil
	}})
	_, err = client.outputAtPath(t.Context(), "/unused", "version")
	require.ErrorIs(t, err, wantCleanup)

	path, _ := fakeAmpPath(t, "")
	client = NewClient(nil, Options{NewProbeClient: func(context.Context) (*Client, func() error, error) {
		return newTestProbeClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()}), func() error { return wantCleanup }, nil
	}})
	_, err = client.outputAtPath(t.Context(), path, "version")
	require.ErrorIs(t, err, wantCleanup)

	client = NewClient(nil, Options{Cwd: t.TempDir(), Env: map[string]string{"bad=key": "x"}, StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return newCoverageNativeProcess(), nil
	}})
	_, err = client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorContains(t, err, "invalid environment key")

	wantStart := errors.New("start refused")
	client = NewClient(nil, Options{Cwd: t.TempDir(), StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, wantStart
	}})
	_, err = client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorIs(t, err, wantStart)

	var waitCalls int
	process := newCoverageNativeProcess()
	process.wait = func(context.Context) (NativeResult, error) {
		waitCalls++
		if waitCalls == 1 {
			return NativeResult{}, context.Canceled
		}

		return NativeResult{Revoked: true}, errors.New("terminal")
	}
	process.revokeErr = errors.New("revoke refused")
	client = NewClient(nil, Options{Cwd: t.TempDir(), StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return process, nil
	}})
	_, err = client.outputAtPath(t.Context(), "amp", "threads", "list")
	require.ErrorContains(t, err, "exit code")
	require.Equal(t, 1, waitCalls)

	process = newCoverageNativeProcess()
	process.wait = func(context.Context) (NativeResult, error) { return NativeResult{ExitCode: 7}, nil }
	client.options.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) { return process, nil }
	_, err = client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorContains(t, err, "exit code 7")
}

func TestClientNativeStarterAndResolverResidualBranches(t *testing.T) {
	want := errors.New("starter refused")
	client := NewClient(nil, Options{StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, want
	}})
	_, err := client.startNative(t.Context(), NativeRequest{})
	require.ErrorIs(t, err, want)

	client.options.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) {
		return nil, nil //nolint:nilnil // Exercise the host contract's unusable nil process.
	}
	_, err = client.startNative(t.Context(), NativeRequest{})
	require.ErrorContains(t, err, "unusable host stdio")

	for _, missing := range []string{"stdin", "stdout", "stderr"} {
		process := newCoverageNativeProcess()
		switch missing {
		case "stdin":
			process.stdin = nil
		case "stdout":
			process.stdout = nil
		case "stderr":
			process.stderr = nil
		}
		client.options.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) { return process, nil }
		_, err = client.startNative(t.Context(), NativeRequest{})
		require.ErrorContains(t, err, "unusable host stdio")
	}

	client = NewClient(nil, Options{ResolvedExecutable: "relative", StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return newCoverageNativeProcess(), nil
	}})
	path, err := client.resolveExecutable(t.Context(), t.TempDir())
	require.NoError(t, err)
	require.Equal(t, "relative", path)

	client = NewClient(nil, Options{ResolutionEnv: map[string]string{"bad=key": "x"}, StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return newCoverageNativeProcess(), nil
	}})
	_, err = client.resolveExecutable(t.Context(), t.TempDir())
	require.ErrorContains(t, err, "invalid environment key")

	client = NewClient(nil, Options{StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return newCoverageNativeProcess(), nil
	}})
	path, err = client.discover(t.Context(), nil, t.TempDir())
	require.NoError(t, err)
	require.Equal(t, ampExecutableName, path)
	client.options.CLIPath = "custom"
	path, err = client.discover(t.Context(), nil, t.TempDir())
	require.NoError(t, err)
	require.Equal(t, "custom", path)

	client = NewClient(nil, Options{OrdinaryEnvironment: map[string]string{"PATH": t.TempDir()}})
	_, err = client.discover(t.Context(), []string{"PATH=" + t.TempDir()}, t.TempDir())
	require.ErrorContains(t, err, "find amp in PATH")
}

func TestOrdinaryAndTrackedProcessResidualBranches(t *testing.T) {
	_, err := startOrdinaryNative(cancelledContext(), NativeRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = startOrdinaryNative(t.Context(), NativeRequest{Executable: filepath.Join(t.TempDir(), "missing")})
	require.Error(t, err)

	for _, stream := range []string{"stdin", "stdout", "stderr"} {
		cmd := exec.Command("ignored")
		switch stream {
		case "stdin":
			cmd.Stdin = bytes.NewReader(nil)
		case "stdout":
			cmd.Stdout = io.Discard
		case "stderr":
			cmd.Stderr = io.Discard
		}
		_, pipeErr := startOrdinaryCommand(cmd)
		require.ErrorContains(t, pipeErr, "create native "+stream)
	}

	want := errors.New("revoke refused")
	ordinary := &ordinaryProcess{cmd: &exec.Cmd{}, waitDone: make(chan struct{})}
	ordinary.revokeOnce.Do(func() { ordinary.revokeErr = want })
	require.ErrorIs(t, ordinary.Revoke(t.Context()), want)

	ordinary = &ordinaryProcess{cmd: &exec.Cmd{}, waitDone: make(chan struct{})}
	require.ErrorIs(t, ordinary.Revoke(cancelledContext()), context.Canceled)
	close(ordinary.waitDone)
	require.NoError(t, ordinary.Revoke(t.Context()))
}

type blockingCoverageProcess struct {
	*coverageNativeProcess
	release chan struct{}
	result  NativeResult
	err     error
}

func (p *blockingCoverageProcess) Wait(context.Context) (NativeResult, error) {
	<-p.release

	return p.result, p.err
}

func TestTrackedProcessWaitHonorsContextThenMemoizesResult(t *testing.T) {
	base := newCoverageNativeProcess()
	underlying := &blockingCoverageProcess{coverageNativeProcess: base, release: make(chan struct{}), result: NativeResult{ExitCode: 3}, err: errors.New("done")}
	tracked := trackProcess(underlying)

	_, err := tracked.Wait(cancelledContext())
	require.ErrorIs(t, err, context.Canceled)
	close(underlying.release)
	result, err := tracked.Wait(t.Context())
	require.Equal(t, NativeResult{ExitCode: 3}, result)
	require.ErrorContains(t, err, "done")
}

func TestTurnReaderAndErrorResidualBranches(t *testing.T) {
	turn := &Turn{
		stdout:       io.NopCloser(bytes.NewBufferString(`{"type":"result","subtype":"success"}` + "\n")),
		messages:     make(chan Message),
		errs:         make(chan error, 4),
		maxLineBytes: 1024,
		stderrDone:   make(chan struct{}),
	}
	turn.readStdout(cancelledContext())
	require.ErrorIs(t, <-turn.errs, context.Canceled)

	turn = &Turn{
		stdout:       failingReadCloser{},
		messages:     make(chan Message),
		errs:         make(chan error, 4),
		maxLineBytes: 1024,
		stderrDone:   make(chan struct{}),
	}
	turn.readStdout(t.Context())
	require.ErrorContains(t, <-turn.errs, "read amp stdout")

	require.NoError(t, (&Turn{}).wait())
	process := newCoverageNativeProcess()
	process.wait = func(context.Context) (NativeResult, error) { return NativeResult{Signal: 9}, nil }
	require.ErrorContains(t, (&Turn{process: process}).wait(), "signal 9")

	turn = &Turn{errs: make(chan error, 1), log: slog.New(slog.DiscardHandler)}
	turn.errs <- errors.New("full")
	turn.sendErr(errors.New("dropped"))

	turn = &Turn{}
	turn.captureStderr("first")
	turn.captureStderr("second")
	require.Equal(t, "first\nsecond", turn.stderrText())
	require.ErrorContains(t, (&Turn{}).exitError(errors.New("failed")), "amp process exited")
}
