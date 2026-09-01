package amp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

type blockedProbeReadCloser struct {
	entered   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
	active    atomic.Int32
	closes    atomic.Int32
}

type eofTrackingReadCloser struct {
	reader     *bytes.Reader
	eof        atomic.Bool
	earlyClose atomic.Bool
}

func newEOFTrackingReadCloser(content string) *eofTrackingReadCloser {
	return &eofTrackingReadCloser{reader: bytes.NewReader([]byte(content))}
}

func (r *eofTrackingReadCloser) Read(data []byte) (int, error) {
	n, err := r.reader.Read(data)
	if errors.Is(err, io.EOF) {
		r.eof.Store(true)
	}

	return n, err
}

func (r *eofTrackingReadCloser) Close() error {
	if !r.eof.Load() {
		r.earlyClose.Store(true)
	}

	return nil
}

func newBlockedProbeReadCloser() *blockedProbeReadCloser {
	return &blockedProbeReadCloser{entered: make(chan struct{}), closed: make(chan struct{})}
}

func (r *blockedProbeReadCloser) Read([]byte) (int, error) {
	r.active.Add(1)
	defer r.active.Add(-1)
	r.enterOnce.Do(func() { close(r.entered) })
	<-r.closed

	return 0, io.EOF
}

func (r *blockedProbeReadCloser) Close() error {
	r.closes.Add(1)
	r.closeOnce.Do(func() { close(r.closed) })

	return nil
}

type blockedProbeProcess struct {
	stdin       *coverageWriteCloser
	stdout      *blockedProbeReadCloser
	stderr      *blockedProbeReadCloser
	waits       atomic.Int32
	stdinCalls  atomic.Int32
	stdoutCalls atomic.Int32
	stderrCalls atomic.Int32
	unresolved  error
}

func (p *blockedProbeProcess) Stdin() io.WriteCloser {
	p.stdinCalls.Add(1)

	return p.stdin
}

func (p *blockedProbeProcess) Stdout() io.ReadCloser {
	p.stdoutCalls.Add(1)

	return p.stdout
}

func (p *blockedProbeProcess) Stderr() io.ReadCloser {
	p.stderrCalls.Add(1)

	return p.stderr
}

func (p *blockedProbeProcess) Wait(ctx context.Context) (NativeResult, error) {
	if p.waits.Add(1) == 1 {
		<-ctx.Done()

		return NativeResult{}, ctx.Err()
	}

	return NativeResult{}, p.unresolved
}

func (*blockedProbeProcess) Revoke(context.Context) error { return nil }

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
	require.Equal(t, 2, waitCalls)

	process = newCoverageNativeProcess()
	process.wait = func(context.Context) (NativeResult, error) { return NativeResult{ExitCode: 7}, nil }
	client.options.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) { return process, nil }
	_, err = client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorContains(t, err, "exit code 7")
}

func TestClientOutputUnresolvedSecondWaitClosesAndJoinsBlockedStreams(t *testing.T) {
	stdout := newBlockedProbeReadCloser()
	stderr := newBlockedProbeReadCloser()
	unresolved := errors.New("second wait unresolved")
	process := &blockedProbeProcess{
		stdin: &coverageWriteCloser{}, stdout: stdout, stderr: stderr, unresolved: unresolved,
	}
	client := NewClient(nil, Options{
		Cwd: t.TempDir(),
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return process, nil
		},
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := client.outputAtPath(ctx, "amp", "version")
		done <- err
	}()

	<-stdout.entered
	<-stderr.entered
	cancel()

	err := <-done
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, unresolved)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.Equal(t, int32(2), process.waits.Load())
	require.Equal(t, int32(1), process.stdinCalls.Load())
	require.Equal(t, int32(1), process.stdoutCalls.Load())
	require.Equal(t, int32(1), process.stderrCalls.Load())
	require.Equal(t, int32(1), stdout.closes.Load())
	require.Equal(t, int32(1), stderr.closes.Load())
	require.Zero(t, stdout.active.Load())
	require.Zero(t, stderr.active.Load())
}

func TestClientOutputManagedIndependentWaitUsesFreshTerminalProofAndNaturalDrain(t *testing.T) {
	stdout := newEOFTrackingReadCloser("preserved output")
	stderr := newEOFTrackingReadCloser("independent wait diagnostic")
	independent := errors.New("independent host wait failure")
	process := newCoverageNativeProcess()
	process.stdout = stdout
	process.stderr = stderr
	waits := 0
	process.wait = func(context.Context) (NativeResult, error) {
		waits++
		if waits == 1 {
			return NativeResult{}, independent
		}

		return NativeResult{}, nil
	}
	client := NewClient(nil, Options{
		Cwd: t.TempDir(),
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return process, nil
		},
	})

	_, err := client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorIs(t, err, independent)
	require.ErrorContains(t, err, "independent wait diagnostic")
	require.Equal(t, 2, waits)
	require.True(t, stdout.eof.Load())
	require.True(t, stderr.eof.Load())
	require.False(t, stdout.earlyClose.Load())
	require.False(t, stderr.earlyClose.Load())
}

func TestClientOutputDetachedFirstWaitAcceptsFreshTerminalProof(t *testing.T) {
	process := newCoverageNativeProcess()
	waits := 0
	process.wait = func(context.Context) (NativeResult, error) {
		waits++
		if waits == 1 {
			return NativeResult{}, context.Canceled
		}

		return NativeResult{}, nil
	}
	client := NewClient(nil, Options{
		Cwd: t.TempDir(),
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return process, nil
		},
	})

	_, err := client.outputAtPath(t.Context(), "amp", "version")
	require.NoError(t, err)
	require.Equal(t, 2, waits)
}

func TestClientOutputFreshWaitGetsFullBoundAfterRevokeExhaustion(t *testing.T) {
	independent := errors.New("initial wait refused")
	process := newCoverageNativeProcess()
	waits := 0
	var revokeDeadline time.Time
	process.wait = func(ctx context.Context) (NativeResult, error) {
		waits++
		if waits == 1 {
			return NativeResult{}, independent
		}

		terminalDeadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, terminalDeadline.After(revokeDeadline),
			"the terminal proof inherited the already-consumed revoke bound")

		return NativeResult{Revoked: true}, nil
	}
	process.revokeErr = context.DeadlineExceeded
	client := NewClient(nil, Options{
		Cwd: t.TempDir(),
		StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
			return &revokeDeadlineProcess{coverageNativeProcess: process, deadline: &revokeDeadline}, nil
		},
	})

	_, err := client.outputAtPath(t.Context(), "amp", "version")
	require.ErrorIs(t, err, independent)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
	require.Equal(t, 2, waits)
}

type revokeDeadlineProcess struct {
	*coverageNativeProcess
	deadline *time.Time
}

func (p *revokeDeadlineProcess) Revoke(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	*p.deadline = deadline

	return p.revokeErr
}

type blockingTurnCloseWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int32
	err     error
}

func (*blockingTurnCloseWriter) Write(data []byte) (int, error) { return len(data), nil }

func (w *blockingTurnCloseWriter) Close() error {
	w.calls.Add(1)
	w.once.Do(func() { close(w.entered) })
	<-w.release

	return w.err
}

func TestTurnCloseReturnsOneStoredResultToEveryCaller(t *testing.T) {
	want := errors.New("turn close failed")
	writer := &blockingTurnCloseWriter{
		entered: make(chan struct{}), release: make(chan struct{}), err: want,
	}
	turn := &Turn{stdin: writer}

	const callers = 8
	start := make(chan struct{})
	results := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			results <- turn.Close()
		}()
	}

	close(start)
	<-writer.entered
	close(writer.release)

	var first error
	for range callers {
		err := <-results
		require.ErrorIs(t, err, want)
		if first == nil {
			first = err
		} else {
			require.Same(t, first, err)
		}
	}

	require.Same(t, first, turn.Close())
	require.Equal(t, int32(1), writer.calls.Load())
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
		require.NotErrorIs(t, err, ErrContainmentIncomplete)
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

func TestClientManagedUnusableStdioGetsFreshTerminalSettlementBound(t *testing.T) {
	revokeErr := errors.New("revoke callback failed")
	waitErr := errors.New("wait callback failed")
	base := newCoverageNativeProcess()
	base.stdout = nil
	base.revokeErr = revokeErr
	var revokeDeadline time.Time
	base.wait = func(ctx context.Context) (NativeResult, error) {
		waitDeadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.True(t, waitDeadline.After(revokeDeadline),
			"managed settlement Wait inherited the Revoke deadline")

		return NativeResult{}, waitErr
	}
	process := &revokeDeadlineProcess{coverageNativeProcess: base, deadline: &revokeDeadline}
	client := NewClient(nil, Options{StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
		return process, nil
	}})

	_, err := client.startNative(t.Context(), NativeRequest{})
	require.ErrorContains(t, err, "unusable host stdio")
	require.ErrorIs(t, err, revokeErr)
	require.ErrorIs(t, err, waitErr)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
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
	waitStarted chan struct{}
	terminal    chan struct{}
	result      NativeResult
	err         error
}

type contextBoundCoverageProcess struct {
	*coverageNativeProcess
	waitStarted chan struct{}
	waitExited  chan struct{}
}

func newContextBoundCoverageProcess() *contextBoundCoverageProcess {
	return &contextBoundCoverageProcess{
		coverageNativeProcess: newCoverageNativeProcess(),
		waitStarted:           make(chan struct{}, 2),
		waitExited:            make(chan struct{}, 2),
	}
}

func (p *contextBoundCoverageProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.waitStarted <- struct{}{}
	<-ctx.Done()
	p.waitExited <- struct{}{}

	return NativeResult{}, ctx.Err()
}

func (p *blockingCoverageProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.waitStarted <- struct{}{}

	select {
	case <-ctx.Done():
		return NativeResult{}, ctx.Err()
	case <-p.terminal:
	}

	return p.result, p.err
}

func TestTrackedProcessWaitDetachesThenRejoinsTheHostCache(t *testing.T) {
	base := newCoverageNativeProcess()
	underlying := &blockingCoverageProcess{
		coverageNativeProcess: base,
		waitStarted:           make(chan struct{}, 2),
		terminal:              make(chan struct{}),
		result:                NativeResult{ExitCode: 3},
		err:                   errors.New("done"),
	}
	tracked := trackProcess(underlying)

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstDone := make(chan error, 1)
	go func() {
		_, err := tracked.Wait(firstCtx)
		firstDone <- err
	}()
	<-underlying.waitStarted
	cancelFirst()
	err := <-firstDone
	require.ErrorIs(t, err, context.Canceled)

	type waitResult struct {
		result NativeResult
		err    error
	}
	secondDone := make(chan waitResult, 1)
	go func() {
		result, waitErr := tracked.Wait(t.Context())
		secondDone <- waitResult{result: result, err: waitErr}
	}()
	<-underlying.waitStarted

	select {
	case <-secondDone:
		require.Fail(t, "rejoined wait returned before the host terminal cache settled")
	default:
	}

	close(underlying.terminal)
	second := <-secondDone
	result, err := second.result, second.err
	require.Equal(t, NativeResult{ExitCode: 3}, result)
	require.ErrorContains(t, err, "done")
}

func TestTurnIncompleteCloseCancelsAndJoinsTerminalObservation(t *testing.T) {
	process := newContextBoundCoverageProcess()
	promptCtx, cancelPrompt := context.WithCancel(t.Context())
	turn := &Turn{
		process:      process,
		stdin:        &coverageWriteCloser{},
		stdout:       io.NopCloser(bytes.NewBufferString(`{"type":"result","subtype":"success"}` + "\n")),
		stderr:       io.NopCloser(bytes.NewReader(nil)),
		maxLineBytes: 1024,
		messages:     make(chan Message),
		errs:         make(chan error, 4),
	}
	turn.start(promptCtx)
	cancelPrompt()
	<-process.waitStarted

	select {
	case <-process.waitExited:
		require.Fail(t, "prompt cancellation stopped terminal observation")
	default:
	}

	closeCtx, cancelClose := context.WithCancel(t.Context())
	closeDone := make(chan error, 1)
	go func() { closeDone <- turn.closeWithContext(closeCtx) }()
	<-process.waitStarted
	cancelClose()

	err := <-closeDone
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	<-process.waitExited
	<-process.waitExited

	for range turn.Messages() {
	}
	for range turn.Errors() {
	}
}

func TestTurnTerminalCloseStopsBlockedDeliveryAndJoinsWorkers(t *testing.T) {
	process := newCoverageNativeProcess()
	turn := &Turn{
		process:      process,
		stdin:        &coverageWriteCloser{},
		stdout:       io.NopCloser(bytes.NewBufferString(`{"type":"result","subtype":"success"}` + "\n")),
		stderr:       io.NopCloser(bytes.NewReader(nil)),
		maxLineBytes: 1024,
		messages:     make(chan Message),
		errs:         make(chan error, 4),
	}
	turn.start(t.Context())

	require.NoError(t, turn.closeWithContext(t.Context()))
	for range turn.Messages() {
	}
	for range turn.Errors() {
	}
}

func TestTurnReaderAndErrorResidualBranches(t *testing.T) {
	turn := &Turn{
		stdout:       io.NopCloser(bytes.NewBufferString(`{"type":"result","subtype":"success"}` + "\n")),
		messages:     make(chan Message),
		errs:         make(chan error, 4),
		maxLineBytes: 1024,
		stderrDone:   make(chan struct{}),
	}
	turn.readStdout(cancelledContext(), t.Context())
	require.ErrorIs(t, <-turn.errs, context.Canceled)

	turn = &Turn{
		stdout:       failingReadCloser{},
		messages:     make(chan Message),
		errs:         make(chan error, 4),
		maxLineBytes: 1024,
		stderrDone:   make(chan struct{}),
	}
	turn.readStdout(t.Context(), t.Context())
	require.ErrorContains(t, <-turn.errs, "read amp stdout")

	require.NoError(t, (&Turn{}).wait(t.Context()))
	process := newCoverageNativeProcess()
	process.wait = func(context.Context) (NativeResult, error) { return NativeResult{Signal: 9}, nil }
	require.ErrorContains(t, (&Turn{process: process}).wait(t.Context()), "signal 9")

	turn = &Turn{errs: make(chan error, 1), log: slog.New(slog.DiscardHandler)}
	turn.errs <- errors.New("full")
	turn.sendErr(errors.New("dropped"))

	turn = &Turn{}
	turn.captureStderr("first")
	turn.captureStderr("second")
	require.Equal(t, "first\nsecond", turn.stderrText())
	require.ErrorContains(t, (&Turn{}).exitError(errors.New("failed")), "amp process exited")
}
