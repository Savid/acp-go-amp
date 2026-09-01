package ampacp

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

type recordingAuthority struct {
	mu             sync.Mutex
	events         []string
	prepared       map[string]bool
	environment    map[string]string
	lockOnPrepare  bool
	inspectPrepare func(string) error
	inspectReclaim func(string) error
	readAppendLog  func(context.Context, string, uint64) ([][]byte, error)
	prepareErr     error
	startErr       error
	startPanic     bool
	waitErr        error
	reclaimErr     error
	reclaimCalls   int
	nilProcess     bool
	unusableStdio  bool
}

func newRecordingAuthority() *recordingAuthority {
	return &recordingAuthority{prepared: map[string]bool{}, environment: nativeamp.CaptureOrdinaryEnvironment()}
}

func (a *recordingAuthority) NativeEnvironment() map[string]string {
	return cloneStringMap(a.environment)
}

func (a *recordingAuthority) PrepareNativeTree(_ context.Context, root string) error {
	if a.inspectPrepare != nil {
		if err := a.inspectPrepare(root); err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.events = append(a.events, "prepare:"+root)
	if a.prepareErr != nil {
		a.mu.Unlock()

		return a.prepareErr
	}
	a.prepared[root] = true
	a.mu.Unlock()
	if a.lockOnPrepare {
		return os.Chmod(root, 0)
	}

	return nil
}

func (a *recordingAuthority) ReadNativeAppendLog(ctx context.Context, path string, offset uint64) ([][]byte, error) {
	if a.readAppendLog == nil {
		return nil, nil
	}

	return a.readAppendLog(ctx, path, offset)
}

func (a *recordingAuthority) ReclaimNativeTree(_ context.Context, root string) error {
	a.mu.Lock()
	a.reclaimCalls++
	a.mu.Unlock()
	if a.reclaimErr != nil {
		return a.reclaimErr
	}
	if a.inspectReclaim != nil {
		if err := a.inspectReclaim(root); err != nil {
			return err
		}
	}
	if a.lockOnPrepare {
		if err := os.Chmod(root, 0o700); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.prepared[root] {
		return ErrHostAuthorityUnavailable
	}
	a.events = append(a.events, "reclaim:"+root)
	delete(a.prepared, root)

	return nil
}

func (a *recordingAuthority) StartNative(_ context.Context, request NativeRequest) (NativeProcess, error) {
	root := environmentRoot(request.Environment)
	a.mu.Lock()
	if root == "" || !a.prepared[root] {
		a.mu.Unlock()

		return nil, ErrHostAuthorityUnavailable
	}
	a.events = append(a.events, "start:"+root)
	a.mu.Unlock()
	if a.startPanic {
		panic("contract-violating StartNative panic")
	}
	if a.startErr != nil {
		return nil, a.startErr
	}
	if a.nilProcess {
		return nil, nil //nolint:nilnil // Deliberately model a contract-violating authority.
	}

	cmd := exec.Command(request.Executable, request.Arguments...)
	cmd.Dir = request.WorkingDirectory
	cmd.Env = append([]string(nil), request.Environment...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return &recordingProcess{
		authority: a, root: root, cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr,
		done: make(chan struct{}), authorityWaitErr: a.waitErr, unusableStdio: a.unusableStdio,
	}, nil
}

func environmentRoot(environment []string) string {
	for _, entry := range environment {
		if strings.HasPrefix(entry, "HOME=") {
			return filepath.Dir(strings.TrimPrefix(entry, "HOME="))
		}
	}

	return ""
}

type recordingProcess struct {
	authority        *recordingAuthority
	root             string
	cmd              *exec.Cmd
	stdin            io.WriteCloser
	stdout, stderr   io.ReadCloser
	once             sync.Once
	done             chan struct{}
	result           NativeResult
	err              error
	authorityWaitErr error
	unusableStdio    bool
	revoked          atomic.Bool
}

func (p *recordingProcess) Stdin() io.WriteCloser {
	if p.unusableStdio {
		return nil
	}

	return p.stdin
}
func (p *recordingProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *recordingProcess) Stderr() io.ReadCloser { return p.stderr }
func (p *recordingProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.once.Do(func() {
		go func() {
			p.err = p.cmd.Wait()
			p.result.ExitCode = p.cmd.ProcessState.ExitCode()
			p.result.Revoked = p.revoked.Load()
			var exitErr *exec.ExitError
			if errors.As(p.err, &exitErr) {
				p.err = nil
			}
			if p.authorityWaitErr != nil {
				p.err = p.authorityWaitErr
			}
			p.authority.mu.Lock()
			p.authority.events = append(p.authority.events, "wait:"+p.root)
			p.authority.mu.Unlock()
			close(p.done)
		}()
	})
	select {
	case <-ctx.Done():
		return NativeResult{}, ctx.Err()
	case <-p.done:
		return p.result, p.err
	}
}
func (p *recordingProcess) Revoke(context.Context) error {
	p.revoked.Store(true)
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}

	return nil
}

func TestSuppliedNilHostAuthorityFailsBeforeMutationOrLaunch(t *testing.T) {
	for _, authority := range []HostAuthority{nil, (*recordingAuthority)(nil)} {
		agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
		_, err := agent.Initialize(t.Context(), acp.InitializeRequest{})
		require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
		require.Empty(t, agent.sessions)
	}
}

func TestHostAuthorityEnvironmentIsValidatedBeforeMutation(t *testing.T) {
	authority := newRecordingAuthority()
	authority.environment = map[string]string{"BAD=KEY": "value"}
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{})
	require.Error(t, err)
	require.Empty(t, authority.events)
}

func TestHostAuthorityPreparedTreeExclusivity(t *testing.T) {
	authority := newRecordingAuthority()
	authority.lockOnPrepare = true
	authority.inspectPrepare = func(root string) error {
		for _, path := range []string{
			filepath.Join(root, "xdg-config", "amp", "settings.json"),
			filepath.Join(root, "mcp.json"),
			filepath.Join(root, "home", "seed.txt"),
		} {
			if _, err := os.Stat(path); err != nil {
				return err
			}
		}

		return nil
	}
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()), WithSeedFiles(map[string]string{"seed.txt": "seed"}))
	session, err := newAgentSession(t.Context(), agent, "session", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	root := session.settingsDir
	require.NoError(t, session.Close(t.Context()))
	_, err = os.Stat(root)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, []string{"prepare:" + root, "reclaim:" + root}, authority.events)
}

func TestHostAuthorityReclaimPrecedesRemoval(t *testing.T) {
	authority := newRecordingAuthority()
	reclaimedWhilePresent := false
	authority.inspectReclaim = func(root string) error {
		_, err := os.Stat(root)
		reclaimedWhilePresent = err == nil

		return err
	}
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))
	session, err := newAgentSession(t.Context(), agent, "session", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	root := session.settingsDir
	require.NoError(t, session.Close(t.Context()))
	require.True(t, reclaimedWhilePresent)
	_, err = os.Stat(root)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestHostAuthorityManagedLaunchTrace(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	authority := newRecordingAuthority()
	agent := NewAgent(WithHostAuthority(authority), WithExecutablePath(path), WithScratchDir(t.TempDir()))
	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.StartupProbe(ctx)
	})
	require.NoError(t, err)

	authority.mu.Lock()
	events := append([]string(nil), authority.events...)
	authority.mu.Unlock()
	require.NotEmpty(t, events)
	for index := 0; index < len(events); index += 4 {
		require.LessOrEqual(t, index+4, len(events))
		root := strings.TrimPrefix(events[index], "prepare:")
		require.Equal(t, []string{"prepare:" + root, "start:" + root, "wait:" + root, "reclaim:" + root}, events[index:index+4])
	}
}

func TestHostAuthorityReadNativeAppendLogDelegatesAndGuardsPanic(t *testing.T) {
	authority := newRecordingAuthority()
	callCtx := t.Context()
	want := [][]byte{[]byte("first"), []byte("second")}
	authority.readAppendLog = func(ctx context.Context, path string, offset uint64) ([][]byte, error) {
		require.Same(t, callCtx, ctx)
		require.Equal(t, "/native/append.log", path)
		require.Equal(t, uint64(17), offset)

		return want, nil
	}

	agent := NewAgent(WithHostAuthority(authority))
	var options nativeamp.Options
	agent.configureNativeClient(&options)
	records, err := options.ReadNativeAppendLog(callCtx, "/native/append.log", 17)
	require.NoError(t, err)
	require.Equal(t, want, records)

	wantErr := errors.New("read refused")
	authority.readAppendLog = func(context.Context, string, uint64) ([][]byte, error) { return nil, wantErr }
	records, err = options.ReadNativeAppendLog(callCtx, "/native/append.log", 17)
	require.Nil(t, records)
	require.ErrorIs(t, err, wantErr)

	authority.readAppendLog = func(context.Context, string, uint64) ([][]byte, error) {
		panic("read append log panic")
	}
	records, err = options.ReadNativeAppendLog(callCtx, "/native/append.log", 17)
	require.Nil(t, records)
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
}

func TestManagedStartupProbeCacheIsAuthorityScoped(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	for range 2 {
		authority := newRecordingAuthority()
		agent := NewAgent(WithHostAuthority(authority), WithExecutablePath(path), WithScratchDir(t.TempDir()))
		err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
			return client.StartupProbe(ctx)
		})
		require.NoError(t, err)
		require.Len(t, authority.events, 20, "each authority runs version plus every startup method probe")
	}
}

func TestHostAuthorityNoOrdinaryFallback(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	want := errors.New("managed launch refused")
	authority := newRecordingAuthority()
	authority.startErr = want
	agent := NewAgent(WithHostAuthority(authority), WithExecutablePath(path), WithScratchDir(t.TempDir()))
	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.StartupProbe(ctx)
	})
	require.ErrorIs(t, err, want)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
	require.NotEmpty(t, authority.events)
	for _, event := range authority.events {
		require.False(t, strings.HasPrefix(event, "wait:"), "refused managed launch must not acquire an ordinary process")
	}
	root := strings.TrimPrefix(authority.events[0], "prepare:")
	_, statErr := os.Stat(root)
	require.ErrorIs(t, statErr, os.ErrNotExist, "a refused start leaves no child and authorizes reclaim")
	require.Equal(t, []string{"prepare:" + root, "start:" + root, "reclaim:" + root}, authority.events)
	require.Equal(t, 1, authority.reclaimCalls)

	agent.mu.Lock()
	latched := agent.lifecycleContainmentErr
	agent.mu.Unlock()
	require.NoError(t, latched, "an ordinary start refusal must not poison later managed admission")
}

func TestHostAuthorityStartPanicRetainsPreparedResidence(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	authority := newRecordingAuthority()
	authority.startPanic = true
	agent := NewAgent(WithHostAuthority(authority), WithExecutablePath(path), WithScratchDir(t.TempDir()))

	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.Len(t, authority.events, 2)
	root := strings.TrimPrefix(authority.events[0], "prepare:")
	require.Equal(t, "start:"+root, authority.events[1])
	_, statErr := os.Stat(root)
	require.NoError(t, statErr, "a panicked start supplies no no-child proof")
	require.Zero(t, authority.reclaimCalls)
}

func TestHostAuthorityFirstLossFansOutAcrossLiveSessions(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	authority := newRecordingAuthority()
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)

	first, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	second, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	agent.mu.Lock()
	firstSession := agent.sessions[first.SessionId]
	secondSession := agent.sessions[second.SessionId]
	agent.mu.Unlock()
	require.NotNil(t, firstSession)
	require.NotNil(t, secondSession)

	authority.startErr = ErrHostAuthorityUnavailable
	var nativeOptions nativeamp.Options
	agent.configureNativeClient(&nativeOptions)
	_, err = nativeOptions.StartNative(t.Context(), nativeamp.NativeRequest{
		Executable:       path,
		Environment:      nativeamp.BuildEnv(firstSession.env, firstSession.cwd),
		WorkingDirectory: firstSession.cwd,
	})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)

	require.Eventually(t, func() bool {
		firstSession.mu.Lock()
		firstClosed := firstSession.closed
		firstSession.mu.Unlock()
		secondSession.mu.Lock()
		secondClosed := secondSession.closed
		secondSession.mu.Unlock()
		agent.mu.Lock()
		_, firstSettling := agent.sessionFlights[first.SessionId]
		_, secondSettling := agent.sessionFlights[second.SessionId]
		agent.mu.Unlock()

		return firstClosed && secondClosed && !firstSettling && !secondSettling
	}, 5*time.Second, 10*time.Millisecond)

	_, err = agent.Prompt(t.Context(), TextPromptRequest(second.SessionId, "turn-after-loss", "must not launch"))
	require.ErrorIs(t, err, errSessionClosed)

	authority.mu.Lock()
	startsBeforeRefusal := len(authority.events)
	authority.mu.Unlock()
	_, err = nativeOptions.StartNative(t.Context(), nativeamp.NativeRequest{
		Executable:       path,
		Environment:      nativeamp.BuildEnv(secondSession.env, secondSession.cwd),
		WorkingDirectory: secondSession.cwd,
	})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	authority.mu.Lock()
	require.Len(t, authority.events, startsBeforeRefusal, "latched authority loss must block new native admission")
	authority.mu.Unlock()
}

func TestHostAuthorityNilProcessRetainsPreparedTree(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	authority := newRecordingAuthority()
	authority.nilProcess = true
	agent := NewAgent(WithHostAuthority(authority), WithExecutablePath(path), WithScratchDir(t.TempDir()))

	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.Equal(t, 2, len(authority.events))
	root := strings.TrimPrefix(authority.events[0], "prepare:")
	_, statErr := os.Stat(root)
	require.NoError(t, statErr)
	require.Zero(t, authority.reclaimCalls)
}

func TestHostAuthorityUnusableStdioFailedSettlementRetainsTree(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	authority := newRecordingAuthority()
	authority.unusableStdio = true
	authority.waitErr = errors.New("stdio child settlement uncertain")
	agent := NewAgent(WithHostAuthority(authority), WithExecutablePath(path), WithScratchDir(t.TempDir()))

	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	})
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.GreaterOrEqual(t, len(authority.events), 3)
	root := strings.TrimPrefix(authority.events[0], "prepare:")
	require.Equal(t, []string{"prepare:" + root, "start:" + root, "wait:" + root}, authority.events[:3])
	_, statErr := os.Stat(root)
	require.NoError(t, statErr)
	require.Zero(t, authority.reclaimCalls)
}

func TestHostAuthorityFailedPrepareRetainsOpaqueTree(t *testing.T) {
	want := errors.New("prepare outcome uncertain")
	authority := newRecordingAuthority()
	authority.prepareErr = want
	agent := NewAgent(WithHostAuthority(authority), WithScratchDir(t.TempDir()))

	_, err := newAgentSession(t.Context(), agent, "session", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.ErrorIs(t, err, want)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.Len(t, authority.events, 1)
	root := strings.TrimPrefix(authority.events[0], "prepare:")
	_, statErr := os.Stat(root)
	require.NoError(t, statErr, "a failed Prepare result leaves the attempted tree opaque and retained")
	require.Zero(t, authority.reclaimCalls)
}

func TestHostAuthorityFailedPrepareQuarantinesAtCapacity(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	authority := newRecordingAuthority()
	authority.prepareErr = errors.New("prepare outcome uncertain")
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}),
	)
	probe := func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	}

	require.ErrorIs(t, agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, probe), ErrContainmentIncomplete)
	require.ErrorContains(t, agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, probe), "backpressure")
	require.Len(t, authority.events, 1, "quarantine prevents unbounded new prepared roots")
}

func TestHostAuthorityBusyReclaimIsRetryable(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	authority := newRecordingAuthority()
	agent := NewAgent(
		WithHostAuthority(authority),
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	session, err := newAgentSession(t.Context(), agent, "session", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	root := session.settingsDir

	authority.reclaimErr = ErrNativeTreeBusy
	err = session.Close(t.Context())
	require.ErrorIs(t, err, ErrNativeTreeBusy)
	require.NotErrorIs(t, err, ErrContainmentIncomplete)
	_, statErr := os.Stat(root)
	require.NoError(t, statErr)
	agent.mu.Lock()
	recorded := agent.lifecycleContainmentErr
	agent.mu.Unlock()
	require.NoError(t, recorded)
	eventsBeforeAdmission := len(authority.events)
	_, err = agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.ErrorIs(t, err, ErrNativeTreeBusy)
	require.Len(t, authority.events, eventsBeforeAdmission, "busy cleanup blocks new managed runtime admission")

	authority.reclaimErr = nil
	require.NoError(t, session.Close(t.Context()))
	_, statErr = os.Stat(root)
	require.ErrorIs(t, statErr, os.ErrNotExist)
	require.Equal(t, 3, authority.reclaimCalls)
}

func TestHostAuthorityWaitFailureRetainsPreparedTree(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	authority := newRecordingAuthority()
	authority.waitErr = errors.New("authority wait lost containment")
	agent := NewAgent(WithHostAuthority(authority), WithExecutablePath(path), WithScratchDir(t.TempDir()))

	err := agent.runStartupWithProbe(t.Context(), t.TempDir(), nil, func(ctx context.Context, client *nativeamp.Client) (string, error) {
		return client.DiscoveryProbe(ctx)
	})
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.GreaterOrEqual(t, len(authority.events), 3)
	root := strings.TrimPrefix(authority.events[0], "prepare:")
	require.Equal(t, []string{"prepare:" + root, "start:" + root, "wait:" + root}, authority.events[:3])
	require.Zero(t, authority.reclaimCalls)
	_, statErr := os.Stat(root)
	require.NoError(t, statErr)
}

type contextRevokeProcess struct{ err error }

func (*contextRevokeProcess) Stdin() io.WriteCloser { return nil }
func (*contextRevokeProcess) Stdout() io.ReadCloser { return nil }
func (*contextRevokeProcess) Stderr() io.ReadCloser { return nil }
func (*contextRevokeProcess) Wait(context.Context) (NativeResult, error) {
	return NativeResult{Revoked: true}, nil
}
func (p *contextRevokeProcess) Revoke(context.Context) error { return p.err }

func TestHostAuthorityDetachedRevokeContextDoesNotLatch(t *testing.T) {
	agent := NewAgent(WithHostAuthority(newRecordingAuthority()))
	process := nativeProcessBridge{agent: agent, process: &contextRevokeProcess{err: context.Canceled}}

	require.ErrorIs(t, process.Revoke(t.Context()), context.Canceled)
	result, err := process.Wait(context.Background())
	require.NoError(t, err)
	require.True(t, result.Revoked)
	agent.mu.Lock()
	latched := agent.lifecycleContainmentErr
	agent.mu.Unlock()
	require.NoError(t, latched)
}

func TestHostAuthorityRevokeLossLatchesAdmission(t *testing.T) {
	agent := NewAgent(WithHostAuthority(newRecordingAuthority()))
	process := nativeProcessBridge{agent: agent, process: &contextRevokeProcess{err: ErrHostAuthorityUnavailable}}

	err := process.Revoke(t.Context())
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	agent.mu.Lock()
	latched := agent.lifecycleContainmentErr
	agent.mu.Unlock()
	require.ErrorIs(t, latched, ErrHostAuthorityUnavailable)
	require.ErrorIs(t, latched, ErrContainmentIncomplete)
}

func TestHostAuthorityMixedContextFailureRemainsContainmentUncertainty(t *testing.T) {
	want := errors.New("authority also lost process state")
	err := authorityBoundaryError(errors.Join(context.Canceled, want))
	require.ErrorIs(t, err, want)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
}
