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
	"testing"

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
	startErr       error
	reclaimErr     error
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
	a.prepared[root] = true
	a.mu.Unlock()
	if a.lockOnPrepare {
		return os.Chmod(root, 0)
	}

	return nil
}

func (a *recordingAuthority) ReclaimNativeTree(_ context.Context, root string) error {
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
	if a.startErr != nil {
		return nil, a.startErr
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

	return &recordingProcess{authority: a, root: root, cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr, done: make(chan struct{})}, nil
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
	authority      *recordingAuthority
	root           string
	cmd            *exec.Cmd
	stdin          io.WriteCloser
	stdout, stderr io.ReadCloser
	once           sync.Once
	done           chan struct{}
	result         NativeResult
	err            error
}

func (p *recordingProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *recordingProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *recordingProcess) Stderr() io.ReadCloser { return p.stderr }
func (p *recordingProcess) Wait(ctx context.Context) (NativeResult, error) {
	p.once.Do(func() {
		go func() {
			p.err = p.cmd.Wait()
			p.result.ExitCode = p.cmd.ProcessState.ExitCode()
			var exitErr *exec.ExitError
			if errors.As(p.err, &exitErr) {
				p.err = nil
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
	p.result.Revoked = true
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
			filepath.Join(root, "browser-shim"),
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
	require.NotEmpty(t, authority.events)
	for _, event := range authority.events {
		require.False(t, strings.HasPrefix(event, "wait:"), "refused managed launch must not acquire an ordinary process")
	}
}
