package ampacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

func TestNewSessionTimeoutKillsNativeTreeAndReleasesResources(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "block-new")
	scratch := t.TempDir()

	var mu sync.Mutex
	acquired := map[RuntimeResourceKind]int{}
	released := map[RuntimeResourceKind]int{}
	reservedScratch := 0
	releasedScratch := 0

	agent := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(scratch),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				mu.Lock()
				acquired[kind]++
				mu.Unlock()

				return func() {
					mu.Lock()
					released[kind]++
					mu.Unlock()
				}, nil
			},
			ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				require.Equal(t, RuntimeResourceSession, kind)
				mu.Lock()
				reservedScratch++
				mu.Unlock()

				return func() {
					mu.Lock()
					releasedScratch++
					mu.Unlock()
				}, nil
			},
		}),
	)
	agent.options.runtime.nativeSessionTimeout = 50 * time.Millisecond

	started := time.Now()
	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	require.Less(t, time.Since(started), 2*time.Second)

	argsRecords := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	require.False(t, slicesContainCommand(nil, "threads", "new"))
	require.True(t, slicesContainCommand(argsRecords, "threads", "new"), "missing blocked threads new invocation: %#v", argsRecords)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, acquired[RuntimeResourceDiscovery])
	require.Equal(t, 1, released[RuntimeResourceDiscovery])
	require.Equal(t, 1, acquired[RuntimeResourceSession])
	require.Equal(t, 1, released[RuntimeResourceSession])
	require.Equal(t, 1, reservedScratch)
	require.Equal(t, 1, releasedScratch)

	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.Empty(t, entries, "session scratch survived timed-out native creation")
}

func slicesContainCommand(records [][]string, parts ...string) bool {
	for _, record := range records {
		cursor := 0
		for _, arg := range record {
			if cursor < len(parts) && arg == parts[cursor] {
				cursor++
			}
		}
		if cursor == len(parts) {
			return true
		}
	}

	return false
}

func TestRuntimeResourceHooks(t *testing.T) {
	options := Options{}
	WithRuntimeResourceHooks(RuntimeResourceHooks{AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return func() {}, nil
	}})(&options)
	require.NotNil(t, options.RuntimeResourceHooks.AcquireNativeRoot)

	release, err := acquireNativeRoot(t.Context(), RuntimeResourceHooks{}, RuntimeResourceSession)
	require.NoError(t, err)
	release()

	wantErr := errors.New("full")
	_, err = reserveScratchRoot(t.Context(), RuntimeResourceHooks{ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, wantErr
	}}, RuntimeResourceSession)
	require.ErrorIs(t, err, wantErr)

	_, err = acquireNativeRoot(t.Context(), RuntimeResourceHooks{AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, nil //nolint:nilnil // A nil release is the invalid hook result under test.
	}}, RuntimeResourcePrompt)
	require.ErrorContains(t, err, "nil release")

	releases := 0
	release, err = acquireNativeRoot(t.Context(), RuntimeResourceHooks{AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
		require.Equal(t, RuntimeResourceDiscovery, kind)

		return func() { releases++ }, nil
	}}, RuntimeResourceDiscovery)
	require.NoError(t, err)
	release()
	release()
	require.Equal(t, 1, releases)

	retainedReleases := 0
	releaseNativeRootWhenQuiescent(func() { retainedReleases++ }, nativeamp.ErrProcessTreeNotQuiescent)
	require.Zero(t, retainedReleases)
	releaseNativeRootWhenQuiescent(func() { retainedReleases++ }, errors.New("ordinary native failure"))
	require.Equal(t, 1, retainedReleases)
}

func TestFinalizeNativePromptRetainsUnprovenTree(t *testing.T) {
	releases := 0
	response := acp.PromptResponse{StopReason: acp.StopReasonEndTurn}
	wantErr := errors.New("turn failed")

	final, err := finalizeNativePrompt(response, wantErr, nativeamp.ErrProcessTreeNotQuiescent, func() { releases++ })
	require.Equal(t, acp.PromptResponse{}, final)
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, nativeamp.ErrProcessTreeNotQuiescent)
	require.Zero(t, releases)

	final, err = finalizeNativePrompt(response, wantErr, nil, func() { releases++ })
	require.Equal(t, response, final)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, releases)
}

func TestSessionScratchReleaseProofBoundaries(t *testing.T) {
	t.Run("ordinary proof error deletes root and releases scratch", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "settings")
		require.NoError(t, os.Mkdir(root, 0o700))
		runtimeErr := errors.New("ordinary close error")
		scratchReleases := 0

		err := finalizeSessionScratch(runtimeErr, runtimeErr, root, func() { scratchReleases++ })

		require.ErrorIs(t, err, runtimeErr)
		require.Equal(t, 1, scratchReleases)
		require.NoDirExists(t, root)
	})

	t.Run("unproven tree retains root and scratch", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "settings")
		require.NoError(t, os.Mkdir(root, 0o700))
		scratchReleases := 0

		err := finalizeSessionScratch(nil, nativeamp.ErrProcessTreeNotQuiescent, root, func() { scratchReleases++ })

		require.ErrorIs(t, err, nativeamp.ErrProcessTreeNotQuiescent)
		require.Zero(t, scratchReleases)
		require.DirExists(t, root)
	})

	t.Run("deletion failure retains scratch", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "settings")
		require.NoError(t, os.Mkdir(root, 0o700))
		originalRemoveAll := removeSessionDir
		deleteErr := errors.New("delete settings root")
		removeSessionDir = func(path string) error {
			require.Equal(t, root, path)

			return deleteErr
		}
		t.Cleanup(func() { removeSessionDir = originalRemoveAll })
		scratchReleases := 0

		err := finalizeSessionScratch(nil, nil, root, func() { scratchReleases++ })

		require.ErrorIs(t, err, deleteErr)
		require.Zero(t, scratchReleases)
		require.DirExists(t, root)
	})
}

func TestSessionCloseRetainsScratchAfterEarlierUnprovenPrompt(t *testing.T) {
	root := filepath.Join(t.TempDir(), "settings")
	require.NoError(t, os.Mkdir(root, 0o700))
	scratchReleases := 0
	session := &agentSession{
		agent:              NewAgent(),
		settingsDir:        root,
		scratchRootRelease: func() { scratchReleases++ },
	}
	session.recordScratchQuiescence(errors.New("ordinary prompt failure"))
	require.NoError(t, session.scratchQuiescenceError())
	session.recordScratchQuiescence(nativeamp.ErrProcessTreeNotQuiescent)
	require.ErrorIs(t, session.ready(), nativeamp.ErrProcessTreeNotQuiescent)
	require.ErrorIs(t, session.verifyContinuable(t.Context()), nativeamp.ErrProcessTreeNotQuiescent)

	err := session.Close(t.Context())

	require.ErrorIs(t, err, nativeamp.ErrProcessTreeNotQuiescent)
	require.Zero(t, scratchReleases)
	require.DirExists(t, root)
	require.ErrorIs(t, session.Delete(t.Context()), nativeamp.ErrProcessTreeNotQuiescent)
}

func TestPromptTurnCompletionProof(t *testing.T) {
	state := newPromptTurnState()
	closeErr := errors.New("close result")
	state.complete(closeErr)
	state.complete(nil)
	require.ErrorIs(t, state.awaitCompletion(t.Context()), closeErr)

	timedOut := newPromptTurnState()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, timedOut.awaitCompletion(ctx), nativeamp.ErrProcessTreeNotQuiescent)
}

func TestSessionConstructionRetainsScratchWhenUnwindDeletionFails(t *testing.T) {
	originalMkdirAll := mkdirAll
	originalRemoveAll := removeSessionDir
	t.Cleanup(func() {
		mkdirAll = originalMkdirAll
		removeSessionDir = originalRemoveAll
	})
	createErr := errors.New("create isolated home")
	deleteErr := errors.New("delete partial settings root")
	mkdirAll = func(string, os.FileMode) error { return createErr }
	removeSessionDir = func(string) error { return deleteErr }
	scratchReleases := 0
	agent := NewAgent(
		WithScratchDir(t.TempDir()),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() { scratchReleases++ }, nil
			},
		}),
	)

	_, err := newAgentSession(t.Context(), agent, "T-session", t.TempDir(), parsedSessionMeta{}, "", nil)

	require.ErrorIs(t, err, createErr)
	require.ErrorIs(t, err, deleteErr)
	require.Zero(t, scratchReleases)
}

func TestAmpSessionResourceAdmission(t *testing.T) {
	wantErr := errors.New("resource exhausted")
	discoveryBlocked := NewAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	_, err := discoveryBlocked.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.ErrorIs(t, err, wantErr)

	_, err = discoveryBlocked.LoadSession(t.Context(), LoadSessionRequest("T-missing", t.TempDir()))
	require.ErrorIs(t, err, wantErr)

	scratchBlocked := NewAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	_, err = newAgentSession(t.Context(), scratchBlocked, "T-session", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.ErrorIs(t, err, wantErr)

	path, _ := fakeAgentAmpPath(t, "")
	sessionBlocked := NewAgent(
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				if kind == RuntimeResourceSession {
					return nil, wantErr
				}

				return func() {}, nil
			},
		}),
	)
	_, err = sessionBlocked.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.ErrorIs(t, err, wantErr)

	nativeBlocked := NewAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	session := &agentSession{agent: nativeBlocked, id: "T-session"}
	require.ErrorIs(t, session.Delete(t.Context()), wantErr)
	require.ErrorIs(t, session.verifyContinuable(t.Context()), wantErr)

	promptBlocked := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	created, err := promptBlocked.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = promptBlocked.Close() })
	active, err := promptBlocked.session(created.SessionId)
	require.NoError(t, err)
	settingsDir := active.settingsDir
	active.settingsDir = filepath.Join(t.TempDir(), "missing")
	active.mcpConfigJSON = `{}`
	_, err = promptBlocked.Prompt(t.Context(), TextPromptRequest(created.SessionId, "ignored", "hello"))
	require.Error(t, err)
	active.settingsDir = settingsDir
	active.mcpConfigJSON = ""
	promptBlocked.options.RuntimeResourceHooks.AcquireNativeRoot = func(context.Context, RuntimeResourceKind) (func(), error) {
		return nil, wantErr
	}
	_, err = promptBlocked.Prompt(t.Context(), TextPromptRequest(created.SessionId, "ignored", "hello"))
	require.ErrorIs(t, err, wantErr)

	configSession := &agentSession{settingsDir: filepath.Join(t.TempDir(), "missing"), mcpConfigJSON: `{}`}
	_, err = configSession.writePromptMCPConfig()
	require.Error(t, err)
}
