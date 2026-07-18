package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

func newBlockedPromptLifecycle(t *testing.T) (*Agent, acp.SessionId, <-chan struct{}, <-chan struct{}, chan<- struct{}) {
	t.Helper()

	agent := newTestAgent()
	sessionID := acp.SessionId("T-prompt-construction")
	agent.sessions[sessionID] = &agentSession{
		agent: agent,
		id:    sessionID,
		cwd:   t.TempDir(),
		turn:  make(chan struct{}, 1),
	}
	started := make(chan struct{})
	cancelled := make(chan struct{})
	unblock := make(chan struct{})
	agent.options.runtime.continueThread = func(ctx context.Context, _ *nativeamp.Client, _ string, _ any) (*nativeamp.Turn, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-unblock

		return nil, nativeamp.ErrProcessContainmentIncomplete
	}

	return agent, sessionID, started, cancelled, unblock
}

func TestAgentCloseFencesInFlightPromptConstructionContainmentFailure(t *testing.T) {
	agent, sessionID, started, cancelled, unblock := newBlockedPromptLifecycle(t)
	promptErr := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(context.Background(), TextPromptRequest(sessionID, "turn", "hello"))
		promptErr <- err
	}()
	<-started

	closeErr := make(chan error, 1)
	go func() { closeErr <- agent.Close() }()
	<-cancelled
	select {
	case err := <-closeErr:
		t.Fatalf("Close settled before fenced prompt construction unwound: %v", err)
	default:
	}
	close(unblock)

	require.Error(t, <-promptErr)
	require.ErrorIs(t, <-closeErr, ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)
}

func TestServeFencesInFlightPromptConstructionContainmentFailure(t *testing.T) {
	agent, sessionID, promptStarted, promptCancelled, unblock := newBlockedPromptLifecycle(t)
	serveStarted := make(chan struct{})
	previous := newAgentForServe
	newAgentForServe = func(...Option) *Agent {
		close(serveStarted)

		return agent
	}
	t.Cleanup(func() { newAgentForServe = previous })

	ctx, cancel := context.WithCancel(context.Background())
	input, inputWriter := io.Pipe()
	t.Cleanup(func() {
		cancel()
		_ = input.Close()
		_ = inputWriter.Close()
	})
	serveErr := make(chan error, 1)
	go func() { serveErr <- serveTest(ctx, input, io.Discard) }()
	<-serveStarted

	promptErr := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(context.Background(), TextPromptRequest(sessionID, "turn", "hello"))
		promptErr <- err
	}()
	<-promptStarted
	cancel()
	<-promptCancelled
	select {
	case err := <-serveErr:
		t.Fatalf("Serve settled before fenced prompt construction unwound: %v", err)
	default:
	}
	close(unblock)

	require.Error(t, <-promptErr)
	require.ErrorIs(t, <-serveErr, ErrProcessContainmentIncomplete)
}

func TestAgentCloseFencesInFlightStartupContainmentFailure(t *testing.T) {
	t.Setenv("AMP_API_KEY", "fake")
	started := make(chan struct{})
	cancelled := make(chan struct{})
	unblock := make(chan struct{})

	var mu sync.Mutex
	releases := map[string]int{}
	agent := newTestAgent(
		WithScratchDir(t.TempDir()),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() {
					mu.Lock()
					releases["native"]++
					mu.Unlock()
				}, nil
			},
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() {
					mu.Lock()
					releases["scratch"]++
					mu.Unlock()
				}, nil
			},
		}),
	)
	agent.options.runtime.startupProbe = func(ctx context.Context, _ *nativeamp.Client) error {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-unblock

		return ErrProcessContainmentIncomplete
	}

	requestErr := make(chan error, 1)
	go func() {
		_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
		requestErr <- err
	}()
	<-started

	closeErr := make(chan error, 1)
	go func() { closeErr <- agent.Close() }()
	<-cancelled
	select {
	case err := <-closeErr:
		t.Fatalf("Close settled before fenced startup returned: %v", err)
	default:
	}
	close(unblock)

	require.ErrorIs(t, <-requestErr, ErrProcessContainmentIncomplete)
	require.ErrorIs(t, <-closeErr, ErrProcessContainmentIncomplete)
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)
	mu.Lock()
	defer mu.Unlock()
	require.Zero(t, releases["native"])
	require.Zero(t, releases["scratch"])
}

func TestInjectedNewThreadContainmentFailureRetainsOnlyOwnedSessionScratch(t *testing.T) {
	t.Setenv("AMP_API_KEY", "fake")
	scratch := t.TempDir()

	var mu sync.Mutex
	acquired := map[RuntimeResourceKind]int{}
	releasedNative := map[RuntimeResourceKind]int{}
	reserved := map[RuntimeResourceKind]int{}
	releasedScratch := map[RuntimeResourceKind]int{}
	agent := newTestAgent(
		WithScratchDir(scratch),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				mu.Lock()
				acquired[kind]++
				mu.Unlock()

				return func() {
					mu.Lock()
					releasedNative[kind]++
					mu.Unlock()
				}, nil
			},
			ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				mu.Lock()
				reserved[kind]++
				mu.Unlock()

				return func() {
					mu.Lock()
					releasedScratch[kind]++
					mu.Unlock()
				}, nil
			},
		}),
	)
	agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) error { return nil }
	agent.options.runtime.newThread = func(context.Context, *nativeamp.Client) (string, error) {
		return "", ErrProcessContainmentIncomplete
	}

	_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)

	mu.Lock()
	require.Zero(t, acquired[RuntimeResourceDiscovery])
	require.Zero(t, releasedNative[RuntimeResourceDiscovery])
	require.Zero(t, reserved[RuntimeResourceDiscovery])
	require.Zero(t, releasedScratch[RuntimeResourceDiscovery])
	require.Zero(t, acquired[RuntimeResourceSession])
	require.Zero(t, releasedNative[RuntimeResourceSession])
	require.Equal(t, 1, reserved[RuntimeResourceSession])
	require.Zero(t, releasedScratch[RuntimeResourceSession])
	mu.Unlock()

	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.NotEmpty(t, entries)
}

func TestExportContainmentFailureRetainsColdSessionResources(t *testing.T) {
	t.Setenv("AMP_API_KEY", "fake")
	scratch := t.TempDir()
	cwd := t.TempDir()
	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-export-boundary", cwd, nil)

	var mu sync.Mutex
	releasedNative := map[RuntimeResourceKind]int{}
	releasedScratch := map[RuntimeResourceKind]int{}
	agent := newTestAgent(
		WithScratchDir(scratch),
		WithSessionStore(store),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				return func() {
					mu.Lock()
					releasedNative[kind]++
					mu.Unlock()
				}, nil
			},
			ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				return func() {
					mu.Lock()
					releasedScratch[kind]++
					mu.Unlock()
				}, nil
			},
		}),
	)
	agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) error { return nil }
	agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
		return nil, ErrProcessContainmentIncomplete
	}

	_, err := agent.LoadSession(t.Context(), LoadSessionRequest("T-export-boundary", cwd))
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.ErrorIs(t, agent.Close(), ErrProcessContainmentIncomplete)

	mu.Lock()
	require.Zero(t, releasedNative[RuntimeResourceDiscovery])
	require.Zero(t, releasedScratch[RuntimeResourceDiscovery])
	require.Zero(t, releasedNative[RuntimeResourceSession])
	require.Zero(t, releasedScratch[RuntimeResourceSession])
	mu.Unlock()

	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	require.NotEmpty(t, entries)
}

func TestNewSessionTimeoutKillsNativeTreeAndReleasesResources(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "block-new")
	scratch := t.TempDir()

	var mu sync.Mutex
	acquired := map[RuntimeResourceKind]int{}
	released := map[RuntimeResourceKind]int{}
	reservedScratch := map[RuntimeResourceKind]int{}
	releasedScratch := map[RuntimeResourceKind]int{}

	agent := newTestAgent(
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
				mu.Lock()
				reservedScratch[kind]++
				mu.Unlock()

				return func() {
					mu.Lock()
					releasedScratch[kind]++
					mu.Unlock()
				}, nil
			},
		}),
	)
	agent.options.runtime.nativeSessionTimeout = 50 * time.Millisecond

	started := time.Now()
	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	// Darwin best-effort cleanup has a fixed five-second group-poll budget.
	// Keep this assertion above that contract boundary so loaded race runs do
	// not mistake scheduler delay for an unbounded cleanup.
	require.Less(t, time.Since(started), 10*time.Second)

	argsRecords := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	require.False(t, slicesContainCommand(nil, "threads", "new"))
	require.True(t, slicesContainCommand(argsRecords, "threads", "new"), "missing blocked threads new invocation: %#v", argsRecords)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, acquired[RuntimeResourceDiscovery], 1)
	require.Equal(t, acquired[RuntimeResourceDiscovery], released[RuntimeResourceDiscovery])
	require.Equal(t, 1, acquired[RuntimeResourceSession])
	require.Equal(t, 1, released[RuntimeResourceSession])
	require.GreaterOrEqual(t, reservedScratch[RuntimeResourceDiscovery], 1)
	require.Equal(t, reservedScratch[RuntimeResourceDiscovery], releasedScratch[RuntimeResourceDiscovery])
	require.GreaterOrEqual(t, reservedScratch[RuntimeResourceSession], 1)
	require.Equal(t, reservedScratch[RuntimeResourceSession], releasedScratch[RuntimeResourceSession])

	entries, readErr := os.ReadDir(scratch)
	require.NoError(t, readErr)
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), "acp-go-amp-session-"), "session scratch survived timed-out native creation")
		require.False(t, strings.HasPrefix(entry.Name(), "acp-go-amp-command-"), "command scratch survived timed-out native creation")
	}
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
}

func TestFinalizeNativePromptRetainsIncompleteBoundary(t *testing.T) {
	response := acp.PromptResponse{StopReason: acp.StopReasonEndTurn}
	wantErr := errors.New("turn failed")

	final, err := finalizeNativePrompt(response, wantErr, nativeamp.ErrProcessContainmentIncomplete)
	require.Equal(t, acp.PromptResponse{}, final)
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, nativeamp.ErrProcessContainmentIncomplete)

	final, err = finalizeNativePrompt(response, wantErr, nil)
	require.Equal(t, response, final)
	require.ErrorIs(t, err, wantErr)
}

func TestSessionScratchReleaseContainmentBoundaries(t *testing.T) {
	t.Run("ordinary error deletes root and releases scratch", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "settings")
		require.NoError(t, os.Mkdir(root, 0o700))
		runtimeErr := errors.New("ordinary close error")
		scratchReleases := 0

		err := finalizeSessionScratch(runtimeErr, runtimeErr, root, func() { scratchReleases++ })

		require.ErrorIs(t, err, runtimeErr)
		require.Equal(t, 1, scratchReleases)
		require.NoDirExists(t, root)
	})

	t.Run("incomplete tree retains root and scratch", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "settings")
		require.NoError(t, os.Mkdir(root, 0o700))
		scratchReleases := 0

		err := finalizeSessionScratch(nil, nativeamp.ErrProcessContainmentIncomplete, root, func() { scratchReleases++ })

		require.ErrorIs(t, err, nativeamp.ErrProcessContainmentIncomplete)
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

func TestSessionCloseRetainsScratchAfterEarlierIncompletePrompt(t *testing.T) {
	root := filepath.Join(t.TempDir(), "settings")
	require.NoError(t, os.Mkdir(root, 0o700))
	scratchReleases := 0
	session := &agentSession{
		agent:              newTestAgent(),
		settingsDir:        root,
		scratchRootRelease: func() { scratchReleases++ },
	}
	session.recordScratchContainment(errors.New("ordinary prompt failure"))
	require.NoError(t, session.scratchContainmentError())
	session.recordScratchContainment(nativeamp.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, session.ready(), nativeamp.ErrProcessContainmentIncomplete)
	require.ErrorIs(t, session.verifyContinuable(t.Context()), nativeamp.ErrProcessContainmentIncomplete)

	err := session.Close(t.Context())

	require.ErrorIs(t, err, nativeamp.ErrProcessContainmentIncomplete)
	require.Zero(t, scratchReleases)
	require.DirExists(t, root)
	require.ErrorIs(t, session.Delete(t.Context()), nativeamp.ErrProcessContainmentIncomplete)
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
	require.ErrorIs(t, timedOut.awaitCompletion(ctx), nativeamp.ErrProcessContainmentIncomplete)
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
	agent := newTestAgent(
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
	discoveryBlocked := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	_, err := discoveryBlocked.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.Contains(t, err.Error(), wantErr.Error())

	_, err = discoveryBlocked.LoadSession(t.Context(), LoadSessionRequest("T-missing", t.TempDir()))
	require.Contains(t, err.Error(), wantErr.Error())

	discoveryNativeReleases := 0
	discoveryScratchBlocked := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return func() { discoveryNativeReleases++ }, nil
		},
		ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
			if kind == RuntimeResourceDiscovery {
				return nil, wantErr
			}

			return func() {}, nil
		},
	}))
	_, err = discoveryScratchBlocked.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.Contains(t, err.Error(), wantErr.Error())
	_, err = discoveryScratchBlocked.LoadSession(t.Context(), LoadSessionRequest("T-missing", t.TempDir()))
	require.Contains(t, err.Error(), wantErr.Error())
	require.Zero(t, discoveryNativeReleases)

	scratchBlocked := newTestAgent(WithScratchDir(t.TempDir()), WithRuntimeResourceHooks(RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	_, err = newAgentSession(t.Context(), scratchBlocked, "T-session", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.Contains(t, err.Error(), wantErr.Error())

	path, _ := fakeAgentAmpPath(t, "")
	sessionBlocked := newTestAgent(
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
	require.Contains(t, err.Error(), wantErr.Error())

	nativeBlocked := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) { return nil, wantErr },
	}))
	session := &agentSession{agent: nativeBlocked, id: "T-session"}
	require.Contains(t, session.Delete(t.Context()).Error(), wantErr.Error())
	require.Contains(t, session.verifyContinuable(t.Context()).Error(), wantErr.Error())

	promptBlocked := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
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
	require.Contains(t, err.Error(), wantErr.Error())

	configSession := &agentSession{settingsDir: filepath.Join(t.TempDir(), "missing"), mcpConfigJSON: `{}`}
	_, err = configSession.writePromptMCPConfig()
	require.Error(t, err)
}

func TestClosedAgentRejectsEveryFencedLifecycleMethod(t *testing.T) {
	agent := newTestAgent()
	require.NoError(t, agent.Close())

	_, err := agent.Prompt(t.Context(), TextPromptRequest("T-closed", "ignored", "hello"))
	require.Error(t, err)
	_, err = agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	_, err = agent.LoadSession(t.Context(), LoadSessionRequest("T-closed", t.TempDir()))
	require.Error(t, err)
	_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest("T-closed", t.TempDir()))
	require.Error(t, err)
	_, err = agent.ListSessions(t.Context(), ListSessionsRequest())
	require.Error(t, err)
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: "T-closed"})
	require.Error(t, err)
	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest("T-closed"))
	require.Error(t, err)
}

func TestActiveLoadStoreFailureAndDeleteCompletionFence(t *testing.T) {
	t.Setenv("AMP_API_KEY", "fake")
	loadErr := errors.New("active transcript load failed")
	agent := newTestAgent(WithSessionStore(&errorStore{loadErr: loadErr}))
	agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) error { return nil }
	agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}
	cwd := t.TempDir()
	session, err := newAgentSession(t.Context(), agent, "T-active-load", cwd, parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	agent.sessions[session.id] = session

	loaded, transcript, started, err := agent.loadOrResume(t.Context(), session.id, cwd, nil, nil, nil)
	require.ErrorIs(t, err, loadErr)
	require.Nil(t, loaded)
	require.Nil(t, transcript)
	require.False(t, started)
	require.NoError(t, agent.removeSession(t.Context(), "T-not-installed", session))
	require.NoError(t, agent.removeSession(t.Context(), session.id, session))

	acquireErr := errors.New("delete native admission failed")
	deleteAgent := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
		AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			return nil, acquireErr
		},
	}))
	completed := newPromptTurnState()
	completed.complete(nil)
	deleteSession := &agentSession{agent: deleteAgent, id: "T-delete-completed", activePrompt: completed}
	require.ErrorIs(t, deleteSession.Delete(t.Context()), acquireErr)
	require.NoError(t, (&agentSession{}).interrupt(t.Context()))
	require.False(t, isNativeMissingError(errors.Join(errors.New("Thread not found"), ErrProcessContainmentIncomplete)))
}

func TestPendingDeleteContainmentFailureStopsEveryLifecycleRetry(t *testing.T) {
	newPendingAgent := func(id acp.SessionId) *Agent {
		agent := newTestAgent(WithRuntimeResourceHooks(RuntimeResourceHooks{
			AcquireNativeRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return nil, ErrProcessContainmentIncomplete
			},
		}))
		agent.markPendingNativeDelete(id)

		return agent
	}

	listAgent := newPendingAgent("T-pending-list")
	_, err := listAgent.ListSessions(t.Context(), ListSessionsRequest())
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)

	loadAgent := newPendingAgent("T-pending-load")
	_, err = loadAgent.LoadSession(t.Context(), LoadSessionRequest("T-target-load", t.TempDir()))
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)

	deleteAgent := newPendingAgent("T-pending-other")
	_, err = deleteAgent.UnstableDeleteSession(t.Context(), DeleteSessionRequest("T-target-delete"))
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
}
