package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type reentrantDeleteStore struct {
	SessionStore
	agent     *Agent
	sessionID acp.SessionId
	closeErr  error
	cancelErr error
}

func (s *reentrantDeleteStore) Delete(ctx context.Context, key SessionKey) error {
	_, s.closeErr = s.agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: s.sessionID})
	s.cancelErr = s.agent.Cancel(ctx, acp.CancelNotification{SessionId: s.sessionID})

	return s.SessionStore.Delete(ctx, key)
}

func TestStoreDeleteCanReenterCloseAndCancel(t *testing.T) {
	store := &reentrantDeleteStore{SessionStore: NewInMemorySessionStore()}
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	store.agent = agent

	session, err := newAgentSession(t.Context(), agent, "T-reentrant-delete", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	store.sessionID = session.id
	activateOwnershipTestSession(t, agent, session)

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.NoError(t, err)
	requireClosedCallbackRefusal(t, store.closeErr)
	requireClosedCallbackRefusal(t, store.cancelErr)
	require.True(t, session.deleteComplete())
	require.NoError(t, agent.Close())
}

type reentrantPersistenceStore struct {
	SessionStore

	mu        sync.Mutex
	armed     bool
	agent     *Agent
	sessionID acp.SessionId
	entered   chan struct{}
	reentered chan error
	release   chan struct{}
}

func (s *reentrantPersistenceStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	s.mu.Lock()
	armed := s.armed
	if armed {
		s.armed = false
	}
	s.mu.Unlock()

	if armed {
		close(s.entered)
		s.reentered <- s.agent.Cancel(ctx, acp.CancelNotification{SessionId: s.sessionID})
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

func TestPersistenceCallbackCanReenterCancelWithoutBlockingAgent(t *testing.T) {
	store := &reentrantPersistenceStore{
		SessionStore: NewInMemorySessionStore(),
		entered:      make(chan struct{}),
		reentered:    make(chan error, 1),
		release:      make(chan struct{}),
	}
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	store.agent = agent

	session, err := newAgentSession(t.Context(), agent, "T-reentrant-persist", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	store.sessionID = session.id
	activateOwnershipTestSession(t, agent, session)

	store.mu.Lock()
	store.armed = true
	store.mu.Unlock()
	persisted := make(chan error, 1)
	go func() {
		persisted <- session.persistAfterTurn(t.Context(), nil)
	}()

	<-store.entered
	requireClosedCallbackRefusal(t, <-store.reentered)
	_, lookupErr := agent.session(session.id)
	require.NoError(t, lookupErr)
	_, listErr := agent.ListSessions(t.Context(), ListSessionsRequest())
	require.NoError(t, listErr)

	close(store.release)
	require.NoError(t, <-persisted)
	require.NoError(t, agent.Close())
}

type blockingDeleteStore struct {
	SessionStore
	entered chan struct{}
	release chan struct{}
}

func (s *blockingDeleteStore) Delete(ctx context.Context, key SessionKey) error {
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	return s.SessionStore.Delete(ctx, key)
}

func TestLoadAndResumeFailClosedAfterDeletePublication(t *testing.T) {
	t.Setenv("AMP_API_KEY", "ownership-test")
	path, _ := fakeAgentAmpPath(t, "")
	store := &blockingDeleteStore{
		SessionStore: NewInMemorySessionStore(),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	agent := newTestAgent(WithExecutablePath(path), WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) (string, error) { return path, nil }

	session, err := newAgentSession(t.Context(), agent, "T-delete-before-load", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	session.nativeID = "T-export-must-not-start"
	activateOwnershipTestSession(t, agent, session)

	exported := make(chan struct{}, 1)
	agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
		exported <- struct{}{}

		return nil, nil
	}

	deleted := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
		deleted <- err
	}()
	<-store.entered

	_, loadErr := agent.LoadSession(t.Context(), LoadSessionRequest(session.id, session.cwd))
	require.Error(t, loadErr)
	_, resumeErr := agent.ResumeSession(t.Context(), ResumeSessionRequest(session.id, session.cwd))
	require.Error(t, resumeErr)
	select {
	case <-exported:
		t.Fatal("continuability export started behind a published delete")
	default:
	}

	session.mu.Lock()
	session.nativeID = ""
	session.mu.Unlock()
	close(store.release)
	require.NoError(t, <-deleted)
	require.NoError(t, agent.Close())
}

func TestDeleteWaitsForAdmittedExportAndReleasesExactWrapperOnce(t *testing.T) {
	t.Setenv("AMP_API_KEY", "ownership-test")
	path, _ := fakeAgentAmpPath(t, "")
	store := &blockingDeleteStore{
		SessionStore: NewInMemorySessionStore(),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	reader := sdkmetric.NewManualReader()
	var releases atomic.Int64
	agent := newTestAgent(
		WithExecutablePath(path),
		WithSessionStore(store),
		WithScratchDir(testScratchDir(t)),
		WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
	)
	agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) (string, error) { return path, nil }

	session, err := newAgentSession(t.Context(), agent, "T-export-before-delete", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	countExactSessionRelease(session, &releases)
	session.nativeID = "T-agent-thread"
	activateOwnershipTestSession(t, agent, session)

	useEntered := make(chan struct{})
	useRelease := make(chan struct{})
	agent.options.runtime.afterSessionUseAdmitted = func(*agentSessionUse) {
		close(useEntered)
		<-useRelease
	}

	loaded := make(chan error, 1)
	go func() {
		_, err := agent.LoadSession(t.Context(), LoadSessionRequest(session.id, session.cwd))
		loaded <- err
	}()
	<-useEntered

	deleteCalled := make(chan struct{})
	deleted := make(chan error, 1)
	go func() {
		close(deleteCalled)
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
		deleted <- err
	}()
	<-deleteCalled
	close(useRelease)
	require.NoError(t, <-loaded)
	<-store.entered
	close(store.release)
	require.NoError(t, <-deleted)

	require.Equal(t, int64(1), releases.Load())
	require.Equal(t, int64(0), collectActiveSessions(t, reader))
	_, pending := agent.cleanupOwner(session.id)
	require.False(t, pending)
	require.NoError(t, agent.Close())
	require.Equal(t, int64(1), releases.Load())
}

type refusingDeleteStore struct {
	SessionStore
	err error
}

func (s *refusingDeleteStore) Delete(context.Context, SessionKey) error {
	return s.err
}

func TestLocalCleanupFailureRetainsExactOwner(t *testing.T) {
	t.Run("live close retries during Agent.Close", func(t *testing.T) {
		reader := sdkmetric.NewManualReader()
		var releases atomic.Int64
		agent := newTestAgent(
			WithSessionStore(NewInMemorySessionStore()),
			WithScratchDir(testScratchDir(t)),
			WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		)
		session, err := newAgentSession(t.Context(), agent, "T-close-cleanup-retry", t.TempDir(), parsedSessionMeta{}, "", nil)
		require.NoError(t, err)
		countExactSessionRelease(session, &releases)
		activateOwnershipTestSession(t, agent, session)

		installFailOnceSessionRemoval(t, session.settingsDir)
		_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		require.Error(t, err)
		retained, lookupErr := agent.session(session.id)
		require.NoError(t, lookupErr)
		require.Same(t, session, retained)
		require.Zero(t, releases.Load())
		require.Equal(t, int64(1), collectActiveSessions(t, reader))

		require.NoError(t, agent.Close())
		require.Equal(t, int64(1), releases.Load())
		require.Equal(t, int64(0), collectActiveSessions(t, reader))
	})

	t.Run("stored-only tombstone refusal retries during Agent.Close", func(t *testing.T) {
		path, _ := fakeAgentAmpPath(t, "")
		base := NewInMemorySessionStore()
		putStoredSession(t, base, "T-stored-cleanup-retry", t.TempDir(), nil)
		refusal := errors.New("tombstone refused")
		store := &refusingDeleteStore{SessionStore: base, err: refusal}
		agent := newTestAgent(
			WithExecutablePath(path),
			WithSessionStore(store),
			WithScratchDir(testScratchDir(t)),
		)

		installFailOnceSessionRemoval(t, "")
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest("T-stored-cleanup-retry"))
		require.ErrorIs(t, err, refusal)
		owner, retained := agent.cleanupOwner("T-stored-cleanup-retry")
		require.True(t, retained)
		require.NotNil(t, owner.session)
		exact := owner.session
		var releases atomic.Int64
		countExactSessionRelease(exact, &releases)
		require.Zero(t, releases.Load())

		require.NoError(t, agent.Close())
		require.True(t, exact.scratchDone)
		require.Equal(t, int64(1), releases.Load())
		main, loadErr := base.Load(t.Context(), SessionKey{SessionID: "T-stored-cleanup-retry", Subpath: SessionStoreMainSubpath})
		require.NoError(t, loadErr)
		require.NotEmpty(t, main)
	})
}

func TestAgentCloseReportsFailedLocalRemovalWithoutRetainingCallableOwner(t *testing.T) {
	var releases atomic.Int64
	agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()), WithScratchDir(testScratchDir(t)))
	session, err := newAgentSession(t.Context(), agent, "T-agent-close-remove-retry", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	countExactSessionRelease(session, &releases)
	agent.mu.Lock()
	agent.activateSessionLocked(session)
	agent.mu.Unlock()
	agent.observe.AddActiveSession(t.Context(), 1)
	installFailOnceSessionRemoval(t, session.settingsDir)

	firstErr := agent.Close()
	require.ErrorContains(t, firstErr, "injected local cleanup failure")
	agent.mu.Lock()
	retained := agent.sessions[session.id]
	agent.mu.Unlock()
	require.Nil(t, retained)
	require.Zero(t, releases.Load())
	session.mu.Lock()
	require.True(t, session.closeBoundaryDone)
	require.True(t, session.closeCommitDone)
	require.False(t, session.scratchDone)
	session.mu.Unlock()

	require.EqualError(t, agent.Close(), firstErr.Error())
	require.Zero(t, releases.Load())
	agent.mu.Lock()
	require.Empty(t, agent.sessions)
	agent.mu.Unlock()
	require.EqualError(t, agent.Close(), firstErr.Error())
	require.Zero(t, releases.Load())
}

func TestConcurrentCleanupRetryCloseAndDeleteSettleAccountingOnce(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "delete-fail-once")
	reader := sdkmetric.NewManualReader()
	var releases atomic.Int64
	agent := newTestAgent(
		WithExecutablePath(path),
		WithSessionStore(NewInMemorySessionStore()),
		WithScratchDir(testScratchDir(t)),
		WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
	)
	session, err := newAgentSession(t.Context(), agent, "T-concurrent-cleanup", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	countExactSessionRelease(session, &releases)
	session.nativeID = "T-agent-thread"
	activateOwnershipTestSession(t, agent, session)

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.Error(t, err)
	owner, retained := agent.cleanupOwner(session.id)
	require.True(t, retained)
	require.Same(t, session, owner.session)
	require.Zero(t, releases.Load())
	require.Equal(t, int64(0), collectActiveSessions(t, reader))

	start := make(chan struct{})
	results := make(chan error, 3)
	go func() {
		<-start
		results <- agent.retryCleanupOwner(t.Context(), session.id)
	}()
	go func() {
		<-start
		_, deleteErr := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
		results <- deleteErr
	}()
	go func() {
		<-start
		_, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		results <- closeErr
	}()
	close(start)

	var success int
	for range 3 {
		if result := <-results; result == nil {
			success++
		}
	}
	require.GreaterOrEqual(t, success, 2)
	require.Equal(t, int64(1), releases.Load())
	require.Equal(t, int64(0), collectActiveSessions(t, reader))
	_, retained = agent.cleanupOwner(session.id)
	require.False(t, retained)
	require.NoError(t, agent.Close())
	require.Equal(t, int64(1), releases.Load())
}

func installFailOnceSessionRemoval(t *testing.T, exactPath string) {
	t.Helper()
	original := removeSessionDir
	failed := false
	removeSessionDir = func(path string) error {
		if !failed && (exactPath == "" || path == exactPath) {
			failed = true

			return errors.New("injected local cleanup failure")
		}

		return original(path)
	}
	t.Cleanup(func() { removeSessionDir = original })
}

func countExactSessionRelease(session *agentSession, releases *atomic.Int64) {
	original := session.scratchRootRelease
	session.scratchRootRelease = func() {
		releases.Add(1)
		if original != nil {
			original()
		}
	}
}

func activateOwnershipTestSession(t *testing.T, agent *Agent, session *agentSession) {
	t.Helper()

	agent.mu.Lock()
	agent.activateSessionLocked(session)
	agent.mu.Unlock()
	agent.observe.AddActiveSession(t.Context(), 1)
}

func requireClosedCallbackRefusal(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, errSessionClosed)
	requireRequestErrorCode(t, err, -32600)
}

type replaceDeleteReentryStore struct {
	SessionStore
	agent     *Agent
	sessionID acp.SessionId
	armed     atomic.Bool
	reentered chan error
}

func (s *replaceDeleteReentryStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	if s.armed.CompareAndSwap(true, false) {
		_, err := s.agent.UnstableDeleteSession(ctx, DeleteSessionRequest(s.sessionID))
		s.reentered <- err
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

func TestReplaceCallbackDeleteReentryIsClosedRefusal(t *testing.T) {
	store := &replaceDeleteReentryStore{SessionStore: NewInMemorySessionStore(), reentered: make(chan error, 1)}
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	store.agent = agent
	session, err := newAgentSession(t.Context(), agent, "T-replace-delete-reentry", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	store.sessionID = session.id
	activateOwnershipTestSession(t, agent, session)

	store.armed.Store(true)
	require.NoError(t, session.persistAfterTurn(t.Context(), nil))
	requireClosedCallbackRefusal(t, <-store.reentered)
	require.NoError(t, agent.Close())
}

type deleteDeleteReentryStore struct {
	SessionStore
	agent     *Agent
	sessionID acp.SessionId
	once      sync.Once
	reentered chan error
}

func (s *deleteDeleteReentryStore) Delete(ctx context.Context, key SessionKey) error {
	s.once.Do(func() {
		_, err := s.agent.UnstableDeleteSession(ctx, DeleteSessionRequest(s.sessionID))
		s.reentered <- err
	})

	return s.SessionStore.Delete(ctx, key)
}

func TestDeleteCallbackDeleteReentryIsClosedRefusal(t *testing.T) {
	store := &deleteDeleteReentryStore{SessionStore: NewInMemorySessionStore(), reentered: make(chan error, 1)}
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	store.agent = agent
	session, err := newAgentSession(t.Context(), agent, "T-delete-delete-reentry", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	store.sessionID = session.id
	activateOwnershipTestSession(t, agent, session)

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.NoError(t, err)
	requireClosedCallbackRefusal(t, <-store.reentered)
	require.NoError(t, agent.Close())
}

type loadReentryStore struct {
	SessionStore
	agent     *Agent
	sessionID acp.SessionId
	cwd       string
	once      sync.Once
	deleteErr error
	loadErr   error
}

func (s *loadReentryStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if key.SessionID == string(s.sessionID) && key.Subpath == SessionStoreMainSubpath {
		s.once.Do(func() {
			_, s.deleteErr = s.agent.UnstableDeleteSession(ctx, DeleteSessionRequest(s.sessionID))
			_, s.loadErr = s.agent.LoadSession(ctx, LoadSessionRequest(s.sessionID, s.cwd))
		})
	}

	return s.SessionStore.Load(ctx, key)
}

func TestLoadCallbackDeleteAndLoadReentryAreClosedRefusals(t *testing.T) {
	t.Setenv("AMP_API_KEY", "ownership-test")
	path, _ := fakeAgentAmpPath(t, "")
	base := NewInMemorySessionStore()
	id := acp.SessionId("T-load-reentry")
	cwd := t.TempDir()
	putStoredSession(t, base, string(id), cwd, nil)
	store := &loadReentryStore{SessionStore: base, sessionID: id, cwd: cwd}
	agent := newTestAgent(WithExecutablePath(path), WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	store.agent = agent
	agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) (string, error) { return path, nil }
	agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	_, err := agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
	require.NoError(t, err)
	requireClosedCallbackRefusal(t, store.deleteErr)
	requireClosedCallbackRefusal(t, store.loadErr)
	require.NoError(t, agent.Close())
}

func TestRuntimeResourceAndObserverReentryRefuseAgentClose(t *testing.T) {
	t.Setenv("AMP_API_KEY", "ownership-test")
	path, _ := fakeAgentAmpPath(t, "")
	var agent *Agent
	var mu sync.Mutex
	var callbackErrs []error
	recordClose := func() {
		err := agent.Close()
		mu.Lock()
		callbackErrs = append(callbackErrs, err)
		mu.Unlock()
	}
	hooks := RuntimeResourceHooks{
		ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
			recordClose()

			return recordClose, nil
		},
		ObserveStartupStage: func(context.Context, RuntimeResourceKind, RuntimeStartupStage, time.Duration, error) {
			recordClose()
		},
	}
	agent = newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithRuntimeResourceHooks(hooks),
	)
	agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) (string, error) { return path, nil }

	created := make(chan struct {
		id  acp.SessionId
		err error
	}, 1)
	go func() {
		resp, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		created <- struct {
			id  acp.SessionId
			err error
		}{id: resp.SessionId, err: err}
	}()
	select {
	case result := <-created:
		require.NoError(t, result.err)
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: result.id})
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("resource or observer callback waited on its own Agent.Close")
	}

	mu.Lock()
	errs := append([]error(nil), callbackErrs...)
	mu.Unlock()
	require.NotEmpty(t, errs)
	for _, err := range errs {
		requireClosedCallbackRefusal(t, err)
	}
	require.NoError(t, agent.Close())
}

func TestCloseJoinsAdmittedConfigPersistenceAndLeavesNoLateWork(t *testing.T) {
	store := newGatedStore(nil)
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	agent.options.runtime.beforePersistenceReplace = store.beforeReplace
	client := &lifecycleClient{}
	agent.setConnection(client)
	session, err := newAgentSession(t.Context(), agent, "T-config-close-fence", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	activateOwnershipTestSession(t, agent, session)

	store.gate()
	configured := make(chan error, 1)
	go func() {
		_, err := agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(session.id, configMode, modeHigh))
		configured <- err
	}()
	<-store.started

	closed := make(chan error, 1)
	go func() {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		closed <- err
	}()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before admitted config Replace and notification: %v", err)
	default:
	}

	close(store.release)
	require.NoError(t, <-configured)
	require.NoError(t, <-closed)
	replacesAtClose := store.replaceCount()
	client.mu.Lock()
	updatesAtClose := len(client.updates)
	client.mu.Unlock()
	require.Equal(t, 1, updatesAtClose)
	require.Equal(t, replacesAtClose, store.replaceCount())
	client.mu.Lock()
	updatesAfterClose := len(client.updates)
	client.mu.Unlock()
	require.Equal(t, updatesAtClose, updatesAfterClose)
	require.NoError(t, agent.Close())
}

func TestColdLoadDeleteFlightUsesOneExactNativeWrapper(t *testing.T) {
	for _, test := range []struct {
		name   string
		id     string
		refuse error
	}{
		{name: "success", id: "success"},
		{name: "refused tombstone", id: "refused", refuse: errors.New("tombstone refused")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("AMP_API_KEY", "ownership-test")
			path, _ := fakeAgentAmpPath(t, "")
			base := NewInMemorySessionStore()
			id := acp.SessionId("T-cold-flight-" + test.id)
			cwd := t.TempDir()
			putStoredSession(t, base, string(id), cwd, nil)
			var store SessionStore = base
			if test.refuse != nil {
				store = &refusingDeleteStore{SessionStore: base, err: test.refuse}
			}

			reserved := make(chan struct{})
			continueReservation := make(chan struct{})
			var acquisitions atomic.Int64
			var releases atomic.Int64
			hooks := RuntimeResourceHooks{
				ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
					if kind != RuntimeResourceSession {
						return func() {}, nil
					}
					if acquisitions.Add(1) == 1 {
						close(reserved)
						<-continueReservation

						return func() { releases.Add(1) }, nil
					}

					return func() {}, nil
				},
			}
			agent := newTestAgent(
				WithExecutablePath(path),
				WithSessionStore(store),
				WithScratchDir(testScratchDir(t)),
				WithRuntimeResourceHooks(hooks),
			)
			agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) (string, error) { return path, nil }
			agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
				return json.RawMessage(`{}`), nil
			}

			loaded := make(chan error, 1)
			go func() {
				_, err := agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
				loaded <- err
			}()
			<-reserved

			flightCtx, flight, use, existing, err := agent.publishSessionFlight(t.Context(), id, agentSessionDeleteFlight, nil)
			require.NoError(t, err)
			require.Nil(t, existing)
			require.NotNil(t, use)
			close(continueReservation)
			require.NoError(t, agent.joinSessionFlightUse(flightCtx, id, flight, use))
			require.Error(t, <-loaded)
			require.NotNil(t, flight.session)
			require.Equal(t, string(id), flight.session.nativeSessionID(), "stored manifest must transfer its native id")
			exact := flight.session

			deleteErr := agent.deleteSession(flightCtx, id, flight)
			agent.finishSessionFlight(id, flight)
			if test.refuse != nil {
				require.ErrorIs(t, deleteErr, test.refuse)
				main, loadErr := base.Load(t.Context(), SessionKey{SessionID: string(id), Subpath: SessionStoreMainSubpath})
				require.NoError(t, loadErr)
				require.NotEmpty(t, main)
			} else {
				require.NoError(t, deleteErr)
				_, deleted := agent.isDeleted(id)
				require.True(t, deleted)
			}
			require.GreaterOrEqual(t, acquisitions.Load(), int64(1))
			require.Equal(t, int64(1), releases.Load())
			require.True(t, exact.scratchDone)
			require.NoError(t, agent.Close())
		})
	}
}

func TestTeardownRejectsPersistenceGenerationDependencyReentry(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	session, err := agent.session(created.SessionId)
	require.NoError(t, err)

	persistCtx, persistence, err := session.beginPersistence(t.Context(), sessionPersistenceOrdinary)
	require.NoError(t, err)
	_, teardown, err := session.beginTeardown(t.Context())
	require.NoError(t, err)

	_, _, reentryErr := session.beginTeardown(persistCtx)
	requireClosedCallbackRefusal(t, reentryErr)

	session.finishPersistence(persistence)
	session.finishTeardown(teardown)
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	require.NoError(t, agent.Close())
}

func TestDeleteHydratesColdOwnerInsteadOfPartialConstructionOwner(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithExecutablePath(path), WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	idText, err := newSessionID()
	require.NoError(t, err)
	id := acp.SessionId(idText)
	cwd := t.TempDir()
	manifest, err := json.Marshal(ampManifest{
		Format: SessionStoreFormat, SessionID: idText, NativeSessionID: "T-partial-owner-native", Cwd: cwd,
		Mode: modeMedium, CreatedAtUnixMilli: 1, UpdatedAtUnixMilli: 2,
	})
	require.NoError(t, err)
	main := SessionKey{SessionID: idText, Subpath: SessionStoreMainSubpath}
	require.NoError(t, store.Replace(t.Context(), main, []SessionStoreReplacement{{Key: main, Entries: []SessionStoreEntry{manifest}}}))

	originalWrite := writeFile
	originalRemove := removeSessionDir
	t.Cleanup(func() {
		writeFile = originalWrite
		removeSessionDir = originalRemove
	})
	constructionErr := errors.New("partial construction")
	cleanupErr := errors.New("partial cleanup retained")
	partialRoot := ""
	writeFile = func(string, []byte, os.FileMode) error { return constructionErr }
	removeSessionDir = func(path string) error {
		partialRoot = path

		return cleanupErr
	}
	_, err = newAgentSession(t.Context(), agent, id, cwd, parsedSessionMeta{}, "", nil)
	writeFile = originalWrite
	removeSessionDir = originalRemove
	require.ErrorIs(t, err, constructionErr)
	require.ErrorIs(t, err, cleanupErr)
	require.NotEmpty(t, partialRoot)
	require.Len(t, agent.cleanupOwners[id], 1)

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(id))
	require.NoError(t, err)
	records := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	deleteCalls := 0
	for _, args := range records {
		if slicesContainCommand([][]string{args}, "threads", "delete", "T-partial-owner-native") {
			deleteCalls++
		}
	}
	require.Equal(t, 1, deleteCalls, "the manifest-hydrated owner performs the native delete exactly once")
	require.Empty(t, agent.cleanupOwners[id], "the partial construction is reclaimed separately from native deletion")
	_, statErr := os.Stat(partialRoot)
	require.True(t, os.IsNotExist(statErr))
	require.NoError(t, agent.Close())
	require.Empty(t, agent.cleanupOwners)
}

func TestSessionActivationTransfersCleanupOwnershipAndCloseAllowsReload(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithExecutablePath(path), WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	cwd := t.TempDir()
	created, err := agent.NewSession(t.Context(), NewSessionRequest(cwd))
	require.NoError(t, err)
	exact, err := agent.session(created.SessionId)
	require.NoError(t, err)
	_, retained := agent.cleanupOwner(created.SessionId)
	require.False(t, retained)

	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	_, retained = agent.cleanupOwner(created.SessionId)
	require.False(t, retained)

	_, err = agent.LoadSession(t.Context(), LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	reloaded, err := agent.session(created.SessionId)
	require.NoError(t, err)
	require.NotSame(t, exact, reloaded)
	_, retained = agent.cleanupOwner(created.SessionId)
	require.False(t, retained)
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	require.NoError(t, agent.Close())
}

type rollbackFenceStore struct {
	SessionStore
	mu         sync.Mutex
	replaceErr error
	deleteErr  error
	replaces   int
}

func (s *rollbackFenceStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	s.mu.Lock()
	s.replaces++
	err := s.replaceErr
	s.mu.Unlock()
	if err != nil {
		return err
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

func (s *rollbackFenceStore) Delete(ctx context.Context, key SessionKey) error {
	s.mu.Lock()
	err := s.deleteErr
	s.mu.Unlock()
	if err != nil {
		return err
	}

	return s.SessionStore.Delete(ctx, key)
}

func (s *rollbackFenceStore) fail(replaceErr, deleteErr error) {
	s.mu.Lock()
	s.replaceErr = replaceErr
	s.deleteErr = deleteErr
	s.mu.Unlock()
}

func (s *rollbackFenceStore) replaceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.replaces
}

func TestFailedDeleteRestoresClosingPersistenceFence(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	store := &rollbackFenceStore{SessionStore: NewInMemorySessionStore()}
	agent := newTestAgent(WithExecutablePath(path), WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	client := &lifecycleClient{}
	agent.setConnection(client)
	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	session, err := agent.session(created.SessionId)
	require.NoError(t, err)
	session.retainUnsynced([]SessionStoreEntry{json.RawMessage(`{"type":"result"}`)})
	replaceErr := errors.New("close commit refused")
	deleteErr := errors.New("delete tombstone refused")
	store.fail(replaceErr, deleteErr)

	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.ErrorIs(t, err, replaceErr)
	session.persistMu.Lock()
	require.Equal(t, sessionPersistenceClosing, session.persistState)
	session.persistMu.Unlock()

	_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(created.SessionId))
	require.ErrorIs(t, err, deleteErr)
	session.persistMu.Lock()
	require.Equal(t, sessionPersistenceClosing, session.persistState)
	session.persistMu.Unlock()

	replaces := store.replaceCount()
	_, err = agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(created.SessionId, configMode, modeHigh))
	require.ErrorIs(t, err, errPersistenceFenced)
	require.Equal(t, replaces, store.replaceCount())
	client.mu.Lock()
	require.Empty(t, client.updates)
	client.mu.Unlock()

	store.fail(nil, nil)
	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	require.NoError(t, agent.Close())
}

func TestRecoveredExternalHookPanicLeavesNoGoroutineProvenance(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	panicValue := "startup observer panic"
	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ObserveStartupStage: func(context.Context, RuntimeResourceKind, RuntimeStartupStage, time.Duration, error) {
				panic(panicValue)
			},
		}),
	)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	}()
	require.Equal(t, panicValue, recovered)
	require.NoError(t, agent.Close())
}

type panickingCallbackClient struct {
	agent    *Agent
	closeErr error
	panic    any
}

func (*panickingCallbackClient) Done() <-chan struct{} { return nil }

func (c *panickingCallbackClient) SessionUpdate(context.Context, acp.SessionNotification) error {
	c.closeErr = c.agent.Close()
	panic(c.panic)
}

func (*panickingCallbackClient) NotifyExtension(context.Context, string, any) error { return nil }

func TestRecoveredClientCallbackPanicLeavesNoGoroutineProvenance(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	client := &panickingCallbackClient{agent: agent, panic: "client callback panic"}
	agent.setConnection(client)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(created.SessionId, configMode, modeHigh))
	}()
	require.Equal(t, client.panic, recovered)
	requireClosedCallbackRefusal(t, client.closeErr)
	require.NoError(t, agent.Close())
}

func TestCallbackProvenanceDefensivePaths(t *testing.T) {
	agent := newTestAgent()
	other := newTestAgent()
	owner := &agentCallbackGeneration{generation: 1, kind: "test"}

	ctx := withCallbackProvenance(nil, nil, nil) //nolint:staticcheck // Explicitly tests the nil-context defense.
	ctx = withCallbackProvenance(ctx, agent, owner)
	require.True(t, contextOwnsCallbackGeneration(ctx, agent, owner))
	require.False(t, contextOwnsCallbackGeneration(nil, agent, owner)) //nolint:staticcheck // Explicit nil-context defense.
	require.False(t, contextOwnsCallbackGeneration(ctx, agent, &agentCallbackGeneration{}))
	require.False(t, contextOwnsAgentCallback(nil, agent)) //nolint:staticcheck // Explicit nil-context defense.
	require.False(t, contextOwnsAgentCallback(ctx, other))
	require.Nil(t, callbackAgent(nil)) //nolint:staticcheck // Explicit nil-context defense.
	require.Equal(t, agent, callbackAgent(ctx))
	require.Equal(t, context.Background(), withExactCallbackGeneration(context.Background(), "none"))
	require.NotEqual(t, ctx, withExactCallbackGeneration(ctx, "exact"))

	leave := enterExternalCallback(withCallbackProvenance(t.Context(), other, owner))
	require.False(t, agent.hasActiveCallbackAuthority())
	require.True(t, other.hasActiveCallbackAuthority())
	leave()
	require.False(t, other.hasActiveCallbackAuthority())
	enterExternalCallback(context.Background())()
	require.False(t, agent.hasActiveCallbackAuthority())
}

func TestAgentCloseJoinsPublishedCloseGeneration(t *testing.T) {
	wantErr := errors.New("published close failed")
	agent := newTestAgent()
	flight := &agentCloseFlight{done: make(chan struct{}), err: wantErr}
	close(flight.done)
	agent.closeFlight = flight

	require.ErrorIs(t, agent.Close(), wantErr)
}
