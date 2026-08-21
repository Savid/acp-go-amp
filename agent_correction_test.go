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
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

const correctionBarrierTimeout = 5 * time.Second

func awaitCorrectionSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(correctionBarrierTimeout):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func receiveCorrection[T any](t *testing.T, values <-chan T, name string) T {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-time.After(correctionBarrierTimeout):
		t.Fatalf("timed out waiting for %s", name)

		var zero T

		return zero
	}
}

func awaitCorrectionCallback(t *testing.T, signal <-chan struct{}, name string) bool {
	t.Helper()

	select {
	case <-signal:
		return true
	case <-time.After(correctionBarrierTimeout):
		t.Errorf("timed out waiting for %s", name)

		return false
	}
}

type synchronousTeardownStore struct {
	SessionStore
	agent     *Agent
	sessionID acp.SessionId
	action    string
	armed     atomic.Bool
	err       error
}

func TestCancelledDeleteRetainsTransferredColdWrapperUntilUseReclaimsIt(t *testing.T) {
	for _, continuation := range []string{"load", "resume", "delete"} {
		t.Run(continuation, func(t *testing.T) {
			testCancelledDeleteColdWrapperTransfer(t, continuation)
		})
	}
}

func TestCancelledColdLoadReclamationPanicCompletesFlightAndQuarantinesOnce(t *testing.T) {
	t.Setenv("AMP_API_KEY", "cold-reclaim-panic")
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	id := acp.SessionId("T-cold-reclaim-panic")
	cwd := t.TempDir()
	putStoredSession(t, store, string(id), cwd, nil)
	prepared := make(chan *agentSession, 1)
	releasePrepared := make(chan struct{})
	var acquisitions atomic.Int64
	var releases atomic.Int64
	agent := newTestAgent(
		WithExecutablePath(path),
		WithSessionStore(store),
		WithScratchDir(testScratchDir(t)),
	)
	agent.options.runtime.afterColdSessionPrepared = func(session *agentSession) {
		acquisitions.Add(1)
		originalRelease := session.scratchRootRelease
		session.scratchRootRelease = func() {
			originalRelease()
			if releases.Add(1) == 1 {
				panic("release completed then panicked")
			}
		}
		prepared <- session
		awaitCorrectionCallback(t, releasePrepared, "cold-reclaim release")
	}

	loaded := make(chan error, 1)
	go func() {
		_, err := agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
		loaded <- err
	}()
	var wrapper *agentSession
	select {
	case wrapper = <-prepared:
	case <-time.After(2 * time.Second):
		t.Fatal("cold wrapper did not reach the ownership barrier")
	}

	deleteCtx, cancelDelete := context.WithCancel(t.Context())
	deleted := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(deleteCtx, DeleteSessionRequest(id))
		deleted <- err
	}()
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		defer agent.mu.Unlock()
		flight := agent.sessionFlights[id]

		return flight != nil && flight.session == wrapper
	}, 2*time.Second, time.Millisecond)
	cancelDelete()
	select {
	case err := <-deleted:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled delete did not return")
	}

	waitingDelete := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(id))
		waitingDelete <- err
	}()
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		defer agent.mu.Unlock()
		flight := agent.sessionFlights[id]

		return flight != nil && flight.waiters == 1
	}, 2*time.Second, time.Millisecond, "same-ID delete did not join the abandoned flight")
	close(releasePrepared)
	select {
	case err := <-loaded:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("cold load did not finish after reclamation panic")
	}
	select {
	case err := <-waitingDelete:
		require.ErrorIs(t, err, errAgentGoroutinePanic)
	case <-time.After(2 * time.Second):
		t.Fatal("same-ID delete did not observe completed reclamation")
	}

	agent.mu.Lock()
	require.Nil(t, agent.sessionFlights[id])
	require.Nil(t, agent.sessionUses[id])
	require.Len(t, agent.cleanupOwners[id], 1)
	require.Same(t, wrapper, agent.cleanupOwners[id][0].session)
	agent.mu.Unlock()
	wrapper.mu.Lock()
	require.True(t, wrapper.scratchDone)
	require.Nil(t, wrapper.scratchRootRelease)
	wrapper.mu.Unlock()
	require.Equal(t, int64(1), releases.Load())

	agent.options.runtime.afterColdSessionPrepared = func(session *agentSession) {
		acquisitions.Add(1)
		originalRelease := session.scratchRootRelease
		session.scratchRootRelease = func() {
			originalRelease()
			releases.Add(1)
		}
	}
	_, err := agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
	require.NoError(t, err)
	agent.mu.Lock()
	require.Empty(t, agent.cleanupOwners[id])
	require.NotNil(t, agent.sessions[id])
	agent.mu.Unlock()
	require.NoError(t, agent.Close())
	require.Equal(t, acquisitions.Load(), releases.Load())
}

func testCancelledDeleteColdWrapperTransfer(t *testing.T, continuation string) {
	t.Helper()
	t.Setenv("AMP_API_KEY", "cold-transfer")
	path, _ := fakeAgentAmpPath(t, "")
	base := NewInMemorySessionStore()
	id := acp.SessionId("T-cold-transfer-cancel-" + continuation)
	cwd := t.TempDir()
	putStoredSession(t, base, string(id), cwd, nil)
	prepared := make(chan *agentSession, 1)
	releasePrepared := make(chan struct{})
	var acquisitions atomic.Int64
	var releases atomic.Int64
	agent := newTestAgent(
		WithExecutablePath(path),
		WithSessionStore(base),
		WithScratchDir(testScratchDir(t)),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(_ context.Context, kind RuntimeResourceKind) (func(), error) {
				if kind != RuntimeResourceSession {
					return func() {}, nil
				}
				acquisitions.Add(1)

				return func() { releases.Add(1) }, nil
			},
		}),
	)
	agent.options.runtime.afterColdSessionPrepared = func(session *agentSession) {
		prepared <- session
		awaitCorrectionCallback(t, releasePrepared, "cold-transfer release")
	}

	loaded := make(chan error, 1)
	go func() {
		_, err := agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
		loaded <- err
	}()
	select {
	case <-prepared:
	case <-time.After(2 * time.Second):
		t.Fatal("cold load did not reach transcript barrier")
	}

	deleteCtx, cancelDelete := context.WithCancel(t.Context())
	deleted := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(deleteCtx, DeleteSessionRequest(id))
		deleted <- err
	}()

	var (
		flight  *agentSessionFlight
		use     *agentSessionUse
		wrapper *agentSession
	)
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		defer agent.mu.Unlock()
		flight = agent.sessionFlights[id]
		use = agent.sessionUses[id]
		if flight == nil || use == nil || flight.use != use || flight.session == nil {
			return false
		}
		wrapper = flight.session

		return use.session == wrapper
	}, 2*time.Second, time.Millisecond, "delete flight did not take the prepared wrapper from the cold-load use")

	cancelDelete()
	select {
	case err := <-deleted:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled delete did not return")
	}

	agent.mu.Lock()
	require.Same(t, flight, agent.sessionFlights[id])
	require.True(t, flight.abandoned)
	require.Same(t, wrapper, flight.session)
	agent.mu.Unlock()

	close(releasePrepared)
	select {
	case err := <-loaded:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("cold load did not finish after release")
	}

	agent.mu.Lock()
	require.Nil(t, agent.sessionFlights[id])
	require.Nil(t, agent.sessionUses[id])
	require.Empty(t, agent.cleanupOwners[id])
	require.Nil(t, agent.sessions[id])
	agent.mu.Unlock()
	require.True(t, wrapper.scratchDone)
	require.Equal(t, acquisitions.Load(), releases.Load())
	require.NoDirExists(t, wrapper.settingsDir)

	var continuationErr error

	switch continuation {
	case "load":
		_, continuationErr = agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
	case "resume":
		_, continuationErr = agent.ResumeSession(t.Context(), ResumeSessionRequest(id, cwd))
	case "delete":
		_, continuationErr = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(id))
	default:
		panic("unknown same-id continuation")
	}

	require.NoError(t, continuationErr, "same-id continuation must not remain cleanup-pending")
	if continuation == "delete" {
		require.Equal(t, acquisitions.Load(), releases.Load())
	} else {
		require.Equal(t, acquisitions.Load()-1, releases.Load())
	}

	require.NoError(t, agent.Close())
	require.Equal(t, acquisitions.Load(), releases.Load())
}

func (s *synchronousTeardownStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	if s.armed.CompareAndSwap(true, false) {
		s.err = invokeSessionTeardown(ctx, s.agent, s.sessionID, s.action)
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

func invokeSessionTeardown(ctx context.Context, agent *Agent, id acp.SessionId, action string) error {
	switch action {
	case "close":
		_, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: id})

		return err
	case "delete":
		_, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(id))

		return err
	default:
		panic("unknown teardown action")
	}
}

type backgroundReentryStore struct {
	SessionStore
	onReplace func()
	onLoad    func(SessionKey)
}

type delegateThenPanicStore struct {
	SessionStore
	tracking     atomic.Bool
	armed        atomic.Bool
	mu           sync.Mutex
	replacements [][]SessionStoreReplacement
}

func (s *delegateThenPanicStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	if !s.tracking.Load() {
		return s.SessionStore.Replace(ctx, key, replacements)
	}

	s.mu.Lock()
	s.replacements = append(s.replacements, cloneSessionStoreReplacements(replacements))
	s.mu.Unlock()

	err := s.SessionStore.Replace(ctx, key, replacements)
	if s.armed.CompareAndSwap(true, false) {
		panic("delegate committed then panicked")
	}

	return err
}

func TestPersistenceRetryReusesExactCommitAfterDelegatePanics(t *testing.T) {
	base := NewInMemorySessionStore()
	store := &delegateThenPanicStore{SessionStore: base}
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	session := installActiveTestSession(t, agent, "T-delegate-then-panic")
	require.NoError(t, session.persistAfterTurn(t.Context(), nil))

	frame := json.RawMessage(`{"type":"result","result":"durable"}`)
	store.tracking.Store(true)
	store.armed.Store(true)
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = session.persistAfterTurn(t.Context(), []SessionStoreEntry{frame})
	}()
	require.Equal(t, "delegate committed then panicked", recovered)

	stored, err := base.Load(t.Context(), SessionKey{SessionID: string(session.id), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.Equal(t, []SessionStoreEntry{frame}, stored)
	session.mu.Lock()
	require.NotNil(t, session.persistenceCommit)
	require.True(t, session.mirrorUnsynced)
	require.Zero(t, session.transcriptFrames)
	session.mu.Unlock()

	successor := json.RawMessage(`{"type":"assistant","message":"successor"}`)
	require.NoError(t, session.persistAfterTurn(t.Context(), []SessionStoreEntry{successor}))
	store.mu.Lock()
	require.Len(t, store.replacements, 3)
	require.Equal(t, store.replacements[0], store.replacements[1], "retry must state the identical commit")
	store.mu.Unlock()
	stored, err = base.Load(t.Context(), SessionKey{SessionID: string(session.id), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.Equal(t, []SessionStoreEntry{frame, successor}, stored)
	session.mu.Lock()
	require.Nil(t, session.persistenceCommit)
	require.False(t, session.mirrorUnsynced)
	require.Equal(t, 2, session.transcriptFrames)
	session.mu.Unlock()
	require.NoError(t, agent.Close())
}

func TestCallbackSpawnedAgentCloseUsesOwnedAuthority(t *testing.T) {
	store := &backgroundReentryStore{SessionStore: NewInMemorySessionStore()}
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	session := installActiveTestSession(t, agent, "T-callback-spawn-close")
	closeResult := make(chan error, 1)
	store.onReplace = func() {
		spawned := make(chan error, 1)
		go func() { spawned <- agent.Close() }()
		select {
		case err := <-spawned:
			closeResult <- err
		case <-time.After(2 * time.Second):
			closeResult <- errors.New("callback-spawned Agent.Close did not return")
		}
	}

	require.NoError(t, session.persistAfterTurn(t.Context(), nil))
	store.onReplace = nil
	select {
	case err := <-closeResult:
		requireClosedCallbackRefusal(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not observe Agent.Close completion")
	}
	requireSessionUnchangedByReentry(t, agent, session, nil)
	require.NoError(t, agent.Close())
}

type closeJoinDeliveryClient struct {
	lifecycleClient
	deliveries atomic.Int64
}

func (c *closeJoinDeliveryClient) SessionUpdate(context.Context, acp.SessionNotification) error {
	c.deliveries.Add(1)

	return nil
}

func TestAgentCloseRetainsDeliveryAuthorityThroughAdmittedCallJoin(t *testing.T) {
	agent := newTestAgent()
	client := &closeJoinDeliveryClient{}
	agent.setConnection(client)
	callCtx, finishCall, err := agent.beginAgentCall(t.Context(), "T-close-delivery-join")
	require.NoError(t, err)
	session := &agentSession{agent: agent, id: "T-close-delivery-join"}
	delivery := &promptTerminalDelivery{
		stream: &promptStream{
			session: session,
			client:  client,
			stream:  lifecycle.NewStream("close-delivery-join", negotiatedAnswer()),
		},
		notifications: []acp.SessionNotification{{SessionId: session.id}},
	}
	delivered := make(chan error, 1)
	go func() {
		if !awaitCorrectionCallback(t, callCtx.Done(), "Agent.Close call fence") {
			finishCall(context.DeadlineExceeded)
			delivered <- context.DeadlineExceeded

			return
		}
		deliveryErr := delivery.deliver(context.WithoutCancel(callCtx))
		finishCall(deliveryErr)
		delivered <- deliveryErr
	}()

	require.NoError(t, agent.Close())
	require.NoError(t, receiveCorrection(t, delivered, "admitted terminal delivery"))
	require.Equal(t, int64(1), client.deliveries.Load())
	require.Nil(t, agent.connection())
}

func (s *backgroundReentryStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	if s.onReplace != nil {
		s.onReplace()
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

func (s *backgroundReentryStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if s.onLoad != nil {
		s.onLoad(key)
	}

	return s.SessionStore.Load(ctx, key)
}

func invokeBackgroundSessionAPI(agent *Agent, id acp.SessionId, action string, cwd string) error {
	switch action {
	case "close":
		_, err := agent.CloseSession(context.Background(), acp.CloseSessionRequest{SessionId: id})

		return err
	case "load":
		_, err := agent.LoadSession(context.Background(), LoadSessionRequest(id, cwd))

		return err
	case "resume":
		_, err := agent.ResumeSession(context.Background(), ResumeSessionRequest(id, cwd))

		return err
	default:
		panic("unknown background reentry action")
	}
}

func TestCallbacksDiscardingContextRefuseEveryReentrantSessionAPI(t *testing.T) {
	for _, callback := range []string{"replace", "load", "resume"} {
		for _, action := range []string{"close", "load", "resume"} {
			t.Run(callback+"/"+action, func(t *testing.T) {
				store := &backgroundReentryStore{SessionStore: NewInMemorySessionStore()}
				agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
				id := acp.SessionId("T-background-reentry-" + callback + "-" + action)
				session := installActiveTestSession(t, agent, id)
				cwd := session.cwd
				reentered := make(chan error, 1)
				invoke := func() {
					reentered <- invokeBackgroundSessionAPI(agent, id, action, cwd)
				}

				switch callback {
				case "replace":
					store.onReplace = invoke
					require.NoError(t, session.persistAfterTurn(t.Context(), nil))
					store.onReplace = nil
				case "load":
					store.onLoad = func(key SessionKey) {
						if key.Subpath == transcriptSubpath {
							store.onLoad = nil
							invoke()
						}
					}
					_, err := agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
					require.NoError(t, err)
				case "resume":
					store.onLoad = func(key SessionKey) {
						if key.Subpath == transcriptSubpath {
							store.onLoad = nil
							invoke()
						}
					}
					_, err := agent.ResumeSession(t.Context(), ResumeSessionRequest(id, cwd))
					require.NoError(t, err)
				}

				select {
				case err := <-reentered:
					requireClosedCallbackRefusal(t, err)
				case <-time.After(2 * time.Second):
					t.Fatal("background-context callback reentry did not refuse immediately")
				}
				requireSessionUnchangedByReentry(t, agent, session, nil)
				require.NoError(t, agent.Close())
			})
		}
	}
}

func installActiveTestSession(t *testing.T, agent *Agent, id acp.SessionId) *agentSession {
	t.Helper()

	session, err := newAgentSession(t.Context(), agent, id, t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	agent.mu.Lock()
	agent.activateSessionLocked(session)
	agent.mu.Unlock()
	agent.observe.AddActiveSession(t.Context(), 1)

	return session
}

func requireSessionUnchangedByReentry(t *testing.T, agent *Agent, session *agentSession, prompt *promptTurnState) {
	t.Helper()

	session.mu.Lock()
	require.False(t, session.closed)
	require.Nil(t, session.scratchContainmentErr)
	session.mu.Unlock()

	session.persistMu.Lock()
	require.Equal(t, sessionPersistenceOpen, session.persistState)
	session.persistMu.Unlock()

	session.teardownMu.Lock()
	require.Nil(t, session.teardownFlight)
	session.teardownMu.Unlock()

	agent.mu.Lock()
	require.Nil(t, agent.sessionFlights[session.id])
	require.Same(t, session, agent.sessions[session.id])
	agent.mu.Unlock()

	if prompt != nil {
		require.False(t, prompt.isCancelled())
		require.Same(t, prompt, session.activePromptState())
	}
}

func TestPersistenceReplaceCallbackRefusesFreshCloseAndDeleteBeforePublication(t *testing.T) {
	for _, action := range []string{"close", "delete"} {
		t.Run(action, func(t *testing.T) {
			store := &synchronousTeardownStore{SessionStore: NewInMemorySessionStore(), action: action}
			agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
			store.agent = agent
			session := installActiveTestSession(t, agent, acp.SessionId("T-replace-reentry-"+action))
			store.sessionID = session.id
			store.armed.Store(true)

			require.NoError(t, session.persistAfterTurn(t.Context(), nil))
			requireClosedCallbackRefusal(t, store.err)
			requireSessionUnchangedByReentry(t, agent, session, nil)

			_, err := agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(session.id, configMode, modeHigh))
			require.NoError(t, err)
			require.NoError(t, agent.Close())
		})
	}
}

type synchronousTeardownClient struct {
	agent     *Agent
	sessionID acp.SessionId
	action    string
	err       error
}

func (*synchronousTeardownClient) Done() <-chan struct{} { return nil }

func (c *synchronousTeardownClient) SessionUpdate(ctx context.Context, _ acp.SessionNotification) error {
	c.err = invokeSessionTeardown(ctx, c.agent, c.sessionID, c.action)

	return nil
}

func (*synchronousTeardownClient) NotifyExtension(context.Context, string, any) error { return nil }

func TestPromptCallbacksRefuseFreshCloseAndDeleteBeforePublication(t *testing.T) {
	for _, source := range []string{"session_update", "runtime"} {
		for _, action := range []string{"close", "delete"} {
			t.Run(source+"/"+action, func(t *testing.T) {
				var (
					agent       *Agent
					callbackErr error
				)
				hooks := RuntimeResourceHooks{}
				if source == "runtime" {
					hooks.ObserveStartupStage = func(ctx context.Context, _ RuntimeResourceKind, _ RuntimeStartupStage, _ time.Duration, _ error) {
						callbackErr = invokeSessionTeardown(ctx, agent, acp.SessionId("T-prompt-reentry-"+action), action)
					}
				}
				agent = newTestAgent(
					WithSessionStore(NewInMemorySessionStore()),
					WithScratchDir(testScratchDir(t)),
					WithRuntimeResourceHooks(hooks),
				)
				id := acp.SessionId("T-prompt-reentry-" + action)
				session := installActiveTestSession(t, agent, id)
				prompt := newPromptTurnState()
				require.NoError(t, session.admitPrompt(prompt))
				promptCtx := withCallbackProvenance(t.Context(), agent, prompt)

				if source == "session_update" {
					client := &synchronousTeardownClient{agent: agent, sessionID: id, action: action}
					agent.setConnection(client)
					require.NoError(t, session.emitUpdate(promptCtx, session.configUpdate()))
					callbackErr = client.err
				} else {
					agent.options.RuntimeResourceHooks.ObserveStartupStage(
						promptCtx, RuntimeResourcePrompt, RuntimeStartupConfiguration, 0, nil,
					)
				}

				requireClosedCallbackRefusal(t, callbackErr)
				requireSessionUnchangedByReentry(t, agent, session, prompt)
				session.clearActivePrompt(prompt)
				prompt.complete(nil)

				_, err := agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(id, configMode, modeHigh))
				require.NoError(t, err)
				require.NoError(t, agent.Close())
			})
		}
	}
}

type panicOnceReplaceStore struct {
	SessionStore
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	value   any
}

type panicIdleLifecycleClient struct {
	lifecycleClient
	armed   atomic.Bool
	session *agentSession
	state   *promptTurnState
}

type panicPromptClient struct {
	lifecycleClient
	stage   string
	session *agentSession
	state   *promptTurnState
	once    sync.Once
}

func (c *panicPromptClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	shouldPanic := false
	if c.stage == "acceptance" {
		if envelope, ok := notification.Meta[lifecycle.MetaKey].(map[string]any); ok {
			if event, ok := envelope["event"].(map[string]any); ok && event["type"] == "prompt_accepted" {
				shouldPanic = true
			}
		}
	}
	if c.stage == "client" && notification.Update.AgentMessageChunk != nil {
		shouldPanic = true
	}

	if shouldPanic {
		c.once.Do(func() {
			c.state = c.session.activePromptState()
			panic("prompt " + c.stage + " panic")
		})
	}

	return c.lifecycleClient.SessionUpdate(ctx, notification)
}

func (c *panicPromptClient) NotifyExtension(context.Context, string, any) error {
	if c.stage == "raw" {
		c.once.Do(func() {
			c.state = c.session.activePromptState()
			panic("prompt raw panic")
		})
	}

	return nil
}

func (c *panicIdleLifecycleClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if c.armed.Load() {
		if envelope, ok := notification.Meta[lifecycle.MetaKey].(map[string]any); ok {
			if event, ok := envelope["event"].(map[string]any); ok && event["state"] == "idle" && c.armed.CompareAndSwap(true, false) {
				c.state = c.session.activePromptState()
				panic("terminal lifecycle delivery panic")
			}
		}
	}

	return c.lifecycleClient.SessionUpdate(ctx, notification)
}

func promptRecovered(agent *Agent, request acp.PromptRequest) <-chan any {
	result := make(chan any, 1)
	go func() {
		defer func() { result <- recover() }()
		_, _ = agent.Prompt(context.Background(), request)
	}()

	return result
}

func TestSettlementPanicsRetainFailureAndExactTerminalRetry(t *testing.T) {
	t.Setenv("AMP_API_KEY", "settlement-panic")

	t.Run("replace", func(t *testing.T) {
		store := &panicOnceReplaceStore{
			SessionStore: NewInMemorySessionStore(),
			entered:      make(chan struct{}),
			release:      make(chan struct{}),
			value:        "settlement replace panic",
		}
		client := &lifecycleClient{}
		agent := NewAgent(testContainmentOptions([]Option{
			WithExecutablePath(lifecycleHarness(t)),
			WithScratchDir(testScratchDir(t)),
			WithSessionStore(store),
		})...)
		agent.setConnection(client)
		_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
		require.NoError(t, err)
		created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		require.NoError(t, err)
		_, err = agent.Prompt(t.Context(), lifecyclePrompt(created.SessionId, "seed", "sub-seed", "nonce-seed"))
		require.NoError(t, err)
		before := len(client.eventTypes(t))

		session, err := agent.session(created.SessionId)
		require.NoError(t, err)
		store.armed.Store(true)
		recovered := promptRecovered(agent, lifecyclePrompt(created.SessionId, "second", "sub-panic", "nonce-panic"))
		select {
		case <-store.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("settlement Replace did not reach panic barrier")
		}
		state := session.activePromptState()
		require.NotNil(t, state)
		close(store.release)
		select {
		case got := <-recovered:
			require.Equal(t, "settlement replace panic", got)
		case <-time.After(2 * time.Second):
			t.Fatal("panicking prompt did not unwind")
		}
		settlement := state.awaitSettlement(t.Context())
		require.ErrorIs(t, settlement.commitErr, errAgentGoroutinePanic)
		require.Nil(t, settlement.deliveryErr)
		require.True(t, session.hasPendingTerminal())
		require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update"}, client.eventTypes(t)[before:])
		agent.mu.Lock()
		require.Same(t, session, agent.sessions[created.SessionId])
		agent.mu.Unlock()

		_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
		require.NoError(t, err)
		require.Equal(t,
			[]string{"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update"},
			client.eventTypes(t)[before:],
		)
		require.NoError(t, agent.Close())
	})

	t.Run("terminal delivery", func(t *testing.T) {
		client := &panicIdleLifecycleClient{}
		agent := NewAgent(testContainmentOptions([]Option{
			WithExecutablePath(lifecycleHarness(t)),
			WithScratchDir(testScratchDir(t)),
		})...)
		agent.setConnection(client)
		_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
		require.NoError(t, err)
		created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		require.NoError(t, err)
		session, err := agent.session(created.SessionId)
		require.NoError(t, err)
		client.session = session
		client.armed.Store(true)

		recovered := promptRecovered(agent, lifecyclePrompt(created.SessionId, "panic idle", "sub-idle", "nonce-idle"))
		select {
		case got := <-recovered:
			require.Equal(t, "terminal lifecycle delivery panic", got)
		case <-time.After(2 * time.Second):
			t.Fatal("terminal delivery panic did not unwind")
		}
		require.NotNil(t, client.state)
		settlement := client.state.awaitSettlement(t.Context())
		require.ErrorIs(t, settlement.deliveryErr, errAgentGoroutinePanic)
		require.Nil(t, settlement.commitErr)
		require.True(t, session.hasPendingTerminal())
		require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update"}, client.eventTypes(t))

		_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
		require.NoError(t, err)
		require.Equal(t,
			[]string{"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update"},
			client.eventTypes(t),
		)
		require.NoError(t, agent.Close())
	})
}

func TestPromptPanicsContainBeforeActiveHandleClears(t *testing.T) {
	t.Setenv("AMP_API_KEY", "prompt-panic")

	for _, stage := range []string{"launch", "acceptance", "client", "raw", "timer", "settle"} {
		t.Run(stage, func(t *testing.T) {
			client := &panicPromptClient{stage: stage}
			options := []Option{
				WithExecutablePath(lifecycleHarness(t)),
				WithScratchDir(testScratchDir(t)),
			}
			if stage == "timer" {
				options = append(options, WithTurnTimeout(time.Hour))
			}
			agent := NewAgent(testContainmentOptions(options)...)
			agent.setConnection(client)
			_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
			require.NoError(t, err)
			requestOptions := []SessionRequestOption{}
			if stage == "raw" {
				requestOptions = append(requestOptions, WithSessionRawEvents(true))
			}
			created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir(), requestOptions...))
			require.NoError(t, err)
			session, err := agent.session(created.SessionId)
			require.NoError(t, err)
			client.session = session

			originalSettle := agent.options.runtime.settleTurn
			containmentEntered := make(chan struct{})
			containmentRelease := make(chan struct{})
			var settleCalls atomic.Int64
			var enteredOnce sync.Once
			agent.options.runtime.settleTurn = func(turn *nativeamp.Turn) (nativeamp.ContainmentProof, error) {
				if stage == "settle" && settleCalls.Add(1) == 1 {
					client.state = session.activePromptState()
					panic("prompt settle panic")
				}

				enteredOnce.Do(func() { close(containmentEntered) })
				awaitCorrectionCallback(t, containmentRelease, "prompt containment release")

				return originalSettle(turn)
			}
			if stage == "launch" {
				agent.options.runtime.executeThread = func(context.Context, *nativeamp.Client, any) (*nativeamp.Turn, error) {
					client.state = session.activePromptState()
					panic("prompt launch panic")
				}
			}
			if stage == "timer" {
				agent.options.runtime.newTurnTimer = func(time.Duration) (<-chan time.Time, func()) {
					client.state = session.activePromptState()
					panic("prompt timer panic")
				}
			}

			recovered := promptRecovered(agent, lifecyclePrompt(created.SessionId, "panic", "sub-"+stage, "nonce-"+stage))
			if stage != "launch" {
				select {
				case <-containmentEntered:
				case <-time.After(2 * time.Second):
					t.Fatal("panic recovery did not enter exact native containment")
				}
				require.NotNil(t, client.state)
				require.Same(t, client.state, session.activePromptState())
				require.False(t, client.state.settled())
				close(containmentRelease)
			}

			select {
			case got := <-recovered:
				require.Equal(t, "prompt "+stage+" panic", got)
			case <-time.After(2 * time.Second):
				t.Fatal("panicking prompt did not unwind")
			}
			require.NotNil(t, client.state)
			require.Nil(t, session.activePromptState())
			settlement := client.state.awaitSettlement(t.Context())
			require.ErrorIs(t, settlement.err(), errAgentGoroutinePanic)
			if stage == "launch" {
				require.ErrorIs(t, settlement.containmentErr, nativeamp.ErrProcessContainmentIncomplete)
			}

			_, closeErr := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
			require.Error(t, closeErr)
			agent.mu.Lock()
			require.Same(t, session, agent.sessions[created.SessionId])
			agent.mu.Unlock()
			require.Error(t, agent.Close())
			agent.mu.Lock()
			require.Empty(t, agent.sessions)
			agent.mu.Unlock()
		})
	}
}

func (s *panicOnceReplaceStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	if s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		panic(s.value)
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

type recoveredCall struct {
	err       error
	recovered any
}

func closeWithRecovery(agent *Agent, result chan<- recoveredCall) {
	call := recoveredCall{}
	func() {
		defer func() { call.recovered = recover() }()
		call.err = agent.Close()
	}()
	result <- call
}

func TestPanickingReplaceSettlesAgentCloseAsOneMemoizedLastWord(t *testing.T) {
	panicValue := "replace panic"
	store := &panicOnceReplaceStore{
		SessionStore: NewInMemorySessionStore(),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
		value:        panicValue,
	}
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	session := installActiveTestSession(t, agent, "T-panic-replace-close")
	session.retainUnsynced([]SessionStoreEntry{json.RawMessage(`{"type":"result"}`)})
	store.armed.Store(true)

	results := make(chan recoveredCall, 1)
	go closeWithRecovery(agent, results)
	awaitCorrectionSignal(t, store.entered, "panicking Replace entry")
	second := agent.Close()
	requireClosedCallbackRefusal(t, second)
	close(store.release)

	first := receiveCorrection(t, results, "memoized Agent.Close result")
	require.Nil(t, first.recovered)
	require.ErrorIs(t, first.err, errAgentGoroutinePanic)
	require.EqualError(t, agent.Close(), first.err.Error())
	agent.mu.Lock()
	require.Empty(t, agent.sessions)
	require.Empty(t, agent.cleanupOwners)
	agent.mu.Unlock()
}

func waitForSessionTeardownJoiner(t *testing.T, session *agentSession) {
	t.Helper()
	require.Eventually(t, func() bool {
		session.teardownMu.Lock()
		flight := session.teardownFlight
		joined := flight != nil && flight.waiters > 0
		session.teardownMu.Unlock()

		return joined
	}, 2*time.Second, time.Millisecond, "second session teardown did not join the published flight")
}

func TestPanickingScratchReleaseReleasesSessionTeardownForJoinAndRetry(t *testing.T) {
	panicValue := "scratch release panic"
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	agent := newTestAgent(
		WithScratchDir(testScratchDir(t)),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				return func() {
					if calls.Add(1) == 1 {
						close(entered)
						awaitCorrectionCallback(t, release, "scratch-release panic barrier")
						panic(panicValue)
					}
				}, nil
			},
		}),
	)
	session := installActiveTestSession(t, agent, "T-panic-scratch-release")

	type sessionResult struct {
		err       error
		recovered any
	}
	closeSession := func(results chan<- sessionResult) {
		result := sessionResult{}
		func() {
			defer func() { result.recovered = recover() }()
			result.err = session.Close(t.Context())
		}()
		results <- result
	}
	results := make(chan sessionResult, 2)
	go closeSession(results)
	awaitCorrectionSignal(t, entered, "scratch-release callback entry")
	go closeSession(results)
	waitForSessionTeardownJoiner(t, session)
	close(release)

	first := receiveCorrection(t, results, "first session-close result")
	second := receiveCorrection(t, results, "joined session-close result")
	if first.recovered == nil {
		first, second = second, first
	}
	require.Equal(t, panicValue, first.recovered)
	require.Nil(t, second.recovered)
	requireClosedCallbackRefusal(t, second.err)
	require.NoError(t, session.Close(t.Context()))
	require.Equal(t, int64(1), calls.Load())

	agent.mu.Lock()
	delete(agent.sessions, session.id)
	agent.mu.Unlock()
	agent.observe.AddActiveSession(t.Context(), -1)
	require.NoError(t, agent.Close())
}

func TestPanickingRuntimeSnapshotCallbacksResetPublisherForRetry(t *testing.T) {
	t.Run("inventory", func(t *testing.T) {
		var calls atomic.Int64
		var snapshots []int
		tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
			ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, count int) {
				snapshots = append(snapshots, count)
			},
		}, true)
		root := tracker.start(t.Context(), func() (int, bool) {
			switch calls.Add(1) {
			case 1:
				return 1, true
			case 2:
				panic("inventory panic")
			default:
				return 2, true
			}
		})

		var recovered any
		func() {
			defer func() { recovered = recover() }()
			root.refresh(t.Context())
		}()
		require.Equal(t, "inventory panic", recovered)
		root.refresh(t.Context())
		require.Equal(t, []int{1, 2}, snapshots)
		tracker.mu.Lock()
		require.False(t, tracker.publishing)
		tracker.mu.Unlock()
	})

	t.Run("publication", func(t *testing.T) {
		var count atomic.Int64
		count.Store(1)
		armed := false
		var snapshots []int
		tracker := newProviderProcessSnapshotTracker(RuntimeResourceHooks{
			ObserveProcessSnapshot: func(_ context.Context, _ RuntimeProcessKind, current int) {
				if armed {
					armed = false
					panic("snapshot panic")
				}
				snapshots = append(snapshots, current)
			},
		}, true)
		root := tracker.start(t.Context(), func() (int, bool) { return int(count.Load()), true })
		count.Store(2)
		armed = true

		var recovered any
		func() {
			defer func() { recovered = recover() }()
			root.refresh(t.Context())
		}()
		require.Equal(t, "snapshot panic", recovered)
		root.refresh(t.Context())
		require.Equal(t, []int{1, 2}, snapshots)
		tracker.mu.Lock()
		require.False(t, tracker.publishing)
		tracker.mu.Unlock()
	})
}

func waitForPersistenceState(t *testing.T, session *agentSession, want sessionPersistenceState) {
	t.Helper()
	require.Eventually(t, func() bool {
		session.persistMu.Lock()
		got := session.persistState
		session.persistMu.Unlock()

		return got == want
	}, 2*time.Second, time.Millisecond, "persistence state did not reach %d", want)
}

func TestCancelledDeleteWaitRollsBackItsExactPersistenceFence(t *testing.T) {
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
	session := installActiveTestSession(t, agent, "T-delete-cancelled-writer")
	require.NoError(t, session.persistAfterTurn(t.Context(), nil))
	_, persistence, err := session.beginPersistence(t.Context(), sessionPersistenceOrdinary)
	require.NoError(t, err)

	deleteCtx, cancelDelete := context.WithCancel(t.Context())
	deleted := make(chan error, 1)
	go func() {
		_, deleteErr := agent.UnstableDeleteSession(deleteCtx, DeleteSessionRequest(session.id))
		deleted <- deleteErr
	}()
	waitForPersistenceState(t, session, sessionPersistenceDeleting)
	cancelDelete()
	require.ErrorIs(t, receiveCorrection(t, deleted, "cancelled delete result"), context.Canceled)

	session.persistMu.Lock()
	require.Equal(t, sessionPersistenceOpen, session.persistState)
	session.persistMu.Unlock()
	session.finishPersistence(persistence)

	_, err = agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(session.id, configMode, modeUltra))
	require.NoError(t, err)
	main, err := store.Load(t.Context(), SessionKey{SessionID: string(session.id), Subpath: SessionStoreMainSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, main)
	require.NoError(t, agent.Close())
}

func TestStoredDeleteAuthorityRequiresMatchingKeyAndAbsoluteCwd(t *testing.T) {
	for _, test := range []struct {
		name       string
		key        string
		manifestID string
		cwd        string
	}{
		{name: "mismatched session id", key: "T-key-A", manifestID: "T-native-B", cwd: "absolute"},
		{name: "relative cwd", key: "T-key-cwd", manifestID: "T-key-cwd", cwd: "relative"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path, state := fakeAgentAmpPath(t, "")
			store := NewInMemorySessionStore()
			cwd := test.cwd
			if cwd == "absolute" {
				cwd = t.TempDir()
			}
			manifest, err := json.Marshal(ampManifest{
				Format:             SessionStoreFormat,
				SessionID:          test.manifestID,
				NativeSessionID:    "T-native-B",
				Cwd:                cwd,
				Mode:               modeMedium,
				CreatedAtUnixMilli: 1,
				UpdatedAtUnixMilli: 2,
			})
			require.NoError(t, err)
			main := SessionKey{SessionID: test.key, Subpath: SessionStoreMainSubpath}
			require.NoError(t, store.Replace(t.Context(), main, []SessionStoreReplacement{{Key: main, Entries: []SessionStoreEntry{manifest}}}))
			agent := newTestAgent(
				WithExecutablePath(path),
				WithSessionStore(store),
				WithScratchDir(testScratchDir(t)),
			)

			_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(acp.SessionId(test.key)))
			require.NoError(t, err)
			entries, err := store.Load(t.Context(), main)
			require.NoError(t, err)
			require.Empty(t, entries)
			records := make([][]string, 0)
			argsPath := filepath.Join(state, "args.jsonl")
			if _, statErr := os.Stat(argsPath); statErr == nil {
				records = readHelperJSON[[]string](t, argsPath)
			} else {
				require.True(t, os.IsNotExist(statErr))
			}
			require.False(t, slicesContainCommand(records, "threads", "delete", "T-native-B"))
			require.NoError(t, agent.Close())
		})
	}
}

func TestEveryEmbeddedACPMethodLosesAfterAgentClosePublication(t *testing.T) {
	agent := newTestAgent(WithScratchDir(testScratchDir(t)))
	session := installActiveTestSession(t, agent, "T-agent-call-gate")
	_, finishAdmitted, err := agent.beginAgentCall(t.Context(), session.id)
	require.NoError(t, err)
	closed := make(chan error, 1)
	go func() { closed <- agent.Close() }()
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		published := agent.closed
		agent.mu.Unlock()

		return published
	}, 2*time.Second, time.Millisecond, "Agent.Close did not publish its admission fence")

	calls := []struct {
		name string
		call func() error
	}{
		{name: "initialize", call: func() error {
			_, err := agent.Initialize(t.Context(), acp.InitializeRequest{})

			return err
		}},
		{name: "authenticate", call: func() error {
			_, err := agent.Authenticate(t.Context(), acp.AuthenticateRequest{})

			return err
		}},
		{name: "logout", call: func() error {
			_, err := agent.Logout(t.Context(), acp.LogoutRequest{})

			return err
		}},
		{name: "extension", call: func() error {
			_, err := agent.HandleExtensionMethod(t.Context(), "_test/closed", nil)

			return err
		}},
		{name: "new", call: func() error {
			_, err := agent.NewSession(t.Context(), acp.NewSessionRequest{})

			return err
		}},
		{name: "load", call: func() error {
			_, err := agent.LoadSession(t.Context(), acp.LoadSessionRequest{})

			return err
		}},
		{name: "resume", call: func() error {
			_, err := agent.ResumeSession(t.Context(), acp.ResumeSessionRequest{})

			return err
		}},
		{name: "list", call: func() error {
			_, err := agent.ListSessions(t.Context(), acp.ListSessionsRequest{})

			return err
		}},
		{name: "prompt", call: func() error {
			_, err := agent.Prompt(t.Context(), acp.PromptRequest{})

			return err
		}},
		{name: "cancel", call: func() error { return agent.Cancel(t.Context(), acp.CancelNotification{}) }},
		{name: "close", call: func() error {
			_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{})

			return err
		}},
		{name: "delete", call: func() error {
			_, err := agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{})

			return err
		}},
		{name: "config", call: func() error {
			_, err := agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{})

			return err
		}},
		{name: "mode", call: func() error {
			_, err := agent.SetSessionMode(t.Context(), acp.SetSessionModeRequest{})

			return err
		}},
	}

	start := make(chan struct{})
	results := make(chan struct {
		name string
		err  error
	}, len(calls))
	for _, call := range calls {
		go func() {
			if !awaitCorrectionCallback(t, start, "closed-method start") {
				return
			}
			results <- struct {
				name string
				err  error
			}{name: call.name, err: call.call()}
		}()
	}
	close(start)
	for range calls {
		result := receiveCorrection(t, results, "closed-method result")
		require.Error(t, result.err, result.name)
		requireRequestErrorCode(t, result.err, -32600)
	}

	finishAdmitted(context.Canceled)
	require.NoError(t, receiveCorrection(t, closed, "Agent.Close completion"))
}

func TestDeleteNativeFailureRemainsPermanentAfterShutdownCleanup(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "delete-fail-twice")
	store := NewInMemorySessionStore()
	agent := newTestAgent(
		WithExecutablePath(path),
		WithSessionStore(store),
		WithScratchDir(testScratchDir(t)),
	)
	session := installActiveTestSession(t, agent, "T-delete-fails-twice")
	session.mu.Lock()
	session.nativeID = "T-native-delete-fails-twice"
	session.mu.Unlock()
	require.NoError(t, session.persistAfterTurn(t.Context(), nil))

	_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(session.id))
	require.Error(t, err)
	require.ErrorIs(t, agent.Close(), errNativeDeleteOpen)
	session.mu.Lock()
	require.True(t, session.scratchDone)
	require.False(t, session.nativeDeleteDone)
	require.False(t, session.deleteDone)
	session.mu.Unlock()

	retainedErr := agent.Close()
	require.ErrorIs(t, retainedErr, errNativeDeleteOpen)
	session.mu.Lock()
	require.False(t, session.nativeDeleteDone)
	require.False(t, session.deleteDone)
	require.ErrorIs(t, session.nativeDeleteErr, errNativeDeleteOpen)
	session.mu.Unlock()
	records := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	deleteCalls := 0
	for _, args := range records {
		if slicesContainCommand([][]string{args}, "threads", "delete", "T-native-delete-fails-twice") {
			deleteCalls++
		}
	}
	require.Equal(t, 2, deleteCalls)
}

func TestExternalCallbackLeaveIsIdempotent(t *testing.T) {
	agent := newTestAgent()
	ctx := withCallbackProvenance(t.Context(), agent, &agentCallbackGeneration{})
	leave := enterExternalCallback(ctx)
	require.True(t, agent.hasActiveCallbackAuthority())
	leave()
	leave()
	require.False(t, agent.hasActiveCallbackAuthority())
}

func TestPublishedFlightPanicAndDefensiveCompletionBranches(t *testing.T) {
	t.Run("wire close panic releases both flights", func(t *testing.T) {
		panicValue := "wire close replace panic"
		store := &panicOnceReplaceStore{
			SessionStore: NewInMemorySessionStore(),
			entered:      make(chan struct{}),
			release:      make(chan struct{}),
			value:        panicValue,
		}
		agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
		session := installActiveTestSession(t, agent, "T-wire-close-panic")
		session.retainUnsynced([]SessionStoreEntry{json.RawMessage(`{"type":"result"}`)})
		store.armed.Store(true)

		var recovered any
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer func() { recovered = recover() }()
			_, _ = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
		}()
		awaitCorrectionSignal(t, store.entered, "wire-close Replace entry")
		close(store.release)
		awaitCorrectionSignal(t, done, "wire-close panic completion")
		require.Equal(t, panicValue, recovered)
		agent.mu.Lock()
		require.Nil(t, agent.sessionFlights[session.id])
		require.Same(t, session, agent.sessions[session.id])
		agent.mu.Unlock()
		session.teardownMu.Lock()
		require.Nil(t, session.teardownFlight)
		session.teardownMu.Unlock()
		require.NoError(t, func() error {
			_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})

			return err
		}())
		require.NoError(t, agent.Close())
	})

	t.Run("session flight panic joins", func(t *testing.T) {
		agent := newTestAgent()
		id := acp.SessionId("T-flight-panic-join")
		panicErr := closedCallbackRefusal()
		flight := &agentSessionFlight{generation: 1, done: make(chan struct{}), panicErr: panicErr}
		close(flight.done)
		agent.sessionFlights[id] = flight
		_, _, err := agent.beginSessionFlight(t.Context(), id, agentSessionDeleteFlight, nil)
		require.ErrorIs(t, err, errSessionClosed)
		delete(agent.sessionFlights, id)

		agent.finishSessionFlightWithPanic(id, nil, panicErr)
		stale := &agentSessionFlight{generation: 2, done: make(chan struct{})}
		agent.finishSessionFlightWithPanic(id, stale, panicErr)
	})

	t.Run("stale agent close completion", func(t *testing.T) {
		agent := newTestAgent()
		current := &agentCloseFlight{done: make(chan struct{})}
		agent.closeFlight = current
		agent.finishCloseFlight(&agentCloseFlight{done: make(chan struct{})}, nil)
		require.Same(t, current, agent.closeFlight)
		agent.finishCloseFlight(current, nil)
	})

	t.Run("nil session teardown completion", func(t *testing.T) {
		session := &agentSession{}
		session.finishTeardownWithPanic(nil, closedCallbackRefusal())
	})

	t.Run("invalid cold delete cwd constructs nothing", func(t *testing.T) {
		agent := newTestAgent()
		flight := &agentSessionFlight{generation: 1, done: make(chan struct{})}
		owner, active, err := agent.prepareDeleteOwner(t.Context(), "T-invalid-cold-cwd", flight, nil, false, ampManifest{
			Format:          SessionStoreFormat,
			SessionID:       "T-invalid-cold-cwd",
			NativeSessionID: "T-native-invalid-cwd",
			Cwd:             "relative",
		}, true)
		require.NoError(t, err)
		require.Nil(t, owner)
		require.False(t, active)
	})
}

func TestCancelledCloseWaitsReleaseEveryCloseFence(t *testing.T) {
	for _, path := range []string{"direct", "shutdown", "wire"} {
		t.Run(path, func(t *testing.T) {
			store := newGatedStore(nil)
			agent := newTestAgent(WithSessionStore(store), WithScratchDir(testScratchDir(t)))
			session, err := newAgentSession(t.Context(), agent, acp.SessionId("T-cancel-close-"+path), t.TempDir(), parsedSessionMeta{}, "", nil)
			require.NoError(t, err)
			if path == "wire" {
				agent.mu.Lock()
				agent.activateSessionLocked(session)
				agent.mu.Unlock()
				agent.observe.AddActiveSession(t.Context(), 1)
			}
			require.NoError(t, session.persistAfterTurn(t.Context(), nil))
			_, persistence, persistErr := session.beginPersistence(t.Context(), sessionPersistenceOrdinary)
			require.NoError(t, persistErr)
			closeCtx, cancelClose := context.WithCancel(t.Context())
			closed := make(chan error, 1)
			go func() {
				switch path {
				case "direct":
					closed <- session.Close(closeCtx)
				case "shutdown":
					closed <- session.closeAtShutdown(closeCtx)
				case "wire":
					_, closeErr := agent.CloseSession(closeCtx, acp.CloseSessionRequest{SessionId: session.id})
					closed <- closeErr
				}
			}()
			waitForPersistenceState(t, session, sessionPersistenceClosing)
			cancelClose()
			require.ErrorIs(t, receiveCorrection(t, closed, "cancelled close result"), context.Canceled)
			session.finishPersistence(persistence)

			switch path {
			case "direct":
				require.NoError(t, session.Close(t.Context()))
				agent.clearCleanupOwner(session.id, session)
			case "shutdown":
				require.NoError(t, session.closeAtShutdown(t.Context()))
				agent.clearCleanupOwner(session.id, session)
			case "wire":
				_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
				require.NoError(t, err)
			}
			require.NoError(t, agent.Close())
		})
	}
}
