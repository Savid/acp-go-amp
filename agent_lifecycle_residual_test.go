package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

type valueBarrierContext struct {
	t        *testing.T
	deadline func() (time.Time, bool)
	done     <-chan struct{}
	err      func() error
	value    func(any) any
	once     sync.Once
	entered  chan struct{}
	release  chan struct{}
}

type lifecycleMutationStore struct {
	SessionStore
	loadErr      error
	afterLoad    func(SessionKey)
	afterReplace func()
	afterDelete  func()
}

type persistenceOwnershipMutationStore struct {
	SessionStore
	mutate func()
}

func (s *persistenceOwnershipMutationStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	if err := s.SessionStore.Replace(ctx, key, replacements); err != nil {
		return err
	}
	if s.mutate != nil {
		s.mutate()
	}

	return nil
}

func (s *lifecycleMutationStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	entries, err := s.SessionStore.Load(ctx, key)
	if s.loadErr != nil {
		err = s.loadErr
	}
	if s.afterLoad != nil {
		s.afterLoad(key)
	}

	return entries, err
}

func (s *lifecycleMutationStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	err := s.SessionStore.Replace(ctx, key, replacements)
	if s.afterReplace != nil {
		s.afterReplace()
	}

	return err
}

func (s *lifecycleMutationStore) Delete(ctx context.Context, key SessionKey) error {
	err := s.SessionStore.Delete(ctx, key)
	if s.afterDelete != nil {
		s.afterDelete()
	}

	return err
}

func newValueBarrierContext(t *testing.T) *valueBarrierContext {
	t.Helper()

	ctx := t.Context()

	return &valueBarrierContext{
		t:        t,
		deadline: ctx.Deadline,
		done:     ctx.Done(),
		err:      ctx.Err,
		value:    ctx.Value,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (c *valueBarrierContext) Deadline() (time.Time, bool) { return c.deadline() }
func (c *valueBarrierContext) Done() <-chan struct{}       { return c.done }
func (c *valueBarrierContext) Err() error                  { return c.err() }

func (c *valueBarrierContext) Value(key any) any {
	c.once.Do(func() { close(c.entered) })
	select {
	case <-c.release:
	case <-time.After(correctionBarrierTimeout):
		c.t.Errorf("timed out waiting for value-context release")
	}

	return c.value(key)
}

func TestSessionUseGenerationDefensivePaths(t *testing.T) {
	agent := newTestAgent()
	id := acp.SessionId("T-use-defensive")
	session := &agentSession{agent: agent, id: id}
	other := &agentSession{agent: agent, id: id}

	flight := &agentSessionFlight{generation: 1, done: make(chan struct{})}
	agent.sessionFlights[id] = flight
	_, _, err := agent.beginSessionUse(withCallbackProvenance(t.Context(), agent, flight), id)
	requireClosedCallbackRefusal(t, err)
	delete(agent.sessionFlights, id)

	existing := &agentSessionUse{generation: 2, done: make(chan struct{})}
	agent.sessionUses[id] = existing
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = agent.beginSessionUse(cancelled, id)
	require.ErrorIs(t, err, context.Canceled)
	delete(agent.sessionUses, id)

	existing = &agentSessionUse{generation: 3, done: make(chan struct{})}
	agent.sessionUses[id] = existing
	barrier := newValueBarrierContext(t)
	result := make(chan *agentSessionUse, 1)
	go func() {
		_, use, useErr := agent.beginSessionUse(barrier, id)
		require.NoError(t, useErr)
		result <- use
	}()
	awaitCorrectionSignal(t, barrier.entered, "session-use context barrier")
	agent.finishSessionUse(id, existing)
	close(barrier.release)
	use := receiveCorrection(t, result, "successor session use")
	agent.finishSessionUse(id, use)

	agent.cleanupOwners[id] = []agentCleanupOwner{{session: session, kind: agentCleanupPrepared}}
	agent.sessions[id] = session
	_, use, err = agent.beginSessionUse(t.Context(), id)
	require.NoError(t, err)
	require.Empty(t, agent.cleanupOwners[id])
	agent.finishSessionUse(id, use)
	delete(agent.sessions, id)

	agent.cleanupOwners[id] = []agentCleanupOwner{{session: session, kind: agentCleanupPrepared}}
	_, _, err = agent.beginSessionUse(t.Context(), id)
	require.Error(t, err)
	delete(agent.cleanupOwners, id)

	require.False(t, agent.bindSessionUse(id, nil, session))
	use = &agentSessionUse{generation: 4, session: session, done: make(chan struct{})}
	agent.sessionUses[id] = use
	require.NoError(t, agent.validateSessionUse(id, use, session))
	require.Error(t, agent.validateSessionUse(id, nil, nil))

	agent.deleted[id] = struct{}{}
	require.Error(t, agent.validateSessionUse(id, use, session))
	delete(agent.deleted, id)

	agent.sessionFlights[id] = &agentSessionFlight{generation: 5, use: &agentSessionUse{}}
	require.Error(t, agent.validateSessionUse(id, use, session))
	agent.sessionFlights[id] = &agentSessionFlight{generation: 6, use: use, session: other}
	require.Error(t, agent.validateSessionUse(id, use, session))
	delete(agent.sessionFlights, id)

	agent.sessions[id] = other
	require.Error(t, agent.validateSessionUse(id, use, session))
	delete(agent.sessions, id)
	agent.finishSessionUse(id, use)
}

func TestSessionFlightGenerationDefensivePaths(t *testing.T) {
	agent := newTestAgent()
	id := acp.SessionId("T-flight-defensive")
	session := &agentSession{agent: agent, id: id}
	other := &agentSession{agent: agent, id: id}

	existing := &agentSessionFlight{generation: 1, done: make(chan struct{})}
	agent.sessionFlights[id] = existing
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := agent.beginSessionFlight(cancelled, id, agentSessionDeleteFlight, nil)
	require.ErrorIs(t, err, context.Canceled)
	delete(agent.sessionFlights, id)

	existing = &agentSessionFlight{generation: 2, done: make(chan struct{})}
	agent.sessionFlights[id] = existing
	barrier := newValueBarrierContext(t)
	result := make(chan *agentSessionFlight, 1)
	go func() {
		_, flight, flightErr := agent.beginSessionFlight(barrier, id, agentSessionDeleteFlight, nil)
		require.NoError(t, flightErr)
		result <- flight
	}()
	awaitCorrectionSignal(t, barrier.entered, "session-flight context barrier")
	agent.finishSessionFlight(id, existing)
	close(barrier.release)
	created := receiveCorrection(t, result, "successor session flight")
	agent.finishSessionFlight(id, created)

	require.True(t, agent.contextOwnsSessionFlightDependency(withCallbackProvenance(t.Context(), agent, existing), existing))
	use := &agentSessionUse{generation: 3, done: make(chan struct{})}
	existing.use = use
	require.True(t, agent.contextOwnsSessionFlightDependency(withCallbackProvenance(t.Context(), agent, use), existing))
	existing.use = nil
	require.False(t, agent.contextOwnsSessionFlightDependency(t.Context(), existing))

	teardown := &sessionTeardownFlight{generation: 1, done: make(chan struct{})}
	session.teardownFlight = teardown
	existing.session = session
	require.True(t, agent.contextOwnsSessionFlightDependency(withCallbackProvenance(t.Context(), agent, teardown), existing))
	session.teardownFlight = nil
	require.False(t, agent.contextOwnsSessionFlightDependency(t.Context(), existing))

	closedUse := &agentSessionUse{generation: 4, session: session, done: make(chan struct{})}
	close(closedUse.done)
	joinFlight := &agentSessionFlight{generation: 5, use: closedUse, done: make(chan struct{})}
	agent.sessionFlights[id] = joinFlight
	require.NoError(t, agent.joinSessionFlightUse(t.Context(), id, joinFlight, closedUse))
	require.Equal(t, session, joinFlight.session)

	agent.sessionFlights[id] = &agentSessionFlight{generation: 6, done: make(chan struct{})}
	require.Error(t, agent.joinSessionFlightUse(t.Context(), id, joinFlight, closedUse))

	joinFlight = &agentSessionFlight{generation: 7, session: session, done: make(chan struct{})}
	agent.sessionFlights[id] = joinFlight
	agent.sessions[id] = other
	require.Error(t, agent.joinSessionFlightUse(t.Context(), id, joinFlight, closedUse))
	delete(agent.sessions, id)

	joinFlight = &agentSessionFlight{generation: 8, done: make(chan struct{})}
	agent.sessionFlights[id] = joinFlight
	closedUse.session = nil
	require.NoError(t, agent.joinSessionFlightUse(t.Context(), id, joinFlight, closedUse))
	require.Nil(t, joinFlight.session)
	agent.finishSessionFlight(id, nil)
	agent.finishSessionFlight(id, joinFlight)

	waitUse := &agentSessionUse{generation: 9, done: make(chan struct{})}
	waitFlight := &agentSessionFlight{generation: 10, use: waitUse, done: make(chan struct{})}
	agent.sessionFlights[id] = waitFlight
	cancelled, cancel = context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, agent.joinSessionFlightUse(cancelled, id, waitFlight, waitUse), context.Canceled)

	admitted := &agentSessionUse{generation: 11, done: make(chan struct{})}
	agent.sessionUses[id] = admitted
	cancelled, cancel = context.WithCancel(t.Context())
	cancel()
	_, _, err = agent.beginSessionFlight(cancelled, id, agentSessionDeleteFlight, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSessionTeardownAndPersistenceGenerationDefensivePaths(t *testing.T) {
	agent := newTestAgent()
	session := &agentSession{agent: agent, id: "T-wrapper-defensive", turn: make(chan struct{}, 1)}

	teardown := &sessionTeardownFlight{generation: 1, done: make(chan struct{})}
	session.teardownFlight = teardown
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := session.beginTeardown(cancelled)
	require.ErrorIs(t, err, context.Canceled)

	barrier := newValueBarrierContext(t)
	result := make(chan *sessionTeardownFlight, 1)
	go func() {
		_, flight, teardownErr := session.beginTeardown(barrier)
		require.NoError(t, teardownErr)
		result <- flight
	}()
	awaitCorrectionSignal(t, barrier.entered, "session-teardown context barrier")
	session.finishTeardown(teardown)
	close(barrier.release)
	created := receiveCorrection(t, result, "successor teardown flight")
	session.finishTeardown(created)

	prompt := newPromptTurnState()
	session.activePrompt = prompt
	teardown = &sessionTeardownFlight{generation: 3, done: make(chan struct{})}
	require.True(t, session.contextOwnsTeardownDependency(withCallbackProvenance(t.Context(), agent, prompt), teardown))
	session.activePrompt = nil

	persistence := &sessionPersistenceFlight{generation: 1, kind: sessionPersistenceOrdinary, done: make(chan struct{})}
	session.persistFlight = persistence
	_, _, err = session.beginPersistence(withCallbackProvenance(t.Context(), agent, persistence), sessionPersistenceOrdinary)
	requireClosedCallbackRefusal(t, err)

	cancelled, cancel = context.WithCancel(t.Context())
	cancel()
	_, _, err = session.beginPersistence(cancelled, sessionPersistenceOrdinary)
	require.ErrorIs(t, err, context.Canceled)

	barrier = newValueBarrierContext(t)
	persistResult := make(chan *sessionPersistenceFlight, 1)
	go func() {
		_, flight, persistErr := session.beginPersistence(barrier, sessionPersistenceOrdinary)
		require.NoError(t, persistErr)
		persistResult <- flight
	}()
	awaitCorrectionSignal(t, barrier.entered, "persistence context barrier")
	session.finishPersistence(persistence)
	close(barrier.release)
	newPersistence := receiveCorrection(t, persistResult, "successor persistence flight")
	session.finishPersistence(newPersistence)

	persistence = &sessionPersistenceFlight{generation: 4, done: make(chan struct{})}
	session.persistFlight = persistence
	cancelled, cancel = context.WithCancel(t.Context())
	cancel()
	_, err = session.installPersistenceFence(cancelled, sessionPersistenceClosing)
	require.ErrorIs(t, err, context.Canceled)
	session.finishPersistence(persistence)

	wrong := &sessionPersistenceFlight{generation: 5, done: make(chan struct{})}
	require.Error(t, session.persistOwned(t.Context(), wrong, nil))
	session.persistFlight = wrong
	session.agent.store = NewInMemorySessionStore()
	session.persistState = sessionPersistenceClosing
	_, _, err = session.beginPersistence(t.Context(), sessionPersistenceOrdinary)
	require.ErrorIs(t, err, errPersistenceFenced)
	session.persistState = sessionPersistenceDeleting
	_, _, err = session.beginPersistence(t.Context(), sessionPersistenceCloseRetry)
	require.ErrorIs(t, err, errPersistenceFenced)

	session.teardownFlight = teardown
	require.Error(t, session.Close(withCallbackProvenance(t.Context(), agent, teardown)))
}

func TestWrapperCloseFenceAndOwnershipDefensivePaths(t *testing.T) {
	agent := newTestAgent()

	newSession := func(id acp.SessionId) *agentSession {
		return &agentSession{
			agent:             agent,
			id:                id,
			turn:              make(chan struct{}, 1),
			closeBoundaryDone: true,
			closeCommitDone:   true,
		}
	}

	closeSession := newSession("T-close-fence")
	persistence := &sessionPersistenceFlight{generation: 1, done: make(chan struct{})}
	closeSession.persistFlight = persistence
	requireClosedCallbackRefusal(t, closeSession.Close(withCallbackProvenance(t.Context(), agent, persistence)))
	closeSession.persistFlight = nil

	shutdown := newSession("T-shutdown-fence")
	teardown := &sessionTeardownFlight{generation: 1, done: make(chan struct{})}
	shutdown.teardownFlight = teardown
	requireClosedCallbackRefusal(t, shutdown.closeAtShutdown(withCallbackProvenance(t.Context(), agent, teardown)))
	shutdown.teardownFlight = nil
	persistence = &sessionPersistenceFlight{generation: 2, done: make(chan struct{})}
	shutdown.persistFlight = persistence
	requireClosedCallbackRefusal(t, shutdown.closeAtShutdown(withCallbackProvenance(t.Context(), agent, persistence)))
	shutdown.persistFlight = nil

	deleting := newSession("T-delete-teardown")
	teardown = &sessionTeardownFlight{generation: 2, done: make(chan struct{})}
	deleting.teardownFlight = teardown
	requireClosedCallbackRefusal(t, deleting.Delete(withCallbackProvenance(t.Context(), agent, teardown)))

	id := acp.SessionId("T-remove-defensive")
	removing := newSession(id)
	agent.sessions[id] = removing
	existingFlight := &agentSessionFlight{generation: 3, session: removing, done: make(chan struct{})}
	agent.sessionFlights[id] = existingFlight
	requireClosedCallbackRefusal(t, agent.removeSession(withCallbackProvenance(t.Context(), agent, existingFlight), id, removing))
	delete(agent.sessionFlights, id)

	teardown = &sessionTeardownFlight{generation: 3, done: make(chan struct{})}
	removing.teardownFlight = teardown
	requireClosedCallbackRefusal(t, agent.removeSession(withCallbackProvenance(t.Context(), agent, teardown), id, removing))
	removing.teardownFlight = nil

	persistence = &sessionPersistenceFlight{generation: 3, done: make(chan struct{})}
	removing.persistFlight = persistence
	requireClosedCallbackRefusal(t, agent.removeSession(withCallbackProvenance(t.Context(), agent, persistence), id, removing))
	removing.persistFlight = nil

	runtimeErr := errors.New("terminal notification failed")
	removing.closeBoundary = closeSettlement{runtimeErr: runtimeErr}
	require.ErrorIs(t, agent.removeSession(t.Context(), id, removing), runtimeErr)
	removing.closeBoundary = closeSettlement{}

	replacement := newSession(id)
	removing.scratchDone = false
	removing.scratchRootRelease = func() { agent.sessions[id] = replacement }
	require.Error(t, agent.removeSession(t.Context(), id, removing))
	delete(agent.sessions, id)
}

func TestPersistenceRejectsPostCallbackOwnershipChange(t *testing.T) {
	base := NewInMemorySessionStore()
	store := &lifecycleMutationStore{SessionStore: base}
	agent := newTestAgent(WithSessionStore(store))
	session := &agentSession{agent: agent, id: "T-persist-callback", turn: make(chan struct{}, 1)}
	ctx, flight, err := session.beginPersistence(t.Context(), sessionPersistenceOrdinary)
	require.NoError(t, err)
	store.afterReplace = func() { session.finishPersistence(flight) }

	require.Error(t, session.persistOwned(ctx, flight, nil))
}

func TestDeleteGenerationValidationAfterExternalBoundaries(t *testing.T) {
	newDelete := func(id acp.SessionId, store SessionStore) (*Agent, *agentSession, *agentSessionFlight) {
		agent := newTestAgent(WithSessionStore(store))
		session := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
		flight := &agentSessionFlight{generation: 1, kind: agentSessionDeleteFlight, session: session, done: make(chan struct{})}
		agent.sessions[id] = session
		agent.sessionFlights[id] = flight

		return agent, session, flight
	}

	t.Run("snapshot", func(t *testing.T) {
		agent := newTestAgent()
		flight := &agentSessionFlight{generation: 1, done: make(chan struct{})}
		require.Error(t, agent.deleteSession(t.Context(), "T-delete-snapshot", flight))
		require.Error(t, agent.validateSessionFlight("T-delete-snapshot", flight, nil))
	})

	t.Run("manifest load", func(t *testing.T) {
		loadErr := errors.New("manifest load failed")
		store := &lifecycleMutationStore{SessionStore: NewInMemorySessionStore(), loadErr: loadErr}
		agent, session, flight := newDelete("T-delete-load", store)
		require.ErrorIs(t, agent.deleteSession(t.Context(), session.id, flight), loadErr)
		require.Equal(t, sessionPersistenceOpen, session.persistState)
	})

	t.Run("post manifest validation", func(t *testing.T) {
		store := &lifecycleMutationStore{SessionStore: NewInMemorySessionStore()}
		agent, session, flight := newDelete("T-delete-post-load", store)
		store.afterLoad = func(SessionKey) {
			agent.sessionFlights[session.id] = &agentSessionFlight{generation: 2, done: make(chan struct{})}
		}
		require.Error(t, agent.deleteSession(t.Context(), session.id, flight))
		require.Equal(t, sessionPersistenceOpen, session.persistState)
	})

	t.Run("post tombstone validation", func(t *testing.T) {
		store := &lifecycleMutationStore{SessionStore: NewInMemorySessionStore()}
		agent, session, flight := newDelete("T-delete-post-tombstone", store)
		store.afterDelete = func() { flight.session = &agentSession{agent: agent, id: session.id} }
		require.Error(t, agent.deleteSession(t.Context(), session.id, flight))
	})

	t.Run("post native delete validation", func(t *testing.T) {
		store := &lifecycleMutationStore{SessionStore: NewInMemorySessionStore()}
		agent, session, flight := newDelete("T-delete-post-owner", store)
		session.scratchRootRelease = func() { flight.session = &agentSession{agent: agent, id: session.id} }
		require.Error(t, agent.deleteSession(t.Context(), session.id, flight))
	})

	t.Run("tombstoned retry validation", func(t *testing.T) {
		agent := newTestAgent()
		id := acp.SessionId("T-delete-retry-validation")
		session := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
		flight := &agentSessionFlight{generation: 1, session: session, done: make(chan struct{})}
		agent.sessionFlights[id] = flight
		session.scratchRootRelease = func() { flight.session = &agentSession{agent: agent, id: id} }
		require.Error(t, agent.retryTombstonedDelete(t.Context(), id, flight, session))
	})
}

func TestConstructionOwnerReclamationBranches(t *testing.T) {
	agent := newTestAgent()
	id := acp.SessionId("T-construction-reclaim")
	except := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
	nonconstructing := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
	refused := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
	teardown := &sessionTeardownFlight{generation: 1, done: make(chan struct{})}
	refused.teardownFlight = teardown
	success := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
	agent.cleanupOwners[id] = []agentCleanupOwner{
		{session: except, kind: agentCleanupConstructing},
		{session: nonconstructing, kind: agentCleanupPrepared},
		{session: refused, kind: agentCleanupConstructing},
		{session: success, kind: agentCleanupConstructing},
	}

	err := agent.reclaimConstructionOwners(withCallbackProvenance(t.Context(), agent, teardown), id, except)
	require.Error(t, err)
	_, retained := agent.cleanupOwnerForSessionLocked(id, success)
	require.False(t, retained)
	_, retained = agent.cleanupOwnerForSessionLocked(id, &agentSession{})
	require.False(t, retained)
}

func TestRetryCleanupOwnerGenerationTransitions(t *testing.T) {
	t.Run("existing dependency", func(t *testing.T) {
		agent := newTestAgent()
		id := acp.SessionId("T-cleanup-existing")
		session := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
		agent.cleanupOwners[id] = []agentCleanupOwner{{session: session, kind: agentCleanupPrepared}}
		flight := &agentSessionFlight{generation: 1, session: session, done: make(chan struct{})}
		agent.sessionFlights[id] = flight
		requireClosedCallbackRefusal(t, agent.retryCleanupOwner(withCallbackProvenance(t.Context(), agent, flight), id))
	})

	t.Run("owner transferred during admitted use", func(t *testing.T) {
		agent := newTestAgent()
		id := acp.SessionId("T-cleanup-transfer")
		session := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
		agent.cleanupOwners[id] = []agentCleanupOwner{{session: session, kind: agentCleanupPrepared}}
		use := &agentSessionUse{generation: 1, session: session, done: make(chan struct{})}
		agent.sessionUses[id] = use
		result := make(chan error, 1)
		go func() { result <- agent.retryCleanupOwner(t.Context(), id) }()

		// The published flight proves retryCleanupOwner reached the exact-use join.
		require.Eventually(t, func() bool {
			agent.mu.Lock()
			flight := agent.sessionFlights[id]
			agent.mu.Unlock()

			return flight != nil
		}, 2*time.Second, time.Millisecond, "cleanup flight was not published")
		agent.mu.Lock()
		delete(agent.cleanupOwners, id)
		delete(agent.sessionUses, id)
		close(use.done)
		agent.mu.Unlock()
		require.NoError(t, receiveCorrection(t, result, "cleanup-owner retry"))
	})

	t.Run("prepared owner close", func(t *testing.T) {
		agent := newTestAgent()
		id := acp.SessionId("T-cleanup-close")
		session := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
		agent.cleanupOwners[id] = []agentCleanupOwner{{session: session, kind: agentCleanupPrepared}}
		require.NoError(t, agent.retryCleanupOwner(t.Context(), id))
	})
}

func TestPrepareDeleteOwnerLosingConstructionTransitions(t *testing.T) {
	originalWrite := writeFile
	originalRemove := removeSessionDir
	t.Cleanup(func() {
		writeFile = originalWrite
		removeSessionDir = originalRemove
	})

	for _, test := range []struct {
		name       string
		id         string
		winner     bool
		cleanupErr error
	}{
		{name: "winner", id: "winner", winner: true},
		{name: "winner cleanup refusal", id: "cleanup", winner: true, cleanupErr: errors.New("loser cleanup refused")},
		{name: "flight replaced", id: "replaced"},
	} {
		t.Run(test.name, func(t *testing.T) {
			id := acp.SessionId("T-prepare-delete-" + test.id)
			agent := newTestAgent(WithScratchDir(testScratchDir(t)))
			flight := &agentSessionFlight{generation: 1, done: make(chan struct{})}
			agent.sessionFlights[id] = flight
			winningSession := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
			mutated := false
			writeFile = func(path string, contents []byte, mode os.FileMode) error {
				err := originalWrite(path, contents, mode)
				if !mutated {
					mutated = true
					if test.winner || test.cleanupErr != nil {
						flight.session = winningSession
					} else {
						agent.sessionFlights[id] = &agentSessionFlight{generation: 2, done: make(chan struct{})}
					}
				}

				return err
			}
			removeSessionDir = originalRemove
			if test.cleanupErr != nil {
				removeSessionDir = func(string) error { return test.cleanupErr }
			}

			owner, active, err := agent.prepareDeleteOwner(
				t.Context(), id, flight, nil, false,
				ampManifest{NativeSessionID: "T-native", Cwd: t.TempDir(), Mode: modeMedium}, true,
			)
			require.False(t, active)
			if test.winner || test.cleanupErr != nil {
				require.Same(t, winningSession, owner)
				require.ErrorIs(t, err, test.cleanupErr)
			} else {
				require.Nil(t, owner)
				require.Error(t, err)
			}
		})
	}
}

func TestColdDeleteFenceRefusalRecoversPreparedOwner(t *testing.T) {
	originalWrite := writeFile
	t.Cleanup(func() { writeFile = originalWrite })

	base := NewInMemorySessionStore()
	id := acp.SessionId("T-cold-delete-fence")
	putStoredSession(t, base, string(id), t.TempDir(), nil)
	agent := newTestAgent(WithSessionStore(base), WithScratchDir(testScratchDir(t)))
	flight := &agentSessionFlight{generation: 1, kind: agentSessionDeleteFlight, done: make(chan struct{})}
	agent.sessionFlights[id] = flight
	persistence := &sessionPersistenceFlight{generation: 1, done: make(chan struct{})}
	close(persistence.done)

	writeFile = func(path string, contents []byte, mode os.FileMode) error {
		err := originalWrite(path, contents, mode)
		agent.mu.Lock()
		for _, owner := range agent.cleanupOwners[id] {
			if owner.kind == agentCleanupConstructing {
				owner.session.persistFlight = persistence
			}
		}
		agent.mu.Unlock()

		return err
	}

	err := agent.deleteSession(withCallbackProvenance(t.Context(), agent, persistence), id, flight)
	requireClosedCallbackRefusal(t, err)
}

func TestLoadGenerationValidationAtExternalBoundaries(t *testing.T) {
	t.Setenv("AMP_API_KEY", "load-generation-test")
	path, _ := fakeAgentAmpPath(t, "")

	t.Run("startup", func(t *testing.T) {
		agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
		id := acp.SessionId("T-load-startup-generation")
		agent.options.runtime.startupProbe = func(context.Context, *nativeamp.Client) (string, error) {
			agent.mu.Lock()
			agent.sessionUses[id] = &agentSessionUse{generation: 999, done: make(chan struct{})}
			agent.mu.Unlock()

			return path, nil
		}
		_, _, _, _, err := agent.loadOrResume(t.Context(), id, t.TempDir(), nil, nil, nil)
		require.Error(t, err)
		require.NoError(t, agent.Close())
	})

	t.Run("active before export", func(t *testing.T) {
		agent := newTestAgent()
		id := acp.SessionId("T-load-active-before-export")
		session := &agentSession{agent: agent, id: id, cwd: t.TempDir(), turn: make(chan struct{}, 1)}
		use := &agentSessionUse{generation: 1, session: session, done: make(chan struct{})}
		agent.sessionUses[id] = &agentSessionUse{generation: 2, session: session, done: make(chan struct{})}
		_, err := agent.loadActiveSession(t.Context(), id, use, session, parsedSessionMeta{}, session.cwd, "", nil)
		require.Error(t, err)
	})

	t.Run("active after export", func(t *testing.T) {
		agent := newTestAgent()
		id := acp.SessionId("T-load-active-after-export")
		session := &agentSession{agent: agent, id: id, nativeID: "T-native", cwd: t.TempDir(), turn: make(chan struct{}, 1)}
		use := &agentSessionUse{generation: 1, session: session, done: make(chan struct{})}
		agent.sessionUses[id] = use
		agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
			agent.mu.Lock()
			agent.sessionUses[id] = &agentSessionUse{generation: 2, session: session, done: make(chan struct{})}
			agent.mu.Unlock()

			return json.RawMessage(`{}`), nil
		}
		_, err := agent.loadActiveSession(t.Context(), id, use, session, parsedSessionMeta{}, session.cwd, "", nil)
		require.Error(t, err)
	})

	t.Run("active after transcript", func(t *testing.T) {
		base := NewInMemorySessionStore()
		store := &lifecycleMutationStore{SessionStore: base}
		agent := newTestAgent(WithSessionStore(store))
		id := acp.SessionId("T-load-active-after-transcript")
		session := &agentSession{agent: agent, id: id, cwd: t.TempDir(), turn: make(chan struct{}, 1)}
		use := &agentSessionUse{generation: 1, session: session, done: make(chan struct{})}
		agent.sessionUses[id] = use
		store.afterLoad = func(key SessionKey) {
			if key.Subpath == transcriptSubpath {
				agent.mu.Lock()
				agent.sessionUses[id] = &agentSessionUse{generation: 2, session: session, done: make(chan struct{})}
				agent.mu.Unlock()
			}
		}
		_, err := agent.loadActiveSession(t.Context(), id, use, session, parsedSessionMeta{}, session.cwd, "", nil)
		require.Error(t, err)
	})

	t.Run("cold before construction", func(t *testing.T) {
		base := NewInMemorySessionStore()
		id := acp.SessionId("T-load-cold-before-construction")
		cwd := t.TempDir()
		putStoredSession(t, base, string(id), cwd, nil)
		agent := newTestAgent(WithSessionStore(base))
		use := &agentSessionUse{generation: 1, done: make(chan struct{})}
		agent.sessionUses[id] = &agentSessionUse{generation: 2, done: make(chan struct{})}
		_, _, err := agent.loadColdSession(t.Context(), id, use, cwd, parsedSessionMeta{}, "", nil)
		require.Error(t, err)
	})

	t.Run("cold after transcript", func(t *testing.T) {
		base := NewInMemorySessionStore()
		store := &lifecycleMutationStore{SessionStore: base}
		id := acp.SessionId("T-load-cold-after-transcript")
		cwd := t.TempDir()
		putStoredSession(t, base, string(id), cwd, nil)
		agent := newTestAgent(WithExecutablePath(path), WithSessionStore(store), WithScratchDir(testScratchDir(t)))
		use := &agentSessionUse{generation: 1, done: make(chan struct{})}
		agent.sessionUses[id] = use
		store.afterLoad = func(key SessionKey) {
			if key.Subpath == transcriptSubpath {
				agent.mu.Lock()
				agent.sessionUses[id] = &agentSessionUse{generation: 2, done: make(chan struct{})}
				agent.mu.Unlock()
			}
		}
		_, _, err := agent.loadColdSession(t.Context(), id, use, cwd, parsedSessionMeta{}, "", nil)
		require.Error(t, err)
		require.NoError(t, agent.Close())
	})

	t.Run("cold after export", func(t *testing.T) {
		base := NewInMemorySessionStore()
		id := acp.SessionId("T-load-cold-after-export")
		cwd := t.TempDir()
		putStoredSession(t, base, string(id), cwd, nil)
		agent := newTestAgent(WithExecutablePath(path), WithSessionStore(base), WithScratchDir(testScratchDir(t)))
		use := &agentSessionUse{generation: 1, done: make(chan struct{})}
		agent.sessionUses[id] = use
		agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
			agent.mu.Lock()
			agent.sessionUses[id] = &agentSessionUse{generation: 2, done: make(chan struct{})}
			agent.mu.Unlock()

			return json.RawMessage(`{}`), nil
		}
		_, _, err := agent.loadColdSession(t.Context(), id, use, cwd, parsedSessionMeta{}, "", nil)
		require.Error(t, err)
		require.NoError(t, agent.Close())
	})
}

func TestColdSessionPublicationGenerationBranches(t *testing.T) {
	id := acp.SessionId("T-cold-publication")
	session := &agentSession{id: id}
	other := &agentSession{id: id}

	t.Run("use changed", func(t *testing.T) {
		agent := newTestAgent()
		use := &agentSessionUse{generation: 1}
		agent.sessionUses[id] = &agentSessionUse{generation: 2}
		require.Error(t, agent.publishColdSession(id, use, session))
	})

	t.Run("flight adopts", func(t *testing.T) {
		agent := newTestAgent()
		use := &agentSessionUse{generation: 1}
		agent.sessionUses[id] = use
		flight := &agentSessionFlight{generation: 2, use: use}
		agent.sessionFlights[id] = flight
		require.Error(t, agent.publishColdSession(id, use, session))
		require.Same(t, session, flight.session)
	})

	t.Run("flight conflicts", func(t *testing.T) {
		agent := newTestAgent()
		use := &agentSessionUse{generation: 1}
		agent.sessionUses[id] = use
		agent.sessionFlights[id] = &agentSessionFlight{generation: 2, use: use, session: other}
		require.Error(t, agent.publishColdSession(id, use, session))
	})

	t.Run("capacity", func(t *testing.T) {
		agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
		use := &agentSessionUse{generation: 1}
		agent.sessionUses[id] = use
		agent.sessions["T-existing"] = other
		require.Error(t, agent.publishColdSession(id, use, session))
	})
}

func TestDeleteAtShutdownBoundaryAndDeliveryRetries(t *testing.T) {
	agent := newTestAgent()

	incomplete := &agentSession{
		agent:                 agent,
		id:                    "T-delete-incomplete",
		turn:                  make(chan struct{}, 1),
		scratchContainmentErr: nativeamp.ErrProcessContainmentIncomplete,
	}
	require.ErrorIs(t, incomplete.deleteAtShutdown(t.Context()), nativeamp.ErrProcessContainmentIncomplete)

	deliveryErr := errors.New("terminal delivery failed")
	prompt := newPromptTurnState()
	prompt.completeSettlement(promptSettlement{deliveryErr: deliveryErr})
	delivery := &agentSession{
		agent:        agent,
		id:           "T-delete-delivery",
		turn:         make(chan struct{}, 1),
		activePrompt: prompt,
	}
	require.ErrorIs(t, delivery.deleteAtShutdown(t.Context()), deliveryErr)
	require.True(t, delivery.deleteComplete())

	done := &agentSession{agent: agent, id: "T-delete-done", turn: make(chan struct{}, 1), deleteDone: true}
	require.NoError(t, done.Delete(t.Context()))

	t.Run("second teardown reentry", func(t *testing.T) {
		foreign := &sessionTeardownFlight{generation: 99, done: make(chan struct{})}
		var reentrant *agentSession
		prompt := newPromptTurnState()
		prompt.setCancelFunc(func() {
			reentrant.teardownMu.Lock()
			reentrant.teardownFlight = foreign
			reentrant.teardownMu.Unlock()
		})
		prompt.completeSettlement(promptSettlement{deliveryErr: deliveryErr})
		reentrant = &agentSession{
			agent:        agent,
			id:           "T-delete-shutdown-reentry",
			turn:         make(chan struct{}, 1),
			activePrompt: prompt,
		}
		err := reentrant.deleteAtShutdown(withCallbackProvenance(t.Context(), agent, foreign))
		requireClosedCallbackRefusal(t, err)
	})

	t.Run("delete completed while reporting delivery", func(t *testing.T) {
		var completed *agentSession
		prompt := newPromptTurnState()
		prompt.setCancelFunc(func() {
			completed.mu.Lock()
			completed.deleteDone = true
			completed.mu.Unlock()
		})
		prompt.completeSettlement(promptSettlement{deliveryErr: deliveryErr})
		completed = &agentSession{
			agent:        agent,
			id:           "T-delete-shutdown-done",
			turn:         make(chan struct{}, 1),
			activePrompt: prompt,
		}
		require.ErrorIs(t, completed.deleteAtShutdown(t.Context()), deliveryErr)
	})
}

type residualTerminalFailureClient struct {
	lifecycleClient
	err error
}

func (c *residualTerminalFailureClient) SessionUpdate(context.Context, acp.SessionNotification) error {
	return c.err
}

func residualPendingTerminal(session *agentSession) *promptTerminalDelivery {
	return &promptTerminalDelivery{
		stream: &promptStream{
			session: session,
			client:  session.agent.connection(),
			stream:  lifecycle.NewStream("residual-terminal-"+string(session.id), negotiatedAnswer()),
		},
		notifications: []acp.SessionNotification{{SessionId: session.id}},
	}
}

func TestPendingTerminalAndRetainedSettlementDefensiveBranches(t *testing.T) {
	deliveryErr := errors.New("terminal delivery refused")

	t.Run("direct close", func(t *testing.T) {
		agent := newTestAgent()
		client := &residualTerminalFailureClient{err: deliveryErr}
		agent.setConnection(client)
		session := &agentSession{agent: agent, id: "T-terminal-close", turn: make(chan struct{}, 1)}
		session.pendingTerminal = residualPendingTerminal(session)

		require.ErrorIs(t, session.Close(t.Context()), deliveryErr)
		client.err = nil
		require.NoError(t, session.Close(t.Context()))
		require.NoError(t, agent.Close())
	})

	t.Run("installed close", func(t *testing.T) {
		agent := newTestAgent()
		client := &residualTerminalFailureClient{err: deliveryErr}
		agent.setConnection(client)
		session := &agentSession{agent: agent, id: "T-terminal-remove", turn: make(chan struct{}, 1)}
		session.pendingTerminal = residualPendingTerminal(session)
		agent.mu.Lock()
		agent.activateSessionLocked(session)
		agent.mu.Unlock()
		agent.observe.AddActiveSession(t.Context(), 1)

		require.ErrorIs(t, agent.removeSession(t.Context(), session.id, session), deliveryErr)
		client.err = nil
		require.NoError(t, agent.Close())
	})

	t.Run("delete retains permanent native failure", func(t *testing.T) {
		nativeErr := errors.New("permanent native delete failure")
		session := &agentSession{
			agent:           newTestAgent(),
			id:              "T-native-delete-error",
			turn:            make(chan struct{}, 1),
			nativeDeleteErr: nativeErr,
		}
		require.ErrorIs(t, session.Delete(t.Context()), nativeErr)
	})

	t.Run("readiness and shutdown predicates", func(t *testing.T) {
		session := &agentSession{agent: newTestAgent(), id: "T-ready-terminal", turn: make(chan struct{}, 1)}
		session.pendingTerminal = &promptTerminalDelivery{}
		require.ErrorContains(t, session.ready(), "terminal lifecycle delivery pending")
		session.pendingTerminal = nil
		settlementErr := errors.New("retained settlement failure")
		session.promptSettlement.deliveryErr = settlementErr
		require.ErrorIs(t, session.ready(), settlementErr)
		require.False(t, session.shutdownComplete())
		session.closeBoundaryDone = true
		session.closeCommitDone = true
		session.scratchDone = true
		session.promptSettlement = promptSettlement{}
		require.True(t, session.shutdownComplete())
		var absent *promptTerminalDelivery
		require.NoError(t, absent.deliver(t.Context()))
	})
}

func TestCallbackOwnershipDefensiveBranches(t *testing.T) {
	id := acp.SessionId("T-callback-ownership-branches")
	agent := newTestAgent()
	agent.deleted[id] = struct{}{}
	_, _, err := agent.beginSessionUse(t.Context(), id)
	requireUnknownSessionError(t, err)
	delete(agent.deleted, id)

	use := &agentSessionUse{generation: 1, done: make(chan struct{})}
	agent.sessionUses[id] = use
	owned := withCallbackProvenance(t.Context(), agent, use)
	_, _, err = agent.beginSessionUse(owned, id)
	requireClosedCallbackRefusal(t, err)
	_, _, err = agent.beginSessionFlight(owned, id, agentSessionDeleteFlight, nil)
	requireClosedCallbackRefusal(t, err)
	delete(agent.sessionUses, id)

	active := &agentSession{agent: agent, id: id, turn: make(chan struct{}, 1)}
	agent.mu.Lock()
	agent.activateSessionLocked(active)
	agent.retainCleanupOwnerLocked(id, active, agentCleanupPrepared)
	agent.mu.Unlock()
	require.NoError(t, agent.retryCleanupOwner(t.Context(), id))
	agent.mu.Lock()
	require.Empty(t, agent.cleanupOwners[id])
	delete(agent.sessions, id)
	agent.mu.Unlock()

	incomplete := &agentSession{
		agent:                 agent,
		id:                    "T-load-cleanup-error",
		turn:                  make(chan struct{}, 1),
		scratchContainmentErr: nativeamp.ErrProcessContainmentIncomplete,
	}
	agent.retainCleanupOwner(incomplete.id, incomplete, agentCleanupPrepared)
	loaded, transcript, started, cleanupUse, err := agent.loadOrResume(t.Context(), incomplete.id, t.TempDir(), nil, nil, nil)
	require.ErrorIs(t, err, nativeamp.ErrProcessContainmentIncomplete)
	require.Nil(t, loaded)
	require.Nil(t, transcript)
	require.False(t, started)
	require.Nil(t, cleanupUse)
	require.ErrorIs(t, agent.Close(), nativeamp.ErrProcessContainmentIncomplete)
	require.False(t, (*Agent)(nil).hasActiveCallbackForSession(id))
	require.False(t, (*Agent)(nil).hasActiveCallbackAuthority())
}

func TestPersistenceCommitOwnershipMutationFailsClosed(t *testing.T) {
	store := &persistenceOwnershipMutationStore{SessionStore: NewInMemorySessionStore()}
	agent := newTestAgent(WithSessionStore(store))
	session := &agentSession{agent: agent, id: "T-persistence-owner-mutation", turn: make(chan struct{}, 1)}
	_, flight, err := session.beginPersistence(t.Context(), sessionPersistenceOrdinary)
	require.NoError(t, err)
	defer session.finishPersistence(flight)

	commit := &sessionPersistenceCommit{
		replacements: []SessionStoreReplacement{{
			Key:     SessionKey{SessionID: string(session.id), Subpath: SessionStoreMainSubpath},
			Entries: []SessionStoreEntry{json.RawMessage(`{"format":"acp-go-amp/session-v1"}`)},
		}},
	}
	session.persistenceCommit = commit
	store.mutate = func() {
		session.mu.Lock()
		session.persistenceCommit = &sessionPersistenceCommit{}
		session.mu.Unlock()
	}

	require.ErrorContains(t, session.replacePersistenceCommit(t.Context(), flight, commit), "persistence ownership changed")
}

func TestCleanupFailureClassificationAndMissingDeliveryAreFixed(t *testing.T) {
	require.Equal(t, "none", cleanupFailureClass(nil))
	require.Equal(t, "callback_panic", cleanupFailureClass(errAgentGoroutinePanic))
	require.Equal(t, "containment_incomplete", cleanupFailureClass(nativeamp.ErrProcessContainmentIncomplete))
	require.Equal(t, "cancelled", cleanupFailureClass(context.Canceled))
	require.Equal(t, "deadline", cleanupFailureClass(context.DeadlineExceeded))
	require.Equal(t, "cleanup_failed", cleanupFailureClass(errors.New("secret callback detail")))

	require.ErrorContains(t, (&promptStream{}).deliver(t.Context(), acp.SessionNotification{}), "lifecycle delivery unavailable")
}

func TestLastWordShutdownRetriesEveryDetachedPanicSeam(t *testing.T) {
	for _, test := range []struct {
		name string
		kind agentCleanupKind
	}{
		{name: "prepared", kind: agentCleanupPrepared},
		{name: "deleted", kind: agentCleanupDeleted},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := newTestAgent(WithScratchDir(testScratchDir(t)))
			session, err := newAgentSession(t.Context(), agent, acp.SessionId("T-cleanup-panic-"+test.name), t.TempDir(), parsedSessionMeta{}, "", nil)
			require.NoError(t, err)
			originalRelease := session.scratchRootRelease
			calls := 0
			session.scratchRootRelease = func() {
				calls++
				if calls == 1 {
					panic("detached cleanup panic")
				}
				originalRelease()
			}
			agent.retainCleanupOwner(session.id, session, test.kind)

			require.ErrorIs(t, agent.Close(), errAgentGoroutinePanic)
			require.Equal(t, 1, calls)
			agent.mu.Lock()
			require.Empty(t, agent.cleanupOwners)
			require.Empty(t, agent.sessions)
			agent.mu.Unlock()
		})
	}

	t.Run("residence", func(t *testing.T) {
		agent := newTestAgent()
		calls := 0
		residence, err := agent.reserveCleanupResidence()
		require.NoError(t, err)
		residence.setRelease(func() {
			calls++
			if calls == 1 {
				panic("residence cleanup panic")
			}
		})

		require.ErrorIs(t, agent.Close(), errAgentGoroutinePanic)
		require.Equal(t, 1, calls)
		agent.mu.Lock()
		require.Empty(t, agent.cleanupResidences)
		agent.mu.Unlock()
	})
}

func TestSettlementPreparationAndPanicContainmentDefensiveBranches(t *testing.T) {
	agent := newTestAgent()
	session := &agentSession{agent: agent, id: "T-settlement-defensive", turn: make(chan struct{}, 1)}
	incarnation := &promptStream{
		session: session,
		stream:  lifecycle.NewStream("stream-defensive", negotiatedAnswer()),
	}
	incarnation.stream.Fence()
	agent.options.runtime.settleTurn = func(*nativeamp.Turn) (nativeamp.ContainmentProof, error) {
		return nativeamp.ContainmentProof{Root: 1, Proven: true}, nil
	}
	state := newPromptTurnState()
	_, err := session.settlePrompt(t.Context(), &nativeamp.Turn{}, state, incarnation, promptResult{})
	require.ErrorContains(t, err, "amp_lifecycle_violation")
	require.Error(t, state.awaitSettlement(t.Context()).deliveryErr)

	agent.options.runtime.settleTurn = func(*nativeamp.Turn) (nativeamp.ContainmentProof, error) {
		panic("containment retry panic")
	}
	_, err = session.settleTurnAfterPanic(&nativeamp.Turn{})
	require.ErrorIs(t, err, nativeamp.ErrProcessContainmentIncomplete)
}
