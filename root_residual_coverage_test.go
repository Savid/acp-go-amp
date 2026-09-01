package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

type residualAuthority struct {
	environment  map[string]string
	envPanic     bool
	preparePanic bool
	reclaimPanic bool
	prepareErr   error
	reclaimErr   error
}

func (a residualAuthority) NativeEnvironment() map[string]string {
	if a.envPanic {
		panic("environment panic")
	}

	return a.environment
}

func (a residualAuthority) PrepareNativeTree(context.Context, string) error {
	if a.preparePanic {
		panic("prepare panic")
	}

	return a.prepareErr
}

func (a residualAuthority) StartNative(context.Context, NativeRequest) (NativeProcess, error) {
	return nil, ErrHostAuthorityUnavailable
}

func (a residualAuthority) ReclaimNativeTree(context.Context, string) error {
	if a.reclaimPanic {
		panic("reclaim panic")
	}

	return a.reclaimErr
}

type emptyMultiError struct{}

func (emptyMultiError) Error() string   { return "empty multi error" }
func (emptyMultiError) Unwrap() []error { return nil }

type residualHookStore struct {
	*InMemorySessionStore
	afterTranscriptLoad func()
	afterReplace        func()
	failReplace         bool
}

func (s *residualHookStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	entries, err := s.InMemorySessionStore.Load(ctx, key)
	if err == nil && key.Subpath == transcriptSubpath && s.afterTranscriptLoad != nil {
		after := s.afterTranscriptLoad
		s.afterTranscriptLoad = nil
		after()
	}

	return entries, err
}

func (s *residualHookStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if s.failReplace {
		return errors.New("replace refused")
	}
	if err := s.InMemorySessionStore.Replace(ctx, main, replacements); err != nil {
		return err
	}
	if s.afterReplace != nil {
		after := s.afterReplace
		s.afterReplace = nil
		after()
	}

	return nil
}

func TestHostAuthorityUtilityResidualBranches(t *testing.T) {
	require.False(t, hostAuthorityNil(residualAuthority{}))
	require.True(t, interfaceValueNil(nil))
	require.True(t, interfaceValueNil([]string(nil)))
	require.False(t, interfaceValueNil(1))
	require.NoError(t, startNativeError(nil))

	environment, err := readHostEnvironment(residualAuthority{environment: map[string]string{"A": "B"}})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"A": "B"}, environment)
	_, err = readHostEnvironment(residualAuthority{})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)
	_, err = readHostEnvironment(residualAuthority{envPanic: true})
	require.ErrorIs(t, err, ErrHostAuthorityUnavailable)

	require.False(t, detachedContextError(nil))
	require.False(t, detachedContextError(ErrContainmentIncomplete))
	require.False(t, detachedContextError(emptyMultiError{}))
	require.True(t, detachedContextError(errors.Join(context.Canceled, context.DeadlineExceeded)))
	require.False(t, detachedContextError(errors.Join(context.Canceled, errors.New("other"))))
	require.True(t, detachedContextError(fmt.Errorf("wrapped: %w", context.Canceled)))
	require.True(t, detachedContextError(context.Canceled))

	for _, boundary := range []error{ErrHostAuthorityUnavailable, ErrNativeTreeBusy, nativeamp.ErrContainmentIncomplete} {
		require.Error(t, nativeInternalError(boundary))
	}
	require.NoError(t, publicContainmentError(nil))
}

func TestHostAuthorityPrepareAndReclaimResidualBranches(t *testing.T) {
	agent := NewAgent()
	require.NoError(t, agent.prepareNativeTree(t.Context(), t.TempDir()))
	require.NoError(t, agent.reclaimNativeTree(t.Context(), t.TempDir()))

	agent.options.hostAuthoritySupplied = true
	agent.options.HostAuthority = residualAuthority{preparePanic: true, environment: map[string]string{}}
	require.ErrorIs(t, agent.prepareNativeTree(t.Context(), t.TempDir()), ErrHostAuthorityUnavailable)

	agent = NewAgent()
	agent.options.hostAuthoritySupplied = true
	agent.options.HostAuthority = residualAuthority{reclaimPanic: true, environment: map[string]string{}}
	require.ErrorIs(t, agent.reclaimNativeTree(t.Context(), t.TempDir()), ErrHostAuthorityUnavailable)

	agent = NewAgent()
	agent.options.hostAuthoritySupplied = true
	agent.options.HostAuthority = residualAuthority{reclaimErr: ErrNativeTreeBusy, environment: map[string]string{}}
	require.ErrorIs(t, agent.reclaimNativeTree(t.Context(), t.TempDir()), ErrNativeTreeBusy)

	agent = NewAgent()
	agent.options.hostAuthoritySupplied = true
	agent.lifecycleContainmentErr = ErrContainmentIncomplete
	require.ErrorIs(t, agent.prepareNativeTree(t.Context(), t.TempDir()), ErrContainmentIncomplete)
	require.ErrorIs(t, agent.reclaimNativeTree(t.Context(), t.TempDir()), ErrContainmentIncomplete)
}

func TestActiveReplacementOwnershipResidualBranches(t *testing.T) {
	agent := NewAgent()
	id := acp.SessionId("T-residual")
	predecessor := &agentSession{agent: agent, id: id}
	successor := &agentSession{agent: agent, id: id}
	use := &agentSessionUse{session: predecessor}

	require.Error(t, agent.beginActiveReplacement(id, use, predecessor))
	_, _, err := agent.replaceActiveSession(t.Context(), id, use, predecessor, parsedSessionMeta{}, "/cwd", "", nil)
	require.Error(t, err)
	require.Error(t, agent.publishActiveReplacement(id, use, predecessor, successor))
	_, err = agent.retireFailedActiveReplacement(id, nil, predecessor)
	require.Error(t, err)

	agent.sessions[id] = predecessor
	agent.sessionUses[id] = use
	agent.sessionFlights[id] = &agentSessionFlight{use: &agentSessionUse{}, session: predecessor}
	_, err = agent.retireFailedActiveReplacement(id, use, predecessor)
	require.Error(t, err)

	delete(agent.sessionFlights, id)
	agent.sessions[id] = successor
	_, err = agent.retireFailedActiveReplacement(id, use, predecessor)
	require.Error(t, err)

	agent.sessions[id] = predecessor
	agent.sessionUses[id] = &agentSessionUse{session: predecessor, replacing: true}
	_, err = agent.sessionForCancel(id)
	require.Error(t, err)
}

func TestActiveReplacementPhaseOwnershipResidualBranches(t *testing.T) {
	for _, phase := range []string{"transcript", "verify", "publish"} {
		t.Run(phase, func(t *testing.T) {
			path, _ := fakeAgentAmpPath(t, "")
			store := &residualHookStore{InMemorySessionStore: NewInMemorySessionStore()}
			agent := newTestAgent(
				WithExecutablePath(path),
				WithScratchDir(testScratchDir(t)),
				WithSessionStore(store),
			)
			cwd := t.TempDir()
			created, err := agent.NewSession(t.Context(), NewSessionRequest(cwd,
				WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "old"}))),
			))
			require.NoError(t, err)
			_, err = agent.Prompt(t.Context(), TextPromptRequest(created.SessionId, "first", "first"))
			require.NoError(t, err)

			mutateOwnership := func() {
				agent.mu.Lock()
				agent.sessions[created.SessionId] = &agentSession{agent: agent, id: created.SessionId}
				agent.mu.Unlock()
			}
			agent.options.runtime.afterReplacementPredecessorClosed = func(*agentSession) {
				switch phase {
				case "transcript":
					store.afterTranscriptLoad = mutateOwnership
				case "verify":
					agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
						mutateOwnership()

						return json.RawMessage(`{}`), nil
					}
				case "publish":
					store.afterReplace = mutateOwnership
				}
			}

			_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest(created.SessionId, cwd,
				WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "new"}))),
			))
			require.Error(t, err)
		})
	}
}

func TestLoadActiveSessionResidualCallSites(t *testing.T) {
	newActive := func(t *testing.T) (*Agent, *carrierFailureStore, *agentSession, *agentSessionUse) {
		t.Helper()
		path, _ := fakeAgentAmpPath(t, "")
		store := &carrierFailureStore{InMemorySessionStore: NewInMemorySessionStore()}
		agent := newTestAgent(
			WithExecutablePath(path),
			WithScratchDir(testScratchDir(t)),
			WithSessionStore(store),
		)
		created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		require.NoError(t, err)
		session, err := agent.session(created.SessionId)
		require.NoError(t, err)
		use := &agentSessionUse{generation: 1, session: session, done: make(chan struct{})}
		agent.sessionUses[created.SessionId] = use

		return agent, store, session, use
	}

	t.Run("request mismatch", func(t *testing.T) {
		agent, _, session, use := newActive(t)
		_, err := agent.loadActiveSession(t.Context(), session.id, use, session, parsedSessionMeta{},
			t.TempDir(), session.mcpConfigJSON, session.additionalDirectories)
		require.Error(t, err)
	})

	t.Run("transcript load", func(t *testing.T) {
		agent, store, session, use := newActive(t)
		store.failTranscriptLoad = true
		_, err := agent.loadActiveSession(t.Context(), session.id, use, session, parsedSessionMeta{},
			session.cwd, session.mcpConfigJSON, session.additionalDirectories)
		require.ErrorContains(t, err, "transcript load failed")
	})
}

func TestColdCarrierPersistenceResidualCallSites(t *testing.T) {
	for _, phase := range []string{"persist", "validate"} {
		t.Run(phase, func(t *testing.T) {
			path, _ := fakeAgentAmpPath(t, "")
			store := &residualHookStore{InMemorySessionStore: NewInMemorySessionStore()}
			agent := newTestAgent(
				WithExecutablePath(path),
				WithScratchDir(testScratchDir(t)),
				WithSessionStore(store),
			)
			cwd := t.TempDir()
			created, err := agent.NewSession(t.Context(), NewSessionRequest(cwd,
				WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "old"}))),
			))
			require.NoError(t, err)
			_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
			require.NoError(t, err)

			switch phase {
			case "persist":
				store.failReplace = true
			case "validate":
				store.afterReplace = func() {
					agent.mu.Lock()
					agent.sessionUses[created.SessionId] = &agentSessionUse{}
					agent.mu.Unlock()
				}
			}
			_, err = agent.ResumeSession(t.Context(), ResumeSessionRequest(created.SessionId, cwd,
				WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "new"}))),
			))
			require.Error(t, err)
		})
	}
}

func TestActiveRequestAndSessionUtilityResidualBranches(t *testing.T) {
	newBase := func() *agentSession {
		return &agentSession{
			cwd:                   "/cwd",
			additionalDirectories: []string{"/extra"},
			mcpConfigJSON:         "{}",
			sessionEnv:            map[string]string{"A": "B"},
			mode:                  modeMedium,
		}
	}
	cases := []struct {
		name string
		edit func(*agentSession, *parsedSessionMeta, *string, *[]string)
	}{
		{"cwd", func(_ *agentSession, _ *parsedSessionMeta, cwd *string, _ *[]string) { *cwd = "/other" }},
		{"directories", func(_ *agentSession, _ *parsedSessionMeta, _ *string, dirs *[]string) { *dirs = nil }},
		{"mcp", func(s *agentSession, _ *parsedSessionMeta, _ *string, _ *[]string) { s.mcpConfigJSON = "other" }},
		{"env", func(s *agentSession, _ *parsedSessionMeta, _ *string, _ *[]string) {
			s.sessionEnv = map[string]string{"A": "C"}
		}},
		{"mode", func(_ *agentSession, meta *parsedSessionMeta, _ *string, _ *[]string) {
			meta.optionFields.mode = true
			meta.options.Mode = "other"
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			session := newBase()
			meta := parsedSessionMeta{options: AmpOptions{Env: map[string]string{"A": "B"}, Mode: modeMedium}}
			cwd := "/cwd"
			dirs := []string{"/extra"}
			test.edit(session, &meta, &cwd, &dirs)
			require.Error(t, session.applyActiveRequest(meta, cwd, "{}", dirs))
		})
	}

	require.False(t, validStoredSessionEnv(map[string]string{envHome: "/caller"}))
	require.Error(t, validateEnvironment(map[string]string{privateEnvPrefix + "X": "secret"}))
	require.NoError(t, firstError(nil, nil))
	require.ErrorIs(t, firstError(nil, context.Canceled), context.Canceled)

	agent := NewAgent()
	session := &agentSession{agent: agent}
	entries, err := session.loadTranscript(t.Context())
	require.NoError(t, err)
	require.Nil(t, entries)
	require.NoError(t, session.interrupt(t.Context()))

	containment := errors.New("lost")
	session.scratchContainmentErr = containment
	require.ErrorIs(t, session.verifyContinuable(t.Context()), containment)
	require.ErrorIs(t, session.ready(), containment)
}

func TestPromptSettlementTimeoutResidualBranch(t *testing.T) {
	state := &promptTurnState{completed: make(chan struct{})}
	settlement := state.awaitSettlement(residualCancelledContext())
	require.ErrorIs(t, settlement.containmentErr, nativeamp.ErrContainmentIncomplete)
}

func TestAgentCleanupResidenceResidualBranches(t *testing.T) {
	closed := NewAgent()
	closed.closed = true
	_, err := closed.reserveCleanupResidence()
	require.Error(t, err)

	agent := NewAgent()
	opaque := &agentCleanupResidence{agent: agent, id: 1, opaque: true, retryable: true}
	agent.cleanupResidences[opaque.id] = opaque
	agent.retryCleanupResidences(t.Context())
	require.Contains(t, agent.cleanupResidences, opaque.id)

	agent = NewAgent()
	root := t.TempDir()
	residence := &agentCleanupResidence{agent: agent, id: 1, root: root, retryable: true}
	agent.cleanupResidences[residence.id] = residence
	original := removeSessionDir
	panicked := false
	removeSessionDir = func(path string) error {
		if !panicked {
			panicked = true
			panic("cleanup panic")
		}

		return original(path)
	}
	t.Cleanup(func() { removeSessionDir = original })
	require.ErrorIs(t, agent.closeAttempt(), errAgentGoroutinePanic)
	require.Empty(t, agent.cleanupResidences)
}

func TestSessionTeardownResidualBranches(t *testing.T) {
	session := &agentSession{agent: NewAgent()}
	session.teardownFlight = &sessionTeardownFlight{done: make(chan struct{})}
	_, _, err := session.beginTeardown(residualCancelledContext())
	require.ErrorIs(t, err, context.Canceled)

	panicErr := errors.New("teardown panic")
	done := make(chan struct{})
	close(done)
	session.teardownFlight = &sessionTeardownFlight{done: done, panicErr: panicErr}
	_, _, err = session.beginTeardown(t.Context())
	require.ErrorIs(t, err, panicErr)

	err = session.closeForReplacement(withCallbackProvenance(t.Context(), session.agent, session.teardownFlight))
	require.Error(t, err)
}

func TestNewSessionMCPWriteFailure(t *testing.T) {
	original := writeFile
	t.Cleanup(func() { writeFile = original })
	writeFile = func(path string, data []byte, mode os.FileMode) error {
		if filepath.Base(path) == "mcp.json" {
			return errors.New("write MCP failed")
		}

		return original(path, data, mode)
	}

	agent := NewAgent(WithScratchDir(t.TempDir()))
	_, err := newAgentSession(t.Context(), agent, "T-write-failure", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.ErrorContains(t, err, "write amp MCP config")
}

func TestManifestStrictDecodeAcceptsTheCurrentTypedShape(t *testing.T) {
	entry := json.RawMessage(`{"format":"amp-thread-mirror-v1","sessionId":"T-1","nativeSessionId":"T-1","cwd":"/cwd","env":{},"updatedAtUnixMilli":1,"createdAtUnixMilli":1}`)
	manifest, ok := manifestFromStoreEntry(entry)
	require.True(t, ok)
	require.Equal(t, "T-1", manifest.SessionID)
}

func TestPublicCleanupRetryResidualBranches(t *testing.T) {
	newBlockedAgent := func() *Agent {
		agent := NewAgent()
		id := acp.SessionId("T-cleanup")
		session := &agentSession{agent: agent, id: id, nativeTreeOpaque: true}
		agent.retainCleanupOwner(id, session, agentCleanupDeleted)

		return agent
	}

	agent := newBlockedAgent()
	_, err := agent.ListSessions(t.Context(), acp.ListSessionsRequest{})
	require.ErrorIs(t, err, ErrContainmentIncomplete)

	agent = newBlockedAgent()
	_, err = agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: "T-other"})
	require.ErrorIs(t, err, ErrContainmentIncomplete)

	agent = newBlockedAgent()
	session, transcript, cold, use, err := agent.loadOrResume(t.Context(), "T-other", t.TempDir(), nil, nil, nil)
	require.ErrorIs(t, err, ErrContainmentIncomplete)
	require.Nil(t, session)
	require.Nil(t, transcript)
	require.False(t, cold)
	require.Nil(t, use)

	agent = NewAgent()
	id := acp.SessionId("T-deleted-owner")
	done := &agentSession{agent: agent, id: id, deleteDone: true}
	agent.retainCleanupOwner(id, done, agentCleanupDeleted)
	require.NoError(t, agent.retryCleanupOwner(t.Context(), id))
}

func TestSetConfigOptionUseAdmissionResidualBranch(t *testing.T) {
	agent := NewAgent()
	id := acp.SessionId("T-config")
	use := &agentSessionUse{done: make(chan struct{})}
	agent.sessionUses[id] = use
	_, err := agent.SetSessionConfigOption(residualCancelledContext(), acp.SetSessionConfigOptionRequest{ValueId: &acp.SetSessionConfigOptionValueId{
		SessionId: id,
		ConfigId:  "mode",
		Value:     "medium",
	}})
	require.Error(t, err)
}

func TestLoadTranscriptWithoutStoreResidualBranch(t *testing.T) {
	agent := NewAgent()
	agent.store = nil
	session := &agentSession{agent: agent, id: "T-no-store"}
	entries, err := session.loadTranscript(t.Context())
	require.NoError(t, err)
	require.Nil(t, entries)
}

func TestHostedAuthFlowResidualRefusals(t *testing.T) {
	t.Run("native start", func(t *testing.T) {
		fixture := newAuthFixture(t, "login")
		original := authStartLogin
		authStartLogin = func(*nativeamp.Client, context.Context) (*nativeamp.AuthLogin, error) {
			return nil, errors.New("native start refused")
		}
		t.Cleanup(func() { authStartLogin = original })

		_, err := fixture.authorize("connection-process", "request-process")
		requireAuthCause(t, err, authCauseProcess)
	})

	t.Run("lineage moved before hosted paste", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flow := fixture.mustAuthorize("connection-moved")
		record, _, err := fixture.broker.ledger.read(authProviderID, "connection-moved")
		require.NoError(t, err)
		record.BindingGeneration++
		require.NoError(t, fixture.broker.ledger.write(record))

		err = fixture.callback(flow.FlowID, "pasted")
		requireAuthCause(t, err, authCauseBindingConflict)
	})
}

func TestNewAuthClientConstructionResidualFailures(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	originalSafety := authCheckLoginSafety
	authCheckLoginSafety = func(*nativeamp.Client, context.Context) error { return nil }
	t.Cleanup(func() { authCheckLoginSafety = originalSafety })

	newSession := func(agent *Agent) *agentSession {
		return &agentSession{agent: agent, id: "T-auth", cwd: t.TempDir(), operationEnv: map[string]string{}}
	}

	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	agent.closed = true
	_, _, err := newSession(agent).newAuthClient(t.Context())
	require.Error(t, err)

	scratchFile := filepath.Join(t.TempDir(), "scratch-file")
	require.NoError(t, os.WriteFile(scratchFile, []byte("x"), 0o600))
	agent = newTestAgent(WithExecutablePath(path), WithScratchDir(scratchFile))
	_, _, err = newSession(agent).newAuthClient(t.Context())
	require.Error(t, err)

	originalMkdirTemp := mkdirTemp
	t.Cleanup(func() { mkdirTemp = originalMkdirTemp })
	mkdirTemp = func(string, string) (string, error) { return "", errors.New("materialize refused") }
	agent = newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	_, _, err = newSession(agent).newAuthClient(t.Context())
	require.ErrorContains(t, err, "materialize refused")
	mkdirTemp = originalMkdirTemp

	shimRoot := filepath.Join(t.TempDir(), "auth-residence")
	require.NoError(t, os.MkdirAll(shimRoot, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(shimRoot, "browser-shim"), []byte("collision"), 0o600))
	mkdirTemp = func(string, string) (string, error) { return shimRoot, nil }
	agent = newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	_, _, err = newSession(agent).newAuthClient(t.Context())
	require.Error(t, err)
	mkdirTemp = originalMkdirTemp

	originalWrite := writeFile
	t.Cleanup(func() { writeFile = originalWrite })
	writeFile = func(name string, data []byte, mode os.FileMode) error {
		if filepath.Base(name) == "seed.txt" {
			return errors.New("seed refused")
		}

		return originalWrite(name, data, mode)
	}
	agent = newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSeedFiles(map[string]string{"seed.txt": "seed"}))
	_, _, err = newSession(agent).newAuthClient(t.Context())
	require.ErrorContains(t, err, "seed refused")
	writeFile = originalWrite

	authority := residualAuthority{environment: nativeamp.CaptureOrdinaryEnvironment(), prepareErr: errors.New("prepare refused")}
	agent = newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithHostAuthority(authority))
	_, _, err = newSession(agent).newAuthClient(t.Context())
	require.ErrorContains(t, err, "prepare Amp auth residence")

	recording := newRecordingAuthority()
	agent = newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithHostAuthority(recording))
	_, cleanup, err := newSession(agent).newAuthClient(t.Context())
	require.NoError(t, err)
	require.NoError(t, cleanup())
}

func TestVerifyContinuableRecordsIncompleteContainment(t *testing.T) {
	agent := newTestAgent()
	agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
		return nil, fmt.Errorf("export: %w", nativeamp.ErrContainmentIncomplete)
	}
	session := &agentSession{agent: agent, id: "T-containment", nativeID: "native-thread"}
	require.Error(t, session.verifyContinuable(t.Context()))
	require.ErrorIs(t, session.scratchContainmentError(), nativeamp.ErrContainmentIncomplete)
}

func TestTerminalDeliveryRefusesUnnegotiatedQuiescence(t *testing.T) {
	negotiated := lifecycle.Negotiated{Version: lifecycle.Version}
	stream := lifecycle.NewStream("stream", negotiated)
	require.NoError(t, emitLifecyclePrefix(stream))

	prompt := &promptStream{
		session:       &agentSession{id: "T-lifecycle"},
		stream:        stream,
		cycleID:       "cycle",
		turnID:        "turn",
		authoritative: true,
	}
	_, err := prompt.terminalDelivery(
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess}, true,
	)
	require.Error(t, err)
}

func emitLifecyclePrefix(stream *lifecycle.Stream) error {
	if _, err := stream.Emit(lifecycle.SnapshotEvent("open", lifecycle.QuiescenceFact{})); err != nil {
		return err
	}
	if _, err := stream.Emit(lifecycle.AcceptedEvent(lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}, "turn")); err != nil {
		return err
	}
	_, err := stream.Emit(lifecycle.RunningEvent("cycle", "turn"))

	return err
}

func residualCancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return ctx
}
