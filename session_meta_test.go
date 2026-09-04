package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func TestSessionMetaStrictness(t *testing.T) {
	_, err := parseSessionMeta(map[string]any{"amp": map[string]any{"bad": true}})
	if err == nil {
		t.Fatal("expected unknown own namespace error")
	}
	meta, err := parseSessionMeta(map[string]any{
		"other": map[string]any{"ignored": true},
		"amp": map[string]any{
			"options":  map[string]any{"mode": "low"},
			"rawEvent": map[string]any{"enabled": true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if meta.options.Mode != "low" || !meta.rawEvent {
		t.Fatalf("bad meta: %+v", meta)
	}
}

func TestActiveOmittedEnvReconstructsAcceptedCarrier(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	extra := t.TempDir()
	server := StdioMCPServer("stdio", "printf", []string{"ok"}, map[string]string{"A": "B"})
	explicitOptions := NewAmpOptions(
		WithAmpEnv(map[string]string{
			"AMP_API_KEY": "session-key",
			"AMP_URL":     "https://session.example.test",
		}),
		WithAmpMode("high"),
	)
	omittedEnvOptions := []SessionRequestOption{
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(NewAmpOptions(WithAmpMode("high"))),
	}

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
	)
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd,
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(explicitOptions),
	))
	if err != nil {
		t.Fatalf("NewSession explicit env: %v", err)
	}
	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd, omittedEnvOptions...)); loadErr != nil {
		t.Fatalf("active load omitted env: %v", loadErr)
	}
	if _, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest(resp.SessionId, cwd, omittedEnvOptions...)); resumeErr != nil {
		t.Fatalf("active resume omitted env: %v", resumeErr)
	}

	changed := append([]SessionRequestOption(nil), omittedEnvOptions...)
	changed = append(changed, WithSessionAmpOptions(NewAmpOptions(
		WithAmpEnv(map[string]string{
			"AMP_API_KEY": "rotated-key",
			"AMP_URL":     "https://session.example.test",
		}),
		WithAmpMode("high"),
	)))
	if _, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest(resp.SessionId, cwd, changed...)); resumeErr != nil {
		t.Fatalf("active resume changed env boundary: %v", resumeErr)
	}
	replacement, err := agent.session(resp.SessionId)
	if err != nil {
		t.Fatalf("replacement session: %v", err)
	}
	if got := activeRequestEnv(replacement.env); got["AMP_API_KEY"] != "rotated-key" || got["AMP_URL"] != "https://session.example.test" {
		t.Fatalf("replacement env = %#v, want rotated carrier", got)
	}
}

func TestSessionResidenceEnvRejectedBeforeMutation(t *testing.T) {
	keys := []string{envHome, envXDGConfigHome, envXDGCacheHome, envXDGDataHome, envXDGStateHome}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			scratch := t.TempDir()
			store := &recordingStore{}
			agent := newTestAgent(WithScratchDir(scratch), WithSessionStore(store))
			_, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir(),
				WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{
					"AMP_API_KEY": "key",
					key:           "/caller-owned",
				}))),
			))
			requireUnsupportedField(t, err, "_meta.amp.options.env."+key)
			if store.replaceCalls != 0 {
				t.Fatalf("invalid residence env made %d store mutations", store.replaceCalls)
			}
			entries, readErr := os.ReadDir(scratch)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("invalid residence env mutated scratch: entries=%d err=%v", len(entries), readErr)
			}
		})
	}

	t.Run("windows case alias", func(t *testing.T) {
		simulateWindowsEnvironment(t)
		err := newTestAgent().validateSessionStartOptions(AmpOptions{Env: map[string]string{"home": "/caller-owned"}})
		requireUnsupportedField(t, err, "_meta.amp.options.env.home")

		entry := json.RawMessage(`{"format":"amp-thread-mirror-v1","sessionId":"T-bad","nativeSessionId":"T-bad","cwd":"/cwd","env":{"amp_api_key":"first","AMP_API_KEY":"second"},"updatedAtUnixMilli":1,"createdAtUnixMilli":1}`)
		if _, ok := manifestFromStoreEntry(entry); ok {
			t.Fatal("stored env accepted a platform-case duplicate")
		}
	})
}

func TestColdLoadReconstructsAndRotatesStoredCarrier(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()
	created := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "create-default"}),
	)
	resp, err := created.NewSession(ctx, NewSessionRequest(cwd, WithSessionAmpOptions(NewAmpOptions(
		WithAmpEnv(map[string]string{"AMP_URL": "https://session.example.test", "PATH": "/create/bin"}),
	))))
	if err != nil {
		t.Fatalf("NewSession explicit env: %v", err)
	}
	if closeErr := created.Close(); closeErr != nil {
		t.Fatalf("Close created agent: %v", closeErr)
	}
	entries, err := store.Load(ctx, SessionKey{SessionID: string(resp.SessionId), Subpath: SessionStoreMainSubpath})
	if err != nil || len(entries) != 1 {
		t.Fatalf("load stored manifest: entries=%d err=%v", len(entries), err)
	}
	var manifest ampManifest
	if decodeErr := json.Unmarshal(entries[0], &manifest); decodeErr != nil {
		t.Fatalf("decode stored manifest: %v", decodeErr)
	}
	if want := map[string]string{"AMP_URL": "https://session.example.test", "PATH": "/create/bin"}; !maps.Equal(manifest.Env, want) {
		t.Fatalf("stored session env = %#v, want %#v", manifest.Env, want)
	}
	matching := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "matching-default"}),
	)
	if _, resumeErr := matching.ResumeSession(ctx, ResumeSessionRequest(resp.SessionId, cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{
			"AMP_URL": "https://session.example.test",
			"PATH":    "/create/bin",
		}))),
	)); resumeErr != nil {
		t.Fatalf("cold resume with matching env: %v", resumeErr)
	}
	if closeErr := matching.Close(); closeErr != nil {
		t.Fatalf("Close matching agent: %v", closeErr)
	}

	restored := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithEnv(map[string]string{
			"AMP_API_KEY":  "restore-default",
			"AMP_URL":      "https://default.example.test",
			"RESTORE_ONLY": "must-not-leak",
		}),
	)
	_, rotateErr := restored.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{
			"AMP_API_KEY": "rotated",
			"PATH":        "/rotated/bin",
		}))),
	))
	if rotateErr != nil {
		t.Fatalf("cold load with changed env boundary: %v", rotateErr)
	}
	rotated, err := restored.session(resp.SessionId)
	if err != nil {
		t.Fatalf("rotated session lookup: %v", err)
	}
	env := activeRequestEnv(rotated.env)
	if env["AMP_API_KEY"] != "rotated" || env["PATH"] != "/rotated/bin" || env["AMP_URL"] != "https://default.example.test" || env["RESTORE_ONLY"] != "must-not-leak" {
		t.Fatalf("cold rotated env = %#v, want complete replacement carrier", env)
	}
	if want := map[string]string{"AMP_API_KEY": "rotated", "PATH": "/rotated/bin"}; !maps.Equal(rotated.sessionEnv, want) {
		t.Fatalf("cold rotated session carrier = %#v, want %#v", rotated.sessionEnv, want)
	}
	storedRotated, manifestErr := restored.loadManifest(ctx, resp.SessionId)
	if manifestErr != nil || !maps.Equal(storedRotated.Env, rotated.sessionEnv) {
		t.Fatalf("cold rotation was not durable before success: manifest=%#v err=%v", storedRotated, manifestErr)
	}
	if closeErr := restored.Close(); closeErr != nil {
		t.Fatalf("Close rotated agent: %v", closeErr)
	}

	omitted := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithEnv(map[string]string{"AMP_API_KEY": "must-not-win"}),
	)
	if _, loadErr := omitted.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd)); loadErr != nil {
		t.Fatalf("cold load omitted rotated env: %v", loadErr)
	}
	session, err := omitted.session(resp.SessionId)
	if err != nil {
		t.Fatalf("omitted rotated session lookup: %v", err)
	}
	env = activeRequestEnv(session.env)
	if env["AMP_API_KEY"] != "rotated" || env["PATH"] != "/rotated/bin" || env["AMP_URL"] != "" {
		t.Fatalf("omitted cold load env = %#v, want durable rotated carrier", env)
	}
}

func TestActiveCarrierRotationContainsPredecessorAndPreservesSession(t *testing.T) {
	ctx := context.Background()
	path, state := fakeAgentAmpPath(t, "record-env")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()
	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
	)
	created, newErr := agent.NewSession(ctx, NewSessionRequest(cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{
			"AMP_API_KEY": "old-key",
			"PATH":        "/old/bin",
		}))),
	))
	if newErr != nil {
		t.Fatalf("NewSession: %v", newErr)
	}
	if _, err := agent.Prompt(ctx, TextPromptRequest(created.SessionId, "first-turn", "first")); err != nil {
		t.Fatalf("first Prompt: %v", err)
	}

	predecessor, predecessorErr := agent.session(created.SessionId)
	if predecessorErr != nil {
		t.Fatalf("predecessor: %v", predecessorErr)
	}
	predecessorRoot := predecessor.settingsDir
	predecessorNativeID := predecessor.nativeSessionID()
	predecessorCreated := predecessor.createdUnix
	beforeTranscript, beforeErr := store.Load(ctx, SessionKey{SessionID: string(created.SessionId), Subpath: transcriptSubpath})
	if beforeErr != nil || len(beforeTranscript) == 0 {
		t.Fatalf("predecessor transcript: entries=%d err=%v", len(beforeTranscript), beforeErr)
	}

	contained := make(chan bool, 1)
	release := make(chan struct{})
	agent.options.runtime.afterReplacementPredecessorClosed = func(session *agentSession) {
		_, statErr := os.Stat(session.settingsDir)
		contained <- os.IsNotExist(statErr)
		<-release
	}

	resumeErr := make(chan error, 1)
	go func() {
		_, callErr := agent.ResumeSession(ctx, ResumeSessionRequest(created.SessionId, cwd,
			WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{
				"AMP_API_KEY": "new-key",
				"PATH":        "/new/bin",
			}))),
		))
		resumeErr <- callErr
	}()

	if wasContained := <-contained; !wasContained {
		t.Fatal("successor boundary reached before predecessor residence removal")
	}
	if _, lookupErr := agent.session(created.SessionId); lookupErr == nil {
		t.Fatal("predecessor remained addressable during replacement gap")
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelWait()
	if _, secondErr := agent.ResumeSession(waitCtx, ResumeSessionRequest(created.SessionId, cwd)); secondErr == nil {
		t.Fatal("concurrent resume bypassed the replacement lease")
	}

	close(release)
	if callErr := <-resumeErr; callErr != nil {
		t.Fatalf("rotating ResumeSession: %v", callErr)
	}
	agent.options.runtime.afterReplacementPredecessorClosed = nil

	successor, successorErr := agent.session(created.SessionId)
	if successorErr != nil {
		t.Fatalf("successor: %v", successorErr)
	}
	if successor == predecessor || successor.settingsDir == predecessorRoot {
		t.Fatalf("successor reused predecessor residence: predecessor=%p successor=%p", predecessor, successor)
	}
	if successor.nativeSessionID() != predecessorNativeID || successor.createdUnix != predecessorCreated {
		t.Fatalf("successor identity = native %q created %d, want %q/%d", successor.nativeSessionID(), successor.createdUnix, predecessorNativeID, predecessorCreated)
	}
	if _, statErr := os.Stat(successor.settingsDir); statErr != nil {
		t.Fatalf("successor residence missing: %v", statErr)
	}
	if want := map[string]string{"AMP_API_KEY": "new-key", "PATH": "/new/bin"}; !maps.Equal(successor.sessionEnv, want) {
		t.Fatalf("successor carrier = %#v, want %#v", successor.sessionEnv, want)
	}

	afterTranscript, afterErr := store.Load(ctx, SessionKey{SessionID: string(created.SessionId), Subpath: transcriptSubpath})
	if afterErr != nil {
		t.Fatalf("load successor transcript: %v", afterErr)
	}
	requireSameSessionTranscript(t, beforeTranscript, afterTranscript)

	manifest, manifestErr := agent.loadManifest(ctx, created.SessionId)
	if manifestErr != nil || manifest.NativeSessionID != predecessorNativeID || !maps.Equal(manifest.Env, successor.sessionEnv) {
		t.Fatalf("successor manifest = %#v err=%v", manifest, manifestErr)
	}

	agent.mu.Lock()
	active, pending, cleanup := len(agent.sessions), agent.pending, agent.cleanupOwnerCountLocked()
	agent.mu.Unlock()
	if active != 1 || pending != 0 || cleanup != 0 {
		t.Fatalf("replacement accounting: active=%d pending=%d cleanup=%d", active, pending, cleanup)
	}

	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(created.SessionId, "second-turn", "second")); promptErr != nil {
		t.Fatalf("successor Prompt: %v", promptErr)
	}
	requireRotatedPromptCarriers(t, childRuns(t, state))
}

func requireSameSessionTranscript(t *testing.T, before, after []SessionStoreEntry) {
	t.Helper()

	if len(after) != len(before) {
		t.Fatalf("successor transcript: before=%d after=%d", len(before), len(after))
	}
	for index := range before {
		if !bytes.Equal(before[index], after[index]) {
			t.Fatalf("transcript frame %d changed across replacement", index)
		}
	}
}

func requireRotatedPromptCarriers(t *testing.T, runs []childRun) {
	t.Helper()

	prompts := make([]childRun, 0, 2)
	for _, run := range runs {
		if run.isPrompt() {
			prompts = append(prompts, run)
		}
	}
	if len(prompts) != 2 {
		t.Fatalf("prompt runs = %d, want predecessor and successor", len(prompts))
	}
	requireChildEnv(t, prompts[0].Env, "PATH", "/old/bin")
	requireChildEnv(t, prompts[0].Env, "AMP_API_KEY", "old-key")
	requireChildEnv(t, prompts[1].Env, "PATH", "/new/bin")
	requireChildEnv(t, prompts[1].Env, "AMP_API_KEY", "new-key")
}

func TestActiveCarrierRotationFailureRetainsRetryablePredecessor(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	cwd := t.TempDir()
	created, newErr := agent.NewSession(t.Context(), NewSessionRequest(cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "old-key"}))),
	))
	if newErr != nil {
		t.Fatalf("NewSession: %v", newErr)
	}
	predecessor, predecessorErr := agent.session(created.SessionId)
	if predecessorErr != nil {
		t.Fatalf("predecessor: %v", predecessorErr)
	}

	originalRemove := removeSessionDir
	t.Cleanup(func() { removeSessionDir = originalRemove })
	failed := false
	removeSessionDir = func(root string) error {
		if root == predecessor.settingsDir && !failed {
			failed = true

			return os.ErrPermission
		}

		return originalRemove(root)
	}
	request := ResumeSessionRequest(created.SessionId, cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "new-key"}))),
	)
	if _, firstErr := agent.ResumeSession(t.Context(), request); firstErr == nil {
		t.Fatal("replacement succeeded despite predecessor cleanup failure")
	}
	current, currentErr := agent.session(created.SessionId)
	if currentErr != nil || current != predecessor {
		t.Fatalf("failed replacement lost predecessor: current=%p predecessor=%p err=%v", current, predecessor, currentErr)
	}
	agent.mu.Lock()
	cleanup := agent.cleanupOwnerCountLocked()
	agent.mu.Unlock()
	if cleanup != 0 {
		t.Fatalf("failed predecessor-only replacement leaked %d cleanup owners", cleanup)
	}

	removeSessionDir = originalRemove
	if _, retryErr := agent.ResumeSession(t.Context(), request); retryErr != nil {
		t.Fatalf("retry replacement: %v", retryErr)
	}
	successor, successorErr := agent.session(created.SessionId)
	if successorErr != nil || successor == predecessor {
		t.Fatalf("retry did not publish successor: successor=%p predecessor=%p err=%v", successor, predecessor, successorErr)
	}
}

type carrierFailureStore struct {
	*InMemorySessionStore
	failTranscriptLoad bool
	failReplace        bool
}

func (s *carrierFailureStore) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	if s.failTranscriptLoad && key.Subpath == transcriptSubpath {
		return nil, errors.New("successor transcript load failed")
	}

	return s.InMemorySessionStore.Load(ctx, key)
}

func (s *carrierFailureStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if s.failReplace {
		return errors.New("successor manifest persist failed")
	}

	return s.InMemorySessionStore.Replace(ctx, main, replacements)
}

func TestActiveCarrierSuccessorFailuresFallBackToColdRecovery(t *testing.T) {
	for _, phase := range []string{"create", "load", "verify", "persist"} {
		t.Run(phase, func(t *testing.T) { testActiveCarrierSuccessorFailure(t, phase) })
	}
}

func testActiveCarrierSuccessorFailure(t *testing.T, phase string) {
	t.Helper()

	path, _ := fakeAgentAmpPath(t, "")
	store := &carrierFailureStore{InMemorySessionStore: NewInMemorySessionStore()}
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	cwd := t.TempDir()
	created, newErr := agent.NewSession(t.Context(), NewSessionRequest(cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "old-key"}))),
	))
	if newErr != nil {
		t.Fatalf("NewSession: %v", newErr)
	}
	if _, promptErr := agent.Prompt(t.Context(), TextPromptRequest(created.SessionId, "first-turn", "first")); promptErr != nil {
		t.Fatalf("Prompt: %v", promptErr)
	}
	predecessor, predecessorErr := agent.session(created.SessionId)
	if predecessorErr != nil {
		t.Fatalf("predecessor: %v", predecessorErr)
	}
	predecessorRoot := predecessor.settingsDir

	originalMkdirTemp := mkdirTemp
	originalExport := agent.options.runtime.exportThread
	agent.options.runtime.afterReplacementPredecessorClosed = func(*agentSession) {
		switch phase {
		case "create":
			mkdirTemp = func(string, string) (string, error) { return "", errors.New("successor create failed") }
		case "load":
			store.failTranscriptLoad = true
		case "verify":
			agent.options.runtime.exportThread = func(context.Context, *nativeamp.Client, string) (json.RawMessage, error) {
				return nil, errors.New("successor export failed")
			}
		case "persist":
			store.failReplace = true
		default:
			t.Fatalf("unknown failure phase %q", phase)
		}
	}
	t.Cleanup(func() { mkdirTemp = originalMkdirTemp })
	rotatedRequest := ResumeSessionRequest(created.SessionId, cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "new-key"}))),
	)
	if _, rotateErr := agent.ResumeSession(t.Context(), rotatedRequest); rotateErr == nil {
		t.Fatalf("replacement succeeded despite successor %s failure", phase)
	}
	mkdirTemp = originalMkdirTemp
	store.failTranscriptLoad = false
	store.failReplace = false
	agent.options.runtime.exportThread = originalExport
	agent.options.runtime.afterReplacementPredecessorClosed = nil

	if _, statErr := os.Stat(predecessorRoot); !os.IsNotExist(statErr) {
		t.Fatalf("failed successor left predecessor residence: %v", statErr)
	}
	agent.mu.Lock()
	_, active := agent.sessions[created.SessionId]
	cleanup := agent.cleanupOwnerCountLocked()
	agent.mu.Unlock()
	if active || cleanup != 0 {
		t.Fatalf("failed successor stranded ownership: active=%t cleanup=%d", active, cleanup)
	}
	committed, manifestErr := agent.loadManifest(t.Context(), created.SessionId)
	if manifestErr != nil || !maps.Equal(committed.Env, map[string]string{"AMP_API_KEY": "old-key"}) {
		t.Fatalf("failed successor changed predecessor manifest: %#v err=%v", committed, manifestErr)
	}

	if _, retryErr := agent.ResumeSession(t.Context(), rotatedRequest); retryErr != nil {
		t.Fatalf("cold retry after successor failure: %v", retryErr)
	}
	successor, successorErr := agent.session(created.SessionId)
	if successorErr != nil || successor == predecessor {
		t.Fatalf("cold retry did not publish successor: successor=%p predecessor=%p err=%v", successor, predecessor, successorErr)
	}
	committed, manifestErr = agent.loadManifest(t.Context(), created.SessionId)
	if manifestErr != nil || !maps.Equal(committed.Env, map[string]string{"AMP_API_KEY": "new-key"}) {
		t.Fatalf("cold retry carrier not durable: %#v err=%v", committed, manifestErr)
	}
}

func TestDeleteRacingActiveCarrierRotationWinsWithoutSuccessorLeak(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	cwd := t.TempDir()
	created, newErr := agent.NewSession(t.Context(), NewSessionRequest(cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "old-key"}))),
	))
	if newErr != nil {
		t.Fatalf("NewSession: %v", newErr)
	}

	closed := make(chan struct{})
	release := make(chan struct{})
	agent.options.runtime.afterReplacementPredecessorClosed = func(*agentSession) {
		close(closed)
		<-release
	}
	resumeErr := make(chan error, 1)
	go func() {
		_, callErr := agent.ResumeSession(t.Context(), ResumeSessionRequest(created.SessionId, cwd,
			WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "new-key"}))),
		))
		resumeErr <- callErr
	}()
	<-closed

	deleteErr := make(chan error, 1)
	go func() {
		_, callErr := agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{SessionId: created.SessionId})
		deleteErr <- callErr
	}()
	deadline := time.After(2 * time.Second)
	for {
		agent.mu.Lock()
		flight := agent.sessionFlights[created.SessionId]
		agent.mu.Unlock()
		if flight != nil {
			break
		}

		select {
		case <-deadline:
			t.Fatal("delete did not publish its ownership flight")
		case <-time.After(time.Millisecond):
		}
	}

	close(release)
	if callErr := <-resumeErr; callErr == nil {
		t.Fatal("replacement published after delete took ownership")
	}
	if callErr := <-deleteErr; callErr != nil {
		t.Fatalf("racing delete: %v", callErr)
	}
	if _, lookupErr := agent.session(created.SessionId); lookupErr == nil {
		t.Fatal("deleted session remained addressable")
	}
	agent.mu.Lock()
	active, cleanup := len(agent.sessions), agent.cleanupOwnerCountLocked()
	agent.mu.Unlock()
	if active != 0 || cleanup != 0 {
		t.Fatalf("delete race leaked ownership: active=%d cleanup=%d", active, cleanup)
	}
	entries, loadErr := store.Load(t.Context(), SessionKey{SessionID: string(created.SessionId), Subpath: SessionStoreMainSubpath})
	if loadErr != nil || len(entries) != 0 {
		t.Fatalf("delete race left durable session: entries=%d err=%v", len(entries), loadErr)
	}
}

// TestPromptErrorAfterCallerContextCancelInterrupts pins that a caller
// cancellation still answers cancelled when a native error follows it. The
// request context is the one the agent derives its operation context from, so
// the cancel is observed by the turn loop itself and the outcome is decided
// before settlement records one — never rewritten over a turn that already
// settled as failed.
func TestPromptErrorAfterCallerContextCancelInterrupts(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "delayed-error")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	promptCtx, cancelPrompt := context.WithCancel(context.Background())
	defer cancelPrompt()

	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(promptCtx, TextPromptRequest(resp.SessionId, "test-turn", "cancel before error"))
		resultCh <- result
		errCh <- promptErr
	}()

	waitForPath(t, filepath.Join(state, "continue-ready"))
	cancelPrompt()

	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if promptErr != nil || result.StopReason != acp.StopReasonCancelled {
			t.Fatalf("prompt error after caller cancel = %#v, %v", result, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prompt error after caller cancel did not return")
	}
}

func TestSessionPromptErrorAfterContextErrInterrupts(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "delayed-error")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	session, err := agent.session(resp.SessionId)
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	promptCtx := &manualErrContext{}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := session.Prompt(promptCtx, TextPromptRequest(resp.SessionId, "test-turn", "cancel before error"))
		resultCh <- result
		errCh <- promptErr
	}()

	waitForPath(t, filepath.Join(state, "continue-ready"))
	promptCtx.cancel()

	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if promptErr != nil || result.StopReason != acp.StopReasonCancelled {
			t.Fatalf("session prompt error after context error = %#v, %v", result, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session prompt error after context error did not return")
	}
}

type manualErrContext struct {
	cancelled atomic.Bool
}

func (c *manualErrContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *manualErrContext) Done() <-chan struct{} { return nil }

func (c *manualErrContext) Err() error {
	if c.cancelled.Load() {
		return context.Canceled
	}

	return nil
}

func (c *manualErrContext) Value(any) any { return nil }

func (c *manualErrContext) cancel() { c.cancelled.Store(true) }
