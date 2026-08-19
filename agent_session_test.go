package ampacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestConfigOptions(t *testing.T) {
	session := &agentSession{mode: "medium"}
	options := session.configOptions()
	if len(options) != 1 {
		t.Fatalf("options=%d", len(options))
	}
	if options[0].Select == nil || options[0].Select.Type != "select" {
		t.Fatalf("bad mode option: %+v", options[0])
	}
}

// The two members that can make a set-config request unsupported are distinct
// caller mistakes and are named as such: a boolean payload chose the wrong
// request discriminator, while a request carrying neither variant supplied no
// value at all. Both variants are marshalled inline, so the discriminator the
// caller got wrong is reachable on the wire only as `type`.
func TestSetSessionConfigOptionNamesTheMemberThatFailed(t *testing.T) {
	agent := newTestAgent()
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	_, err := agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{
		Boolean: &acp.SetSessionConfigOptionBoolean{SessionId: "T-config", ConfigId: "mode", Value: true},
	})
	requireUnsupportedField(t, err, fieldType)

	_, err = agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{})
	requireUnsupportedField(t, err, fieldValue)
}

func TestActiveLoadResumeSemantics(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	extra := t.TempDir()
	server := StdioMCPServer("stdio", "printf", []string{"ok"}, map[string]string{"A": "B"})
	options := NewAmpOptions(WithAmpEnv(map[string]string{"AMP_URL": "https://amp.example.test"}), WithAmpMode("high"))
	requestOptions := func(raw bool) []SessionRequestOption {
		return []SessionRequestOption{
			WithSessionAdditionalDirectories(extra),
			WithSessionMCPServers(server),
			WithSessionAmpOptions(options),
			WithSessionRawEvents(raw),
		}
	}
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, newErr := agent.NewSession(ctx, NewSessionRequest(cwd, requestOptions(false)...))
	if newErr != nil {
		t.Fatalf("NewSession: %v", newErr)
	}
	id := resp.SessionId

	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(id, t.TempDir(), requestOptions(false)...)); !isMismatchField(loadErr, "cwd") {
		t.Fatalf("different active cwd = %v, want cwd mismatch", loadErr)
	}
	if _, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd,
		WithSessionMCPServers(HTTPMCPServer("http", "https://example.test/mcp", nil)),
		WithSessionAdditionalDirectories(extra),
		WithSessionAmpOptions(options),
	)); !isMismatchField(resumeErr, "mcpServers") {
		t.Fatalf("different active mcp = %v, want mcpServers mismatch", resumeErr)
	}
	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(id, cwd,
		WithSessionAdditionalDirectories(t.TempDir()),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(options),
	)); !isMismatchField(loadErr, "additionalDirectories") {
		t.Fatalf("different active additionalDirectories = %v, want mismatch", loadErr)
	}
	if _, resumeErr := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd,
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_URL": "https://other.example.test"}), WithAmpMode("high"))),
	)); !isMismatchField(resumeErr, "env") {
		t.Fatalf("different active env = %v, want env mismatch", resumeErr)
	}
	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(id, cwd,
		WithSessionAdditionalDirectories(extra),
		WithSessionMCPServers(server),
		WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"AMP_URL": "https://amp.example.test"}), WithAmpMode("low"))),
	)); !isMismatchField(loadErr, "mode") {
		t.Fatalf("different active mode = %v, want mode mismatch", loadErr)
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd, requestOptions(true)...)); err != nil {
		t.Fatalf("active load applying raw events: %v", err)
	}
	session, err := agent.session(id)
	if err != nil {
		t.Fatalf("session lookup: %v", err)
	}
	if !session.rawEvents {
		t.Fatal("active load did not apply rawEvent=true")
	}
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, requestOptions(false)...)); err != nil {
		t.Fatalf("active resume applying raw events: %v", err)
	}
	if session.rawEvents {
		t.Fatal("active resume did not apply rawEvent=false")
	}
}

func TestActiveLoadRetriesMirrorBeforeReplay(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	store := &flakyReplaceStore{InMemorySessionStore: NewInMemorySessionStore()}
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := resp.SessionId

	if _, err := agent.Prompt(ctx, TextPromptRequest(id, "test-turn", "seed thread")); err != nil {
		t.Fatalf("seed prompt: %v", err)
	}

	store.failReplaces = 1
	if _, err := agent.Prompt(ctx, TextPromptRequest(id, "test-turn", "persist after native success")); err == nil {
		t.Fatal("prompt with failing persist returned no error")
	}
	store.failReplaces = 1
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd)); err == nil || !strings.Contains(err.Error(), "mirror_unsynced") {
		t.Fatalf("active LoadSession did not fail on retry outage: %v", err)
	}
	before := len(client.updatesSnapshot())
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd)); err != nil {
		t.Fatalf("active LoadSession after store recovery: %v", err)
	}
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) > before })
	if len(client.updatesSnapshot()) <= before {
		t.Fatal("active load replayed stale transcript before mirror retry")
	}
}

func TestActiveLoadVerifiesContinuability(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "missing-export")
	cwd := t.TempDir()
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(NewInMemorySessionStore()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "seed thread")); err != nil {
		t.Fatalf("seed prompt: %v", err)
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd)); err != nil {
		t.Fatalf("active LoadSession with missing native thread should replay only: %v", err)
	}
	if _, err := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "should fail")); err == nil || !strings.Contains(err.Error(), "native_state_missing") {
		t.Fatalf("prompt after active load missing export = %v, want native_state_missing", err)
	}
}

func TestActiveLoadPropagatesContinuabilityFailure(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "export-fail")
	cwd := t.TempDir()
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(NewInMemorySessionStore()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "seed thread")); err != nil {
		t.Fatalf("seed prompt: %v", err)
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd)); err == nil || !strings.Contains(err.Error(), "export failed") {
		t.Fatalf("active LoadSession export failure = %v, want export failed", err)
	}
}

func isMismatchField(err error, field string) bool {
	if err == nil {
		return false
	}
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		return false
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		return strings.Contains(err.Error(), field) && strings.Contains(err.Error(), valMismatch)
	}

	return data[jsonFieldError] == valMismatch && data[jsonFieldField] == field
}

func TestNewSessionFailsFastWithoutAPIKey(t *testing.T) {
	t.Setenv("AMP_API_KEY", "")
	agent := newTestAgent(WithScratchDir(testScratchDir(t)))
	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "AMP_API_KEY") {
		t.Fatalf("missing key error = %v", err)
	}
}

func TestNewSessionFailsFastWithEmptyAPIKeyOverride(t *testing.T) {
	t.Setenv("AMP_API_KEY", "process-key")
	agent := newTestAgent(
		WithScratchDir(testScratchDir(t)),
		WithEnv(map[string]string{"AMP_API_KEY": ""}),
	)
	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "AMP_API_KEY") {
		t.Fatalf("empty override error = %v", err)
	}
}

func TestNewSessionAcceptsProcessEnvAPIKey(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if resp.SessionId == "" {
		t.Fatal("empty session id")
	}
}

func TestKeylessProviderAuthBootstrapPromptFailsBeforeNativeWork(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "")
	t.Setenv("AMP_API_KEY", "")

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithProviderAuthRoot(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": ""}),
	)
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	resp, err := agent.NewSession(context.Background(), NewSessionRequest(
		t.TempDir(),
		WithSessionMCPServers(StdioMCPServer("server", "unused", nil, nil)),
	))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	beforePrompt := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	if len(beforePrompt) != 1 || !slices.Equal(beforePrompt[0], []string{"version"}) {
		t.Fatalf("keyless bootstrap native calls = %#v, want version probe only", beforePrompt)
	}

	if _, promptErr := agent.Prompt(context.Background(), TextPromptRequest(resp.SessionId, "turn-1", "hello")); promptErr == nil || !strings.Contains(promptErr.Error(), "AMP_API_KEY") {
		t.Fatalf("Prompt error = %v, want missing AMP_API_KEY", promptErr)
	}

	session, err := agent.session(resp.SessionId)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	if _, err := os.Stat(filepath.Join(session.settingsDir, "mcp.json")); !os.IsNotExist(err) {
		t.Fatalf("MCP config created before key check: %v", err)
	}
	afterPrompt := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	if !slices.EqualFunc(beforePrompt, afterPrompt, func(a, b []string) bool {
		return slices.Equal(a, b)
	}) {
		t.Fatalf("prompt started native work without a key: before=%#v after=%#v", beforePrompt, afterPrompt)
	}
}

func TestKeylessProviderAuthBootstrapChecksNativeReadiness(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "bad-version")
	t.Setenv("AMP_API_KEY", "")

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithProviderAuthRoot(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": ""}),
	)
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "below required") {
		t.Fatalf("NewSession error = %v, want native version rejection", err)
	}
}

func TestLoadAndResumeRemainCredentialGatedWithProviderAuth(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()

	creator := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithProviderAuthRoot(t.TempDir()),
	)
	resp, err := creator.NewSession(context.Background(), NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := creator.Close(); err != nil {
		t.Fatalf("close creator: %v", err)
	}

	t.Setenv("AMP_API_KEY", "")
	restored := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithProviderAuthRoot(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": ""}),
	)
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	for name, load := range map[string]func() error{
		"load": func() error {
			_, loadErr := restored.LoadSession(context.Background(), LoadSessionRequest(resp.SessionId, cwd))

			return loadErr
		},
		"resume": func() error {
			_, resumeErr := restored.ResumeSession(context.Background(), ResumeSessionRequest(resp.SessionId, cwd))

			return resumeErr
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := load()
			if err == nil || !strings.Contains(err.Error(), "AMP_API_KEY") {
				t.Fatalf("error = %v, want missing AMP_API_KEY", err)
			}
		})
	}
}

func TestSessionSlotFilesystemServeAndCloseEdges(t *testing.T) {
	ctx := context.Background()

	previousWriteFile := writeFile
	writeFile = func(string, []byte, os.FileMode) error { return errors.New("write settings failed") }
	t.Cleanup(func() { writeFile = previousWriteFile })
	if _, err := newAgentSession(t.Context(), newTestAgent(WithScratchDir(testScratchDir(t))), "T-write", t.TempDir(), parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("settings write failure was ignored")
	}
	writeFile = previousWriteFile

	limited := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	if err := limited.reserveSessionSlot(); err != nil {
		t.Fatalf("reserve first slot: %v", err)
	}
	if err := limited.reserveSessionSlot(); err == nil || !strings.Contains(err.Error(), "backpressure") {
		t.Fatalf("reserve beyond pending limit = %v", err)
	}
	limited.releaseSessionSlot("")
	if err := limited.reserveSessionSlot(); err != nil {
		t.Fatalf("reserve after release: %v", err)
	}
	limited.releaseSessionSlot("")
	limited.releaseSessionSlot("")

	inputR, inputW := io.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- serveTest(ctx, inputR, io.Discard) }()
	_ = inputW.Close()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve peer close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not exit after peer close")
	}

	agent := newTestAgent()
	agent.sessions["T-close"] = &agentSession{agent: agent, id: "T-close", turn: make(chan struct{}, 1)}
	if err := agent.Close(); err != nil {
		t.Fatalf("Close with live session: %v", err)
	}
}

// TestCloseSessionCannotEvictASessionInstalledDuringItsTeardown pins the
// close-versus-load ownership boundary. Close gives up the id before the
// unlocked native teardown runs, so a session/load landing on that id while the
// teardown is still in flight becomes the sole owner: the closer must leave it
// installed, must not decrement the active-session gauge on its behalf, and
// must leave it reachable by agent shutdown rather than orphaning its amp
// process, scratch dir and pipes.
func TestCloseSessionCannotEvictASessionInstalledDuringItsTeardown(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()

	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-own", cwd, nil)

	reader := sdkmetric.NewManualReader()
	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
		WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
	)

	if _, err := agent.LoadSession(ctx, LoadSessionRequest("T-own", cwd)); err != nil {
		t.Fatalf("first load: %v", err)
	}

	closing := agent.sessions["T-own"]
	if closing == nil {
		t.Fatal("first load installed no session")
	}

	// The scratch-root release runs at the tail of the native teardown, after
	// the closer has given up the id, so it is the one point where a competing
	// load can be linearized against a close that is still running.
	gaveUpID := make(chan struct{})
	finishTeardown := make(chan struct{})
	releaseScratch := closing.scratchRootRelease
	closing.scratchRootRelease = func() {
		close(gaveUpID)
		<-finishTeardown

		if releaseScratch != nil {
			releaseScratch()
		}
	}

	closed := make(chan error, 1)

	go func() {
		_, closeErr := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: "T-own"})
		closed <- closeErr
	}()

	<-gaveUpID

	if _, err := agent.LoadSession(ctx, LoadSessionRequest("T-own", cwd)); err != nil {
		t.Fatalf("load during close teardown: %v", err)
	}

	reloaded := agent.sessions["T-own"]
	if reloaded == nil || reloaded == closing {
		t.Fatalf("load during close teardown reused the closing session: %v", reloaded == closing)
	}

	close(finishTeardown)

	if err := <-closed; err != nil {
		t.Fatalf("CloseSession: %v", err)
	}

	if agent.sessions["T-own"] != reloaded {
		t.Fatal("the settled closer evicted the session loaded during its teardown")
	}
	if got := collectActiveSessions(t, reader); got != 1 {
		t.Fatalf("active sessions = %d, want 1 (the reloaded owner)", got)
	}

	if err := agent.Close(); err != nil {
		t.Fatalf("agent Close: %v", err)
	}
	if got := collectActiveSessions(t, reader); got != 0 {
		t.Fatalf("active sessions after shutdown = %d, want 0", got)
	}
}

// TestStaleCloserCannotEvictTheReinstalledOwner pins the identity guard a
// retried or overtaken closer hits: it holds a pointer the map has already
// replaced, so it evicts nothing, closes nothing, and leaves the gauge to the
// caller that does own the slot.
func TestStaleCloserCannotEvictTheReinstalledOwner(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	agent := newTestAgent(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))

	stale := &agentSession{agent: agent, id: "T-stale", turn: make(chan struct{}, 1)}
	owner := &agentSession{agent: agent, id: "T-stale", turn: make(chan struct{}, 1)}
	agent.sessions["T-stale"] = owner
	agent.observe.AddActiveSession(t.Context(), 1)

	if err := agent.removeSession(t.Context(), "T-stale", stale); err != nil {
		t.Fatalf("stale close = %v", err)
	}

	if agent.sessions["T-stale"] != owner {
		t.Fatal("the stale closer evicted the installed owner")
	}

	owner.mu.Lock()
	ownerClosed := owner.closed
	owner.mu.Unlock()

	if ownerClosed {
		t.Fatal("the stale closer tore down the installed owner")
	}
	if got := collectActiveSessions(t, reader); got != 1 {
		t.Fatalf("active sessions = %d, want 1 (no double decrement)", got)
	}
}

// collectActiveSessions reads the current active-session gauge total.
func collectActiveSessions(t *testing.T, reader sdkmetric.Reader) int64 {
	t.Helper()

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	return sumInt64Metric(metrics, "acp_go_amp.session.active")
}

func TestLoadReplayDeleteAndConfigEdges(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()

	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-load-edge", cwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-load-edge"}`),
	})
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	agent.markDeleted("T-load-edge")
	listResp, err := agent.ListSessions(ctx, ListSessionsRequest())
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != 0 {
		t.Fatalf("deleted session listed: %#v", listResp.Sessions)
	}

	deleteErr := errors.New("delete store failed")
	if _, deleteGotErr := newTestAgent(WithSessionStore(&errorStore{deleteErr: deleteErr})).UnstableDeleteSession(ctx, DeleteSessionRequest("T-delete-store")); !errors.Is(deleteGotErr, deleteErr) {
		t.Fatalf("delete store error = %v", deleteGotErr)
	}

	loadErr := errors.New("load manifest failed")
	if _, loadGotErr := newTestAgent(WithSessionStore(&errorStore{loadErr: loadErr})).loadManifest(ctx, "T-any"); !errors.Is(loadGotErr, loadErr) {
		t.Fatalf("loadManifest store error = %v", loadGotErr)
	}

	badReplay := NewInMemorySessionStore()
	putStoredSession(t, badReplay, "T-bad-replay", cwd, []SessionStoreEntry{json.RawMessage(`{`)})
	badReplayAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(badReplay))
	if _, replayErr := badReplayAgent.LoadSession(ctx, LoadSessionRequest("T-bad-replay", cwd)); replayErr == nil {
		t.Fatal("bad transcript replay succeeded")
	}
	if _, active := badReplayAgent.sessions["T-bad-replay"]; active {
		t.Fatal("failed cold load retained its materialized session")
	}

	updateAgent := newTestAgent()
	updateAgent.setConnection(newClosedAgentConnection(t))
	updateSession := &agentSession{agent: updateAgent, id: "T-update-error"}
	updateEntries := []SessionStoreEntry{
		json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-update-error"}`),
	}
	if replayErr := updateSession.replayTranscriptEntries(ctx, updateEntries); replayErr == nil {
		t.Fatal("replay update failure was ignored")
	}

	validStore := NewInMemorySessionStore()
	putStoredSession(t, validStore, "T-meta", cwd, nil)
	if _, loadErr := newTestAgent(WithSessionStore(validStore)).LoadSession(ctx, LoadSessionRequest("T-meta", cwd, WithSessionMeta(map[string]any{"amp": "bad"}))); loadErr == nil {
		t.Fatal("load bad meta accepted after manifest")
	}
	if _, resumeErr := newTestAgent(WithSessionStore(validStore), WithDefaultModel("model")).ResumeSession(ctx, ResumeSessionRequest("T-meta", cwd)); resumeErr == nil {
		t.Fatal("resume default model accepted after manifest")
	}
	if _, loadErr := newTestAgent(WithSessionStore(validStore)).LoadSession(ctx, LoadSessionRequest("T-meta", cwd, WithSessionMCPServers(acp.McpServer{}))); loadErr == nil {
		t.Fatal("empty MCP transport accepted after manifest")
	}

	if _, _, err := parseAmpOptionsWithPresence(map[string]any{"model": 42}); err == nil {
		t.Fatal("non-string model accepted")
	}
	if _, _, err := parseAmpOptionsWithPresence(map[string]any{"outputSchema": map[string]any{}}); err == nil {
		t.Fatal("empty output schema accepted")
	}

	replaceErr := errors.New("replace failed")
	configAgent := newTestAgent(WithExecutablePath("/does/not/exist"), WithSessionStore(&errorStore{replaceErr: replaceErr}))
	configSession := &agentSession{agent: configAgent, id: "T-config", mode: "medium", turn: make(chan struct{}, 1)}
	if err := configSession.setConfig(ctx, configMode, "low"); !errors.Is(err, replaceErr) {
		t.Fatalf("setConfig replace error = %v", err)
	}
}

func TestPromptAndPersistenceEdges(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")

	closed := &agentSession{agent: newTestAgent(), id: "T-closed", closed: true, turn: make(chan struct{}, 1)}
	if _, err := closed.Prompt(ctx, TextPromptRequest("T-closed", "test-turn", "x")); !errors.Is(err, errSessionClosed) {
		t.Fatalf("closed prompt = %v", err)
	}

	busy := &agentSession{agent: newTestAgent(), id: "T-busy", turn: make(chan struct{}, 1)}
	busy.turn <- struct{}{}
	if _, err := busy.Prompt(ctx, TextPromptRequest("T-busy", "test-turn", "x")); err == nil || !strings.Contains(err.Error(), "backpressure") {
		t.Fatalf("busy prompt = %v", err)
	}

	badInput := &agentSession{agent: newTestAgent(), id: "T-input", turn: make(chan struct{}, 1)}
	if _, err := badInput.Prompt(ctx, acp.PromptRequest{SessionId: "T-input", Prompt: []acp.ContentBlock{acp.AudioBlock("audio", "audio/wav")}}); err == nil {
		t.Fatal("unsupported prompt input accepted")
	}

	continueErr := &agentSession{agent: newTestAgent(WithExecutablePath("/does/not/exist")), id: "T-continue", cwd: t.TempDir(), turn: make(chan struct{}, 1)}
	if _, err := continueErr.Prompt(ctx, TextPromptRequest("T-continue", "test-turn", "x")); err == nil {
		t.Fatal("native continue error ignored")
	}

	for _, tc := range []struct {
		name string
		mode string
		want string
	}{
		{name: "result error", mode: "result-error", want: "native failed"},
		{name: "no result", mode: "no-result", want: "stream ended without result"},
		{name: "malformed only", mode: "malformed-only", want: "decode amp json line"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			modePath, _ := fakeAgentAmpPath(t, tc.mode)
			agent := newTestAgent(WithExecutablePath(modePath), WithScratchDir(testScratchDir(t)))
			newResp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			_, err = agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "test-turn", "x"))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Prompt error = %v, want %q", err, tc.want)
			}
		})
	}

	updateAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	updateResp, err := updateAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession update: %v", err)
	}
	updateAgent.setConnection(newClosedAgentConnection(t))
	if _, updateErr := updateAgent.Prompt(ctx, TextPromptRequest(updateResp.SessionId, "test-turn", "x")); updateErr == nil {
		t.Fatal("session update failure was ignored")
	}

	persistAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	persistResp, err := persistAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession persist: %v", err)
	}
	persistErr := errors.New("persist replace failed")
	persistAgent.store = &errorStore{replaceErr: persistErr}
	if _, persistGotErr := persistAgent.Prompt(ctx, TextPromptRequest(persistResp.SessionId, "test-turn", "x")); !errors.Is(persistGotErr, persistErr) {
		t.Fatalf("prompt persist error = %v", persistGotErr)
	}

	cancelPath, state := fakeAgentAmpPath(t, "hang")
	cancelAgent := newTestAgent(WithExecutablePath(cancelPath), WithScratchDir(testScratchDir(t)))
	cancelResp, err := cancelAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession cancel: %v", err)
	}
	promptCtx, cancel := context.WithCancel(ctx)
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, promptErr := cancelAgent.Prompt(promptCtx, TextPromptRequest(cancelResp.SessionId, "test-turn", "x"))
		resultCh <- resp
		errCh <- promptErr
	}()
	waitForPath(t, state+"/stdin.jsonl")
	cancel()
	select {
	case promptErr := <-errCh:
		resp := <-resultCh
		if promptErr != nil || resp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("cancel prompt resp=%#v err=%v", resp, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel prompt did not return")
	}

	nilStoreAgent := newTestAgent()
	nilStoreAgent.store = nil
	nilStoreSession := &agentSession{agent: nilStoreAgent, id: "T-nil-store"}
	if persistErr := nilStoreSession.persistAfterTurn(ctx, []SessionStoreEntry{json.RawMessage(`{"type":"result"}`)}); persistErr != nil {
		t.Fatalf("nil-store persist: %v", persistErr)
	}
	atomicStore := &recordingStore{}
	atomicSession := &agentSession{agent: newTestAgent(WithSessionStore(atomicStore)), id: "T-atomic"}
	if persistErr := atomicSession.persistAfterTurn(ctx, []SessionStoreEntry{json.RawMessage(`{"type":"result"}`)}); persistErr != nil {
		t.Fatalf("atomic persist: %v", persistErr)
	}
	if atomicStore.appendCalls != 0 || atomicStore.replaceCalls != 1 {
		t.Fatalf("persist calls append=%d replace=%d", atomicStore.appendCalls, atomicStore.replaceCalls)
	}
	if len(atomicStore.lastReplacements) != 2 || atomicStore.lastReplacements[1].Key.Subpath != transcriptSubpath || len(atomicStore.lastReplacements[1].Entries) != 1 {
		t.Fatalf("atomic replacements = %#v", atomicStore.lastReplacements)
	}
}

func TestEmitAndRawEventEdges(t *testing.T) {
	ctx := context.Background()
	agent := newTestAgent()
	session := &agentSession{agent: agent, id: "T-emit", rawEvents: true}

	if err := session.emitRawEvent(ctx, "off", fakeAmpMessage{raw: map[string]any{"type": strings.Repeat("x", rawEventMaxBytes)}}); err != nil {
		t.Fatalf("raw truncation without connection: %v", err)
	}

	agent.setConnection(newClosedAgentConnection(t))
	if err := session.emitMessage(ctx, &amp.UserMessage{Content: []amp.ContentBlock{amp.TextBlock{Text: "user"}}}, true, ""); err == nil {
		t.Fatal("user text update failure ignored")
	}
	if err := session.emitMessage(ctx, &amp.UserMessage{Content: []amp.ContentBlock{amp.ToolResultBlock{ToolUseID: "TU", Content: "out"}}}, true, ""); err == nil {
		t.Fatal("tool result update failure ignored")
	}
	if err := session.emitMessage(ctx, &amp.AssistantMessage{Content: []amp.ContentBlock{amp.TextBlock{Text: "assistant"}}}, true, "message-id"); err == nil {
		t.Fatal("assistant text update failure ignored")
	}
	if err := session.emitMessage(ctx, &amp.AssistantMessage{Content: []amp.ContentBlock{amp.ToolUseBlock{ID: "TU", Name: "Read"}}}, true, "message-id"); err == nil {
		t.Fatal("tool use update failure ignored")
	}

	errs := make(chan error, 1)
	errs <- errors.New("turn failed")
	if err := receiveTurnError(fakeTurnErrors{errs: errs}); err == nil || !strings.Contains(err.Error(), "turn failed") {
		t.Fatalf("receiveTurnError = %v", err)
	}
	emptyErrs := make(chan error)
	if err := receiveTurnError(fakeTurnErrors{errs: emptyErrs}); err != nil {
		t.Fatalf("receiveTurnError empty = %v", err)
	}
	streamErrs := make(chan error, 1)
	streamErrs <- errors.New("stream failed")
	if _, err := streamEndedWithoutTerminal(ctx, nil, nil, nil, fakeTurnErrors{errs: streamErrs}); err == nil || !strings.Contains(err.Error(), "stream failed") {
		t.Fatalf("stream ended error = %v", err)
	}
	if _, err := streamEndedWithoutTerminal(ctx, nil, nil, nil, fakeTurnErrors{errs: emptyErrs}); err == nil || !strings.Contains(err.Error(), "stream ended without result") {
		t.Fatalf("stream ended default = %v", err)
	}
	messageID := "mid"
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	resp, err := promptErrorResponse(cancelCtx, nil, &acp.Usage{TotalTokens: 1}, &messageID, errors.New("late native error"))
	if err != nil || resp.StopReason != acp.StopReasonCancelled || resp.UserMessageId == nil || *resp.UserMessageId != messageID {
		t.Fatalf("cancel prompt error response = %#v err=%v", resp, err)
	}
	if _, err := promptErrorResponse(ctx, nil, nil, nil, errors.New("native error")); err == nil || !strings.Contains(err.Error(), "native error") {
		t.Fatalf("native prompt error response = %v", err)
	}
}

func TestStoreSortingAndTombstoneEdges(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-b", "/b", nil)
	putStoredSession(t, store, "T-a", "/a", nil)
	newer, err := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-new", NativeSessionID: "T-new", UpdatedAtUnixMilli: 3})
	if err != nil {
		t.Fatal(err)
	}
	if replaceErr := store.Replace(ctx, SessionKey{SessionID: "T-new", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{
		{Key: SessionKey{SessionID: "T-new", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{newer}},
	}); replaceErr != nil {
		t.Fatal(replaceErr)
	}
	store.mu.Lock()
	store.deleted[SessionKey{SessionID: "T-z", Subpath: SessionStoreMainSubpath}] = struct{}{}
	store.entries[SessionKey{SessionID: "T-z", Subpath: SessionStoreMainSubpath}] = []SessionStoreEntry{json.RawMessage(`{"format":"amp-thread-mirror-v1","sessionId":"T-z"}`)}
	// Ordering follows the store-tracked updatedAt, newest first.
	store.updatedAt[SessionKey{SessionID: "T-b", Subpath: SessionStoreMainSubpath}] = 1
	store.updatedAt[SessionKey{SessionID: "T-a", Subpath: SessionStoreMainSubpath}] = 2
	store.updatedAt[SessionKey{SessionID: "T-new", Subpath: SessionStoreMainSubpath}] = 3
	store.mu.Unlock()
	summaries, err := store.ListSessions(ctx)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(summaries) != 3 || summaries[0].SessionID != "T-new" || summaries[1].SessionID != "T-a" || summaries[2].SessionID != "T-b" {
		t.Fatalf("sorted summaries = %#v", summaries)
	}

	if appendErr := store.Append(ctx, SessionKey{SessionID: "T-a", Subpath: "z"}, []SessionStoreEntry{json.RawMessage(`"z"`)}); appendErr != nil {
		t.Fatal(appendErr)
	}
	if appendErr := store.Append(ctx, SessionKey{SessionID: "T-a", Subpath: "a"}, []SessionStoreEntry{json.RawMessage(`"a"`)}); appendErr != nil {
		t.Fatal(appendErr)
	}
	if deleteErr := store.Delete(ctx, SessionKey{SessionID: "T-a", Subpath: "z"}); deleteErr != nil {
		t.Fatal(deleteErr)
	}
	store.mu.Lock()
	store.entries[SessionKey{SessionID: "T-a", Subpath: "z"}] = []SessionStoreEntry{json.RawMessage(`"z"`)}
	store.deleted[SessionKey{SessionID: "T-a", Subpath: "z"}] = struct{}{}
	store.mu.Unlock()
	subkeys, err := store.ListSubkeys(ctx, SessionKey{SessionID: "T-a"})
	if err != nil {
		t.Fatalf("ListSubkeys: %v", err)
	}
	if len(subkeys) != 1 || subkeys[0] != "a" {
		t.Fatalf("subkeys = %#v", subkeys)
	}
}

func putStoredSession(t *testing.T, store *InMemorySessionStore, id string, cwd string, transcript []SessionStoreEntry) {
	t.Helper()
	manifest, err := json.Marshal(ampManifest{
		Format:             SessionStoreFormat,
		SessionID:          id,
		NativeSessionID:    id,
		Cwd:                cwd,
		Mode:               "medium",
		CreatedAtUnixMilli: 1,
		UpdatedAtUnixMilli: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	replacements := []SessionStoreReplacement{
		{Key: SessionKey{SessionID: id, Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{manifest}},
	}
	if transcript != nil {
		replacements = append(replacements, SessionStoreReplacement{Key: SessionKey{SessionID: id, Subpath: transcriptSubpath}, Entries: transcript})
	}
	if err := store.Replace(context.Background(), SessionKey{SessionID: id, Subpath: SessionStoreMainSubpath}, replacements); err != nil {
		t.Fatal(err)
	}
}

func waitForPath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s was not created", path)
}

type fakeTurnErrors struct {
	errs <-chan error
}

func (f fakeTurnErrors) Errors() <-chan error { return f.errs }

// TestReconcileNativeConfigReadBack pins R5-7: when amp's stream-json init frame
// reports a mode that diverges from what the host requested, the wrapper
// reconciles session state to amp's truth, emits a config_option_update, and
// subsequent config-option reads report the native values rather than the echoed
// request.
func TestReconcileNativeConfigReadBack(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "reconcile-config")
	conn, client, cleanup := startTestServe(t,
		WithExecutablePath(path),
		WithScratchDir(testScratchDir(t)),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	defer cleanup()
	ctx := context.Background()

	if _, err := conn.Initialize(ctx, acp.InitializeRequest{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	cwd := t.TempDir()
	newResp, err := conn.NewSession(ctx, NewSessionRequest(cwd,
		WithSessionAmpOptions(NewAmpOptions(WithAmpMode("medium"))),
	))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// Requested surface is echoed before any native report.
	requireConfigMode(t, newResp.ConfigOptions, "medium")

	if _, promptErr := conn.Prompt(ctx, acp.PromptRequest{
		SessionId: newResp.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock("hello")},
	}); promptErr != nil {
		t.Fatalf("Prompt: %v", promptErr)
	}

	// The turn's init frame reports high/max: a config_option_update carries the
	// reconciled native truth to the host.
	var reconciled []acp.SessionConfigOption
	for _, notification := range client.updatesSnapshot() {
		if update := notification.Update.ConfigOptionUpdate; update != nil {
			reconciled = update.ConfigOptions
		}
	}
	if reconciled == nil {
		t.Fatalf("no config_option_update emitted; updates = %#v", client.updatesSnapshot())
	}
	requireConfigMode(t, reconciled, "high")

	// A subsequent read-back (resume of the active session) reports amp's truth,
	// not the originally requested medium/low.
	resumeResp, err := conn.ResumeSession(ctx, ResumeSessionRequest(newResp.SessionId, cwd))
	if err != nil {
		t.Fatalf("ResumeSession: %v", err)
	}
	requireConfigMode(t, resumeResp.ConfigOptions, "high")
}

// TestReconcileNativeConfigEmitFailureAbortsTurn covers the reconcile branch in
// the prompt loop: when the config_option_update carrying reconciled native
// mode cannot be delivered, the turn aborts with the delivery error.
func TestReconcileNativeConfigEmitFailureAbortsTurn(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "reconcile-config")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	newResp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	agent.setConnection(newClosedAgentConnection(t))
	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "test-turn", "x")); promptErr == nil {
		t.Fatal("reconcile config update failure was ignored")
	}
}

func requireConfigMode(t *testing.T, options []acp.SessionConfigOption, wantMode string) {
	t.Helper()
	got := make(map[string]string, len(options))
	for _, option := range options {
		if option.Select == nil {
			continue
		}
		got[string(option.Select.Id)] = string(option.Select.CurrentValue)
	}
	if got[string(configMode)] != wantMode {
		t.Fatalf("mode current value = %q, want %q", got[string(configMode)], wantMode)
	}
}

// TestListSessionsMergePaginationAndCwd pins the session/list contract: active
// in-memory sessions merge with store-backed summaries and dedupe, the cwd
// filter keeps empty-Cwd summaries, ordering is deterministic, and the cursor
// is a base64 RawURL offset whose past-end and undecodable forms are invalid
// params.
func TestListSessionsMergePaginationAndCwd(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))

	// One active session, whose id also exists in the store (dedupe), one
	// store-only session in another cwd, and one store-only empty-cwd session.
	newResp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	putStoredSession(t, store, "T-other-cwd", "/tmp/elsewhere", nil)
	putStoredSession(t, store, "T-no-cwd", "", nil)

	resp, err := agent.ListSessions(ctx, ListSessionsRequest())
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(resp.Sessions) != 3 || resp.NextCursor != nil {
		t.Fatalf("unfiltered sessions = %#v next=%v", resp.Sessions, resp.NextCursor)
	}

	// The cwd filter keeps the active session and the empty-Cwd summary but
	// drops the mismatching store summary; the active id appears exactly once.
	resp, err = agent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd(cwd)))
	if err != nil {
		t.Fatalf("ListSessions cwd: %v", err)
	}
	ids := make(map[acp.SessionId]int, len(resp.Sessions))
	for _, info := range resp.Sessions {
		ids[info.SessionId]++
	}
	if len(resp.Sessions) != 2 || ids[newResp.SessionId] != 1 || ids["T-no-cwd"] != 1 {
		t.Fatalf("cwd-filtered sessions = %#v", resp.Sessions)
	}

	// A cwd filter that matches no active session drops it while keeping the
	// matching store summary and the empty-Cwd summary.
	resp, err = agent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd("/tmp/elsewhere")))
	if err != nil {
		t.Fatalf("ListSessions other cwd: %v", err)
	}
	for _, info := range resp.Sessions {
		if info.SessionId == newResp.SessionId {
			t.Fatalf("active session leaked past cwd filter: %#v", resp.Sessions)
		}
	}
	if len(resp.Sessions) != 2 {
		t.Fatalf("other-cwd sessions = %#v", resp.Sessions)
	}

	// A relative cwd filter is invalid params.
	_, err = agent.ListSessions(ctx, ListSessionsRequest(WithListSessionsCwd("relative")))
	requireRequestErrorCode(t, err, -32602)

	// An undecodable cursor and a past-end cursor are invalid params.
	badCursor := "not-base64!"
	_, err = agent.ListSessions(ctx, acp.ListSessionsRequest{Cursor: &badCursor})
	requireRequestErrorCode(t, err, -32602)
	pastEnd := encodeListCursor(100)
	_, err = agent.ListSessions(ctx, acp.ListSessionsRequest{Cursor: &pastEnd})
	requireRequestErrorCode(t, err, -32602)

	// A valid in-range cursor pages from that offset.
	fromOne := encodeListCursor(1)
	resp, err = agent.ListSessions(ctx, acp.ListSessionsRequest{Cursor: &fromOne})
	if err != nil || len(resp.Sessions) != 2 {
		t.Fatalf("offset page = %#v err=%v", resp.Sessions, err)
	}
}

// TestPaginateSessionInfosCursorEdges covers the raw cursor helpers: page-size
// windows emit NextCursor, negative and non-numeric cursors fail decode, and
// ordering helper compares by UpdatedAt then SessionId.
func TestPaginateSessionInfosCursorEdges(t *testing.T) {
	infos := make([]acp.SessionInfo, 0, listSessionsPageSize+2)
	for i := 0; i < listSessionsPageSize+2; i++ {
		infos = append(infos, acp.SessionInfo{SessionId: acp.SessionId(fmt.Sprintf("T-%03d", i))})
	}

	paged, next, err := paginateSessionInfos(infos, nil)
	if err != nil || len(paged) != listSessionsPageSize || next == nil {
		t.Fatalf("first page = %d next=%v err=%v", len(paged), next, err)
	}

	paged, next, err = paginateSessionInfos(infos, next)
	if err != nil || len(paged) != 2 || next != nil {
		t.Fatalf("second page = %d next=%v err=%v", len(paged), next, err)
	}

	negative := base64.RawURLEncoding.EncodeToString([]byte("-1"))
	if _, err := decodeListCursor(&negative); err == nil {
		t.Fatal("negative cursor decoded")
	}
	alpha := base64.RawURLEncoding.EncodeToString([]byte("abc"))
	if _, err := decodeListCursor(&alpha); err == nil {
		t.Fatal("non-numeric cursor decoded")
	}

	older := "2020-01-01T00:00:00Z"
	newer := "2024-01-01T00:00:00Z"
	left := acp.SessionInfo{SessionId: "T-a", UpdatedAt: &older}
	right := acp.SessionInfo{SessionId: "T-b", UpdatedAt: &newer}
	if compareSessionInfos(left, right) <= 0 {
		t.Fatal("newer session did not sort first")
	}
	tied := acp.SessionInfo{SessionId: "T-c", UpdatedAt: &older}
	if compareSessionInfos(left, tied) >= 0 {
		t.Fatal("session id tie-break failed")
	}
	if compareSessionInfos(acp.SessionInfo{SessionId: "T-a"}, acp.SessionInfo{SessionId: "T-a"}) != 0 {
		t.Fatal("identical infos not equal")
	}
}

// TestSessionStoreTimeoutFallback pins the load-timeout resolution: a
// non-positive configured timeout falls back to the package default.
func TestSessionStoreTimeoutFallback(t *testing.T) {
	agent := newTestAgent(WithSessionStoreLoadTimeout(-1))
	if got := agent.sessionStoreLoadTimeout(); got != defaultSessionStoreTimeout {
		t.Fatalf("fallback timeout = %v", got)
	}
	agent = newTestAgent(WithSessionStoreLoadTimeout(time.Minute))
	if got := agent.sessionStoreLoadTimeout(); got != time.Minute {
		t.Fatalf("configured timeout = %v", got)
	}
}

func TestLifecycleSessionConstructionErrorsPropagate(t *testing.T) {
	originalNew := newLifecycleAgentSession
	t.Cleanup(func() { newLifecycleAgentSession = originalNew })
	want := errors.New("construct session")
	newLifecycleAgentSession = func(context.Context, *Agent, acp.SessionId, string, parsedSessionMeta, string, []string) (*agentSession, error) {
		return nil, want
	}

	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	cwd := t.TempDir()
	if _, err := agent.NewSession(t.Context(), NewSessionRequest(cwd)); !errors.Is(err, want) {
		t.Fatalf("new-session construction error = %v", err)
	}

	sessionID := acp.SessionId("T-load-construction")
	manifest, err := json.Marshal(ampManifest{
		Format: SessionStoreFormat, SessionID: string(sessionID), NativeSessionID: string(sessionID), Cwd: cwd,
		CreatedAtUnixMilli: 1, UpdatedAtUnixMilli: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	main := SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath}
	if err := store.Replace(t.Context(), main, []SessionStoreReplacement{{Key: main, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := agent.loadOrResume(t.Context(), sessionID, cwd, nil, nil, nil); !errors.Is(err, want) {
		t.Fatalf("load-session construction error = %v", err)
	}
}

func TestZeroTurnSessionDeleteRunsNoNativeCommand(t *testing.T) {
	ctx := context.Background()
	path, state := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))

	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if out, versionErr := exec.Command(path, "version").CombinedOutput(); versionErr != nil {
		t.Fatalf("seed fake amp recording: %v\n%s", versionErr, out)
	}
	before := len(readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl")))

	if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(resp.SessionId)); err != nil {
		t.Fatalf("zero-turn delete: %v", err)
	}
	after := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	if len(after) != before {
		t.Fatalf("zero-turn delete launched native commands: %#v", after[before:])
	}
	if _, loadErr := agent.LoadSession(ctx, LoadSessionRequest(resp.SessionId, "")); loadErr == nil {
		t.Fatal("deleted zero-turn session loaded")
	}
}
