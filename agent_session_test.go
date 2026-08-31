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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

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

// The members that can make a set-config request unsupported are distinct
// caller mistakes and are named as such: a boolean payload chose the wrong
// request discriminator, while a request carrying neither variant — or a value
// variant carrying the empty string — supplied no value at all. Both variants
// are marshalled inline, so the discriminator the caller got wrong is reachable
// on the wire only as `type`. The empty value is refused on the request's shape
// and not as a value gate: it is the one string that names no mode, while every
// mode amp might know travels through untouched.
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

	// The empty value is refused on the same member and before the session is
	// even looked up, because a request that names no mode is malformed whether
	// or not the session it names exists.
	_, err = agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest("T-config", configMode, ""))
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
	if _, err := os.Stat(filepath.Join(session.settingsDir, "mcp.json")); err != nil {
		t.Fatalf("materialized MCP config: %v", err)
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

// TestLoadFailsClosedDuringExactCloseFlight pins the hard ownership boundary:
// close keeps the installed wrapper until local cleanup settles, while a load
// arriving behind the published close flight starts no replacement wrapper.
func TestLoadFailsClosedDuringExactCloseFlight(t *testing.T) {
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

	cleanupStarted := make(chan struct{})
	finishTeardown := make(chan struct{})
	releaseScratch := closing.scratchRootRelease
	closing.scratchRootRelease = func() {
		close(cleanupStarted)
		awaitCorrectionCallback(t, finishTeardown, "session cleanup release")

		if releaseScratch != nil {
			releaseScratch()
		}
	}

	closed := make(chan error, 1)

	go func() {
		_, closeErr := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: "T-own"})
		closed <- closeErr
	}()

	awaitCorrectionSignal(t, cleanupStarted, "session cleanup start")

	if _, err := agent.LoadSession(ctx, LoadSessionRequest("T-own", cwd)); err == nil {
		t.Fatal("load entered behind the published close flight")
	}
	if agent.sessions["T-own"] != closing {
		t.Fatal("close flight lost its exact installed wrapper during cleanup")
	}

	close(finishTeardown)

	if err := <-closed; err != nil {
		t.Fatalf("CloseSession: %v", err)
	}

	if agent.sessions["T-own"] != nil {
		t.Fatal("settled close retained its wrapper")
	}
	if got := collectActiveSessions(t, reader); got != 0 {
		t.Fatalf("active sessions = %d, want 0", got)
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

// TestCloseContainmentFailureRetainsTheExactInstalledSession pins the close
// ownership boundary: an incomplete process proof cannot evict the wrapper or
// release the settings and scratch state a surviving descendant may still use.
// Once a later attempt can prove the boundary, that same pointer is reclaimed.
func TestCloseContainmentFailureRetainsTheExactInstalledSession(t *testing.T) {
	authority := newRecordingAuthority()
	agent := newTestAgent(
		WithHostAuthority(authority),
		WithScratchDir(testScratchDir(t)),
	)

	session, err := newAgentSession(t.Context(), agent, "T-close-containment", t.TempDir(), parsedSessionMeta{}, "", nil)
	if err != nil {
		t.Fatalf("construct session: %v", err)
	}
	agent.mu.Lock()
	agent.activateSessionLocked(session)
	agent.mu.Unlock()
	agent.observe.AddActiveSession(t.Context(), 1)

	authority.reclaimErr = ErrContainmentIncomplete

	_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id})
	if !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("CloseSession = %v, want containment sentinel", err)
	}

	retained, lookupErr := agent.session(session.id)
	if lookupErr != nil || retained != session {
		t.Fatalf("retained session = %p err=%v, want exact %p", retained, lookupErr, session)
	}
	listed, listErr := agent.ListSessions(t.Context(), ListSessionsRequest())
	if listErr != nil || len(listed.Sessions) != 1 || listed.Sessions[0].SessionId != session.id {
		t.Fatalf("list after failed close = %#v err=%v", listed.Sessions, listErr)
	}
	if _, statErr := os.Stat(session.settingsDir); statErr != nil {
		t.Fatalf("failed close removed settings dir: %v", statErr)
	}

	// A later successful reclaim proves that path ownership returned, while the
	// authority failure remains latched against new native admission.
	authority.reclaimErr = nil

	if _, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.id}); err != nil {
		t.Fatalf("retry CloseSession: %v", err)
	}
	if _, lookupErr := agent.session(session.id); lookupErr == nil {
		t.Fatal("successful retry left the session addressable")
	}
	if _, statErr := os.Stat(session.settingsDir); !os.IsNotExist(statErr) {
		t.Fatalf("successful retry retained settings dir: %v", statErr)
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
//
// The requested mode is deliberately one this build does not advertise. Nothing
// local judges a mode value, so this read-back is the whole safety net: it is
// what turns a server-side substitution into something the host can see, and it
// has to work for exactly the values the adapter itself cannot recognize.
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
		WithSessionAmpOptions(NewAmpOptions(WithAmpMode("turbo-experimental"))),
	))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// Requested surface is echoed before any native report, unadvertised value
	// and all: establishment carries the host's word to amp without judging it.
	requireConfigMode(t, newResp.ConfigOptions, "turbo-experimental")

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
	// not the mode the host originally asked for.
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
	if _, _, _, _, err := agent.loadOrResume(t.Context(), sessionID, cwd, nil, nil, nil); !errors.Is(err, want) {
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
	agent := newTestAgent(
		WithExecutablePath(path),
		WithSessionStore(base),
		WithScratchDir(testScratchDir(t)),
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
	require.NoError(t, agent.Close())
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
	t.Setenv("AMP_API_KEY", "ownership-test")
	path, _ := fakeAgentAmpPath(t, "")

	for _, callback := range []string{"replace", "load", "resume"} {
		for _, action := range []string{"close", "load", "resume"} {
			t.Run(callback+"/"+action, func(t *testing.T) {
				store := &backgroundReentryStore{SessionStore: NewInMemorySessionStore()}
				agent := newTestAgent(
					WithExecutablePath(path),
					WithSessionStore(store),
					WithScratchDir(testScratchDir(t)),
				)
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
	for _, action := range []string{"close", "delete"} {
		t.Run(action, func(t *testing.T) {
			agent := newTestAgent(WithSessionStore(NewInMemorySessionStore()), WithScratchDir(testScratchDir(t)))
			id := acp.SessionId("T-prompt-reentry-" + action)
			session := installActiveTestSession(t, agent, id)
			prompt := newPromptTurnState()
			require.NoError(t, session.admitPrompt(prompt))
			promptCtx := withCallbackProvenance(t.Context(), agent, prompt)
			client := &synchronousTeardownClient{agent: agent, sessionID: id, action: action}
			agent.setConnection(client)
			require.NoError(t, session.emitUpdate(promptCtx, session.configUpdate()))

			requireClosedCallbackRefusal(t, client.err)
			requireSessionUnchangedByReentry(t, agent, session, prompt)
			session.clearActivePrompt(prompt)
			prompt.complete(nil)

			_, err := agent.SetSessionConfigOption(t.Context(), SetConfigOptionRequest(id, configMode, modeHigh))
			require.NoError(t, err)
			require.NoError(t, agent.Close())
		})
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
			agent.options.runtime.settleTurn = func(turn *amp.Turn) error {
				if stage == "settle" && settleCalls.Add(1) == 1 {
					client.state = session.activePromptState()
					panic("prompt settle panic")
				}

				enteredOnce.Do(func() { close(containmentEntered) })
				awaitCorrectionCallback(t, containmentRelease, "prompt containment release")

				return originalSettle(turn)
			}
			if stage == "launch" {
				agent.options.runtime.executeThread = func(context.Context, *amp.Client, any) (*amp.Turn, error) {
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
				require.ErrorIs(t, settlement.containmentErr, amp.ErrContainmentIncomplete)
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
	agent.options.runtime.startupProbe = func(context.Context, *amp.Client) (string, error) { return path, nil }

	session, err := newAgentSession(t.Context(), agent, "T-delete-before-load", t.TempDir(), parsedSessionMeta{}, "", nil)
	require.NoError(t, err)
	session.nativeID = "T-export-must-not-start"
	activateOwnershipTestSession(t, agent, session)

	exported := make(chan struct{}, 1)
	agent.options.runtime.exportThread = func(context.Context, *amp.Client, string) (json.RawMessage, error) {
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
	agent.options.runtime.startupProbe = func(context.Context, *amp.Client) (string, error) { return path, nil }

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
	agent.options.runtime.startupProbe = func(context.Context, *amp.Client) (string, error) { return path, nil }
	agent.options.runtime.exportThread = func(context.Context, *amp.Client, string) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	}

	_, err := agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
	require.NoError(t, err)
	requireClosedCallbackRefusal(t, store.deleteErr)
	requireClosedCallbackRefusal(t, store.loadErr)
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
	require.Eventually(t, func() bool {
		agent.mu.Lock()
		defer agent.mu.Unlock()
		flight := agent.sessionFlights[session.id]

		return flight != nil && flight.kind == agentSessionCloseFlight && flight.use != nil
	}, time.Second, time.Millisecond, "close did not publish its join on the admitted config use")

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

			prepared := make(chan *agentSession, 1)
			continuePrepared := make(chan struct{})
			agent := newTestAgent(
				WithExecutablePath(path),
				WithSessionStore(store),
				WithScratchDir(testScratchDir(t)),
			)
			agent.options.runtime.afterColdSessionPrepared = func(session *agentSession) {
				prepared <- session
				<-continuePrepared
			}
			agent.options.runtime.startupProbe = func(context.Context, *amp.Client) (string, error) { return path, nil }
			agent.options.runtime.exportThread = func(context.Context, *amp.Client, string) (json.RawMessage, error) {
				return json.RawMessage(`{}`), nil
			}

			loaded := make(chan error, 1)
			go func() {
				_, err := agent.LoadSession(t.Context(), LoadSessionRequest(id, cwd))
				loaded <- err
			}()
			preparedSession := <-prepared

			flightCtx, flight, use, existing, err := agent.publishSessionFlight(t.Context(), id, agentSessionDeleteFlight, nil)
			require.NoError(t, err)
			require.Nil(t, existing)
			require.NotNil(t, use)
			close(continuePrepared)
			require.NoError(t, agent.joinSessionFlightUse(flightCtx, id, flight, use))
			require.Error(t, <-loaded)
			require.NotNil(t, flight.session)
			require.Same(t, preparedSession, flight.session)
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
		agent.options.runtime.startupProbe = func(context.Context, *amp.Client) (string, error) {
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
		agent.options.runtime.exportThread = func(context.Context, *amp.Client, string) (json.RawMessage, error) {
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
		agent.options.runtime.exportThread = func(context.Context, *amp.Client, string) (json.RawMessage, error) {
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
		scratchContainmentErr: amp.ErrContainmentIncomplete,
	}
	require.ErrorIs(t, incomplete.deleteAtShutdown(t.Context()), amp.ErrContainmentIncomplete)

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
		scratchContainmentErr: amp.ErrContainmentIncomplete,
	}
	agent.retainCleanupOwner(incomplete.id, incomplete, agentCleanupPrepared)
	loaded, transcript, started, cleanupUse, err := agent.loadOrResume(t.Context(), incomplete.id, t.TempDir(), nil, nil, nil)
	require.ErrorIs(t, err, amp.ErrContainmentIncomplete)
	require.Nil(t, loaded)
	require.Nil(t, transcript)
	require.False(t, started)
	require.Nil(t, cleanupUse)
	require.ErrorIs(t, agent.Close(), amp.ErrContainmentIncomplete)
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
	require.Equal(t, "containment_incomplete", cleanupFailureClass(amp.ErrContainmentIncomplete))
	require.Equal(t, "cancelled", cleanupFailureClass(context.Canceled))
	require.Equal(t, "deadline", cleanupFailureClass(context.DeadlineExceeded))
	require.Equal(t, "cleanup_failed", cleanupFailureClass(errors.New("secret callback detail")))

	require.ErrorContains(t, (&promptStream{}).deliver(t.Context(), acp.SessionNotification{}), "lifecycle delivery unavailable")
}

func TestLastWordShutdownContainsDetachedSessionCleanupPanic(t *testing.T) {
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
}

func TestSettlementPreparationAndPanicContainmentDefensiveBranches(t *testing.T) {
	agent := newTestAgent()
	session := &agentSession{agent: agent, id: "T-settlement-defensive", turn: make(chan struct{}, 1)}
	incarnation := &promptStream{
		session: session,
		stream:  lifecycle.NewStream("stream-defensive", negotiatedAnswer()),
	}
	incarnation.stream.Fence()
	agent.options.runtime.settleTurn = func(*amp.Turn) error {
		return nil
	}
	state := newPromptTurnState()
	_, err := session.settlePrompt(t.Context(), &amp.Turn{}, state, incarnation, promptResult{})
	require.ErrorContains(t, err, "amp_lifecycle_violation")
	require.Error(t, state.awaitSettlement(t.Context()).deliveryErr)

	agent.options.runtime.settleTurn = func(*amp.Turn) error {
		panic("containment retry panic")
	}
	err = session.settleTurnAfterPanic(&amp.Turn{})
	require.ErrorIs(t, err, amp.ErrContainmentIncomplete)
}
