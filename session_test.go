//nolint:nlreturn // Edge tests keep related contract branches together.
package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestAgentLifecycleErrorBranches(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	if _, err := NewAgent(WithExecutablePath(path)).NewSession(ctx, acp.NewSessionRequest{Meta: map[string]any{"amp": "bad"}}); err == nil {
		t.Fatal("bad meta accepted")
	}
	if _, err := NewAgent(WithExecutablePath(path), WithDefaultModel("model")).NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("default model accepted at session start")
	}
	if _, err := NewAgent(WithExecutablePath(path)).NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "s", Url: "u"}}))); err == nil {
		t.Fatal("sse mcp accepted")
	}
	fileHome := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(fileHome, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAgent(WithExecutablePath(path), WithScratchDir(fileHome)).NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("file scratch dir accepted")
	}
	badPath, _ := fakeAgentAmpPath(t, "bad-new-id")
	if _, err := NewAgent(WithExecutablePath(badPath), WithScratchDir(t.TempDir())).NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("bad native thread id accepted")
	}
	storeErr := &errorStore{loadErr: errors.New("load failed")}
	if _, err := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(storeErr)).NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("persist load error ignored")
	}
	limited := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	if _, err := limited.NewSession(ctx, NewSessionRequest(t.TempDir())); err != nil {
		t.Fatalf("first limited NewSession: %v", err)
	}
	if _, err := limited.NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil || !strings.Contains(err.Error(), "backpressure") {
		t.Fatalf("second limited NewSession = %v", err)
	}
	limited.closed = true
	if err := limited.reserveSessionSlot(); err == nil {
		t.Fatal("closed agent reserved slot")
	}
	limited.releaseSessionSlot("T-unused")
}

func TestLoadResumeManifestAndConfigBranches(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	store := NewInMemorySessionStore()
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-load", Cwd: cwd, Mode: "high", Effort: "max", CreatedAtUnixMilli: 1, UpdatedAtUnixMilli: 2})
	if err := store.Replace(ctx, SessionKey{SessionID: "T-load", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{
		{Key: SessionKey{SessionID: "T-load", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{manifest}},
		{Key: SessionKey{SessionID: "T-load", Subpath: transcriptSubpath}, Entries: []SessionStoreEntry{
			json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-load"}`),
			json.RawMessage(`{"type":"result","subtype":"success","is_error":false,"session_id":"T-load"}`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 2}))
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	if _, err := agent.LoadSession(ctx, LoadSessionRequest("T-load", cwd, WithSessionRawEvents(true))); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	// Authoritative load replay emits session/update frames only. Raw events are
	// live-turn only and are never replayed from the store, even with raw events
	// enabled on the load request.
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) > 0 })
	if len(client.updatesSnapshot()) == 0 {
		t.Fatal("load did not replay transcript")
	}
	if len(client.rawSnapshot()) != 0 {
		t.Fatalf("load replayed raw events: %d", len(client.rawSnapshot()))
	}
	before := len(client.updatesSnapshot())
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest("T-load", cwd)); err != nil {
		t.Fatalf("ResumeSession active: %v", err)
	}
	if len(client.updatesSnapshot()) != before {
		t.Fatal("resume replayed active session")
	}

	if _, err := agent.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{Boolean: &acp.SetSessionConfigOptionBoolean{SessionId: "T-load", ConfigId: "mode", Value: true}}); err == nil {
		t.Fatal("boolean config accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, acp.SetSessionConfigOptionRequest{}); err == nil {
		t.Fatal("missing value config accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("T-load", "mode", "low")); err != nil {
		t.Fatalf("set mode: %v", err)
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("T-load", "effort", "low")); err != nil {
		t.Fatalf("set effort: %v", err)
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("T-load", "mode", "bad")); err == nil {
		t.Fatal("bad mode accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("T-load", "effort", "bad")); err == nil {
		t.Fatal("bad effort accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("T-load", "unknown", "x")); err == nil {
		t.Fatal("unknown config accepted")
	}
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("T-missing", "mode", "low")); err == nil {
		t.Fatal("unknown config session accepted")
	}

	for _, entry := range []SessionStoreEntry{json.RawMessage(`{`)} {
		badStore := NewInMemorySessionStore()
		if err := badStore.Replace(ctx, SessionKey{SessionID: "T-bad", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{
			{Key: SessionKey{SessionID: "T-bad", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{manifest}},
			{Key: SessionKey{SessionID: "T-bad", Subpath: transcriptSubpath}, Entries: []SessionStoreEntry{entry}},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(badStore)).LoadSession(ctx, LoadSessionRequest("T-bad", cwd)); err == nil {
			t.Fatal("bad transcript replay accepted")
		}
	}
}

func TestLoadManifestErrorsAndListFilters(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	if _, err := agent.loadManifest(ctx, "T-missing"); err == nil {
		t.Fatal("missing manifest accepted")
	}
	for _, entry := range []SessionStoreEntry{
		json.RawMessage(`{`),
		json.RawMessage(`{"format":"wrong","threadId":"T-bad"}`),
		json.RawMessage(`{"format":"amp-thread-mirror-v1","threadId":"other"}`),
	} {
		store := NewInMemorySessionStore()
		if err := store.Replace(ctx, SessionKey{SessionID: "T-bad", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "T-bad", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{entry}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := NewAgent(WithSessionStore(store)).loadManifest(ctx, "T-bad"); err == nil {
			t.Fatalf("bad manifest accepted: %s", entry)
		}
	}
	errStore := &errorStore{listErr: errors.New("list failed")}
	if _, err := NewAgent(WithSessionStore(errStore)).ListSessions(ctx, acp.ListSessionsRequest{}); err == nil {
		t.Fatal("list error ignored")
	}
	store := NewInMemorySessionStore()
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-list", Cwd: "/cwd", UpdatedAtUnixMilli: 0})
	if err := store.Replace(ctx, SessionKey{SessionID: "T-list", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "T-list", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}
	resp, err := NewAgent(WithSessionStore(store)).ListSessions(ctx, acp.ListSessionsRequest{Cwd: acp.Ptr("/other")})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Sessions) != 0 {
		t.Fatalf("cwd filter failed: %#v", resp.Sessions)
	}
}

func TestRemainingAgentBranches(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	agent := NewAgent(WithExecutablePath(path))
	if millisToRFC3339(0) != "" {
		t.Fatal("zero millis formatted")
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest("T-x", "", WithSessionMeta(map[string]any{"amp": "bad"}))); err == nil {
		t.Fatal("load bad meta accepted")
	}
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest("T-x", "", WithSessionAmpOptions(AmpOptions{Model: "bad"}))); err == nil {
		t.Fatal("resume bad options accepted")
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest("T-x", t.TempDir(), WithSessionMCPServers(acp.McpServer{Acp: &acp.McpServerAcpInline{Name: "a", Id: "id"}}))); err == nil {
		t.Fatal("load bad mcp accepted")
	}
	fileHome := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileHome, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewInMemorySessionStore()
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-file", Cwd: t.TempDir()})
	if err := store.Replace(ctx, SessionKey{SessionID: "T-file", Subpath: ""}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "T-file", Subpath: ""}, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAgent(WithExecutablePath(path), WithScratchDir(fileHome), WithSessionStore(store)).LoadSession(ctx, LoadSessionRequest("T-file", t.TempDir())); err == nil {
		t.Fatal("load with file scratch dir accepted")
	}
	activeLimited := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 0}))
	activeLimited.options.ConcurrencyLimits.MaxActiveSessions = 0
	if _, err := activeLimited.loadOrResume(ctx, "T-file", t.TempDir(), nil, nil, nil); err != nil {
		t.Fatalf("loadOrResume direct: %v", err)
	}
	activeLimited.options.ConcurrencyLimits.MaxActiveSessions = 1
	manifest2, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-file-2", Cwd: t.TempDir()})
	if err := store.Replace(ctx, SessionKey{SessionID: "T-file-2", Subpath: ""}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "T-file-2", Subpath: ""}, Entries: []SessionStoreEntry{manifest2}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := activeLimited.loadOrResume(ctx, "T-file-2", t.TempDir(), nil, nil, nil); err == nil {
		t.Fatal("active load backpressure not enforced")
	}
	agent.markDeleted("T-deleted")
	// A tombstoned session is wire-indistinguishable from one that never
	// existed: prompt/cancel/close all yield the uniform unknown-session shape.
	for _, id := range []acp.SessionId{"T-deleted", "T-missing"} {
		_, promptErr := agent.Prompt(ctx, TextPromptRequest(id, "test-turn", "x"))
		requireUnknownSessionError(t, promptErr)
		requireUnknownSessionError(t, agent.Cancel(ctx, acp.CancelNotification{SessionId: id}))
		_, closeErr := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: id})
		requireUnknownSessionError(t, closeErr)
	}
	options := ampOptionsPayload(AmpOptions{Model: "m", OutputSchema: map[string]any{"type": "object"}})
	if options["model"] != "m" || options["outputSchema"] == nil {
		t.Fatalf("ampOptionsPayload missing fields: %#v", options)
	}
}

func TestPromptInputAndEmitBranches(t *testing.T) {
	title, mime, desc := "Title", "text/plain", "desc"
	payload, err := promptInput([]acp.ContentBlock{
		acp.TextBlock("text"),
		acp.ImageBlock("aW1n", "image/png"),
		{ResourceLink: &acp.ContentBlockResourceLink{Name: "n", Uri: "file:///x", Title: &title, MimeType: &mime, Description: &desc}},
		acp.ResourceBlock(acp.EmbeddedResourceResource{TextResourceContents: &acp.TextResourceContents{Uri: "file:///t", Text: "body", MimeType: &mime}}),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///i", Blob: "aW1n", MimeType: acp.Ptr("image/png")}}),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///b", Blob: "YmxvYg==", MimeType: &mime}}),
	})
	if err != nil {
		t.Fatal(err)
	}
	message, ok := payload["message"].(map[string]any)
	if !ok {
		t.Fatalf("message = %#v", payload["message"])
	}
	content, ok := message["content"].([]map[string]any)
	if !ok {
		t.Fatalf("content = %#v", message["content"])
	}
	if len(content) != 6 {
		t.Fatalf("content len = %d", len(content))
	}
	// An unsupported content block (e.g. audio) is rejected fail-closed with the
	// uniform -32602 shape {error:"unsupported", field:"prompt"}.
	_, audioErr := promptInput([]acp.ContentBlock{acp.AudioBlock("audio", "audio/wav")})
	requireInvalidParamsData(t, audioErr, map[string]any{
		jsonFieldError: valUnsupported,
		jsonFieldField: "prompt",
	})

	if _, err := promptInput([]acp.ContentBlock{acp.ResourceBlock(acp.EmbeddedResourceResource{})}); err == nil {
		t.Fatal("empty embedded resource accepted")
	}
	session := &agentSession{agent: NewAgent(), id: "T-emit", rawEvents: true}
	if err := session.emitUsage(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := session.emitUpdate(context.Background(), acp.UpdateAgentMessageText("no conn")); err != nil {
		t.Fatal(err)
	}
	if err := session.emitRawEvent(context.Background(), "none", fakeAmpMessage{raw: map[string]any{"type": "x"}}); err != nil {
		t.Fatal(err)
	}
	session.agent.setConnection(newClosedAgentConnection(t))
	if err := session.emitUpdate(context.Background(), acp.UpdateAgentMessageText("fail")); err == nil {
		t.Fatal("update failure ignored")
	}
	if err := session.emitRawEvent(context.Background(), "bad", fakeAmpMessage{raw: map[string]any{"bad": func() {}}}); err == nil {
		t.Fatal("raw marshal failure ignored")
	}
	if usageFromAmp(nil) != nil {
		t.Fatal("nil usage converted")
	}
	if err := classifyNativePromptError(nil); err != nil {
		t.Fatalf("nil native error = %v", err)
	}
	if err := classifyNativePromptError(errors.New("plain")); err == nil || !strings.Contains(err.Error(), "plain") {
		t.Fatalf("plain native error = %v", err)
	}
	if got := mergeEnv(nil, nil); len(got) != 0 {
		t.Fatalf("empty env = %#v", got)
	}
}

type fakeAmpMessage struct{ raw map[string]any }

func (m fakeAmpMessage) AmpType() string { return "fake" }

func (m fakeAmpMessage) RawMessage() map[string]any { return m.raw }

func (m fakeAmpMessage) RawJSON() string { return `{"type":"fake"}` }

func attachRecordingClient(t *testing.T, agent *Agent) (*recordingClient, func()) {
	t.Helper()
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	client := &recordingClient{}
	_ = acp.NewClientSideConnection(client, c2aW, a2cR)
	conn := newLocalAgentConnection(agent, a2cW, c2aR)
	agent.setConnection(conn)
	return client, func() {
		_ = c2aW.Close()
		_ = c2aR.Close()
		_ = a2cW.Close()
		_ = a2cR.Close()
	}
}

func waitForRecorded(t *testing.T, ready func() bool) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if ready() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("recorded notification did not arrive")
}

func newClosedAgentConnection(t *testing.T) *localAgentConnection {
	t.Helper()
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	conn := newLocalAgentConnection(NewAgent(), a2cW, c2aR)
	_ = a2cR.Close()
	t.Cleanup(func() {
		_ = c2aW.Close()
		_ = c2aR.Close()
		_ = a2cW.Close()
		_ = a2cR.Close()
	})
	return conn
}

type errorStore struct {
	appendErr  error
	loadErr    error
	replaceErr error
	deleteErr  error
	listErr    error
}

func (s *errorStore) Append(context.Context, SessionKey, []SessionStoreEntry) error {
	return s.appendErr
}

func (s *errorStore) Load(context.Context, SessionKey) ([]SessionStoreEntry, error) {
	return nil, s.loadErr
}

func (s *errorStore) Replace(context.Context, SessionKey, []SessionStoreReplacement) error {
	return s.replaceErr
}

func (s *errorStore) Delete(context.Context, SessionKey) error { return s.deleteErr }

func (s *errorStore) ListSessions(context.Context) ([]SessionSummary, error) {
	return nil, s.listErr
}

func (s *errorStore) ListSubkeys(context.Context, SessionKey) ([]string, error) { return nil, nil }

type recordingStore struct {
	appendCalls      int
	replaceCalls     int
	lastReplacements []SessionStoreReplacement
	entries          []SessionStoreEntry
}

func (s *recordingStore) Append(context.Context, SessionKey, []SessionStoreEntry) error {
	s.appendCalls++
	return nil
}

func (s *recordingStore) Load(context.Context, SessionKey) ([]SessionStoreEntry, error) {
	return cloneEntries(s.entries), nil
}

func (s *recordingStore) Replace(_ context.Context, _ SessionKey, replacements []SessionStoreReplacement) error {
	s.replaceCalls++
	s.lastReplacements = replacements
	for _, replacement := range replacements {
		if replacement.Key.Subpath == transcriptSubpath {
			s.entries = cloneEntries(replacement.Entries)
		}
	}
	return nil
}

func (s *recordingStore) Delete(context.Context, SessionKey) error { return nil }

func (s *recordingStore) ListSessions(context.Context) ([]SessionSummary, error) { return nil, nil }

func (s *recordingStore) ListSubkeys(context.Context, SessionKey) ([]string, error) { return nil, nil }

// TestActiveLoadResumeValidation proves an already-active session cannot bypass
// cold-path validation on session/load or session/resume.
func TestActiveLoadResumeValidation(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := resp.SessionId

	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd, WithSessionMeta(map[string]any{"amp": "bad"}))); err == nil {
		t.Fatal("active load with bad _meta.amp accepted")
	}
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, "relative/cwd")); err == nil {
		t.Fatal("active load with relative cwd accepted")
	}
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://example.test/sse"}}))); err == nil {
		t.Fatal("active resume with SSE MCP accepted")
	}
	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest(id, cwd, WithSessionAmpOptions(AmpOptions{Model: "opus"}))); err == nil {
		t.Fatal("active resume with non-empty model accepted")
	}

	before := len(agent.sessions)
	if _, err := agent.LoadSession(ctx, LoadSessionRequest(id, cwd)); err != nil {
		t.Fatalf("valid active reload: %v", err)
	}
	if len(agent.sessions) != before {
		t.Fatalf("active reload changed session count %d -> %d (second native process?)", before, len(agent.sessions))
	}
}

// flakyReplaceStore fails the next failReplaces Replace calls, then delegates.
type flakyReplaceStore struct {
	*InMemorySessionStore
	failReplaces int
}

func (s *flakyReplaceStore) Replace(ctx context.Context, main SessionKey, replacements []SessionStoreReplacement) error {
	if s.failReplaces > 0 {
		s.failReplaces--

		return errors.New("replace unavailable")
	}

	return s.InMemorySessionStore.Replace(ctx, main, replacements)
}

// TestMirrorUnsyncedRetention proves a completed native turn
// whose Replace fails is retained in memory, blocks the next prompt loudly, and
// is durably re-committed on retry so load replay still contains the turn.
func TestMirrorUnsyncedRetention(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	store := &flakyReplaceStore{InMemorySessionStore: NewInMemorySessionStore()}
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store))
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := resp.SessionId

	// Fail the completed turn's persist and the first retry.
	store.failReplaces = 2

	if _, err = agent.Prompt(ctx, TextPromptRequest(id, "test-turn", "turn one")); err == nil {
		t.Fatal("prompt with failing persist returned no error")
	}
	if _, err = agent.Prompt(ctx, TextPromptRequest(id, "test-turn", "blocked")); err == nil || !strings.Contains(err.Error(), "mirror_unsynced") {
		t.Fatalf("second prompt not blocked with mirror_unsynced: %v", err)
	}
	// Third prompt: retry of the exact frames succeeds, then the new turn runs.
	if _, err = agent.Prompt(ctx, TextPromptRequest(id, "test-turn", "turn three")); err != nil {
		t.Fatalf("prompt after store recovery: %v", err)
	}

	entries, err := store.Load(ctx, SessionKey{SessionID: string(id), Subpath: transcriptSubpath})
	if err != nil {
		t.Fatalf("load transcript: %v", err)
	}
	results := 0
	for _, entry := range entries {
		if bytes.Contains(entry, []byte(`"type":"result"`)) {
			results++
		}
	}
	if results != 2 {
		t.Fatalf("persisted transcript has %d result frames, want both turns (2)", results)
	}

	// Load replay on a fresh agent must succeed and see the retained turns.
	restored := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store))
	if _, err := restored.LoadSession(ctx, LoadSessionRequest(id, cwd)); err != nil {
		t.Fatalf("load replay after retention: %v", err)
	}
}

// TestCancelAlreadyCancelledBranch deterministically covers Cancel
// on an already-cancelled active prompt returning nil without re-interrupting.
func TestCancelAlreadyCancelledBranch(t *testing.T) {
	session := &agentSession{agent: NewAgent()}
	state := newPromptTurnState()
	state.cancel()
	session.setActivePrompt(state)
	if err := session.Cancel(context.Background()); err != nil {
		t.Fatalf("cancel on already-cancelled prompt = %v", err)
	}
}

// TestTombstoneCascade proves a main-key tombstone hides future
// subpath appends/loads/listings and is cleared only by a valid Replace.
func TestTombstoneCascade(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	main := SessionKey{SessionID: "T-cascade", Subpath: SessionStoreMainSubpath}
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-cascade"})
	if err := store.Replace(ctx, main, []SessionStoreReplacement{{Key: main, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, main); err != nil {
		t.Fatal(err)
	}

	future := SessionKey{SessionID: "T-cascade", Subpath: transcriptSubpath}
	if err := store.Append(ctx, future, []SessionStoreEntry{json.RawMessage(`"x"`)}); err != nil {
		t.Fatal(err)
	}
	if entries, err := store.Load(ctx, future); err != nil || len(entries) != 0 {
		t.Fatalf("future subpath append survived main tombstone: entries=%d err=%v", len(entries), err)
	}
	if subkeys, err := store.ListSubkeys(ctx, SessionKey{SessionID: "T-cascade"}); err != nil || len(subkeys) != 0 {
		t.Fatalf("tombstoned subkeys listed: %#v err=%v", subkeys, err)
	}

	if err := store.Replace(ctx, main, []SessionStoreReplacement{{Key: main, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, future, []SessionStoreEntry{json.RawMessage(`"y"`)}); err != nil {
		t.Fatal(err)
	}
	if entries, err := store.Load(ctx, future); err != nil || len(entries) != 1 {
		t.Fatalf("append after tombstone clear failed: entries=%d err=%v", len(entries), err)
	}
}

func TestSessionDirectBranches(t *testing.T) {
	ctx := context.Background()
	fileScratch := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileScratch, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newAgentSession(t.Context(), NewAgent(WithScratchDir(fileScratch)), "T-1", "", parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("newAgentSession with file scratch dir succeeded")
	}

	path, _ := fakeAgentAmpPath(t, "")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	session, err := newAgentSession(t.Context(), agent, "T-1", t.TempDir(), parsedSessionMeta{rawEvent: true}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	session.turn <- struct{}{}
	if _, err := session.acquireTurn(ctx); err == nil {
		t.Fatal("expected session_prompt backpressure")
	}
	<-session.turn
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	session.turn <- struct{}{}
	if _, err := session.acquireTurn(cancelCtx); err == nil {
		t.Fatal("expected canceled acquireTurn")
	}
	<-session.turn
	session.poisonCause = "poisoned"
	if err := session.ready(); err == nil {
		t.Fatal("poisoned session ready")
	}
	session.poisonCause = ""
	session.closed = true
	if err := session.ready(); !errors.Is(err, errSessionClosed) {
		t.Fatalf("closed ready = %v", err)
	}
	session.closed = false
	if err := session.Cancel(ctx); err != nil {
		t.Fatalf("Cancel without turn: %v", err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := session.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}
