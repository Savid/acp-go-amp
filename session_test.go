//nolint:nlreturn // Edge tests keep related contract branches together.
package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	ampnative "github.com/savid/acp-go-amp/internal/amp"
)

func TestAgentLifecycleErrorBranches(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	if _, err := newTestAgent(WithExecutablePath(path)).NewSession(ctx, acp.NewSessionRequest{Meta: map[string]any{"amp": "bad"}}); err == nil {
		t.Fatal("bad meta accepted")
	}
	if _, err := newTestAgent(WithExecutablePath(path), WithDefaultModel("model")).NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("default model accepted at session start")
	}
	if _, err := newTestAgent(WithExecutablePath(path)).NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "s", Url: "u"}}))); err == nil {
		t.Fatal("sse mcp accepted")
	}
	fileHome := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(fileHome, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newTestAgent(WithExecutablePath(path), WithScratchDir(fileHome)).NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("file scratch dir accepted")
	}
	storeErr := &errorStore{loadErr: errors.New("load failed")}
	if _, err := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(storeErr)).NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("persist load error ignored")
	}
	limited := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
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
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-load", NativeSessionID: "T-load", Cwd: cwd, Mode: "high", CreatedAtUnixMilli: 1, UpdatedAtUnixMilli: 2})
	if err := store.Replace(ctx, SessionKey{SessionID: "T-load", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{
		{Key: SessionKey{SessionID: "T-load", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{manifest}},
		{Key: SessionKey{SessionID: "T-load", Subpath: transcriptSubpath}, Entries: []SessionStoreEntry{
			json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-load"}`),
			json.RawMessage(`{"type":"result","subtype":"success","is_error":false,"session_id":"T-load"}`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 2}))
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
	waitForRecorded(t, func() bool { return len(client.updatesSnapshot()) == before+1 })
	updates := client.updatesSnapshot()
	if len(updates) != before+1 || updates[len(updates)-1].Update.SessionInfoUpdate == nil {
		t.Fatalf("resume did not emit exactly one identity-only checkpoint: before=%d updates=%#v", before, updates)
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
	if _, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest("T-load", "mode", "bad")); err == nil {
		t.Fatal("bad mode accepted")
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
		if _, err := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(badStore)).LoadSession(ctx, LoadSessionRequest("T-bad", cwd)); err == nil {
			t.Fatal("bad transcript replay accepted")
		}
	}
}

func TestLoadManifestErrorsAndListFilters(t *testing.T) {
	ctx := context.Background()
	agent := newTestAgent()
	if _, err := agent.loadManifest(ctx, "T-missing"); err == nil {
		t.Fatal("missing manifest accepted")
	}
	for _, entry := range []SessionStoreEntry{
		json.RawMessage(`{`),
		json.RawMessage(`{"format":"wrong","threadId":"T-bad"}`),
		json.RawMessage(`{"format":"amp-thread-mirror-v1","sessionId":"other"}`),
	} {
		store := NewInMemorySessionStore()
		if err := store.Replace(ctx, SessionKey{SessionID: "T-bad", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "T-bad", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{entry}}}); err != nil {
			t.Fatal(err)
		}
		if _, err := newTestAgent(WithSessionStore(store)).loadManifest(ctx, "T-bad"); err == nil {
			t.Fatalf("bad manifest accepted: %s", entry)
		}
	}
	overlongID := acp.SessionId("T-" + strings.Repeat("x", ampnative.MaxThreadIDBytes))
	overlong, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: string(overlongID), NativeSessionID: string(overlongID)})
	overlongStore := NewInMemorySessionStore()
	if err := overlongStore.Replace(ctx, SessionKey{SessionID: string(overlongID)}, []SessionStoreReplacement{{
		Key: SessionKey{SessionID: string(overlongID)}, Entries: []SessionStoreEntry{overlong},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := newTestAgent(WithSessionStore(overlongStore)).loadManifest(ctx, overlongID); err == nil {
		t.Fatal("overlong stored thread id admitted")
	}
	errStore := &errorStore{listErr: errors.New("list failed")}
	if _, err := newTestAgent(WithSessionStore(errStore)).ListSessions(ctx, acp.ListSessionsRequest{}); err == nil {
		t.Fatal("list error ignored")
	}
	store := NewInMemorySessionStore()
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-list", NativeSessionID: "T-list", Cwd: "/cwd", UpdatedAtUnixMilli: 0})
	if err := store.Replace(ctx, SessionKey{SessionID: "T-list", Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "T-list", Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}
	resp, err := newTestAgent(WithSessionStore(store)).ListSessions(ctx, acp.ListSessionsRequest{Cwd: acp.Ptr("/other")})
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
	agent := newTestAgent(WithExecutablePath(path))
	if _, err := newAgentSession(ctx, agent, acp.SessionId("T-"+strings.Repeat("x", ampnative.MaxThreadIDBytes)), t.TempDir(), parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("overlong thread id admitted")
	}
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
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-file", NativeSessionID: "T-file", Cwd: t.TempDir()})
	if err := store.Replace(ctx, SessionKey{SessionID: "T-file", Subpath: ""}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "T-file", Subpath: ""}, Entries: []SessionStoreEntry{manifest}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := newTestAgent(WithExecutablePath(path), WithScratchDir(fileHome), WithSessionStore(store)).LoadSession(ctx, LoadSessionRequest("T-file", t.TempDir())); err == nil {
		t.Fatal("load with file scratch dir accepted")
	}
	activeLimited := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 0}))
	activeLimited.options.ConcurrencyLimits.MaxActiveSessions = 0
	if _, _, _, err := activeLimited.loadOrResume(ctx, "T-file", t.TempDir(), nil, nil, nil); err != nil {
		t.Fatalf("loadOrResume direct: %v", err)
	}
	activeLimited.options.ConcurrencyLimits.MaxActiveSessions = 1
	manifest2, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-file-2", NativeSessionID: "T-file-2", Cwd: t.TempDir()})
	if err := store.Replace(ctx, SessionKey{SessionID: "T-file-2", Subpath: ""}, []SessionStoreReplacement{{Key: SessionKey{SessionID: "T-file-2", Subpath: ""}, Entries: []SessionStoreEntry{manifest2}}}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := activeLimited.loadOrResume(ctx, "T-file-2", t.TempDir(), nil, nil, nil); err == nil {
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
	payload, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
		acp.TextBlock("text"),
		acp.ImageBlock(validPNGBase64, "image/png"),
		{ResourceLink: &acp.ContentBlockResourceLink{Name: "n", Uri: "file:///x", Title: &title, MimeType: &mime, Description: &desc}},
		acp.ResourceBlock(acp.EmbeddedResourceResource{TextResourceContents: &acp.TextResourceContents{Uri: "file:///t", Text: "body", MimeType: &mime}}),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///i", Blob: validPNGBase64, MimeType: acp.Ptr("image/png")}}),
		acp.ResourceBlock(acp.EmbeddedResourceResource{BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///b", Blob: "YmxvYg==", MimeType: &mime}}),
	}, defaultPolicy())
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
	_, audioErr := promptInputWithPolicy(t.Context(), []acp.ContentBlock{acp.AudioBlock("audio", "audio/wav")}, defaultPolicy())
	requireInvalidParamsData(t, audioErr, map[string]any{
		jsonFieldError: valUnsupported,
		jsonFieldField: "prompt",
	})

	if _, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{acp.ResourceBlock(acp.EmbeddedResourceResource{})}, defaultPolicy()); err == nil {
		t.Fatal("empty embedded resource accepted")
	}
	session := &agentSession{agent: newTestAgent(), id: "T-emit", rawEvents: true}
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
	if got := composeEnv(nil, nil); len(got) != 0 {
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
	conn := newLocalAgentConnection(newTestAgent(), a2cW, c2aR)
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := resp.SessionId

	if _, err = agent.Prompt(ctx, TextPromptRequest(id, "test-turn", "seed thread")); err != nil {
		t.Fatalf("seed prompt: %v", err)
	}

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
	if results != 3 {
		t.Fatalf("persisted transcript has %d result frames, want all three turns", results)
	}

	// Load replay on a fresh agent must succeed and see the retained turns.
	restored := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	if _, err := restored.LoadSession(ctx, LoadSessionRequest(id, cwd)); err != nil {
		t.Fatalf("load replay after retention: %v", err)
	}
}

// TestCancelAlreadyCancelledBranch deterministically covers Cancel
// on an already-cancelled active prompt returning nil without re-interrupting.
func TestCancelAlreadyCancelledBranch(t *testing.T) {
	session := &agentSession{agent: newTestAgent()}
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
	manifest, _ := json.Marshal(ampManifest{Format: SessionStoreFormat, SessionID: "T-cascade", NativeSessionID: "T-cascade"})
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
	if _, err := newAgentSession(t.Context(), newTestAgent(WithScratchDir(fileScratch)), "T-1", "", parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("newAgentSession with file scratch dir succeeded")
	}
	handoffAgent := NewAgent(
		WithScratchDir(t.TempDir()),
		WithProcessIsolation(ProcessIsolation{
			UID: uint32(os.Geteuid()) + 1, GID: uint32(os.Getegid()) + 1,
			BaseEnvironment: map[string]string{},
		}),
	)
	if _, err := newAgentSession(t.Context(), handoffAgent, "T-handoff", "", parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("unsupported session ownership handoff succeeded")
	}

	path, _ := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
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

// The marker prints the path it was resolved to, so a recorded run names the
// exact file the native process found on the PATH it received.
const carrierMarkerSource = `package main

import (
	"fmt"
	"os"
)

func main() { fmt.Println(os.Args[0]) }
`

// carrierMarkerBinary compiles the marker once per test.
func carrierMarkerBinary(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	source := filepath.Join(dir, "marker.go")

	if err := os.WriteFile(source, []byte(carrierMarkerSource), 0o600); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(dir, "marker")
	if out, err := exec.Command("go", "build", "-o", binary, source).CombinedOutput(); err != nil {
		t.Fatalf("build carrier marker: %v\n%s", err, out)
	}

	return binary
}

// carrierDirectory materializes one session-scoped operation directory: the
// session's own marker command plus an amp stand-in that must never be chosen,
// because executable resolution belongs to the static agent base. The stand-in
// records its own launch before failing, so "was never selected" is an
// observation rather than an inference from a passing turn.
func carrierDirectory(t *testing.T, marker, name, state string) string {
	t.Helper()

	dir := t.TempDir()

	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, name), data, 0o700); err != nil { // #nosec G306 -- executable test marker.
		t.Fatal(err)
	}

	shadow := "#!/bin/sh\nprintf '%s\\n' \"$0\" >> '" + filepath.Join(state, shadowLogName) + "'\n" +
		"echo 'session PATH shadowed the amp harness' >&2\nexit 9\n"
	if err := os.WriteFile(filepath.Join(dir, "amp"), []byte(shadow), 0o700); err != nil { // #nosec G306 -- executable test stub.
		t.Fatal(err)
	}

	return dir
}

// shadowLogName is where a planted session-directory amp records that it ran.
const shadowLogName = "shadow-amp.log"

// requireNoShadowedHarness pins that the amp planted on a session PATH was
// never launched at all.
func requireNoShadowedHarness(t *testing.T, state string) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(state, shadowLogName))
	if errors.Is(err, os.ErrNotExist) {
		return
	}

	if err != nil {
		t.Fatalf("read shadow harness log: %v", err)
	}

	t.Fatalf("the amp planted on a session PATH ran: %s", data)
}

// carrier is one logical session's complete session-scoped configuration. Amp
// declares no ordered path option, so the raw env.PATH is the whole search path
// its short-lived prompt process runs with.
type carrier struct {
	bearer string
	dir    string
	marker string
}

func (c carrier) env() map[string]string {
	return map[string]string{
		"AMP_API_KEY":        c.bearer,
		"PATH":               c.dir,
		"AMP_CARRIER_MARKER": c.marker,
	}
}

type carrierRun struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	Bearer   string `json:"bearer"`
	Resolved string `json:"resolved"`
	Output   string `json:"output"`
	Error    string `json:"error"`
	RunError string `json:"runError"`
}

func carrierRuns(t *testing.T, state string) []carrierRun {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(state, "marker.jsonl"))
	if err != nil {
		t.Fatalf("read recorded marker runs: %v", err)
	}

	runs := make([]carrierRun, 0, 4)
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		var run carrierRun
		if err := json.Unmarshal([]byte(line), &run); err != nil {
			t.Fatalf("decode recorded marker run %q: %v", line, err)
		}

		runs = append(runs, run)
	}

	return runs
}

// requireCarrierRuns pins that every native turn recorded since offset resolved
// and ran its own session's marker out of its own raw PATH, and that no other
// session's values appear anywhere in it.
func requireCarrierRuns(t *testing.T, runs []carrierRun, offset int, want map[string]carrier) {
	t.Helper()

	seen := map[string]int{}

	for _, run := range runs[offset:] {
		expected, known := want[run.Bearer]
		if !known {
			t.Fatalf("a native turn carried unexpected bearer %q", run.Bearer)
		}

		if run.Error != "" || run.RunError != "" {
			t.Fatalf("marker %q did not run: %s %s", run.Name, run.Error, run.RunError)
		}

		if run.Name != expected.marker {
			t.Fatalf("bearer %q ran marker %q, want %q", run.Bearer, run.Name, expected.marker)
		}

		resolved := filepath.Join(expected.dir, expected.marker)
		if run.Resolved != resolved || run.Output != resolved {
			t.Fatalf("marker resolved to %q and reported %q, want %q", run.Resolved, run.Output, resolved)
		}

		// Amp owns no ordered path option, so the session's raw PATH is the
		// whole search path and its first component is the operation directory.
		components := filepath.SplitList(run.Path)
		if run.Path != expected.dir || len(components) != 1 || components[0] != expected.dir {
			t.Fatalf("bearer %q ran with PATH %q, want exactly %q", run.Bearer, run.Path, expected.dir)
		}

		seen[run.Bearer]++
	}

	for bearer := range want {
		if seen[bearer] == 0 {
			t.Fatalf("no native turn ran for bearer %q", bearer)
		}
	}
}

func newCarrierSession(t *testing.T, agent *Agent, c carrier) (acp.SessionId, string) {
	t.Helper()

	cwd := t.TempDir()

	resp, err := agent.NewSession(context.Background(), NewSessionRequest(cwd, WithSessionAmpOptions(
		NewAmpOptions(WithAmpEnv(c.env())),
	)))
	if err != nil {
		t.Fatalf("prepare carrier session for %q: %v", c.bearer, err)
	}

	return resp.SessionId, cwd
}

func promptCarriersConcurrently(t *testing.T, agent *Agent, ids ...acp.SessionId) {
	t.Helper()

	var wait sync.WaitGroup

	errs := make([]error, len(ids))

	for index, id := range ids {
		wait.Go(func() {
			_, errs[index] = agent.Prompt(context.Background(), TextPromptRequest(id, "test-turn", "x"))
		})
	}

	wait.Wait()

	for index, err := range errs {
		if err != nil {
			t.Fatalf("concurrent prompt %d: %v", index, err)
		}
	}
}

// TestAmpSessionCarrierRunsRealMarkersAndRotatesWithoutCrossing is the amp side
// of the six-carrier session-CLI acceptance. Amp declares no ordered path
// option, so one complete raw PATH in session env is the whole carrier. Two
// sessions run concurrent turns that resolve and execute their own marker
// command out of their own PATH, a second turn follows a resume, one carrier is
// rotated across a close-and-re-prepare boundary, and the retired values reach
// nothing afterwards. Each operation directory also holds an amp stand-in that
// exits nonzero: a session PATH that could shadow the harness would fail every
// turn instead of running the real one.
func TestAmpSessionCarrierRunsRealMarkersAndRotatesWithoutCrossing(t *testing.T) {
	harness, state := fakeAgentAmpPath(t, "record-env")

	t.Setenv("AMP_API_KEY", "")

	marker := carrierMarkerBinary(t)
	first := carrier{bearer: "bearer-first", marker: "amp-marker-first"}
	second := carrier{bearer: "bearer-second", marker: "amp-marker-second"}
	rotated := carrier{bearer: "bearer-rotated", marker: "amp-marker-rotated"}

	for _, c := range []*carrier{&first, &second, &rotated} {
		c.dir = carrierDirectory(t, marker, c.marker, state)
	}

	// The static agent base owns executable resolution: only this PATH may
	// select the amp harness.
	agent := newTestAgent(
		WithScratchDir(testScratchDir(t)),
		WithEnv(map[string]string{"PATH": filepath.Dir(harness)}),
	)
	t.Cleanup(func() { _ = agent.Close() })

	ctx := context.Background()
	firstID, firstCwd := newCarrierSession(t, agent, first)
	secondID, secondCwd := newCarrierSession(t, agent, second)

	promptCarriersConcurrently(t, agent, firstID, secondID)
	requireCarrierRuns(t, carrierRuns(t, state), 0, map[string]carrier{
		first.bearer: first, second.bearer: second,
	})

	// A second turn after a resume keeps each session on its own carrier.
	settled := len(carrierRuns(t, state))
	for id, request := range map[acp.SessionId]acp.ResumeSessionRequest{
		firstID:  ResumeSessionRequest(firstID, firstCwd, WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(first.env())))),
		secondID: ResumeSessionRequest(secondID, secondCwd, WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(second.env())))),
	} {
		if _, err := agent.ResumeSession(ctx, request); err != nil {
			t.Fatalf("resume %s: %v", id, err)
		}
	}

	promptCarriersConcurrently(t, agent, firstID, secondID)
	requireCarrierRuns(t, carrierRuns(t, state), settled, map[string]carrier{
		first.bearer: first, second.bearer: second,
	})

	// Rotation happens at the idle close-and-re-prepare boundary: the session
	// holding the retired bearer and directory is closed, and a fresh one
	// carries the new values.
	if _, err := agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: firstID}); err != nil {
		t.Fatalf("close the rotated-out session: %v", err)
	}

	settled = len(carrierRuns(t, state))
	settledChildren := len(childEnvironments(t, state))
	rotatedID, _ := newCarrierSession(t, agent, rotated)

	promptCarriersConcurrently(t, agent, secondID, rotatedID)
	requireCarrierRuns(t, carrierRuns(t, state), settled, map[string]carrier{
		second.bearer: second, rotated.bearer: rotated,
	})

	for _, entries := range childEnvironments(t, state)[settledChildren:] {
		for _, value := range entries {
			if strings.Contains(value, first.bearer) || strings.Contains(value, first.dir) {
				t.Fatalf("a retired carrier value survived rotation: %q", value)
			}
		}
	}

	// No compatibility behavior survives the rotation: the closed session is
	// gone rather than retried against the new carrier, and the untouched
	// session refuses to adopt the rotated one on an active request.
	if _, err := agent.Prompt(ctx, TextPromptRequest(firstID, "test-turn", "x")); err == nil {
		t.Fatal("a prompt on the rotated-out session succeeded")
	}

	_, err := agent.ResumeSession(ctx, ResumeSessionRequest(secondID, secondCwd, WithSessionAmpOptions(
		NewAmpOptions(WithAmpEnv(rotated.env())),
	)))
	requireInvalidParamsData(t, err, map[string]any{jsonFieldError: valMismatch, jsonFieldField: optionEnvKey})

	requireNoShadowedHarness(t, state)
}

// TestAmpStaticProbeEnvironmentIsSeparateFromThePromptCarrier is the Unix half
// of the probe/prompt cut, stated by correlating each recorded argv with the
// environment that exact child received. `amp version` and the startup
// method-present probes run on the static agent PATH; only the prompt receives
// the session's complete raw PATH. The session directory holds a real amp that
// would record its own launch and fail the turn, and it never runs; the marker
// command in that same directory is resolved and executed by the prompt child,
// so the carrier is live rather than merely present.
func TestAmpStaticProbeEnvironmentIsSeparateFromThePromptCarrier(t *testing.T) {
	harness, state := fakeAgentAmpPath(t, "record-env")

	t.Setenv("AMP_API_KEY", "")

	agentDir := filepath.Dir(harness)
	sessionDir := carrierDirectory(t, carrierMarkerBinary(t), "amp-marker", state)

	agent := newTestAgent(
		WithScratchDir(testScratchDir(t)),
		WithEnv(map[string]string{"PATH": agentDir, "AMP_API_KEY": "agent-key"}),
	)
	t.Cleanup(func() { _ = agent.Close() })

	ctx := context.Background()
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionAmpOptions(NewAmpOptions(
		WithAmpEnv(map[string]string{
			"PATH":               sessionDir,
			"AMP_API_KEY":        "session-key",
			"AMP_CARRIER_MARKER": "amp-marker",
		}),
	))))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x")); promptErr != nil {
		t.Fatalf("Prompt: %v", promptErr)
	}

	runs := childRuns(t, state)
	requireProbeAndPromptPaths(t, runs, agentDir, sessionDir)

	for _, run := range runs {
		// The credential is a named operation value, so the authenticated
		// probes and the prompt agree on the key admission approved.
		requireChildEnv(t, run.Env, "AMP_API_KEY", "session-key")

		if run.isPrompt() {
			requireChildEnv(t, run.Env, "AMP_CARRIER_MARKER", "amp-marker")

			continue
		}

		if values := childEnvValues(run.Env, "AMP_CARRIER_MARKER"); len(values) != 0 {
			t.Fatalf("probe child %v received session-only values %#v", run.Args, values)
		}
	}

	// The harness that passed version and startup validation is retained, so a
	// later launch runs that exact file and resolves nothing again. Repointing
	// the static agent PATH at a decoy amp after the probe changes nothing: the
	// next turn still runs the validated harness and the decoy never starts.
	agent.options.Env["PATH"] = carrierDirectory(t, carrierMarkerBinary(t), "decoy-marker", state)

	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "again")); promptErr != nil {
		t.Fatalf("Prompt after the static PATH moved: %v", promptErr)
	}

	requireNoShadowedHarness(t, state)
	requireCarrierRuns(t, carrierRuns(t, state), 0, map[string]carrier{
		"session-key": {bearer: "session-key", dir: sessionDir, marker: "amp-marker"},
	})
}

// TestConsumerHeldBearerCarriesAcrossAgentRebuild pins the other half of the
// carrier: a bearer a consumer holds and re-supplies through WithEnv survives
// an Agent rebuild and a cold load, a session value overrides it consistently
// in both the preflight gate and the child, and dropping it makes the stored
// session unusable rather than falling back to anything.
func TestConsumerHeldBearerCarriesAcrossAgentRebuild(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "record-env")

	t.Setenv("AMP_API_KEY", "")

	store := NewInMemorySessionStore()
	scratch := testScratchDir(t)
	cwd := t.TempDir()

	build := func(bearer string) *Agent {
		options := []Option{WithExecutablePath(path), WithScratchDir(scratch), WithSessionStore(store)}
		if bearer != "" {
			options = append(options, WithEnv(map[string]string{"AMP_API_KEY": bearer}))
		}

		agent := newTestAgent(options...)
		t.Cleanup(func() { _ = agent.Close() })

		return agent
	}

	ctx := context.Background()
	held := build("consumer-key")

	resp, err := held.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, promptErr := held.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x")); promptErr != nil {
		t.Fatalf("Prompt: %v", promptErr)
	}

	rebuilt := build("consumer-key")
	if _, loadErr := rebuilt.LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd)); loadErr != nil {
		t.Fatalf("cold load with the consumer-held bearer: %v", loadErr)
	}

	settled := len(childEnvironments(t, state))
	if _, promptErr := rebuilt.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "again")); promptErr != nil {
		t.Fatalf("Prompt after rebuild: %v", promptErr)
	}

	for _, entries := range childEnvironments(t, state)[settled:] {
		requireChildEnv(t, entries, "AMP_API_KEY", "consumer-key")
	}

	// A session value outranks the consumer-held one in the same direction at
	// the gate and in the child.
	overridden, err := rebuilt.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionAmpOptions(
		NewAmpOptions(WithAmpEnv(map[string]string{"AMP_API_KEY": "session-key"})),
	)))
	if err != nil {
		t.Fatalf("NewSession with a session bearer: %v", err)
	}

	settled = len(childEnvironments(t, state))
	if _, promptErr := rebuilt.Prompt(ctx, TextPromptRequest(overridden.SessionId, "test-turn", "x")); promptErr != nil {
		t.Fatalf("Prompt with a session bearer: %v", promptErr)
	}

	for _, entries := range childEnvironments(t, state)[settled:] {
		requireChildEnv(t, entries, "AMP_API_KEY", "session-key")
	}

	// Dropping the consumer-held bearer cannot reuse the stored session.
	_, err = build("").LoadSession(ctx, LoadSessionRequest(resp.SessionId, cwd))
	if err == nil || !strings.Contains(err.Error(), "AMP_API_KEY is not set") {
		t.Fatalf("load without the consumer-held bearer = %v, want the missing-key refusal", err)
	}
}
