package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

// TestActiveLoadResumeValidation proves an already-active session cannot bypass
// cold-path validation on session/load or session/resume.
func TestActiveLoadResumeValidation(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
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
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(store))
	resp, err := agent.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	id := resp.SessionId

	// Fail the completed turn's persist and the first retry.
	store.failReplaces = 2

	if _, err = agent.Prompt(ctx, TextPromptRequest(id, "turn one")); err == nil {
		t.Fatal("prompt with failing persist returned no error")
	}
	if _, err = agent.Prompt(ctx, TextPromptRequest(id, "blocked")); err == nil || !strings.Contains(err.Error(), "mirror_unsynced") {
		t.Fatalf("second prompt not blocked with mirror_unsynced: %v", err)
	}
	// Third prompt: retry of the exact frames succeeds, then the new turn runs.
	if _, err = agent.Prompt(ctx, TextPromptRequest(id, "turn three")); err != nil {
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
	restored := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(store))
	if _, err := restored.LoadSession(ctx, LoadSessionRequest(id, cwd)); err != nil {
		t.Fatalf("load replay after retention: %v", err)
	}
}

// TestConcurrentPromptsRejected proves MaxConcurrentPrompts>1
// is invalid at construction and Initialize names the limit.
func TestConcurrentPromptsRejected(t *testing.T) {
	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentPrompts: 2}))
	_, err := agent.Initialize(context.Background(), acp.InitializeRequest{})
	if err == nil || !strings.Contains(err.Error(), "MaxConcurrentPrompts") {
		t.Fatalf("Initialize accepted MaxConcurrentPrompts>1: %v", err)
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
