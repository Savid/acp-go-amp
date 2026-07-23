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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(NewInMemorySessionStore()))
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
		return strings.Contains(err.Error(), field) && strings.Contains(err.Error(), "mismatch")
	}

	return data[jsonFieldError] == "mismatch" && data[jsonFieldField] == field
}

func TestNewSessionFailsFastWithoutAPIKey(t *testing.T) {
	t.Setenv("AMP_API_KEY", "")
	agent := newTestAgent(WithScratchDir(t.TempDir()))
	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "AMP_API_KEY") {
		t.Fatalf("missing key error = %v", err)
	}
}

func TestNewSessionFailsFastWithEmptyAPIKeyOverride(t *testing.T) {
	t.Setenv("AMP_API_KEY", "process-key")
	agent := newTestAgent(
		WithScratchDir(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": ""}),
	)
	_, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "AMP_API_KEY") {
		t.Fatalf("empty override error = %v", err)
	}
}

func TestNewSessionAcceptsProcessEnvAPIKey(t *testing.T) {
	path, _ := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if resp.SessionId == "" {
		t.Fatal("empty session id")
	}
}

func TestSessionSlotFilesystemServeAndCloseEdges(t *testing.T) {
	ctx := context.Background()

	previousWriteFile := writeFile
	writeFile = func(string, []byte, os.FileMode) error { return errors.New("write settings failed") }
	t.Cleanup(func() { writeFile = previousWriteFile })
	if _, err := newAgentSession(t.Context(), newTestAgent(WithScratchDir(t.TempDir())), "T-write", t.TempDir(), parsedSessionMeta{}, "", nil); err == nil {
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

func TestLoadReplayDeleteAndConfigEdges(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()

	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-load-edge", cwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-load-edge"}`),
	})
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store))
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
	badReplayAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(badReplay))
	if _, replayErr := badReplayAgent.LoadSession(ctx, LoadSessionRequest("T-bad-replay", cwd)); replayErr == nil {
		t.Fatal("bad transcript replay succeeded")
	}
	if _, active := badReplayAgent.sessions["T-bad-replay"]; active {
		t.Fatal("failed cold load retained its materialized session")
	}

	replayLoadErr := errors.New("transcript load failed")
	replayAgent := newTestAgent(WithSessionStore(&errorStore{loadErr: replayLoadErr}))
	if replayGotErr := (&agentSession{agent: replayAgent, id: "T-replay"}).replayTranscript(ctx); !errors.Is(replayGotErr, replayLoadErr) {
		t.Fatalf("replay load error = %v", replayGotErr)
	}
	nilStoreAgent := newTestAgent()
	nilStoreAgent.store = nil
	if replayErr := (&agentSession{agent: nilStoreAgent, id: "T-replay"}).replayTranscript(ctx); replayErr != nil {
		t.Fatalf("nil-store replay: %v", replayErr)
	}

	updateErrStore := NewInMemorySessionStore()
	putStoredSession(t, updateErrStore, "T-update-error", cwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-update-error"}`),
	})
	updateAgent := newTestAgent(WithSessionStore(updateErrStore))
	updateAgent.setConnection(newClosedAgentConnection(t))
	if replayErr := (&agentSession{agent: updateAgent, id: "T-update-error"}).replayTranscript(ctx); replayErr == nil {
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

	if _, err := parseAmpOptions(map[string]any{"model": 42}); err == nil {
		t.Fatal("non-string model accepted")
	}
	if _, err := parseAmpOptions(map[string]any{"outputSchema": map[string]any{}}); err == nil {
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
			agent := newTestAgent(WithExecutablePath(modePath), WithScratchDir(t.TempDir()))
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

	updateAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	updateResp, err := updateAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession update: %v", err)
	}
	updateAgent.setConnection(newClosedAgentConnection(t))
	if _, updateErr := updateAgent.Prompt(ctx, TextPromptRequest(updateResp.SessionId, "test-turn", "x")); updateErr == nil {
		t.Fatal("session update failure was ignored")
	}

	persistAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
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
	cancelAgent := newTestAgent(WithExecutablePath(cancelPath), WithScratchDir(t.TempDir()))
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
		WithScratchDir(t.TempDir()),
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

// TestLaunchNeverUsesRemovedEffortFlag pins the current Amp CLI launch surface:
// mode is forwarded and the removed --effort flag is never emitted.
func TestLaunchNeverUsesRemovedEffortFlag(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "")
	conn, _, cleanup := startTestServe(t,
		WithExecutablePath(path),
		WithScratchDir(t.TempDir()),
		WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
	)
	defer cleanup()
	ctx := context.Background()

	if _, err := conn.Initialize(ctx, acp.InitializeRequest{}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	cwd := t.TempDir()
	newResp, err := conn.NewSession(ctx, NewSessionRequest(cwd))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, promptErr := conn.Prompt(ctx, acp.PromptRequest{
		SessionId: newResp.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock("hello")},
	}); promptErr != nil {
		t.Fatalf("Prompt: %v", promptErr)
	}
	if _, promptErr := conn.Prompt(ctx, acp.PromptRequest{
		SessionId: newResp.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock("again")},
	}); promptErr != nil {
		t.Fatalf("second Prompt: %v", promptErr)
	}

	argsRecords := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	var executeArgs, continueArgs []string
	for _, args := range argsRecords {
		if !slices.Contains(args, "threads") && slices.Contains(args, "-x") {
			executeArgs = args
		}
		if slices.Contains(args, "continue") && slices.Contains(args, "T-agent-thread") {
			continueArgs = args
		}
	}
	if executeArgs == nil {
		t.Fatalf("no thread-less execute invocation recorded: %#v", argsRecords)
	}
	if !slices.Contains(executeArgs, "--no-archive-after-execute") {
		t.Fatalf("execute launch missing --no-archive-after-execute: %#v", executeArgs)
	}
	if slices.Contains(executeArgs, "--effort") {
		t.Fatalf("--effort passed on execute launch: %#v", executeArgs)
	}
	if continueArgs == nil {
		t.Fatalf("no real continue invocation recorded: %#v", argsRecords)
	}
	if slices.Contains(continueArgs, "--effort") {
		t.Fatalf("--effort passed with no host-set effort: %#v", continueArgs)
	}
	if i := slices.Index(continueArgs, "-m"); i < 0 || i+1 >= len(continueArgs) || continueArgs[i+1] != "medium" {
		t.Fatalf("mode flag missing or not medium: %#v", continueArgs)
	}
}

// TestReconcileNativeConfigEmitFailureAbortsTurn covers the reconcile branch in
// the prompt loop: when the config_option_update carrying reconciled native
// mode cannot be delivered, the turn aborts with the delivery error.
func TestReconcileNativeConfigEmitFailureAbortsTurn(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "reconcile-config")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store))

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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithSessionStore(store))

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
