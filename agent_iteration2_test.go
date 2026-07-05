package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

func TestIteration2SessionSlotFilesystemServeAndCloseEdges(t *testing.T) {
	ctx := context.Background()

	previousWriteFile := writeFile
	writeFile = func(string, []byte, os.FileMode) error { return errors.New("write settings failed") }
	t.Cleanup(func() { writeFile = previousWriteFile })
	if _, err := newAgentSession(NewAgent(WithHome(t.TempDir())), "T-write", t.TempDir(), parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("settings write failure was ignored")
	}
	writeFile = previousWriteFile

	limited := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
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
	go func() { errCh <- Serve(ctx, inputR, io.Discard) }()
	_ = inputW.Close()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve peer close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not exit after peer close")
	}

	agent := NewAgent()
	agent.sessions["T-close"] = &agentSession{agent: agent, id: "T-close", turn: make(chan struct{}, 1)}
	if err := agent.Close(); err != nil {
		t.Fatalf("Close with live session: %v", err)
	}
}

func TestIteration2LoadReplayDeleteAndConfigEdges(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	cwd := t.TempDir()

	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-load-edge", cwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-load-edge"}`),
	})
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(store))
	agent.markDeleted("T-load-edge")
	listResp, err := agent.ListSessions(ctx, ListSessionsRequest())
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(listResp.Sessions) != 0 {
		t.Fatalf("deleted session listed: %#v", listResp.Sessions)
	}

	deleteErr := errors.New("delete store failed")
	if _, deleteGotErr := NewAgent(WithSessionStore(&errorStore{deleteErr: deleteErr})).UnstableDeleteSession(ctx, DeleteSessionRequest("T-delete-store")); !errors.Is(deleteGotErr, deleteErr) {
		t.Fatalf("delete store error = %v", deleteGotErr)
	}

	loadErr := errors.New("load manifest failed")
	if _, loadGotErr := NewAgent(WithSessionStore(&errorStore{loadErr: loadErr})).loadManifest(ctx, "T-any"); !errors.Is(loadGotErr, loadErr) {
		t.Fatalf("loadManifest store error = %v", loadGotErr)
	}

	badReplay := NewInMemorySessionStore()
	putStoredSession(t, badReplay, "T-bad-replay", cwd, []SessionStoreEntry{json.RawMessage(`{`)})
	if _, replayErr := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(badReplay)).LoadSession(ctx, LoadSessionRequest("T-bad-replay", cwd)); replayErr == nil {
		t.Fatal("bad transcript replay succeeded")
	}

	replayLoadErr := errors.New("transcript load failed")
	replayAgent := NewAgent(WithSessionStore(&errorStore{loadErr: replayLoadErr}))
	if replayGotErr := (&agentSession{agent: replayAgent, id: "T-replay"}).replayTranscript(ctx); !errors.Is(replayGotErr, replayLoadErr) {
		t.Fatalf("replay load error = %v", replayGotErr)
	}
	nilStoreAgent := NewAgent()
	nilStoreAgent.store = nil
	if replayErr := (&agentSession{agent: nilStoreAgent, id: "T-replay"}).replayTranscript(ctx); replayErr != nil {
		t.Fatalf("nil-store replay: %v", replayErr)
	}

	updateErrStore := NewInMemorySessionStore()
	putStoredSession(t, updateErrStore, "T-update-error", cwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-update-error"}`),
	})
	updateAgent := NewAgent(WithSessionStore(updateErrStore))
	updateAgent.setConnection(newClosedAgentConnection(t))
	if replayErr := (&agentSession{agent: updateAgent, id: "T-update-error"}).replayTranscript(ctx); replayErr == nil {
		t.Fatal("replay update failure was ignored")
	}

	rawErrStore := NewInMemorySessionStore()
	putStoredSession(t, rawErrStore, "T-raw-error", cwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"system","subtype":"init","session_id":"T-raw-error"}`),
	})
	rawAgent := NewAgent(WithSessionStore(rawErrStore))
	rawAgent.setConnection(newClosedAgentConnection(t))
	if replayErr := (&agentSession{agent: rawAgent, id: "T-raw-error", rawEvents: true}).replayTranscript(ctx); replayErr == nil {
		t.Fatal("replay raw-event failure was ignored")
	}

	validStore := NewInMemorySessionStore()
	putStoredSession(t, validStore, "T-meta", cwd, nil)
	if _, loadErr := NewAgent(WithSessionStore(validStore)).LoadSession(ctx, LoadSessionRequest("T-meta", cwd, WithSessionMeta(map[string]any{"amp": "bad"}))); loadErr == nil {
		t.Fatal("load bad meta accepted after manifest")
	}
	if _, resumeErr := NewAgent(WithSessionStore(validStore), WithDefaultModel("model")).ResumeSession(ctx, ResumeSessionRequest("T-meta", cwd)); resumeErr == nil {
		t.Fatal("resume default model accepted after manifest")
	}
	if _, loadErr := NewAgent(WithSessionStore(validStore)).LoadSession(ctx, LoadSessionRequest("T-meta", cwd, WithSessionMCPServers(acp.McpServer{}))); loadErr == nil {
		t.Fatal("empty MCP transport accepted after manifest")
	}

	if _, err := parseAmpOptions(map[string]any{"model": 42}); err == nil {
		t.Fatal("non-string model accepted")
	}
	if _, err := parseAmpOptions(map[string]any{"outputSchema": map[string]any{}}); err == nil {
		t.Fatal("empty output schema accepted")
	}

	replaceErr := errors.New("replace failed")
	configAgent := NewAgent(WithExecutablePath("/does/not/exist"), WithSessionStore(&errorStore{replaceErr: replaceErr}))
	configSession := &agentSession{agent: configAgent, id: "T-config", mode: "smart", effort: "high", turn: make(chan struct{}, 1)}
	if err := configSession.setConfig(ctx, configMode, "rush"); !errors.Is(err, replaceErr) {
		t.Fatalf("setConfig replace error = %v", err)
	}
}

func TestIteration2PromptAndPersistenceEdges(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")

	closed := &agentSession{agent: NewAgent(), id: "T-closed", closed: true, turn: make(chan struct{}, 1)}
	if _, err := closed.Prompt(ctx, TextPromptRequest("T-closed", "x")); !errors.Is(err, errSessionClosed) {
		t.Fatalf("closed prompt = %v", err)
	}

	busy := &agentSession{agent: NewAgent(), id: "T-busy", turn: make(chan struct{}, 1)}
	busy.turn <- struct{}{}
	if _, err := busy.Prompt(ctx, TextPromptRequest("T-busy", "x")); err == nil || !strings.Contains(err.Error(), "backpressure") {
		t.Fatalf("busy prompt = %v", err)
	}

	badInput := &agentSession{agent: NewAgent(), id: "T-input", turn: make(chan struct{}, 1)}
	if _, err := badInput.Prompt(ctx, acp.PromptRequest{SessionId: "T-input", Prompt: []acp.ContentBlock{acp.AudioBlock("audio", "audio/wav")}}); err == nil {
		t.Fatal("unsupported prompt input accepted")
	}

	continueErr := &agentSession{agent: NewAgent(WithExecutablePath("/does/not/exist")), id: "T-continue", cwd: t.TempDir(), turn: make(chan struct{}, 1)}
	if _, err := continueErr.Prompt(ctx, TextPromptRequest("T-continue", "x")); err == nil {
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
			agent := NewAgent(WithExecutablePath(modePath), WithHome(t.TempDir()))
			newResp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			_, err = agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "x"))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Prompt error = %v, want %q", err, tc.want)
			}
		})
	}

	rawAgent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	rawResp, err := rawAgent.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionRawEvents(true)))
	if err != nil {
		t.Fatalf("NewSession raw: %v", err)
	}
	rawAgent.setConnection(newClosedAgentConnection(t))
	if _, rawErr := rawAgent.Prompt(ctx, TextPromptRequest(rawResp.SessionId, "x")); rawErr == nil {
		t.Fatal("raw event notify failure was ignored")
	}

	updateAgent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	updateResp, err := updateAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession update: %v", err)
	}
	updateAgent.setConnection(newClosedAgentConnection(t))
	if _, updateErr := updateAgent.Prompt(ctx, TextPromptRequest(updateResp.SessionId, "x")); updateErr == nil {
		t.Fatal("session update failure was ignored")
	}

	persistAgent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	persistResp, err := persistAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession persist: %v", err)
	}
	persistErr := errors.New("persist replace failed")
	persistAgent.store = &errorStore{replaceErr: persistErr}
	if _, persistGotErr := persistAgent.Prompt(ctx, TextPromptRequest(persistResp.SessionId, "x")); !errors.Is(persistGotErr, persistErr) {
		t.Fatalf("prompt persist error = %v", persistGotErr)
	}

	cancelPath, state := fakeAgentAmpPath(t, "hang")
	cancelAgent := NewAgent(WithExecutablePath(cancelPath), WithHome(t.TempDir()))
	cancelResp, err := cancelAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession cancel: %v", err)
	}
	promptCtx, cancel := context.WithCancel(ctx)
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, promptErr := cancelAgent.Prompt(promptCtx, TextPromptRequest(cancelResp.SessionId, "x"))
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

	nilStoreAgent := NewAgent()
	nilStoreAgent.store = nil
	nilStoreSession := &agentSession{agent: nilStoreAgent, id: "T-nil-store"}
	if persistErr := nilStoreSession.persistAfterTurn(ctx, []SessionStoreEntry{json.RawMessage(`{"type":"result"}`)}); persistErr != nil {
		t.Fatalf("nil-store persist: %v", persistErr)
	}
	atomicStore := &recordingStore{}
	atomicSession := &agentSession{agent: NewAgent(WithSessionStore(atomicStore)), id: "T-atomic"}
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

func TestIteration2EmitAndRawEventEdges(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	session := &agentSession{agent: agent, id: "T-emit", rawEvents: true}

	if err := session.emitRawEvent(ctx, "off", fakeAmpMessage{raw: map[string]any{"type": strings.Repeat("x", rawEventMaxBytes)}}); err != nil {
		t.Fatalf("raw truncation without connection: %v", err)
	}

	agent.setConnection(newClosedAgentConnection(t))
	if err := session.emitMessage(ctx, &amp.UserMessage{Content: []amp.ContentBlock{amp.TextBlock{Text: "user"}}}); err == nil {
		t.Fatal("user text update failure ignored")
	}
	if err := session.emitMessage(ctx, &amp.UserMessage{Content: []amp.ContentBlock{amp.ToolResultBlock{ToolUseID: "TU", Content: "out"}}}); err == nil {
		t.Fatal("tool result update failure ignored")
	}
	if err := session.emitMessage(ctx, &amp.AssistantMessage{Content: []amp.ContentBlock{amp.TextBlock{Text: "assistant"}}}); err == nil {
		t.Fatal("assistant text update failure ignored")
	}
	if err := session.emitMessage(ctx, &amp.AssistantMessage{Content: []amp.ContentBlock{amp.ToolUseBlock{ID: "TU", Name: "Read"}}}); err == nil {
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

func TestIteration2StoreSortingAndTombstoneEdges(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-b", "/b", nil)
	putStoredSession(t, store, "T-a", "/a", nil)
	newer, err := json.Marshal(ampManifest{Format: SessionStoreFormat, ThreadID: "T-new", UpdatedAtUnixMilli: 3})
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
	store.entries[SessionKey{SessionID: "T-z", Subpath: SessionStoreMainSubpath}] = []SessionStoreEntry{json.RawMessage(`{"format":"amp-thread-mirror-v1","threadId":"T-z"}`)}
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
		ThreadID:           id,
		Cwd:                cwd,
		Mode:               "smart",
		Effort:             "high",
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
