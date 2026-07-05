package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestReview1CancelDeterminismAndNativeCancelResult(t *testing.T) {
	ctx := context.Background()
	idlePath, _ := fakeAgentAmpPath(t, "")
	idleAgent := NewAgent(WithExecutablePath(idlePath), WithHome(t.TempDir()))
	idleResp, err := idleAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession idle: %v", err)
	}
	if cancelErr := idleAgent.Cancel(ctx, acp.CancelNotification{SessionId: idleResp.SessionId}); cancelErr != nil {
		t.Fatalf("idle cancel: %v", cancelErr)
	}
	idlePrompt, err := idleAgent.Prompt(ctx, TextPromptRequest(idleResp.SessionId, "after idle cancel"))
	if err != nil || idlePrompt.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("prompt after idle cancel = %#v, %v", idlePrompt, err)
	}

	path, state := fakeAgentAmpPath(t, "sigint-ignore")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "cancel me"))
		resultCh <- result
		errCh <- promptErr
	}()
	waitForPath(t, filepath.Join(state, "stdin.jsonl"))
	if cancelErr := agent.Cancel(ctx, acp.CancelNotification{SessionId: resp.SessionId}); cancelErr != nil {
		t.Fatalf("first cancel: %v", cancelErr)
	}
	if cancelErr := agent.Cancel(ctx, acp.CancelNotification{SessionId: resp.SessionId}); cancelErr != nil {
		t.Fatalf("repeat cancel: %v", cancelErr)
	}
	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if promptErr != nil || result.StopReason != acp.StopReasonCancelled {
			t.Fatalf("prompt after cancel = %#v, %v", result, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled prompt did not return")
	}

	cancelResultPath, _ := fakeAgentAmpPath(t, "sigint-result")
	cancelResultAgent := NewAgent(WithExecutablePath(cancelResultPath), WithHome(t.TempDir()))
	cancelResultResp, err := cancelResultAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession cancel result: %v", err)
	}
	cancelPrompt, err := cancelResultAgent.Prompt(ctx, TextPromptRequest(cancelResultResp.SessionId, "x"))
	if err != nil || cancelPrompt.StopReason != acp.StopReasonCancelled {
		t.Fatalf("native cancel result = %#v, %v", cancelPrompt, err)
	}
}

func TestReview1StrictMetaAndConfigResponse(t *testing.T) {
	for _, tc := range []struct {
		name  string
		meta  map[string]any
		field string
	}{
		{name: "amp not object", meta: map[string]any{"amp": "bad"}, field: "_meta.amp"},
		{name: "rawEvent not object", meta: map[string]any{"amp": map[string]any{"rawEvent": true}}, field: "_meta.amp.rawEvent"},
		{name: "rawEvent enabled not bool", meta: map[string]any{"amp": map[string]any{"rawEvent": map[string]any{"enabled": "yes"}}}, field: "_meta.amp.rawEvent.enabled"},
		{name: "rawEvent unknown", meta: map[string]any{"amp": map[string]any{"rawEvent": map[string]any{"extra": true}}}, field: "_meta.amp.rawEvent.extra"},
		{name: "model not string", meta: map[string]any{"amp": map[string]any{"options": map[string]any{"model": 1}}}, field: "_meta.amp.options.model"},
		{name: "outputSchema empty", meta: map[string]any{"amp": map[string]any{"options": map[string]any{"outputSchema": map[string]any{}}}}, field: "_meta.amp.options.outputSchema"},
		{name: "own namespace unknown", meta: map[string]any{"amp": map[string]any{"unknown": true}}, field: "_meta.amp.unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSessionMeta(tc.meta)
			requireUnsupportedField(t, err, tc.field)
		})
	}

	path, _ := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	cwd := t.TempDir()
	putStoredSession(t, store, "T-config-response", cwd, nil)
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(store))
	if _, err := agent.LoadSession(ctxWithTimeout(t), LoadSessionRequest("T-config-response", cwd)); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	resp, err := agent.SetSessionConfigOption(context.Background(), SetConfigOptionRequest("T-config-response", "mode", "rush"))
	if err != nil {
		t.Fatalf("SetSessionConfigOption: %v", err)
	}
	if len(resp.ConfigOptions) != 2 {
		t.Fatalf("config response options = %#v", resp.ConfigOptions)
	}
}

func TestReview1ClientBackpressureAndSessionIDDrift(t *testing.T) {
	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	_ = client
	agent.clientCalls <- struct{}{}
	err := (&agentSession{agent: agent, id: "T-client-calls"}).emitUpdate(context.Background(), acp.UpdateAgentMessageText("x"))
	<-agent.clientCalls
	if err == nil || !strings.Contains(err.Error(), "client_calls") {
		t.Fatalf("client call backpressure = %v", err)
	}

	path, _ := fakeAgentAmpPath(t, "session-drift")
	driftAgent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	resp, err := driftAgent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession drift: %v", err)
	}
	_, err = driftAgent.Prompt(context.Background(), TextPromptRequest(resp.SessionId, "x"))
	if err == nil || !strings.Contains(err.Error(), "native session_id drift") {
		t.Fatalf("drift prompt error = %v", err)
	}
	_, err = driftAgent.Prompt(context.Background(), TextPromptRequest(resp.SessionId, "again"))
	if err == nil || !strings.Contains(err.Error(), "native session_id drift") {
		t.Fatalf("poisoned prompt error = %v", err)
	}
}

func TestReview1DeleteOrderingRetryAndManifestShape(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	storeErr := errors.New("delete store failed")
	store := &errorStore{deleteErr: storeErr}
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(store))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, deleteErr := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(resp.SessionId)); !errors.Is(deleteErr, storeErr) {
		t.Fatalf("delete store error = %v", deleteErr)
	}
	if _, deleted := agent.isDeleted(resp.SessionId); deleted {
		t.Fatal("session hidden before durable tombstone")
	}
	if _, sessionErr := agent.session(resp.SessionId); sessionErr != nil {
		t.Fatalf("active session removed before durable tombstone: %v", sessionErr)
	}

	failOncePath, _ := fakeAgentAmpPath(t, "delete-fail-once")
	failOnceStore := NewInMemorySessionStore()
	failOnce := NewAgent(WithExecutablePath(failOncePath), WithHome(t.TempDir()), WithSessionStore(failOnceStore))
	failOnceResp, err := failOnce.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession fail once: %v", err)
	}
	if _, deleteErr := failOnce.UnstableDeleteSession(ctx, DeleteSessionRequest(failOnceResp.SessionId)); deleteErr == nil {
		t.Fatal("first native delete succeeded unexpectedly")
	}
	if got := failOnce.pendingNativeDeleteIDs(); len(got) != 1 || got[0] != failOnceResp.SessionId {
		t.Fatalf("pending native deletes = %#v", got)
	}
	if _, listErr := failOnce.ListSessions(ctx, ListSessionsRequest()); listErr != nil {
		t.Fatalf("ListSessions retry: %v", listErr)
	}
	if got := failOnce.pendingNativeDeleteIDs(); len(got) != 0 {
		t.Fatalf("pending native delete not retried: %#v", got)
	}

	shapeStore := NewInMemorySessionStore()
	shapeAgent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(shapeStore))
	shapeResp, err := shapeAgent.NewSession(ctx, NewSessionRequest(t.TempDir(), WithSessionAdditionalDirectories("/tmp/extra")))
	if err != nil {
		t.Fatalf("NewSession shape: %v", err)
	}
	entries, err := shapeStore.Load(ctx, SessionKey{SessionID: string(shapeResp.SessionId), Subpath: SessionStoreMainSubpath})
	if err != nil || len(entries) != 1 {
		t.Fatalf("manifest entries = %d, %v", len(entries), err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(entries[0], &manifest); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"nativeExport", "additionalDirectories", "meta"} {
		if _, ok := manifest[forbidden]; ok {
			t.Fatalf("manifest contains %s: %s", forbidden, entries[0])
		}
	}
}

func TestReview1PathHomeContinuabilityAndStartup(t *testing.T) {
	if _, err := NewAgent(WithExecutablePath("/does/not/exist")).NewSession(context.Background(), NewSessionRequest("relative")); err == nil {
		t.Fatal("relative new cwd accepted")
	}
	path, _ := fakeAgentAmpPath(t, "")
	if _, err := NewAgent(WithExecutablePath(path)).NewSession(context.Background(), NewSessionRequest(t.TempDir(), WithSessionAdditionalDirectories("relative"))); err == nil {
		t.Fatal("relative additional directory accepted")
	}
	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-path", t.TempDir(), nil)
	if _, err := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()), WithSessionStore(store)).LoadSession(context.Background(), LoadSessionRequest("T-path", "")); err == nil {
		t.Fatal("empty load cwd accepted")
	}

	homeParent := t.TempDir()
	session, err := newAgentSession(
		NewAgent(WithHome(homeParent), WithEnv(map[string]string{"HOME": "/should/not/leak", "AMP_API_KEY": "fake"})),
		"T-home",
		t.TempDir(),
		parsedSessionMeta{},
		"",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close(context.Background()) }()
	for _, key := range []string{"HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"} {
		if !strings.HasPrefix(session.env[key], session.settingsDir) {
			t.Fatalf("%s not isolated under session dir: %q vs %q", key, session.env[key], session.settingsDir)
		}
	}
	if session.env["AMP_API_KEY"] != "fake" {
		t.Fatalf("AMP_API_KEY not preserved: %#v", session.env)
	}
	if !strings.HasPrefix(session.settingsFile, session.settingsDir) {
		t.Fatalf("settings file not under isolated dir: %s", session.settingsFile)
	}

	missingPath, _ := fakeAgentAmpPath(t, "missing-export")
	missingStore := NewInMemorySessionStore()
	missingCwd := t.TempDir()
	putStoredSession(t, missingStore, "T-agent-thread", missingCwd, []SessionStoreEntry{
		json.RawMessage(`{"type":"assistant","message":{"content":[{"type":"text","text":"stored"}]},"session_id":"T-agent-thread"}`),
	})
	missingAgent := NewAgent(WithExecutablePath(missingPath), WithHome(t.TempDir()), WithSessionStore(missingStore))
	if _, err := missingAgent.LoadSession(context.Background(), LoadSessionRequest("T-agent-thread", missingCwd)); err != nil {
		t.Fatalf("load missing native thread should replay mirror: %v", err)
	}
	if _, err := missingAgent.Prompt(context.Background(), TextPromptRequest("T-agent-thread", "x")); err == nil || !strings.Contains(err.Error(), "native_state_missing") {
		t.Fatalf("native missing prompt error = %v", err)
	}

	badVersionPath, _ := fakeAgentAmpPath(t, "bad-version")
	if _, err := NewAgent(WithExecutablePath(badVersionPath), WithHome(t.TempDir())).NewSession(context.Background(), NewSessionRequest(t.TempDir())); err == nil || !strings.Contains(err.Error(), "below required") {
		t.Fatalf("bad version startup error = %v", err)
	}
	probeListFailPath, _ := fakeAgentAmpPath(t, "probe-list-fail")
	if _, err := NewAgent(WithExecutablePath(probeListFailPath), WithHome(t.TempDir())).NewSession(context.Background(), NewSessionRequest(t.TempDir())); err == nil || !strings.Contains(err.Error(), "threads list --json probe failed") {
		t.Fatalf("probe list failure startup error = %v", err)
	}

	exportFailPath, _ := fakeAgentAmpPath(t, "export-fail")
	exportFailStore := NewInMemorySessionStore()
	exportFailCwd := t.TempDir()
	putStoredSession(t, exportFailStore, "T-agent-thread", exportFailCwd, nil)
	if _, err := NewAgent(WithExecutablePath(exportFailPath), WithHome(t.TempDir()), WithSessionStore(exportFailStore)).LoadSession(context.Background(), LoadSessionRequest("T-agent-thread", exportFailCwd)); err == nil || !strings.Contains(err.Error(), "export failed") {
		t.Fatalf("load export failure = %v", err)
	}
	if _, err := NewAgent(WithExecutablePath("/does/not/exist"), WithHome(t.TempDir()), WithSessionStore(exportFailStore)).LoadSession(context.Background(), LoadSessionRequest("T-agent-thread", exportFailCwd)); err == nil {
		t.Fatal("load startup failure accepted")
	}
}

func TestReview1RemainingBranches(t *testing.T) {
	ctx := context.Background()
	if _, err := NewAgent().UnstableDeleteSession(ctx, DeleteSessionRequest("")); err == nil {
		t.Fatal("empty delete id accepted")
	}
	fileHome := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(fileHome, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := NewAgent(WithHome(fileHome)).deleteNativeThread(ctx, "T-file-home", nil); err == nil {
		t.Fatal("deleteNativeThread ignored session creation error")
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	cancelAgent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
	cancelAgent.clientCalls <- struct{}{}
	if _, err := cancelAgent.acquireClientCall(cancelCtx); err == nil {
		t.Fatal("client call acquire ignored canceled context")
	}
	<-cancelAgent.clientCalls
	if maxConcurrentClientCalls(ConcurrencyLimits{MaxConcurrentClientCalls: 3}) != 3 || maxConcurrentClientCalls(ConcurrencyLimits{}) != defaultMaxConcurrentCalls {
		t.Fatal("client call limit normalization failed")
	}
	options, err := parseAmpOptions(map[string]any{"model": "m", "mode": "rush", "effort": "low"})
	if err != nil || options.Model != "m" || options.Mode != "rush" || options.Effort != "low" {
		t.Fatalf("parse valid options = %#v, %v", options, err)
	}
	for _, raw := range []map[string]any{
		{"mode": 1},
		{"effort": 1},
	} {
		if _, err := parseAmpOptions(raw); err == nil {
			t.Fatalf("invalid options accepted: %#v", raw)
		}
	}
	state := newPromptTurnState()
	if state.isCancelled() {
		t.Fatal("new turn state is cancelled")
	}
	state.cancel()
	if !state.isCancelled() {
		t.Fatal("cancelled turn state not observed")
	}

	previousMkdirAll := mkdirAll
	t.Cleanup(func() { mkdirAll = previousMkdirAll })
	mkdirAll = func(path string, perm os.FileMode) error {
		if strings.Contains(path, "xdg-cache") {
			return errors.New("mkdir isolated failed")
		}

		return previousMkdirAll(path, perm)
	}
	if _, err := newAgentSession(NewAgent(WithHome(t.TempDir())), "T-mkdir", t.TempDir(), parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("isolated mkdir error ignored")
	}
	previousMkdirTemp := mkdirTemp
	t.Cleanup(func() { mkdirTemp = previousMkdirTemp })
	mkdirTemp = func(string, string) (string, error) { return "", errors.New("mkdir temp failed") }
	if _, err := newAgentSession(NewAgent(), "T-temp", t.TempDir(), parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("temp dir error ignored")
	}

	if err := (&agentSession{agent: NewAgent()}).interruptState(ctx, nil); err != nil {
		t.Fatalf("nil interrupt state: %v", err)
	}
	if err := (&agentSession{agent: NewAgent()}).interruptState(ctx, newPromptTurnState()); err != nil {
		t.Fatalf("nil turn interrupt state: %v", err)
	}
	for _, msg := range []struct {
		message any
		want    string
	}{
		{message: fakeAmpMessage{}, want: ""},
	} {
		typed, ok := msg.message.(interface {
			AmpType() string
			RawMessage() map[string]any
			RawJSON() string
		})
		if !ok {
			t.Fatal("bad fake message")
		}
		if got := frameSessionID(typed); got != msg.want {
			t.Fatalf("frameSessionID = %q, want %q", got, msg.want)
		}
	}
	if isNativeMissingError(nil) {
		t.Fatal("nil missing error")
	}
	if isNativeCancelError(nil) {
		t.Fatal("nil cancel error")
	}
	cancelled := newPromptTurnState()
	cancelled.cancel()
	if resp, err := streamEndedWithoutTerminal(ctx, cancelled, nil, nil, fakeTurnErrors{errs: make(chan error)}); err != nil || resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("cancelled stream end = %#v, %v", resp, err)
	}

	rawAgent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
	rawClient, rawCleanup := attachRecordingClient(t, rawAgent)
	defer rawCleanup()
	_ = rawClient
	rawAgent.clientCalls <- struct{}{}
	rawErr := (&agentSession{agent: rawAgent, id: "T-raw-backpressure", rawEvents: true}).emitRawEvent(ctx, "stream-json", fakeAmpMessage{raw: map[string]any{"type": "x"}})
	<-rawAgent.clientCalls
	if rawErr == nil || !strings.Contains(rawErr.Error(), "client_calls") {
		t.Fatalf("raw event backpressure = %v", rawErr)
	}
}

func TestReview1CancelWhileContinueIsStarting(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "block-stdin")
	agent := NewAgent(WithExecutablePath(path), WithHome(t.TempDir()))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	largePrompt := strings.Repeat("x", 2*1024*1024)
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(context.Background(), TextPromptRequest(resp.SessionId, largePrompt))
		resultCh <- result
		errCh <- promptErr
	}()
	waitForPath(t, filepath.Join(state, "continue-started"))
	if err := agent.Cancel(context.Background(), acp.CancelNotification{SessionId: resp.SessionId}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case promptErr := <-errCh:
		result := <-resultCh
		if promptErr != nil || result.StopReason != acp.StopReasonCancelled {
			t.Fatalf("prompt during Continue cancel = %#v, %v", result, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prompt blocked during Continue cancellation")
	}
}

func requireUnsupportedField(t *testing.T, err error, field string) {
	t.Helper()
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error = %T %v", err, err)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("error data = %#v", reqErr.Data)
	}
	if data[jsonFieldError] != "unsupported" || data[jsonFieldField] != field {
		t.Fatalf("error data = %#v, want unsupported %s", data, field)
	}
}

func ctxWithTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	return ctx
}

func TestReview1WithHomeCreatesDirectories(t *testing.T) {
	parent := t.TempDir()
	session, err := newAgentSession(NewAgent(WithHome(parent)), "T-dirs", t.TempDir(), parsedSessionMeta{}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close(context.Background()) }()
	for _, path := range []string{
		session.env["HOME"],
		session.env["XDG_CONFIG_HOME"],
		session.env["XDG_CACHE_HOME"],
		session.env["XDG_DATA_HOME"],
		session.env["XDG_STATE_HOME"],
		filepath.Dir(session.settingsFile),
	} {
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			t.Fatalf("isolated path %s stat=%v info=%#v", path, err, info)
		}
	}
}
