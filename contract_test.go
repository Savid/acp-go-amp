package ampacp

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/stretchr/testify/require"
)

func TestCancelDeterminismAndNativeCancelResult(t *testing.T) {
	ctx := context.Background()
	idlePath, _ := fakeAgentAmpPath(t, "")
	idleAgent := newTestAgent(WithExecutablePath(idlePath), WithScratchDir(testScratchDir(t)))
	idleResp, err := idleAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession idle: %v", err)
	}
	if cancelErr := idleAgent.Cancel(ctx, acp.CancelNotification{SessionId: idleResp.SessionId}); cancelErr != nil {
		t.Fatalf("idle cancel: %v", cancelErr)
	}
	idlePrompt, err := idleAgent.Prompt(ctx, TextPromptRequest(idleResp.SessionId, "test-turn", "after idle cancel"))
	if err != nil || idlePrompt.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("prompt after idle cancel = %#v, %v", idlePrompt, err)
	}

	path, state := fakeAgentAmpPath(t, "sigint-ignore")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "cancel me"))
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
	cancelResultAgent := newTestAgent(WithExecutablePath(cancelResultPath), WithScratchDir(testScratchDir(t)))
	cancelResultResp, err := cancelResultAgent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession cancel result: %v", err)
	}
	cancelPrompt, err := cancelResultAgent.Prompt(ctx, TextPromptRequest(cancelResultResp.SessionId, "test-turn", "x"))
	if err != nil || cancelPrompt.StopReason != acp.StopReasonCancelled {
		t.Fatalf("native cancel result = %#v, %v", cancelPrompt, err)
	}
}

func TestStrictMetaAndConfigResponse(t *testing.T) {
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
		{name: "options unknown", meta: map[string]any{"amp": map[string]any{"options": map[string]any{"unknown": "x"}}}, field: "_meta.amp.options.unknown"},
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	if _, err := agent.LoadSession(ctxWithTimeout(t), LoadSessionRequest("T-config-response", cwd)); err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	resp, err := agent.SetSessionConfigOption(context.Background(), SetConfigOptionRequest("T-config-response", "mode", "low"))
	if err != nil {
		t.Fatalf("SetSessionConfigOption: %v", err)
	}
	if len(resp.ConfigOptions) != 1 {
		t.Fatalf("config response options = %#v", resp.ConfigOptions)
	}
}

// TestModeTravelsToNativeOnBothRoutes pins full mode passthrough on the
// mode-only config surface. Amp's mode is its native model selection — it picks
// the model, the system prompt, and the tool set — so the value namespace
// belongs to amp and this adapter keeps no list to refuse one against. A mode
// the advert does not name reaches the native `-m` flag unchanged over both
// doors a host can push a mode through: the establishment `_meta.amp.options`
// and a later `session/set_config_option`. `ultra` ships with the CLI, so it is
// accepted and advertised; `turbo-experimental` is a value no build here knows,
// and it travels just as far.
func TestModeTravelsToNativeOnBothRoutes(t *testing.T) {
	const unknownMode = "turbo-experimental"

	ctx := context.Background()
	path, state := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	_, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	newResp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir(),
		WithSessionAmpOptions(NewAmpOptions(WithAmpMode(modeUltra))),
	))
	if err != nil {
		t.Fatalf("NewSession %q: %v", modeUltra, err)
	}
	requireConfigMode(t, newResp.ConfigOptions, modeUltra)
	requireAdvertisedModes(t, newResp.ConfigOptions)

	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "turn-1", "x")); promptErr != nil {
		t.Fatalf("Prompt %q: %v", modeUltra, promptErr)
	}
	// The first prompt is the thread-less execute, so it is the argv carrying
	// `-x` without a `threads` subcommand.
	requireNativeModeFlag(t, state, modeUltra, func(args []string) bool {
		return slices.Contains(args, "-x") && !slices.Contains(args, "threads")
	})

	setResp, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(newResp.SessionId, configMode, unknownMode))
	if err != nil {
		t.Fatalf("SetSessionConfigOption %q: %v", unknownMode, err)
	}
	// A current value the advertised list does not contain is the accepted
	// shape: the list is a menu a host renders, and amp is the authority on what
	// it will actually run.
	requireConfigMode(t, setResp.ConfigOptions, unknownMode)
	requireAdvertisedModes(t, setResp.ConfigOptions)

	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "turn-2", "x")); promptErr != nil {
		t.Fatalf("Prompt %q: %v", unknownMode, promptErr)
	}
	// Every prompt after the first continues the adopted server-side thread, so
	// the argv naming that thread is the one to read.
	requireNativeModeFlag(t, state, unknownMode, func(args []string) bool {
		return slices.Contains(args, "continue") && slices.Contains(args, "T-agent-thread")
	})
}

// requireNativeModeFlag reads the recorded native argv, selects the most recent
// child the predicate accepts, and requires `-m` to be followed by exactly the
// requested mode. Presence of the token is not enough: the pin is that the value
// arrives unrewritten, in the flag's own position.
func requireNativeModeFlag(t *testing.T, state string, want string, match func([]string) bool) {
	t.Helper()

	var selected []string
	for _, args := range readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl")) {
		if match(args) {
			selected = args
		}
	}
	if selected == nil {
		t.Fatalf("no recorded native child matched for mode %q", want)
	}

	flag := slices.Index(selected, "-m")
	if flag < 0 || flag+1 >= len(selected) {
		t.Fatalf("native argv carries no -m value: %#v", selected)
	}
	if selected[flag+1] != want {
		t.Fatalf("native -m value = %q, want %q: %#v", selected[flag+1], want, selected)
	}
}

// requireAdvertisedModes pins the advertised menu itself, so the list a host
// renders stays the shipping CLI's documented set even as the current value
// ranges outside it.
func requireAdvertisedModes(t *testing.T, options []acp.SessionConfigOption) {
	t.Helper()

	for _, option := range options {
		if option.Select == nil || option.Select.Id != configMode {
			continue
		}
		if option.Select.Options.Ungrouped == nil {
			t.Fatalf("mode option advertises no ungrouped values: %#v", option.Select.Options)
		}

		got := make([]string, 0, len(*option.Select.Options.Ungrouped))
		for _, value := range *option.Select.Options.Ungrouped {
			got = append(got, string(value.Value))
		}
		if !slices.Equal(got, []string{modeLow, modeMedium, modeHigh, modeUltra}) {
			t.Fatalf("advertised modes = %#v", got)
		}

		return
	}

	t.Fatalf("no mode config option: %#v", options)
}

func TestClientBackpressureAndSessionIDDrift(t *testing.T) {
	agent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
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
	driftStore := NewInMemorySessionStore()
	driftCwd := t.TempDir()
	putStoredSession(t, driftStore, "T-agent-thread", driftCwd, nil)
	driftAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(driftStore))
	if _, loadErr := driftAgent.LoadSession(context.Background(), LoadSessionRequest("T-agent-thread", driftCwd)); loadErr != nil {
		t.Fatalf("LoadSession drift: %v", loadErr)
	}
	_, err = driftAgent.Prompt(context.Background(), TextPromptRequest("T-agent-thread", "test-turn", "x"))
	if err == nil || !strings.Contains(err.Error(), "native session_id drift") {
		t.Fatalf("drift prompt error = %v", err)
	}
	_, err = driftAgent.Prompt(context.Background(), TextPromptRequest("T-agent-thread", "test-turn", "again"))
	if err == nil || !strings.Contains(err.Error(), "native session_id drift") {
		t.Fatalf("poisoned prompt error = %v", err)
	}

	adoptPath, _ := fakeAgentAmpPath(t, "bad-adopt")
	adoptAgent := newTestAgent(WithExecutablePath(adoptPath), WithScratchDir(testScratchDir(t)))
	adoptResp, err := adoptAgent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession adopt: %v", err)
	}
	_, err = adoptAgent.Prompt(context.Background(), TextPromptRequest(adoptResp.SessionId, "test-turn", "x"))
	if err == nil || !strings.Contains(err.Error(), "native session_id invalid") {
		t.Fatalf("invalid adopted id prompt error = %v", err)
	}
}

func TestDeleteOrderingRetryAndManifestShape(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	storeErr := errors.New("delete store failed")
	store := &errorStore{deleteErr: storeErr}
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
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
	failOnce := newTestAgent(WithExecutablePath(failOncePath), WithScratchDir(testScratchDir(t)), WithSessionStore(failOnceStore))
	failOnceResp, err := failOnce.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession fail once: %v", err)
	}
	if _, promptErr := failOnce.Prompt(ctx, TextPromptRequest(failOnceResp.SessionId, "test-turn", "seed thread")); promptErr != nil {
		t.Fatalf("seed prompt fail once: %v", promptErr)
	}
	if _, deleteErr := failOnce.UnstableDeleteSession(ctx, DeleteSessionRequest(failOnceResp.SessionId)); deleteErr == nil {
		t.Fatal("first native delete succeeded unexpectedly")
	}
	if got := failOnce.pendingNativeDeleteIDs(); len(got) != 1 || got[0] != failOnceResp.SessionId {
		t.Fatalf("pending native deletes = %#v", got)
	}
	if _, deleteErr := failOnce.UnstableDeleteSession(ctx, DeleteSessionRequest(failOnceResp.SessionId)); deleteErr != nil {
		t.Fatalf("explicit pending native delete retry: %v", deleteErr)
	}
	if _, listErr := failOnce.ListSessions(ctx, ListSessionsRequest()); listErr != nil {
		t.Fatalf("ListSessions retry: %v", listErr)
	}
	if got := failOnce.pendingNativeDeleteIDs(); len(got) != 0 {
		t.Fatalf("pending native delete not retried: %#v", got)
	}

	pendingFailure := newTestAgent(WithExecutablePath("/does/not/exist"), WithScratchDir(testScratchDir(t)))
	pendingFailure.markPendingNativeDelete("T-pending-failure", "T-pending-native")
	if _, deleteErr := pendingFailure.UnstableDeleteSession(ctx, DeleteSessionRequest("T-pending-failure")); deleteErr == nil {
		t.Fatal("pending native delete retry failure was swallowed")
	}

	shapeStore := NewInMemorySessionStore()
	shapeAgent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(shapeStore))
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

func TestDeleteUsesStoreAsSoleNativeAuthority(t *testing.T) {
	ctx := context.Background()
	path, state := fakeAgentAmpPath(t, "")
	store := NewInMemorySessionStore()
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store))
	if out, versionErr := exec.Command(path, "version").CombinedOutput(); versionErr != nil {
		t.Fatalf("seed fake amp recording: %v\n%s", versionErr, out)
	}
	before := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))

	for range 2 {
		if _, err := agent.UnstableDeleteSession(ctx, DeleteSessionRequest("T-arbitrary-native-id")); err != nil {
			t.Fatalf("unknown delete: %v", err)
		}
	}
	afterUnknown := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	if len(afterUnknown) != len(before) {
		t.Fatalf("unknown store id launched native command: before=%#v after=%#v", before, afterUnknown)
	}

	active, err := newAgentSession(ctx, agent, "T-active-without-store", t.TempDir(), parsedSessionMeta{}, "", nil)
	if err != nil {
		t.Fatalf("construct store-absent active session: %v", err)
	}
	agent.mu.Lock()
	agent.sessions[active.id] = active
	agent.mu.Unlock()
	if _, activeDeleteErr := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(active.id)); activeDeleteErr != nil {
		t.Fatalf("store-absent active delete: %v", activeDeleteErr)
	}
	afterActive := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	if len(afterActive) != len(before) {
		t.Fatalf("store-absent active id launched native delete: before=%#v after=%#v", before, afterActive)
	}

	known, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession known: %v", err)
	}
	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(known.SessionId, "test-turn", "seed thread")); promptErr != nil {
		t.Fatalf("seed prompt known: %v", promptErr)
	}
	if _, knownDeleteErr := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(known.SessionId)); knownDeleteErr != nil {
		t.Fatalf("known delete: %v", knownDeleteErr)
	}
	if _, repeatedDeleteErr := agent.UnstableDeleteSession(ctx, DeleteSessionRequest(known.SessionId)); repeatedDeleteErr != nil {
		t.Fatalf("tombstoned repeat delete: %v", repeatedDeleteErr)
	}

	records := readHelperJSON[[]string](t, filepath.Join(state, "args.jsonl"))
	deleteCalls := 0
	for _, args := range records {
		if slicesContainCommand([][]string{args}, "threads", "delete", "T-agent-thread") {
			deleteCalls++
		}
	}
	if deleteCalls != 1 {
		t.Fatalf("known/tombstoned native delete calls = %d, want 1: %#v", deleteCalls, records)
	}

	loadErr := errors.New("delete authority load failed")
	if _, membershipErr := newTestAgent(WithSessionStore(&errorStore{loadErr: loadErr})).UnstableDeleteSession(ctx, DeleteSessionRequest("T-load-error")); !errors.Is(membershipErr, loadErr) {
		t.Fatalf("delete store membership error = %v", membershipErr)
	}

	nilStore := newTestAgent()
	nilStore.store = nil
	_, knownInNil, err := nilStore.storedManifest(ctx, "T-any")
	if err != nil || knownInNil {
		t.Fatalf("nil store membership = %t, %v", knownInNil, err)
	}
}

func TestPathHomeContinuabilityAndStartup(t *testing.T) {
	if _, err := newTestAgent(WithExecutablePath("/does/not/exist")).NewSession(context.Background(), NewSessionRequest("relative")); err == nil {
		t.Fatal("relative new cwd accepted")
	}
	path, _ := fakeAgentAmpPath(t, "")
	if _, err := newTestAgent(WithExecutablePath(path)).NewSession(context.Background(), NewSessionRequest(t.TempDir(), WithSessionAdditionalDirectories("relative"))); err == nil {
		t.Fatal("relative additional directory accepted")
	}
	store := NewInMemorySessionStore()
	putStoredSession(t, store, "T-path", t.TempDir(), nil)
	if _, err := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store)).LoadSession(context.Background(), LoadSessionRequest("T-path", "")); err == nil {
		t.Fatal("empty load cwd accepted")
	}

	scratchRoot := testScratchDir(t)
	session, err := newAgentSession(
		t.Context(),
		newTestAgent(WithScratchDir(scratchRoot), WithEnv(map[string]string{"HOME": "/should/not/leak", "AMP_API_KEY": "fake"})),
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
	missingAgent := newTestAgent(WithExecutablePath(missingPath), WithScratchDir(testScratchDir(t)), WithSessionStore(missingStore))
	if _, err := missingAgent.LoadSession(context.Background(), LoadSessionRequest("T-agent-thread", missingCwd)); err != nil {
		t.Fatalf("load missing native thread should replay mirror: %v", err)
	}
	if _, err := missingAgent.Prompt(context.Background(), TextPromptRequest("T-agent-thread", "test-turn", "x")); err == nil || !strings.Contains(err.Error(), "native_state_missing") {
		t.Fatalf("native missing prompt error = %v", err)
	}

	badVersionPath, _ := fakeAgentAmpPath(t, "bad-version")
	if _, err := newTestAgent(WithExecutablePath(badVersionPath), WithScratchDir(testScratchDir(t))).NewSession(context.Background(), NewSessionRequest(t.TempDir())); err == nil || !strings.Contains(err.Error(), "below required") {
		t.Fatalf("bad version startup error = %v", err)
	}
	probeListFailPath, _ := fakeAgentAmpPath(t, "probe-list-fail")
	if _, err := newTestAgent(WithExecutablePath(probeListFailPath), WithScratchDir(testScratchDir(t))).NewSession(context.Background(), NewSessionRequest(t.TempDir())); err == nil || !strings.Contains(err.Error(), "threads list --json probe failed") {
		t.Fatalf("probe list failure startup error = %v", err)
	}

	exportFailPath, _ := fakeAgentAmpPath(t, "export-fail")
	exportFailStore := NewInMemorySessionStore()
	exportFailCwd := t.TempDir()
	putStoredSession(t, exportFailStore, "T-agent-thread", exportFailCwd, nil)
	if _, err := newTestAgent(WithExecutablePath(exportFailPath), WithScratchDir(testScratchDir(t)), WithSessionStore(exportFailStore)).LoadSession(context.Background(), LoadSessionRequest("T-agent-thread", exportFailCwd)); err == nil || !strings.Contains(err.Error(), "export failed") {
		t.Fatalf("load export failure = %v", err)
	}
	if _, err := newTestAgent(WithExecutablePath("/does/not/exist"), WithScratchDir(testScratchDir(t)), WithSessionStore(exportFailStore)).LoadSession(context.Background(), LoadSessionRequest("T-agent-thread", exportFailCwd)); err == nil {
		t.Fatal("load startup failure accepted")
	}
}

func TestRemainingBranches(t *testing.T) {
	ctx := context.Background()
	if _, err := newTestAgent().UnstableDeleteSession(ctx, DeleteSessionRequest("")); err == nil {
		t.Fatal("empty delete id accepted")
	}
	fileHome := filepath.Join(t.TempDir(), "home-file")
	if err := os.WriteFile(fileHome, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := newTestAgent(WithScratchDir(fileHome)).deleteNativeThread(ctx, "T-file-home", "T-file-home", nil); err == nil {
		t.Fatal("deleteNativeThread ignored session creation error")
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	cancelAgent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
	cancelAgent.clientCalls <- struct{}{}
	if _, err := cancelAgent.acquireClientCall(cancelCtx); err == nil {
		t.Fatal("client call acquire ignored canceled context")
	}
	<-cancelAgent.clientCalls
	if maxConcurrentClientCalls(ConcurrencyLimits{MaxConcurrentClientCalls: 3}) != 3 || maxConcurrentClientCalls(ConcurrencyLimits{}) != defaultMaxConcurrentCalls {
		t.Fatal("client call limit normalization failed")
	}
	options, _, err := parseAmpOptionsWithPresence(map[string]any{"model": "m", "mode": "low"})
	if err != nil || options.Model != "m" || options.Mode != "low" {
		t.Fatalf("parse valid options = %#v, %v", options, err)
	}
	for _, raw := range []map[string]any{
		{"mode": 1},
		{"unknown": "x"},
	} {
		if _, _, err := parseAmpOptionsWithPresence(raw); err == nil {
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
	if _, err := newAgentSession(t.Context(), newTestAgent(WithScratchDir(testScratchDir(t))), "T-mkdir", t.TempDir(), parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("isolated mkdir error ignored")
	}
	previousMkdirTemp := mkdirTemp
	t.Cleanup(func() { mkdirTemp = previousMkdirTemp })
	mkdirTemp = func(string, string) (string, error) { return "", errors.New("mkdir temp failed") }
	if _, err := newAgentSession(t.Context(), newTestAgent(), "T-temp", t.TempDir(), parsedSessionMeta{}, "", nil); err == nil {
		t.Fatal("temp dir error ignored")
	}

	if err := (&agentSession{agent: newTestAgent()}).interruptState(ctx, nil); err != nil {
		t.Fatalf("nil interrupt state: %v", err)
	}
	if err := (&agentSession{agent: newTestAgent()}).interruptState(ctx, newPromptTurnState()); err != nil {
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
	// A cancel and a successful terminal frame can both be ready at the same
	// select. A turn the host already cancelled reports cancelled whichever one
	// the loop happened to read.
	success := &amp.ResultMessage{Subtype: "success"}
	if resp, err := (&agentSession{}).resolveTerminal(ctx, cancelled, success, nil, nil, "", "", fakeTurnErrors{errs: make(chan error)}); err != nil || resp.StopReason != acp.StopReasonCancelled {
		t.Fatalf("cancelled terminal = %#v, %v", resp, err)
	}

	rawAgent := newTestAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: 1}))
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

func TestCancelWhileContinueIsStarting(t *testing.T) {
	path, state := fakeAgentAmpPath(t, "block-stdin")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(context.Background(), NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	largePrompt := strings.Repeat("x", 2*1024*1024)
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := agent.Prompt(context.Background(), TextPromptRequest(resp.SessionId, "test-turn", largePrompt))
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

// ctxWithTimeout bounds a lifecycle call so a hang fails the case instead of
// the whole binary. It is a safety net, not a performance assertion: a real
// native launch claims the standalone identity, which proves the identity
// vacant across every task on the host, so the bound has to clear that.
func ctxWithTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	return ctx
}

func TestHomeUnsupportedAtSessionStart(t *testing.T) {
	ctx := context.Background()
	agent := newTestAgent(WithHome(t.TempDir()))

	if _, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("new session accepted a configured home")
	} else {
		requireUnsupportedField(t, err, optionFieldHome)
	}

	if _, err := agent.LoadSession(ctx, LoadSessionRequest("T-home", t.TempDir())); err == nil {
		t.Fatal("load session accepted a configured home")
	} else {
		requireUnsupportedField(t, err, optionFieldHome)
	}

	if _, err := agent.ResumeSession(ctx, ResumeSessionRequest("T-home", t.TempDir())); err == nil {
		t.Fatal("resume session accepted a configured home")
	} else {
		requireUnsupportedField(t, err, optionFieldHome)
	}
}

func TestSessionDirFailurePropagates(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")

	previousMkdirTemp := mkdirTemp
	t.Cleanup(func() { mkdirTemp = previousMkdirTemp })
	mkdirTemp = func(string, string) (string, error) { return "", errors.New("session dir failed") }

	// NewSession: startup probe succeeds (its own temp dir), then the per-session
	// scratch dir creation fails and the error propagates out of NewSession.
	if _, err := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t))).NewSession(ctx, NewSessionRequest(t.TempDir())); err == nil {
		t.Fatal("new session ignored per-session scratch dir failure")
	}

	// loadOrResume: same failure after the manifest loads.
	store := NewInMemorySessionStore()
	cwd := t.TempDir()
	putStoredSession(t, store, "T-temp", cwd, nil)
	if _, err := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithSessionStore(store)).LoadSession(ctx, LoadSessionRequest("T-temp", cwd)); err == nil {
		t.Fatal("load session ignored per-session scratch dir failure")
	}
}

func TestWithScratchDirCreatesDirectories(t *testing.T) {
	parent := testScratchDir(t)
	session, err := newAgentSession(t.Context(), newTestAgent(WithScratchDir(parent)), "T-dirs", t.TempDir(), parsedSessionMeta{}, "", nil)
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

func TestInitializeAdvertisesTheFamilyMediaEnvelope(t *testing.T) {
	meta := initializeMeta(t)

	require.Equal(t, []string{metaMediaEnvelopeKey, ampMetaKey}, sortedMetaKeys(meta))

	envelope, ok := meta[metaMediaEnvelopeKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{
		keyMaxBytes:        int64(921_600),
		keyMaxPromptBytes:  int64(6_291_456),
		keyMaxDimension:    int64(8000),
		keyImageFormats:    []string{imageMIMEPNG, imageMIMEJPEG, imageMIMEGIF, imageMIMEWebP},
		keyDocumentFormats: []string{},
	}, envelope)
}

// TestMediaEnvelopeIsNotConditionedOnRouteSupport pins that Amp advertises the
// family media envelope while advertising no route surface at all, so a host must
// never infer one from the other.
func TestMediaEnvelopeIsNotConditionedOnRouteSupport(t *testing.T) {
	meta := initializeMeta(t)

	require.NotContains(t, meta, "acp-go.dev/route")
	require.Contains(t, meta, metaMediaEnvelopeKey)
}

func TestHandoffAdvertisementFollowsTheConfiguredReadRoot(t *testing.T) {
	require.NotContains(t, initializeMeta(t), metaHandoffKey)

	meta := initializeMeta(t, WithInputHandoffRoot(t.TempDir()))
	require.Equal(t, []string{metaHandoffKey, metaMediaEnvelopeKey, ampMetaKey}, sortedMetaKeys(meta))
	require.Equal(t, map[string]any{keyVersions: []int64{1}}, meta[metaHandoffKey])

	encoded, err := json.Marshal(meta[metaHandoffKey])
	require.NoError(t, err)
	require.JSONEq(t, `{"versions":[1]}`, string(encoded))
}

// loadGate parks the first main-subpath Load after arming, once the underlying
// read has already returned. That puts a concurrent load exactly where
// loadOrResume has passed its entry tombstone check and holds a pre-delete
// manifest, so a delete completing behind it races the install.
type loadGate struct {
	SessionStore

	mu      sync.Mutex
	armed   bool
	started chan struct{}
	release chan struct{}
}

func (g *loadGate) Load(ctx context.Context, key SessionKey) ([]SessionStoreEntry, error) {
	entries, err := g.SessionStore.Load(ctx, key)

	g.mu.Lock()
	armed := g.armed && key.Subpath == SessionStoreMainSubpath
	if armed {
		g.armed = false
	}
	g.mu.Unlock()

	if armed {
		g.started <- struct{}{}
		<-g.release
	}

	return entries, err
}

func (g *loadGate) arm() {
	g.mu.Lock()
	g.armed = true
	g.mu.Unlock()
}

// TestLoadRacingADeleteInstallsNothingAndLeaksNothing pins the concurrent
// tombstone class: a load already past its entry check when a delete completes
// installs nothing, tears its fully prepared replacement down, and leaves the
// deletion marker set. The leak this refuses is total — the installed session
// would be unreachable through every door, holding its active-session slot, its
// settings directory and its scratch reservation for the rest of the agent's
// life.
func TestLoadRacingADeleteInstallsNothingAndLeaksNothing(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	scratch := testScratchDir(t)
	store := &loadGate{
		SessionStore: NewInMemorySessionStore(),
		started:      make(chan struct{}, 1),
		release:      make(chan struct{}),
	}

	var reserved, released atomic.Int64

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(scratch),
		WithSessionStore(store),
		WithRuntimeResourceHooks(RuntimeResourceHooks{
			ReserveScratchRoot: func(context.Context, RuntimeResourceKind) (func(), error) {
				reserved.Add(1)

				return func() { released.Add(1) }, nil
			},
		}),
	)
	t.Cleanup(func() { _ = agent.Close() })

	created, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	sessionID := created.SessionId
	cwd := t.TempDir()

	// Evict the live session so the delete below finds only the store row: with
	// no session to fence, the tombstone is the sole thing the racing load has
	// to lose to.
	_, err = agent.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)

	store.arm()

	loaded := make(chan error, 1)

	go func() {
		_, loadErr := agent.LoadSession(ctx, LoadSessionRequest(sessionID, cwd))
		loaded <- loadErr
	}()

	// The load is parked inside loadManifest now: past its entry check, holding
	// a manifest the delete is about to invalidate.
	<-store.started

	_, err = agent.UnstableDeleteSession(ctx, DeleteSessionRequest(sessionID))
	require.NoError(t, err)

	close(store.release)

	loadErr := <-loaded
	require.Error(t, loadErr, "a load that lost the race hands back no session")
	requireRequestErrorCode(t, loadErr, invalidParamsCode)

	agent.mu.Lock()
	_, mapped := agent.sessions[sessionID]
	deleted := agent.isDeletedLocked(sessionID)
	agent.mu.Unlock()

	require.False(t, mapped, "the replacement that lost the race is never installed")
	require.True(t, deleted, "installing never clears the deletion marker")

	_, reachable := agent.session(sessionID)
	require.Error(t, reachable, "the tombstoned id answers unknown at every door")

	main, storeErr := store.Load(ctx, SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	require.NoError(t, storeErr)
	require.Empty(t, main, "the tombstone is the last word on a deleted session")

	require.Equal(t, reserved.Load(), released.Load(), "the prepared replacement releases its scratch reservation")

	sessionDirs, err := filepath.Glob(filepath.Join(scratch, "acp-go-amp-session-*"))
	require.NoError(t, err)
	require.Empty(t, sessionDirs, "the prepared replacement releases its settings directory")
}
