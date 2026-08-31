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
		// A mode key that arrived carrying nothing is the same shape defect as
		// one carrying a non-string: it states a selection and names none.
		{name: "mode empty", meta: map[string]any{"amp": map[string]any{"options": map[string]any{"mode": ""}}}, field: "_meta.amp.options.mode"},
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

// TestEmptyModeIsRefusedWhileAbsenceIsNot pins the one mode value this adapter
// still judges, at both doors a host can push a mode through. Deleting the enum
// gates handed amp the whole namespace, but the empty string is not a member of
// it: it names no mode, and the argv builder would answer it by omitting `-m`
// altogether — the session would run amp's default while the host read back a
// selection it never got. So it is refused on the member that carried it, the
// same way a mode that is not a string is, and for the same reason: request
// shape, not a value gate. Absence is the other request entirely and stays
// legal — a session established with no mode key names no selection, builds no
// `-m`, and lets amp's own default stand.
func TestEmptyModeIsRefusedWhileAbsenceIsNot(t *testing.T) {
	ctx := context.Background()
	path, state := fakeAgentAmpPath(t, "")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	_, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	// Door one, establishment. The Go request builder cannot even express this
	// request — an empty Mode is omitted from the payload — so the refusal is
	// pinned on the wire shape a host can actually send.
	emptyMode := WithSessionMeta(map[string]any{
		ampMetaKey: map[string]any{ampOptionsKey: map[string]any{optionModeKey: ""}},
	})
	_, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir(), emptyMode))
	requireUnsupportedField(t, err, "_meta.amp.options.mode")

	// The same door on the load and resume routes, which validate the request
	// identically before an active session may be reused.
	_, err = agent.LoadSession(ctx, LoadSessionRequest("T-empty-mode", t.TempDir(), emptyMode))
	requireUnsupportedField(t, err, "_meta.amp.options.mode")
	_, err = agent.ResumeSession(ctx, ResumeSessionRequest("T-empty-mode", t.TempDir(), emptyMode))
	requireUnsupportedField(t, err, "_meta.amp.options.mode")

	// Absence is the accepted request: it states no selection, so the session
	// takes the default it advertises and carries that real value to the native
	// child. What absence never produces is a turn with no `-m` at all.
	newResp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession without a mode: %v", err)
	}
	requireConfigMode(t, newResp.ConfigOptions, modeMedium)
	if _, promptErr := agent.Prompt(ctx, TextPromptRequest(newResp.SessionId, "turn-1", "x")); promptErr != nil {
		t.Fatalf("Prompt without a mode: %v", promptErr)
	}
	requireNativeModeFlag(t, state, modeMedium, func(args []string) bool {
		return slices.Contains(args, "-x") && !slices.Contains(args, "threads")
	})

	// Door two, session/set_config_option. The refusal names `value`, and the
	// session keeps the mode it had rather than recording a selection nothing
	// would carry to the native child.
	setResp, err := agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(newResp.SessionId, configMode, modeHigh))
	if err != nil {
		t.Fatalf("SetSessionConfigOption %q: %v", modeHigh, err)
	}
	requireConfigMode(t, setResp.ConfigOptions, modeHigh)

	_, err = agent.SetSessionConfigOption(ctx, SetConfigOptionRequest(newResp.SessionId, configMode, ""))
	requireUnsupportedField(t, err, fieldValue)

	session, err := agent.session(newResp.SessionId)
	if err != nil {
		t.Fatalf("session after the refusal: %v", err)
	}
	requireConfigMode(t, session.configOptions(), modeHigh)
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
	if got := failOnce.cleanupOwnerIDs(); len(got) != 1 || got[0] != failOnceResp.SessionId {
		t.Fatalf("pending deletes = %#v", got)
	}
	if _, deleteErr := failOnce.UnstableDeleteSession(ctx, DeleteSessionRequest(failOnceResp.SessionId)); deleteErr != nil {
		t.Fatalf("explicit pending native delete retry: %v", deleteErr)
	}
	if _, listErr := failOnce.ListSessions(ctx, ListSessionsRequest()); listErr != nil {
		t.Fatalf("ListSessions retry: %v", listErr)
	}
	if got := failOnce.cleanupOwnerIDs(); len(got) != 0 {
		t.Fatalf("pending delete not retried: %#v", got)
	}

	pendingFailure := newTestAgent(WithExecutablePath("/does/not/exist"), WithScratchDir(testScratchDir(t)))
	pendingWrapper := &agentSession{agent: pendingFailure, id: "T-pending-failure", nativeID: "T-pending-native", turn: make(chan struct{}, 1)}
	pendingFailure.cleanupOwners[pendingWrapper.id] = []agentCleanupOwner{{session: pendingWrapper, kind: agentCleanupDeleted}}
	pendingFailure.deleted[pendingWrapper.id] = struct{}{}
	if _, deleteErr := pendingFailure.UnstableDeleteSession(ctx, DeleteSessionRequest("T-pending-failure")); deleteErr == nil {
		t.Fatal("pending delete retry failure was swallowed")
	}
	if got, ok := pendingFailure.cleanupOwner(pendingWrapper.id); !ok || got.session != pendingWrapper {
		t.Fatal("failed pending delete lost exact wrapper ownership")
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

// TestFailedDeleteRetainsExactWrapperOwnership pins both ownership sources. A
// no-store live wrapper and a store-backed live wrapper are transferred intact
// behind the tombstone; explicit retry and Agent.Close respectively finish the
// same wrapper and release its scratch reservation exactly once.
func TestFailedDeleteRetainsExactWrapperOwnership(t *testing.T) {
	t.Run("no-store explicit retry", func(t *testing.T) {
		path, _ := fakeAgentAmpPath(t, "delete-fail-once")
		agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))

		wrapper, err := newAgentSession(t.Context(), agent, "T-delete-no-store", t.TempDir(), parsedSessionMeta{}, "", nil)
		require.NoError(t, err)
		wrapper.nativeID = "T-agent-thread"

		releases := 0
		originalRelease := wrapper.scratchRootRelease
		wrapper.scratchRootRelease = func() {
			releases++
			originalRelease()
		}

		agent.store = nil
		agent.mu.Lock()
		agent.activateSessionLocked(wrapper)
		agent.mu.Unlock()
		agent.observe.AddActiveSession(t.Context(), 1)

		_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(wrapper.id))
		require.Error(t, err)

		owned, pending := agent.cleanupOwner(wrapper.id)
		require.True(t, pending)
		require.Same(t, wrapper, owned.session)
		require.Equal(t, "T-agent-thread", owned.session.nativeSessionID())
		require.Zero(t, releases)
		require.DirExists(t, wrapper.settingsDir)
		_, lookupErr := agent.session(wrapper.id)
		require.Error(t, lookupErr, "the tombstone hides the internally owned wrapper")

		_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(wrapper.id))
		require.NoError(t, err)
		_, pending = agent.cleanupOwner(wrapper.id)
		require.False(t, pending)
		require.Equal(t, 1, releases)
		require.NoDirExists(t, wrapper.settingsDir)
		require.NoError(t, agent.Close())
	})

	t.Run("stored row Agent.Close sweep", func(t *testing.T) {
		path, _ := fakeAgentAmpPath(t, "delete-fail-once")
		store := NewInMemorySessionStore()
		agent := newTestAgent(
			WithExecutablePath(path),
			WithScratchDir(testScratchDir(t)),
			WithSessionStore(store),
		)

		created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		require.NoError(t, err)
		_, err = agent.Prompt(t.Context(), TextPromptRequest(created.SessionId, "delete-owner", "seed thread"))
		require.NoError(t, err)

		wrapper, err := agent.session(created.SessionId)
		require.NoError(t, err)
		releases := 0
		originalRelease := wrapper.scratchRootRelease
		wrapper.scratchRootRelease = func() {
			releases++
			originalRelease()
		}

		_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(created.SessionId))
		require.Error(t, err)

		owned, pending := agent.cleanupOwner(created.SessionId)
		require.True(t, pending)
		require.Same(t, wrapper, owned.session)
		require.Equal(t, wrapper.nativeSessionID(), owned.session.nativeSessionID())
		require.Zero(t, releases)
		require.DirExists(t, wrapper.settingsDir)

		main, loadErr := store.Load(t.Context(), SessionKey{SessionID: string(created.SessionId), Subpath: SessionStoreMainSubpath})
		require.NoError(t, loadErr)
		require.Empty(t, main, "the failed teardown remains hidden behind its durable tombstone")
		_, lookupErr := agent.session(created.SessionId)
		require.Error(t, lookupErr)

		require.NoError(t, agent.Close(), "shutdown retries the exact pending wrapper")
		require.Equal(t, 1, releases)
		require.NoDirExists(t, wrapper.settingsDir)
		_, pending = agent.cleanupOwner(created.SessionId)
		require.False(t, pending)
	})

	t.Run("stored-only tombstone failure releases unused wrapper", func(t *testing.T) {
		path, _ := fakeAgentAmpPath(t, "")
		baseStore := NewInMemorySessionStore()
		store := &fencedDeleteStore{SessionStore: baseStore}
		scratch := testScratchDir(t)
		agent := newTestAgent(
			WithExecutablePath(path),
			WithScratchDir(scratch),
			WithSessionStore(store),
		)

		created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
		require.NoError(t, err)
		_, err = agent.Prompt(t.Context(), TextPromptRequest(created.SessionId, "stored-only-delete", "seed thread"))
		require.NoError(t, err)
		_, err = agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
		require.NoError(t, err)
		_, err = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(created.SessionId))
		require.ErrorContains(t, err, "tombstone write refused")
		_, pending := agent.cleanupOwner(created.SessionId)
		require.False(t, pending)
		_, deleted := agent.isDeleted(created.SessionId)
		require.False(t, deleted)
		main, loadErr := baseStore.Load(t.Context(), SessionKey{SessionID: string(created.SessionId), Subpath: SessionStoreMainSubpath})
		require.NoError(t, loadErr)
		require.NotEmpty(t, main)
		require.NoError(t, agent.Close())
	})
}

type deleteCallGate struct {
	SessionStore
	started chan struct{}
	release chan struct{}
}

func (g *deleteCallGate) Delete(ctx context.Context, key SessionKey) error {
	g.started <- struct{}{}
	select {
	case <-g.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	return g.SessionStore.Delete(ctx, key)
}

// TestConcurrentCloseAndDeleteReleaseExactWrapperOnce pins the adjacent
// ownership race: delete holds the wrapper's teardown authority while its
// tombstone is in flight, so an already-resolved CloseSession cannot release
// the same settings tree or scratch reservation a second time.
func TestConcurrentCloseAndDeleteReleaseExactWrapperOnce(t *testing.T) {
	t.Setenv("AMP_API_KEY", "ownership-test")
	path, _ := fakeAgentAmpPath(t, "")
	store := &deleteCallGate{
		SessionStore: NewInMemorySessionStore(),
		started:      make(chan struct{}, 1),
		release:      make(chan struct{}),
	}
	agent := newTestAgent(
		WithExecutablePath(path),
		WithSessionStore(store),
		WithScratchDir(testScratchDir(t)),
	)

	created, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	wrapper, err := agent.session(created.SessionId)
	require.NoError(t, err)

	releases := 0
	originalRelease := wrapper.scratchRootRelease
	wrapper.scratchRootRelease = func() {
		releases++
		originalRelease()
	}

	deleteErr := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(created.SessionId))
		deleteErr <- err
	}()
	awaitCorrectionSignal(t, store.started, "delete store entry")

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- agent.removeSession(t.Context(), created.SessionId, wrapper)
	}()

	close(store.release)
	require.NoError(t, receiveCorrection(t, deleteErr, "concurrent delete result"))
	require.NoError(t, receiveCorrection(t, closeErr, "concurrent close result"))
	require.Equal(t, 1, releases)
	_, pending := agent.cleanupOwner(created.SessionId)
	require.False(t, pending)
	_, lookupErr := agent.session(created.SessionId)
	require.Error(t, lookupErr)
	require.NoError(t, agent.Close())
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
	agent.activateSessionLocked(active)
	agent.mu.Unlock()
	agent.observe.AddActiveSession(ctx, 1)
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
	fileStore := NewInMemorySessionStore()
	putStoredSession(t, fileStore, "T-file-home", t.TempDir(), nil)
	fileAgent := newTestAgent(WithScratchDir(fileHome), WithSessionStore(fileStore))
	if _, err := fileAgent.UnstableDeleteSession(ctx, DeleteSessionRequest("T-file-home")); err == nil {
		t.Fatal("stored delete ignored wrapper construction error")
	}
	if entries, err := fileStore.Load(ctx, SessionKey{SessionID: "T-file-home", Subpath: SessionStoreMainSubpath}); err != nil || len(entries) == 0 {
		t.Fatalf("wrapper construction failure tombstoned stored row: entries=%d err=%v", len(entries), err)
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
}

func TestRemainingSessionConstructionBranches(t *testing.T) {
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
}

func TestRemainingPromptBranches(t *testing.T) {
	ctx := context.Background()

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
	require.Equal(t, map[string]any{"version": 1}, meta[metaHandoffKey])

	encoded, err := json.Marshal(meta[metaHandoffKey])
	require.NoError(t, err)
	require.JSONEq(t, `{"version":1}`, string(encoded))
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
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return entries, err
}

func (g *loadGate) arm() {
	g.mu.Lock()
	g.armed = true
	g.mu.Unlock()
}

// TestLoadRacingADeleteInstallsNothingAndLeaksNothing pins the admitted-use
// transfer class. Delete publishes its exact flight while a cold load is inside
// Store.Load, then joins that use. The load hands the one prepared wrapper to
// the flight instead of installing or reclaiming it, and delete settles that
// exact pointer after the use finishes.
func TestLoadRacingADeleteInstallsNothingAndLeaksNothing(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "")
	scratch := testScratchDir(t)
	store := &loadGate{
		SessionStore: NewInMemorySessionStore(),
		started:      make(chan struct{}, 1),
		release:      make(chan struct{}),
	}

	agent := newTestAgent(
		WithExecutablePath(path),
		WithScratchDir(scratch),
		WithSessionStore(store),
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
	awaitCorrectionSignal(t, store.started, "load manifest entry")

	flightCtx, flight, use, existing, err := agent.publishSessionFlight(ctx, sessionID, agentSessionDeleteFlight, nil)
	require.NoError(t, err)
	require.Nil(t, existing)
	require.NotNil(t, use)
	close(store.release)
	require.NoError(t, agent.joinSessionFlightUse(flightCtx, sessionID, flight, use))

	loadErr := <-loaded
	require.Error(t, loadErr, "a load that lost the race hands back no session")
	requireRequestErrorCode(t, loadErr, invalidParamsCode)
	require.NotNil(t, flight.session)

	require.NoError(t, agent.deleteSession(flightCtx, sessionID, flight))
	agent.finishSessionFlight(sessionID, flight)

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

	sessionDirs, err := filepath.Glob(filepath.Join(scratch, "acp-go-amp-session-*"))
	require.NoError(t, err)
	require.Empty(t, sessionDirs, "the prepared replacement releases its settings directory")
}
