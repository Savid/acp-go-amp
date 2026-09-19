package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestWireTurnsHaveDistinctIncarnations(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	initialized := h.initialize(withLifecycle())
	require.Empty(t, initialized.AuthMethods)
	capability, ok := initialized.Meta[wire.LifecycleKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, false, capability["updatesOutsidePrompt"])
	session := h.newSession(WithSessionRawEvents(true))
	for i, text := range []string{"TOOL", "DUPLICATE"} {
		response, err := h.prompt(session.SessionId, text, promptMeta(i))
		require.NoError(t, err)
		require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
		require.Equal(t, 13, response.Usage.TotalTokens)
	}
	require.NotContains(t, agentText(h.rec.snapshot()), "late output")
	require.Positive(t, h.rec.rawEventCount())
	streams := map[string]int{}
	idle := 0
	for _, notification := range h.rec.snapshot() {
		if meta, ok := notification.Meta[wire.LifecycleKey].(map[string]any); ok {
			stream, ok := meta["streamId"].(string)
			require.True(t, ok)
			streams[stream]++
			event, ok := meta["event"].(map[string]any)
			require.True(t, ok)
			if event["type"] == "state_update" && event["state"] == "idle" {
				idle++
			}
		}
	}
	require.Len(t, streams, 2)
	for _, count := range streams {
		require.Equal(t, 4, count)
	}
	require.Equal(t, 2, idle)
}

func TestNativeContinuationAndCarrierRestore(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	native := t.TempDir()
	options := []Option{WithSessionStore(store), WithEnv(map[string]string{"ACP_GO_AMP_TEST_NATIVE": native, "GORACE": "atexit_sleep_ms=0"})}
	h := newHarness(t, options...)
	h.initialize()
	cwd := t.TempDir()
	dirs := []string{filepath.Join(t.TempDir(), "bin")}
	carrier := NewAmpOptions(WithAmpMode("ultra"), WithAmpEnv(map[string]string{"PATH": "/usr/bin", "HOME": t.TempDir(), "SESSION_VALUE": "chosen", "ACP_GO_AMP_INTERNAL_CALLER": "drop"}), WithAmpExtraPathDirs(dirs...))
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, WithSessionAmpOptions(carrier)))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "remember first", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	path := filepath.Join(native, string(session.SessionId)+".json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var snapshot nativeSnapshot
	require.NoError(t, json.Unmarshal(data, &snapshot))
	snapshot.Messages = append(snapshot.Messages, nativeMessage{ID: int64(len(snapshot.Messages) + 1), Role: "user", Content: []map[string]any{{"type": "text", "text": "native continuation"}}})
	fakeSave(native, snapshot)
	cold := newHarness(t, options...)
	cold.initialize()
	restored, err := cold.conn.LoadSession(cold.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("ultra"), restored.ConfigOptions[0].Select.CurrentValue)
	require.Contains(t, agentText(cold.rec.snapshot()), "remember first")
	var restoredUsage *acp.SessionUsageUpdate
	for _, update := range cold.rec.snapshot() {
		if update.Update.UsageUpdate != nil {
			restoredUsage = update.Update.UsageUpdate
		}
	}
	require.NotNil(t, restoredUsage)
	require.Equal(t, 123456, restoredUsage.Size)
	require.Equal(t, 13, restoredUsage.Used)
	_, err = cold.prompt(session.SessionId, "ENV", nil)
	require.NoError(t, err)
	text := agentText(cold.rec.snapshot())
	require.Contains(t, text, "chosen")
	require.Contains(t, text, dirs[0]+":/usr/bin")
	require.Contains(t, text, `"internal":""`)
	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.Contains(t, string(rows[0]), "native continuation")
	_, err = cold.conn.CloseSession(cold.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))
	_, err = cold.conn.LoadSession(cold.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.Equal(t, "amp_restore_failed", requestErrorData(t, err)["error"])
	after, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.Equal(t, rows, after)
}

func TestCancelCrashAndDelete(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"cancel", "crash", "delete"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.initialize()
			session := h.newSession()
			prompt := "SLOW"
			if kind == "crash" {
				prompt = "CRASH"
			}
			type outcome struct {
				response acp.PromptResponse
				err      error
			}
			done := make(chan outcome, 1)
			go func() { response, err := h.prompt(session.SessionId, prompt, nil); done <- outcome{response, err} }()
			h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
				for _, u := range updates {
					if u.Update.UserMessageChunk != nil {
						return true
					}
				}

				return false
			})
			switch kind {
			case "cancel":
				require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
			case "delete":
				_, err := h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(session.SessionId))
				require.NoError(t, err)
			}
			var result outcome
			select {
			case result = <-done:
			case <-time.After(testTimeout):
				t.Fatal("turn did not settle")
			}
			switch kind {
			case "crash":
				require.Equal(t, wire.CauseProcessExit, requestErrorData(t, result.err)["cause"])
			default:
				require.NoError(t, result.err)
				require.Equal(t, acp.StopReasonCancelled, result.response.StopReason)
			}
			if kind != "delete" {
				_, err := h.prompt(session.SessionId, "next", nil)
				require.NoError(t, err)
			} else {
				_, err := h.prompt(session.SessionId, "next", nil)
				require.Error(t, err)
			}
		})
	}
}

func TestImagesReplayFromNativeExport(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	session := h.newSession(WithSessionRawEvents(true))
	_, err := h.prompt(session.SessionId, "IMAGE", nil)
	require.NoError(t, err)
	require.Equal(t, 1, toolImages(h.rec.snapshot()))
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, sessionCwd(t, h, session.SessionId)))
	require.NoError(t, err)
	require.Equal(t, 2, toolImages(h.rec.snapshot()))
	h.rec.mu.Lock()
	defer h.rec.mu.Unlock()
	for _, raw := range h.rec.raw {
		require.NotContains(t, string(raw), "iVBOR")
	}
}

func sessionCwd(t *testing.T, h *harness, id acp.SessionId) string {
	t.Helper()
	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	for _, s := range list.Sessions {
		if s.SessionId == id {
			return s.Cwd
		}
	}
	t.Fatal("missing session")

	return ""
}

type failingStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *failingStore) Replace(ctx context.Context, key acpcore.SessionKey, entries []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("fixture store failure")
	}

	return s.SessionStore.Replace(ctx, key, entries)
}

func TestCommitFailureCannotReportSuccess(t *testing.T) {
	t.Parallel()
	store := &failingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize(withLifecycle())
	session := h.newSession()
	store.fail.Store(true)
	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(0))
	require.Equal(t, "amp_turn_failed", requestErrorData(t, err)["error"])
	kinds := lifecycleEventKinds(h.rec.snapshot())
	require.Contains(t, kinds, "prompt_accepted", "the turn never reached the lifecycle stream")
	require.NotContains(t, kinds, "state_update:idle", "a turn whose foreground commit failed published a terminal idle")
	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.NotContains(t, string(rows[0]), "HELLO")
	store.fail.Store(false)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	rows, err = loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.Contains(t, string(rows[0]), "HELLO")
}

func TestNativeTerminalClassification(t *testing.T) {
	t.Parallel()
	for _, prompt := range []string{"PROVIDER_ERROR", "TURN_LIMIT", "DRIFT"} {
		t.Run(prompt, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.initialize()
			session := h.newSession()
			response, err := h.prompt(session.SessionId, prompt, nil)
			switch prompt {
			case "PROVIDER_ERROR":
				require.Equal(t, wire.CauseProvider, requestErrorData(t, err)["cause"])
			case "TURN_LIMIT":
				require.NoError(t, err)
				require.Equal(t, acp.StopReasonMaxTurnRequests, response.StopReason)
			case "DRIFT":
				require.Equal(t, wire.CauseTransport, requestErrorData(t, err)["cause"])
				_, err = h.prompt(session.SessionId, "next", nil)
				require.Equal(t, "amp_session_poisoned", requestErrorData(t, err)["error"])
				require.NotContains(t, agentText(h.rec.snapshot()), "wrong")
			}
		})
	}
}

func TestFailedActiveRestorePreservesCapturedGeneration(t *testing.T) {
	t.Parallel()
	store := &failingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	native := t.TempDir()
	h := newHarness(t, WithSessionStore(store), WithEnv(map[string]string{"ACP_GO_AMP_TEST_NATIVE": native, "GORACE": "atexit_sleep_ms=0"}))
	h.initialize()
	session := h.newSession()
	cwd := sessionCwd(t, h, session.SessionId)
	store.fail.Store(true)
	_, err := h.prompt(session.SessionId, "captured before failure", nil)
	require.Error(t, err)
	path := filepath.Join(native, string(session.SessionId)+".json")
	require.NoError(t, os.Remove(path))
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.Equal(t, "amp_restore_failed", requestErrorData(t, err)["error"])
	store.fail.Store(false)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.Contains(t, string(rows[0]), "captured before failure")
}

func TestResumeRotatesCarrier(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	session := h.newSession()
	cwd := sessionCwd(t, h, session.SessionId)
	dirs := []string{t.TempDir(), t.TempDir()}
	_, err := h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd, WithSessionAmpOptions(NewAmpOptions(WithAmpEnv(map[string]string{"PATH": "/usr/bin", "SESSION_VALUE": "rotated"}), WithAmpExtraPathDirs(dirs...)))))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "ENV", nil)
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "rotated")
	require.Contains(t, agentText(h.rec.snapshot()), dirs[0]+":"+dirs[1]+":/usr/bin")
}

// A prompt or a restore arriving while a turn is in flight is refused as busy,
// and the running turn still settles with end_turn.
func TestPromptAdmissionRefusesConcurrentWork(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	session := h.newSession()
	cwd := sessionCwd(t, h, session.SessionId)
	gate := filepath.Join(t.TempDir(), "release")
	type outcome struct {
		response acp.PromptResponse
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		response, err := h.prompt(session.SessionId, "GATE "+gate, nil)
		done <- outcome{response, err}
	}()
	waitForUserChunk(t, h, 0)
	_, err := h.prompt(session.SessionId, "competing", nil)
	require.Equal(t, "session_prompt", requestErrorData(t, err)["limit"])
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd))
	require.Equal(t, "session_restore", requestErrorData(t, err)["limit"])
	require.NoError(t, os.WriteFile(gate, nil, 0o600))
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Equal(t, acp.StopReasonEndTurn, result.response.StopReason)
	case <-time.After(testTimeout):
		t.Fatal("gated turn did not settle")
	}
}

func TestCancelEndsTheTurnWithACancelledIdle(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()
	type outcome struct {
		response acp.PromptResponse
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		response, err := h.prompt(session.SessionId, "SLOW", promptMeta(0))
		done <- outcome{response, err}
	}()
	waitForUserChunk(t, h, 0)
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Equal(t, acp.StopReasonCancelled, result.response.StopReason)
	case <-time.After(testTimeout):
		t.Fatal("cancelled turn did not join")
	}
	idle := map[string]any{}
	for _, notification := range h.rec.snapshot() {
		meta, ok := notification.Meta[wire.LifecycleKey].(map[string]any)
		if !ok {
			continue
		}
		if event, ok := meta["event"].(map[string]any); ok && event["state"] == "idle" {
			idle = event
		}
	}
	require.Equal(t, "state_update", idle["type"])
	require.Equal(t, "cancelled", idle["outcome"])
	require.Equal(t, "cancelled", idle["stopReason"])
}

// A close whose final commit fails reports the failure and still releases the
// session, leaving the stored generation loadable.
func TestFailedCloseReleasesTheSession(t *testing.T) {
	t.Parallel()
	store := &failingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()
	cwd := sessionCwd(t, h, session.SessionId)
	_, err := h.prompt(session.SessionId, "durable", nil)
	require.NoError(t, err)
	store.fail.Store(true)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.Equal(t, "amp_internal_failure", requestErrorData(t, err)["error"])
	store.fail.Store(false)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])
	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, list.Sessions, 1)
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "durable")
}

// waitForUserChunk blocks until the turn's own user message has been mirrored
// back, which happens only once the native process is running the turn.
func waitForUserChunk(t *testing.T, h *harness, after int) {
	t.Helper()
	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		for _, update := range updates[min(after, len(updates)):] {
			if update.Update.UserMessageChunk != nil {
				return true
			}
		}

		return false
	})
}

// A turn abandoned on native identity drift reaches prompt_accepted and
// publishes no terminal idle, because it attempts no commit.
func TestIdentityDriftEndsTheIncarnationWithoutATerminalIdle(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "DRIFT", promptMeta(0))
	require.Equal(t, wire.CauseTransport, requestErrorData(t, err)["cause"])
	kinds := lifecycleEventKinds(h.rec.snapshot())
	require.Contains(t, kinds, "prompt_accepted", "the turn never reached the lifecycle stream")
	require.NotContains(t, kinds, "state_update:idle", "a turn the store never received published a terminal idle")
}

// A failing mirror commit leaves the native provider cause in place instead of
// relabelling the turn, even when the turn emitted an image.
func TestCommitFailureKeepsTheNativeCause(t *testing.T) {
	t.Parallel()
	store := &failingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()
	store.fail.Store(true)
	_, err := h.prompt(session.SessionId, "IMAGE_PROVIDER_ERROR", nil)
	require.Equal(t, 1, toolImages(h.rec.snapshot()), "the turn emitted no image, so the storage verdict was never in reach")
	data := requestErrorData(t, err)
	require.Equal(t, wire.CauseProvider, data["cause"])
	require.Contains(t, data["message"], "provider fixture refusal")
	require.NotContains(t, data, "reason")
}

// A frame the adapter refuses fails the turn with a transport cause naming the
// frame, not with the killed child's exit status.
func TestStreamErrorOutranksTheExitStatus(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "NO_IDENTITY", nil)
	data := requestErrorData(t, err)
	require.Equal(t, wire.CauseTransport, data["cause"])
	require.Contains(t, data["message"], "native message lacks session identity")
}

// A resume of a live session turns raw events off when it omits the opt-in and
// back on when it carries it.
func TestRestoreAppliesTheRawEventOptIn(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	session := h.newSession(WithSessionRawEvents(true))
	cwd := sessionCwd(t, h, session.SessionId)
	_, err := h.prompt(session.SessionId, "first", nil)
	require.NoError(t, err)
	enabled := h.rec.rawEventCount()
	require.Positive(t, enabled)
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "second", nil)
	require.NoError(t, err)
	require.Equal(t, enabled, h.rec.rawEventCount(), "a restore that omitted the opt-in left raw events on")
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd, WithSessionRawEvents(true)))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "third", nil)
	require.NoError(t, err)
	require.Greater(t, h.rec.rawEventCount(), enabled, "a restore that carried the opt-in left raw events off")
}

// A seed file the adapter must not overwrite refuses the caller by naming
// seedFiles; a seed path it cannot write is a native-start internal failure.
func TestSeedFileFailuresSeparateTheRefusalFromTheInternalFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, root string)
		want   map[string]any
	}{
		{
			// Removing the adapter's ownership record leaves the seeded file
			// unmanaged, which is the state an operator-authored file has.
			name: "unmanaged target",
			damage: func(t *testing.T, root string) {
				t.Helper()
				require.NoError(t, os.Remove(filepath.Join(root, ".seed-manifest.json")))
			},
			want: map[string]any{wire.FieldError: "unsupported", wire.FieldField: "seedFiles"},
		},
		{
			name: "unwritable path",
			damage: func(t *testing.T, root string) {
				t.Helper()
				require.NoError(t, os.RemoveAll(filepath.Join(root, "nested")))
				require.NoError(t, os.WriteFile(filepath.Join(root, "nested"), nil, 0o600))
			},
			want: map[string]any{wire.FieldError: "amp_internal_failure", wire.FieldClass: internalClassNativeStart},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			config := t.TempDir()
			h := newHarness(t,
				WithSeedFiles(map[string]string{"nested/settings.json": "{}"}),
				WithEnv(map[string]string{"XDG_CONFIG_HOME": config, "ACP_GO_AMP_TEST_NATIVE": filepath.Join(t.TempDir(), "native"), "GORACE": "atexit_sleep_ms=0"}))
			h.initialize()
			session := h.newSession()
			root := filepath.Join(config, vendor)
			require.FileExists(t, filepath.Join(root, "nested", "settings.json"))
			tc.damage(t, root)
			_, err := h.prompt(session.SessionId, "seeded", nil)
			data := requestErrorData(t, err)
			for key, want := range tc.want {
				require.Equal(t, want, data[key])
			}
		})
	}
}

// A $/cancel_request ends only the addressed handler's context: the turn it
// was driving stays the session's, completes successfully once, and the
// session keeps serving prompts.
func TestCancelRequestSettlesTheOriginalRequestOnce(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	gate := filepath.Join(t.TempDir(), "release")
	request := wire.TextPromptRequest(session.SessionId, "GATE "+gate)
	request.Meta = promptMeta(1)
	failed := make(chan error, 1)

	go func() {
		response, err := h.conn.Prompt(h.ctx(), request)
		if err == nil && response.StopReason != acp.StopReasonEndTurn {
			err = errors.New("request cancellation ended the native turn")
		}
		failed <- err
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEventKinds(updates)) >= 3 })
	require.NoError(t, h.input.cancelPrompt())

	_, busyErr := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "backpressure", requestErrorData(t, busyErr)["error"])
	require.NoError(t, os.WriteFile(gate, nil, 0o600))
	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		return slices.Contains(lifecycleEventKinds(updates), "state_update:idle")
	})

	idles := 0

	for _, update := range h.rec.snapshot() {
		envelope, _ := update.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["type"] == "state_update" && event["state"] == "idle" {
			idles++
			require.Equal(t, "success", event["outcome"])
			require.Equal(t, string(acp.StopReasonEndTurn), event["stopReason"])
		}
	}

	require.Equal(t, 1, idles, "the turn the cancelled request started settles exactly once")
	require.NoError(t, <-failed)

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(3))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}
