package ampacp

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestIdenticalStaleExportsCannotPublishDone(t *testing.T) {
	t.Parallel()
	native := t.TempDir()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store), WithEnv(map[string]string{"ACP_GO_AMP_TEST_NATIVE": native, "GORACE": "atexit_sleep_ms=0"}))
	h.initialize(withLifecycle())
	session := h.newSession()
	before, err := store.Load(t.Context(), string(session.SessionId))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, promptErr := h.prompt(session.SessionId, "STALE_EXPORT", promptMeta(1)); done <- promptErr }()
	stale := filepath.Join(native, string(session.SessionId)+".stale")
	require.Eventually(t, func() bool {
		data, _ := os.ReadFile(stale + ".reads")

		return bytes.Count(data, []byte("read\n")) >= 2
	}, 10*time.Second, 10*time.Millisecond)
	after, err := store.Load(t.Context(), string(session.SessionId))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NotContains(t, lifecycleEventKinds(h.rec.snapshot()), "state_update:idle")
	select {
	case promptErr := <-done:
		t.Fatalf("stale export ended prompt: %v", promptErr)
	default:
	}
	require.NoError(t, os.Remove(stale))
	require.NoError(t, <-done)
	require.Contains(t, lifecycleEventKinds(h.rec.snapshot()), "state_update:idle")
	after, err = store.Load(t.Context(), string(session.SessionId))
	require.NoError(t, err)
	require.NotEqual(t, before, after)
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, sessionCwd(t, h, session.SessionId)))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "hello STALE_EXPORT")
}

func TestDisconnectReconcilesRemoteStateBeforeAnotherPrompt(t *testing.T) {
	t.Parallel()
	for _, running := range []bool{false, true} {
		t.Run(map[bool]string{false: "remote completed", true: "remote still running"}[running], func(t *testing.T) {
			t.Parallel()
			native := t.TempDir()
			store := acpcore.NewInMemorySessionStore()
			h := newHarness(t, WithSessionStore(store), WithEnv(map[string]string{"ACP_GO_AMP_TEST_NATIVE": native, "GORACE": "atexit_sleep_ms=0"}))
			h.initialize(withLifecycle())
			session := h.newSession()
			before, err := store.Load(t.Context(), string(session.SessionId))
			require.NoError(t, err)
			prompt := "DETACH_DONE"
			if running {
				prompt = "DETACH_RUNNING"
			}
			_, err = h.prompt(session.SessionId, prompt, promptMeta(1))
			require.Equal(t, wire.CauseProcessExit, requestErrorData(t, err)["cause"])
			after, err := store.Load(t.Context(), string(session.SessionId))
			require.NoError(t, err)
			path := filepath.Join(native, string(session.SessionId)+".json")
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			var snapshot nativeSnapshot
			require.NoError(t, json.Unmarshal(data, &snapshot))
			if running {
				require.Equal(t, before, after)
				require.NotContains(t, lifecycleEventKinds(h.rec.snapshot()), "state_update:idle")
				_, err = h.prompt(session.SessionId, "must not be submitted", promptMeta(2))
				require.Error(t, err)
				unchanged, readErr := os.ReadFile(path)
				require.NoError(t, readErr)
				require.Equal(t, data, unchanged)
				_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, sessionCwd(t, h, session.SessionId)))
				require.Equal(t, "amp_restore_failed", requestErrorData(t, err)["error"])
				snapshot.Messages = append(snapshot.Messages, nativeMessage{ID: 2, Role: roleAssistant, Content: []map[string]any{{"type": "text", "text": "remote completed after disconnect"}}, State: map[string]any{"type": "complete"}})
				fakeSave(native, snapshot)
				require.NoError(t, os.Remove(filepath.Join(native, string(session.SessionId)+".state")))
			} else {
				require.NotEqual(t, before, after)
				require.NotContains(t, agentText(h.rec.snapshot()), "hello DETACH_DONE")
				require.Contains(t, lifecycleEventKinds(h.rec.snapshot()), "state_update:idle")
			}
			_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, sessionCwd(t, h, session.SessionId)))
			require.NoError(t, err)
			require.Contains(t, agentText(h.rec.snapshot()), snapshot.Messages[len(snapshot.Messages)-1].Content[len(snapshot.Messages[len(snapshot.Messages)-1].Content)-1]["text"])
			response, err := h.prompt(session.SessionId, "next", promptMeta(3))
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
		})
	}
}

func TestNativeReceiptControlsCancellationVerdict(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, status string
		requested    bool
		want         acp.StopReason
		failure      bool
	}{
		{"native cancellation", "cancelled", true, acp.StopReasonCancelled, false},
		{"external cancellation", "cancelled", false, acp.StopReasonCancelled, false},
		{"native completed before cancel", "done", true, acp.StopReasonEndTurn, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			turn := &turn{evidence: amp.Outcome{Receipt: &amp.Receipt{Status: tc.status}, Stopped: true}, state: cycleState{terminal: true, failed: tc.status != amp.StatusDone, errorMessage: "User cancelled (SIGINT/SIGTERM)"}}
			reason, err := turnVerdict(turn, process.Result{}, "", nil, tc.requested)
			require.Equal(t, tc.want, reason)
			if tc.failure {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestMissingCompletionEvidenceFailsThePrompt(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		receipt          *amp.Receipt
		terminal, failed bool
	}{
		{"result without receipt", nil, true, false},
		{"receipt without result", &amp.Receipt{Status: amp.StatusDone}, false, false},
		{"stream failed after remote completion", &amp.Receipt{Status: amp.StatusDone}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			current := &turn{evidence: amp.Outcome{Receipt: tc.receipt, Stopped: true}, state: cycleState{terminal: tc.terminal, failed: tc.failed}}
			_, err := turnVerdict(current, process.Result{}, "", nil, false)
			require.Equal(t, wire.CauseTransport, requestErrorData(t, err)["cause"])
		})
	}
}

// A prompt that makes Amp compact the thread settles: the export carries the
// summary the plugin view omits, the mirror keeps it, and later prompts and
// restores read past it.
func TestCompactionSummaryOutsideThePluginViewSettlesTheTurn(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize(withLifecycle())
	session := h.newSession()
	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "COMPACT", promptMeta(2))
	require.NoError(t, err, "the compacting turn settles against the export's summary")
	require.Contains(t, lifecycleEventKinds(h.rec.snapshot()), "state_update:idle")
	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	snapshot, err := decodeSnapshot(rows[0], string(session.SessionId))
	require.NoError(t, err)
	roles := make([]string, 0, len(snapshot.Messages))
	for _, message := range snapshot.Messages {
		roles = append(roles, message.Role)
	}
	require.Equal(t, []string{roleUser, roleAssistant, roleInfo, roleUser, roleAssistant}, roles, "the mirror preserves the compaction summary in place")
	_, err = h.prompt(session.SessionId, "AGAIN", promptMeta(3))
	require.NoError(t, err, "a mirror holding a summary still matches a plugin view without one")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, sessionCwd(t, h, session.SessionId)))
	require.NoError(t, err)
	text := agentText(h.rec.snapshot())
	require.Contains(t, text, "hello COMPACT")
	require.Contains(t, text, "hello AGAIN")
	require.NotContains(t, text, "earlier turns summarized", "the summary is native context, not conversation")
}
