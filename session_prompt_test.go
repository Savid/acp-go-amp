package ampacp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromptInputResourceBlocks(t *testing.T) {
	payload, err := promptInput([]acp.ContentBlock{
		acp.ResourceLinkBlock("notes.md", "file:///tmp/notes.md"),
		acp.ResourceBlock(acp.EmbeddedResourceResource{
			TextResourceContents: &acp.TextResourceContents{
				Text: "embedded notes",
				Uri:  "file:///tmp/embedded.md",
			},
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	message, ok := payload["message"].(map[string]any)
	if !ok {
		t.Fatalf("message=%T", payload["message"])
	}
	content, ok := message["content"].([]map[string]any)
	if !ok {
		t.Fatalf("content=%T", message["content"])
	}
	if len(content) != 2 {
		t.Fatalf("content blocks=%d", len(content))
	}
	linkText, ok := content[0]["text"].(string)
	if !ok {
		t.Fatalf("resource link text=%T", content[0]["text"])
	}
	if !strings.Contains(linkText, "file:///tmp/notes.md") {
		t.Fatalf("resource link text=%q", linkText)
	}
	embeddedText, ok := content[1]["text"].(string)
	if !ok {
		t.Fatalf("embedded text=%T", content[1]["text"])
	}
	if !strings.Contains(embeddedText, "embedded notes") {
		t.Fatalf("embedded text=%q", embeddedText)
	}
}

// parentTagOf extracts _meta.amp.parentToolUseId from whichever update variant
// is populated, reporting whether the tag is present.
func parentTagOf(t *testing.T, update acp.SessionUpdate) (string, bool) {
	t.Helper()

	var meta map[string]any

	switch {
	case update.UserMessageChunk != nil:
		meta = update.UserMessageChunk.Meta
	case update.AgentMessageChunk != nil:
		meta = update.AgentMessageChunk.Meta
	case update.AgentThoughtChunk != nil:
		meta = update.AgentThoughtChunk.Meta
	case update.ToolCall != nil:
		meta = update.ToolCall.Meta
	case update.ToolCallUpdate != nil:
		meta = update.ToolCallUpdate.Meta
	}

	ampMeta, ok := meta[ampMetaKey].(map[string]any)
	if !ok {
		return "", false
	}

	id, ok := ampMeta[metaParentToolUseIDKey].(string)

	return id, ok
}

// TestParentToolUseTagLiveGate confirms provenance is stamped only for live
// turns: a replayed frame (live=false) drops the id so replay stays untagged,
// and a live main-agent frame with no parent id yields no tag.
func TestParentToolUseTagLiveGate(t *testing.T) {
	assert.Equal(t, "toolu_1", parentToolUseTag("toolu_1", true), "live delegated frame keeps id")
	assert.Empty(t, parentToolUseTag("toolu_1", false), "replay drops id")
	assert.Empty(t, parentToolUseTag("", true), "live main-agent frame stays untagged")
	assert.Empty(t, parentToolUseTag("", false), "replayed main-agent frame stays untagged")
}

// TestTagParentToolUseAllUpdateKinds proves every frame-derived update variant
// amp can emit carries _meta.amp.parentToolUseId when the source frame carried a
// non-empty parent_tool_use_id.
func TestTagParentToolUseAllUpdateKinds(t *testing.T) {
	const parent = "toolu_parent"

	kinds := map[string]acp.SessionUpdate{
		"userMessageChunk":  acp.UpdateUserMessageText("delegated user"),
		"agentMessageChunk": acp.UpdateAgentMessageText("delegated agent"),
		"agentThoughtChunk": acp.UpdateAgentThoughtText("delegated thought"),
		"toolCall": {ToolCall: &acp.SessionUpdateToolCall{
			ToolCallId: "TU",
			Title:      "Read",
			Status:     acp.ToolCallStatusPending,
		}},
		"toolCallUpdate": {ToolCallUpdate: &acp.SessionToolCallUpdate{
			ToolCallId: "TU",
		}},
	}

	for name, update := range kinds {
		t.Run(name, func(t *testing.T) {
			tagged := tagParentToolUse(update, parent)
			id, ok := parentTagOf(t, tagged)
			require.True(t, ok, "%s must carry the provenance tag", name)
			assert.Equal(t, parent, id)
		})
	}
}

// TestTagParentToolUseUntaggedForMainAgent confirms an empty id leaves every
// update variant untouched, so main-agent activity is never tagged.
func TestTagParentToolUseUntaggedForMainAgent(t *testing.T) {
	updates := []acp.SessionUpdate{
		acp.UpdateUserMessageText("main user"),
		acp.UpdateAgentMessageText("main agent"),
		acp.UpdateAgentThoughtText("main thought"),
		{ToolCall: &acp.SessionUpdateToolCall{ToolCallId: "TU", Title: "Read"}},
		{ToolCallUpdate: &acp.SessionToolCallUpdate{ToolCallId: "TU"}},
	}

	for _, update := range updates {
		out := tagParentToolUse(update, "")
		_, ok := parentTagOf(t, out)
		assert.False(t, ok, "empty id must not tag %#v", update)
	}
}

// TestWithParentToolUseMetaPreservesSiblingKeys confirms the provenance tag is
// merged into an existing _meta / _meta.amp block without disturbing siblings.
func TestWithParentToolUseMetaPreservesSiblingKeys(t *testing.T) {
	existing := map[string]any{
		"hostMeta": "abc",
		ampMetaKey: map[string]any{
			"serviceTier": "priority",
		},
	}

	merged := withParentToolUseMeta(existing, "toolu_9")

	assert.Equal(t, "abc", merged["hostMeta"], "sibling _meta key preserved")

	ampMeta, ok := merged[ampMetaKey].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "priority", ampMeta["serviceTier"], "sibling _meta.amp key preserved")
	assert.Equal(t, "toolu_9", ampMeta[metaParentToolUseIDKey], "provenance tag added")
}

// TestEmitMessageTagsDelegatedFrames drives emitMessage end-to-end over a real
// connection and asserts the four update kinds amp produces from delegated
// frames all carry _meta.amp.parentToolUseId, while identical main-agent frames
// stay untagged.
func TestEmitMessageTagsDelegatedFrames(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	session := &agentSession{agent: agent, id: "T-parent"}

	const parent = "toolu_spawn"

	frames := []amp.Message{
		&amp.UserMessage{ParentToolUseID: parent, Content: []amp.ContentBlock{amp.TextBlock{Text: "delegated user"}}},
		&amp.UserMessage{ParentToolUseID: parent, Content: []amp.ContentBlock{amp.ToolResultBlock{ToolUseID: "TU1", Content: "out"}}},
		&amp.AssistantMessage{ParentToolUseID: parent, Content: []amp.ContentBlock{amp.TextBlock{Text: "delegated agent"}}},
		&amp.AssistantMessage{ParentToolUseID: parent, Content: []amp.ContentBlock{amp.ToolUseBlock{ID: "TU2", Name: "Read"}}},
		// Main-agent frames (no parent) interleaved to prove they stay untagged.
		&amp.UserMessage{Content: []amp.ContentBlock{amp.TextBlock{Text: "main user"}}},
		&amp.AssistantMessage{Content: []amp.ContentBlock{amp.ToolUseBlock{ID: "TU3", Name: "Grep"}}},
	}

	for _, frame := range frames {
		require.NoError(t, session.emitMessage(ctx, frame, true))
	}

	waitForRecorded(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()

		return len(client.updates) >= len(frames)
	})

	client.mu.Lock()
	updates := append([]acp.SessionNotification(nil), client.updates...)
	client.mu.Unlock()

	tagged := map[string]string{}
	untagged := 0

	for _, notification := range updates {
		id, ok := parentTagOf(t, notification.Update)
		if !ok {
			untagged++

			continue
		}

		assert.Equal(t, parent, id)

		switch {
		case notification.Update.UserMessageChunk != nil:
			tagged["userMessageChunk"] = id
		case notification.Update.AgentMessageChunk != nil:
			tagged["agentMessageChunk"] = id
		case notification.Update.ToolCall != nil:
			tagged["toolCall"] = id
		case notification.Update.ToolCallUpdate != nil:
			tagged["toolCallUpdate"] = id
		}
	}

	assert.Contains(t, tagged, "userMessageChunk")
	assert.Contains(t, tagged, "agentMessageChunk")
	assert.Contains(t, tagged, "toolCall")
	assert.Contains(t, tagged, "toolCallUpdate")
	assert.Equal(t, 2, untagged, "the two main-agent frames stay untagged")
}

// TestEmitMessageReplayNeverTags confirms replay (live=false) never stamps
// provenance even when the stored frame carries parent_tool_use_id, keeping
// session/load replay semantics unchanged.
func TestEmitMessageReplayNeverTags(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	session := &agentSession{agent: agent, id: "T-replay"}

	frames := []amp.Message{
		&amp.UserMessage{ParentToolUseID: "toolu_x", Content: []amp.ContentBlock{amp.TextBlock{Text: "delegated user"}}},
		&amp.AssistantMessage{ParentToolUseID: "toolu_x", Content: []amp.ContentBlock{amp.ToolUseBlock{ID: "TU", Name: "Read"}}},
	}

	for _, frame := range frames {
		require.NoError(t, session.emitMessage(ctx, frame, false))
	}

	waitForRecorded(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()

		return len(client.updates) >= len(frames)
	})

	client.mu.Lock()
	updates := append([]acp.SessionNotification(nil), client.updates...)
	client.mu.Unlock()

	for _, notification := range updates {
		_, ok := parentTagOf(t, notification.Update)
		assert.False(t, ok, "replayed frame must not carry provenance tag")
	}
}

// requireTurnFailure pins the uniform native-turn-failure shape: JSON-RPC -32603
// with data {error:"amp_turn_failed", cause:<class>, message:<real cause>}. The
// message must carry the real native cause, never a fixed placeholder or bare
// "EOF".
func requireTurnFailure(t *testing.T, err error, wantCause, wantMessageSubstr string) {
	t.Helper()
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error = %T %v, want RequestError", err, err)
	}
	if reqErr.Code != -32603 {
		t.Fatalf("code = %d, want -32603 (%v)", reqErr.Code, err)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %#v, want map", reqErr.Data)
	}
	if data[jsonFieldError] != turnFailedError {
		t.Fatalf("data.error = %#v, want %q", data[jsonFieldError], turnFailedError)
	}
	if data["cause"] != wantCause {
		t.Fatalf("data.cause = %#v, want %q", data["cause"], wantCause)
	}
	message, _ := data["message"].(string)
	if message == "" || message == "EOF" {
		t.Fatalf("data.message must be a real cause, got %q", message)
	}
	if !strings.Contains(message, wantMessageSubstr) {
		t.Fatalf("data.message = %q, want substring %q", message, wantMessageSubstr)
	}
}

// T1: a provider error inside the harness terminates session/prompt with the
// uniform failure error (cause "provider"), never a PromptResponse and never
// end_turn.
func TestTurnFailureProviderError(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		mode string
		want string
	}{
		{name: "generic", mode: "result-error", want: "native failed"},
		{name: "auth", mode: "provider-auth-error", want: "invalid API key"},
		{name: "rate limit", mode: "provider-rate-error", want: "429 too many requests"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := fakeAgentAmpPath(t, tc.mode)
			agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
			resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
			if promptErr == nil {
				t.Fatalf("provider error returned success: %#v", promptResp)
			}
			if promptResp.StopReason == acp.StopReasonEndTurn {
				t.Fatalf("provider failure reported as end_turn")
			}
			requireTurnFailure(t, promptErr, causeProvider, tc.want)
		})
	}
}

// L1: when result.error is empty the real cause is recovered from result.result.
func TestTurnFailureFallsBackToResultField(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "result-only-in-result")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
	requireTurnFailure(t, promptErr, causeProvider, "failure carried in result field")
}

// T2: a transport failure mid-turn surfaces the real cause, never a bare "EOF"
// or a fixed placeholder string.
func TestTurnFailureTransportRecoversCause(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		mode string
		want string
	}{
		{name: "stream ended", mode: "no-result", want: "stream ended without result"},
		{name: "malformed line", mode: "malformed-only", want: "decode amp json line"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := fakeAgentAmpPath(t, tc.mode)
			agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
			resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
			requireTurnFailure(t, promptErr, causeTransport, tc.want)
		})
	}
}

// T3: a non-zero process exit mid-turn surfaces cause "process_exit" with the
// exit/stderr cause, and the session stays addressable and retriable.
func TestTurnFailureProcessDeathIsRetriable(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "delayed-error")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
	requireTurnFailure(t, promptErr, causeProcessExit, "delayed failure")

	// The session is neither removed nor poisoned: it re-drives the native turn.
	if _, sessionErr := agent.session(resp.SessionId); sessionErr != nil {
		t.Fatalf("session removed after process death: %v", sessionErr)
	}
	_, retryErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "again"))
	requireTurnFailure(t, retryErr, causeProcessExit, "delayed failure")
}

// T4: a single malformed native line is a structured transport failure, never a
// process-exit misclassification and never a silent hang; the session survives.
func TestTurnFailureMalformedLineNotFatal(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "malformed-only")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
	requireTurnFailure(t, promptErr, causeTransport, "decode amp json line")

	if _, sessionErr := agent.session(resp.SessionId); sessionErr != nil {
		t.Fatalf("session torn down by malformed line: %v", sessionErr)
	}
	_, retryErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "again"))
	requireTurnFailure(t, retryErr, causeTransport, "decode amp json line")
}

// T5: a cancel delivered while the harness is failing yields StopReason
// cancelled with a nil error; the native failure is suppressed.
func TestTurnFailureCancelNotConflated(t *testing.T) {
	ctx := context.Background()
	path, state := fakeAgentAmpPath(t, "delayed-error")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
		resultCh <- promptResp
		errCh <- promptErr
	}()
	waitForPath(t, filepath.Join(state, "continue-ready"))
	if cancelErr := agent.Cancel(ctx, acp.CancelNotification{SessionId: resp.SessionId}); cancelErr != nil {
		t.Fatalf("cancel: %v", cancelErr)
	}
	select {
	case promptErr := <-errCh:
		promptResp := <-resultCh
		if promptErr != nil || promptResp.StopReason != acp.StopReasonCancelled {
			t.Fatalf("cancel conflated with failure: resp=%#v err=%v", promptResp, promptErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled prompt did not return")
	}
}

// T6: with WithTurnTimeout, a silent-hang harness yields cause "timeout" (a
// failure), never cancelled, and the prompt returns rather than hanging.
func TestTurnFailureTimeout(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "hang")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithTurnTimeout(150*time.Millisecond))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
		resultCh <- promptResp
		errCh <- promptErr
	}()
	select {
	case promptErr := <-errCh:
		promptResp := <-resultCh
		if promptResp.StopReason == acp.StopReasonCancelled {
			t.Fatalf("timeout reported as cancelled: %#v", promptResp)
		}
		requireTurnFailure(t, promptErr, causeTimeout, "WithTurnTimeout")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout prompt did not return")
	}
}

// R5-2: when a session/cancel and the WithTurnTimeout deadline land in the same
// scheduling quantum, the cancel guard wins deterministically: the turn always
// resolves as a cancelled PromptResponse with a nil error, never the cause
// "timeout" failure, and it resolves exactly once (no double-send). The turn
// deadline is driven through the newTurnTimer seam so both the cancel signal and
// the fired timeout are guaranteed ready before the prompt loop's select
// observes either one, forcing the random select tie-break over many iterations.
func TestTurnFailureCancelWinsOnTimeoutCoincidence(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "hang")
	agent := NewAgent(WithExecutablePath(path), WithScratchDir(t.TempDir()), WithTurnTimeout(time.Hour))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	session := agent.sessions[resp.SessionId]
	if session == nil {
		t.Fatal("session not tracked")
	}

	const iterations = 40
	for i := 0; i < iterations; i++ {
		timeoutC := make(chan time.Time, 1)
		created := make(chan struct{})
		release := make(chan struct{})
		agent.options.runtime.newTurnTimer = func(time.Duration) (<-chan time.Time, func()) {
			close(created)
			<-release

			return timeoutC, func() {}
		}

		resultCh := make(chan acp.PromptResponse, 1)
		errCh := make(chan error, 1)
		go func() {
			promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "x"))
			resultCh <- promptResp
			errCh <- promptErr
		}()

		// The prompt goroutine is parked in the timer seam, past setup but before
		// the loop's select. Arm both signals, then release it so the select sees
		// the cancel and the fired deadline ready at once.
		<-created
		state := session.activePromptState()
		if state == nil {
			t.Fatalf("iter %d: no active prompt state", i)
		}
		state.cancel()
		timeoutC <- time.Now()
		close(release)

		select {
		case promptErr := <-errCh:
			promptResp := <-resultCh
			if promptErr != nil {
				t.Fatalf("iter %d: coincident cancel+timeout returned failure error: %v", i, promptErr)
			}
			if promptResp.StopReason != acp.StopReasonCancelled {
				t.Fatalf("iter %d: stop reason = %q, want cancelled", i, promptResp.StopReason)
			}
			// Exactly one resolution reached the channels; a stray second send
			// would block the goroutine and leave these non-empty.
			if len(errCh) != 0 || len(resultCh) != 0 {
				t.Fatalf("iter %d: turn resolved more than once", i)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("iter %d: coincident cancel+timeout did not return", i)
		}
	}
}

// TestFirstNonEmpty covers the local cause-selection helper, including the
// all-empty fallthrough.
func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("", " ", "value"); got != "value" {
		t.Fatalf("firstNonEmpty picked %q", got)
	}
	if got := firstNonEmpty("first", "second"); got != "first" {
		t.Fatalf("firstNonEmpty picked %q", got)
	}
	if got := firstNonEmpty("", "  "); got != "" {
		t.Fatalf("firstNonEmpty all-empty = %q", got)
	}
}

func TestPromptResultForObserver(t *testing.T) {
	if got := promptResultForObserver(acp.PromptResponse{StopReason: acp.StopReasonCancelled}, errors.New("boom"), "amp-model"); got.Err == nil || got.Model != "amp-model" || got.StopReason != string(acp.StopReasonCancelled) {
		t.Fatalf("no-usage result = %#v", got)
	}

	resp := acp.PromptResponse{
		StopReason: acp.StopReasonEndTurn,
		Usage: &acp.Usage{
			InputTokens:       11,
			OutputTokens:      22,
			TotalTokens:       33,
			CachedReadTokens:  acp.Ptr(4),
			CachedWriteTokens: acp.Ptr(5),
			ThoughtTokens:     acp.Ptr(6),
		},
	}
	got := promptResultForObserver(resp, nil, "")
	if got.InputTokens != 11 || got.OutputTokens != 22 || got.TotalTokens != 33 {
		t.Fatalf("token totals = %#v", got)
	}
	if got.CachedReadTokens != 4 || got.CachedWriteTokens != 5 || got.ThoughtTokens != 6 {
		t.Fatalf("cache/thought tokens = %#v", got)
	}
}

func usageUpdates(client *recordingClient) []*acp.SessionUsageUpdate {
	updates := make([]*acp.SessionUsageUpdate, 0)
	for _, notification := range client.updatesSnapshot() {
		if notification.Update.UsageUpdate != nil {
			updates = append(updates, notification.Update.UsageUpdate)
		}
	}

	return updates
}

// TestUsageUpdateSizeIsContextWindowNotUsed pins the usage_update.size contract:
// size is amp's context-window (usage.max_tokens), it is never the summed used
// tokens, and it is 0 (unknown) when amp omits the field. This regression fails
// if size were ever fabricated from `used` or dropped.
func TestUsageUpdateSizeIsContextWindowNotUsed(t *testing.T) {
	ctx := context.Background()
	agent := NewAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()
	session := &agentSession{agent: agent, id: "T-usage"}

	if err := session.emitUsage(ctx, &amp.Usage{InputTokens: 13, OutputTokens: 17, MaxTokens: 300}); err != nil {
		t.Fatalf("emitUsage with window: %v", err)
	}
	waitForRecorded(t, func() bool { return len(usageUpdates(client)) == 1 })
	updates := usageUpdates(client)
	if updates[0].Used != 30 {
		t.Fatalf("used = %d, want 30", updates[0].Used)
	}
	if updates[0].Size != 300 {
		t.Fatalf("size = %d, want 300 (context window from max_tokens)", updates[0].Size)
	}
	if updates[0].Size == updates[0].Used {
		t.Fatalf("size must never equal used: %d", updates[0].Size)
	}

	if err := session.emitUsage(ctx, &amp.Usage{InputTokens: 5, OutputTokens: 5}); err != nil {
		t.Fatalf("emitUsage without window: %v", err)
	}
	waitForRecorded(t, func() bool { return len(usageUpdates(client)) == 2 })
	updates = usageUpdates(client)
	if updates[1].Used != 10 {
		t.Fatalf("used = %d, want 10", updates[1].Used)
	}
	if updates[1].Size != 0 {
		t.Fatalf("unknown size = %d, want 0 (never fabricated)", updates[1].Size)
	}
}

// TestPromptInputFailClosedShapes pins the fail-closed prompt-content rules:
// an empty prompt and a data-less image block are rejected -32602 with the
// uniform data shapes.
func TestPromptInputFailClosedShapes(t *testing.T) {
	_, err := promptInput(nil)
	requireInvalidParamsData(t, err, map[string]any{jsonFieldError: valUnsupported, jsonFieldField: fieldPrompt})

	_, err = promptInput([]acp.ContentBlock{acp.ImageBlock("", "image/png")})
	requireInvalidParamsData(t, err, map[string]any{jsonFieldField: "prompt.image", jsonFieldError: "missing image data or uri"})

	// An image with data still forwards as base64 source content.
	input, err := promptInput([]acp.ContentBlock{acp.ImageBlock("aGk=", "image/png")})
	if err != nil {
		t.Fatalf("image prompt input: %v", err)
	}
	if input == nil {
		t.Fatal("image prompt input empty")
	}
}

// TestEmitRawEventNilPayloadSkipsSequence pins the nil-payload guard: a frame
// with no native payload is skipped without consuming a sequence, so the next
// real event still starts at 1.
func TestEmitRawEventNilPayloadSkipsSequence(t *testing.T) {
	agent := NewAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	session := &agentSession{agent: agent, id: "T-nil-raw", rawEvents: true}
	if err := session.emitRawEvent(context.Background(), "stream-json", fakeAmpMessage{raw: nil}); err != nil {
		t.Fatalf("nil payload emit = %v", err)
	}
	if got := session.rawEventSeq.Load(); got != 0 {
		t.Fatalf("nil payload consumed sequence %d", got)
	}

	if err := session.emitRawEvent(context.Background(), "stream-json", fakeAmpMessage{raw: map[string]any{"type": "x"}}); err != nil {
		t.Fatalf("real payload emit = %v", err)
	}
	waitForRecorded(t, func() bool { return len(client.rawSnapshot()) == 1 })
	events := decodeRawEvents(t, client.rawSnapshot())
	if len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("first emitted sequence = %#v", events)
	}
}
