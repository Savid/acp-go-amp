package ampacp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validPNGBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="

func TestPromptInputResourceBlocks(t *testing.T) {
	payload, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
		acp.ResourceLinkBlock("notes.md", "file:///tmp/notes.md"),
		acp.ResourceBlock(acp.EmbeddedResourceResource{
			TextResourceContents: &acp.TextResourceContents{
				Text: "embedded notes",
				Uri:  "file:///tmp/embedded.md",
			},
		}),
	}, defaultPolicy())
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

// TestParentToolUseTagPreservesFrameIdentity confirms provenance is a
// deterministic property of the native frame, so both live mapping and replay
// can stamp it without adapter-owned state.
func TestParentToolUseTagPreservesFrameIdentity(t *testing.T) {
	assert.Equal(t, "toolu_1", parentToolUseTag("toolu_1"))
	assert.Empty(t, parentToolUseTag(""), "main-agent frames stay untagged")
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
	agent := newTestAgent()
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
		require.NoError(t, session.emitMessage(ctx, frame, true, ""))
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

// TestEmitMessageReplayPreservesDelegatedTags confirms replay derives the same
// provenance from the byte-verbatim native frame as the live turn.
func TestEmitMessageReplayPreservesDelegatedTags(t *testing.T) {
	ctx := context.Background()
	agent := newTestAgent()
	client, cleanup := attachRecordingClient(t, agent)
	defer cleanup()

	session := &agentSession{agent: agent, id: "T-replay"}

	frames := []amp.Message{
		&amp.UserMessage{ParentToolUseID: "toolu_x", Content: []amp.ContentBlock{amp.TextBlock{Text: "delegated user"}}},
		&amp.AssistantMessage{ParentToolUseID: "toolu_x", Content: []amp.ContentBlock{amp.ToolUseBlock{ID: "TU", Name: "Read"}}},
	}

	for _, frame := range frames {
		require.NoError(t, session.emitMessage(ctx, frame, false, ""))
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
		id, ok := parentTagOf(t, notification.Update)
		assert.True(t, ok, "replayed delegated frame must carry provenance tag")
		assert.Equal(t, "toolu_x", id)
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
			agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
			resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x"))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x"))
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
			agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
			resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x"))
			requireTurnFailure(t, promptErr, causeTransport, tc.want)
		})
	}
}

// T3: a non-zero process exit mid-turn surfaces cause "process_exit" with the
// exit/stderr cause, and the session stays addressable and retriable.
func TestTurnFailureProcessDeathIsRetriable(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "delayed-error")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x"))
	requireTurnFailure(t, promptErr, causeProcessExit, "delayed failure")

	// The session is neither removed nor poisoned: it re-drives the native turn.
	if _, sessionErr := agent.session(resp.SessionId); sessionErr != nil {
		t.Fatalf("session removed after process death: %v", sessionErr)
	}
	_, retryErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "again"))
	requireTurnFailure(t, retryErr, causeProcessExit, "delayed failure")
}

// T4: a single malformed native line is a structured transport failure, never a
// process-exit misclassification and never a silent hang; the session survives.
func TestTurnFailureMalformedLineNotFatal(t *testing.T) {
	ctx := context.Background()
	path, _ := fakeAgentAmpPath(t, "malformed-only")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	_, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x"))
	requireTurnFailure(t, promptErr, causeTransport, "decode amp json line")

	if _, sessionErr := agent.session(resp.SessionId); sessionErr != nil {
		t.Fatalf("session torn down by malformed line: %v", sessionErr)
	}
	_, retryErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "again"))
	requireTurnFailure(t, retryErr, causeTransport, "decode amp json line")
}

// T5: a cancel delivered while the harness is failing yields StopReason
// cancelled with a nil error; the native failure is suppressed.
func TestTurnFailureCancelNotConflated(t *testing.T) {
	ctx := context.Background()
	path, state := fakeAgentAmpPath(t, "delayed-error")
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x"))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithTurnTimeout(150*time.Millisecond))
	agent.options.runtime.nativeCancelTimeout = 50 * time.Millisecond
	resp, err := agent.NewSession(ctx, NewSessionRequest(t.TempDir()))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resultCh := make(chan acp.PromptResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x"))
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
	agent := newTestAgent(WithExecutablePath(path), WithScratchDir(testScratchDir(t)), WithTurnTimeout(time.Hour))
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
			promptResp, promptErr := agent.Prompt(ctx, TextPromptRequest(resp.SessionId, "test-turn", "x"))
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
	agent := newTestAgent()
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
	_, err := promptInputWithPolicy(t.Context(), nil, defaultPolicy())
	requireInvalidParamsData(t, err, map[string]any{jsonFieldError: valUnsupported, jsonFieldField: fieldPrompt})

	_, err = promptInputWithPolicy(t.Context(), []acp.ContentBlock{acp.ImageBlock("", "image/png")}, defaultPolicy())
	requireInvalidParamsData(t, err, map[string]any{
		jsonFieldField: "prompt.image",
		jsonFieldError: imageErrorMissingData,
		"index":        0,
	})

	// An image with data still forwards as base64 source content.
	input, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{acp.ImageBlock(validPNGBase64, "image/png")}, defaultPolicy())
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
	agent := newTestAgent()
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

// gatedStore observes and holds durable commits. Replace announces itself on
// started and waits for release, so a test can hold a settlement open and drive
// a concurrent close or delete against the exact window the races live in.
type gatedStore struct {
	SessionStore

	mu       sync.Mutex
	started  chan struct{}
	release  chan struct{}
	gated    bool
	replaces int
	failWith error
	record   func(string)
}

func newGatedStore(record func(string)) *gatedStore {
	return &gatedStore{
		SessionStore: NewInMemorySessionStore(),
		started:      make(chan struct{}, 8),
		release:      make(chan struct{}),
		record:       record,
	}
}

// gate arms the hold. The early adoption commit lands before it, so a test gates
// only the settlement commit it means to hold.
func (s *gatedStore) gate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gated = true
}

// heal disarms both the hold and the outage. Agent shutdown carries the same
// durable rung a wire close does, so a scenario that left the store broken would
// have its cleanup either park on the gate or answer with the outage — reporting
// the scenario's own store defect as a shutdown failure. The scenario is over by
// then; the frames it retained are what shutdown is there to land.
func (s *gatedStore) heal() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gated = false
	s.failWith = nil
}

func (s *gatedStore) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failWith = err
}

func (s *gatedStore) replaceCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.replaces
}

func (s *gatedStore) Replace(ctx context.Context, key SessionKey, replacements []SessionStoreReplacement) error {
	s.mu.Lock()
	gated, failWith := s.gated, s.failWith
	s.replaces++
	s.mu.Unlock()

	if s.record != nil {
		s.record("commit")
	}

	if gated {
		s.started <- struct{}{}
		<-s.release
	}

	if failWith != nil {
		return failWith
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}

// settlementLedger records the order settlement steps complete in, so ordering
// is observed rather than asserted.
type settlementLedger struct {
	mu    sync.Mutex
	steps []string
}

func (l *settlementLedger) record(step string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.steps = append(l.steps, step)
}

func (l *settlementLedger) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.steps...)
}

// settlementAgent opens an agent whose store is gated and whose client records
// the terminal idle into the same ledger the commits write to.
func settlementAgent(t *testing.T, ledger *settlementLedger) (*Agent, *gatedStore, *orderingClient, acp.SessionId) {
	t.Helper()
	t.Setenv("AMP_API_KEY", "conformance-key")

	store := newGatedStore(ledger.record)
	client := &orderingClient{record: ledger.record}

	agent := NewAgent(testContainmentOptions([]Option{
		WithExecutablePath(lifecycleHarness(t)),
		WithScratchDir(testScratchDir(t)),
		WithSessionStore(store),
	})...)
	t.Cleanup(func() {
		store.heal()
		require.NoError(t, agent.Close())
	})

	agent.setConnection(client)

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{Meta: lifecycleOffer(1.0)})
	require.NoError(t, err)

	session, err := agent.NewSession(t.Context(), NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	// The ledger starts at the prompt: session creation commits its own manifest
	// generation before any turn exists.
	ledger.mu.Lock()
	ledger.steps = nil
	ledger.mu.Unlock()

	return agent, store, client, session.SessionId
}

// TestSettlementSurvivesRequestCancellation pins that settlement is detached
// from the request context. The store fails a cancelled context immediately, so
// a settlement that reused the request's would lose the commit and the terminal
// boundary while telling the host the turn ended cleanly.
func TestSettlementSurvivesRequestCancellation(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client.onAgentChunk = cancel

	resp, err := agent.Prompt(ctx, lifecyclePrompt(sessionID, "hang", "sub-1", "nonce-1"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonCancelled, resp.StopReason)

	stored, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.NotEmpty(t, stored, "a cancelled request still commits the frames it streamed")

	require.Equal(t, []string{"commit", "commit", "idle"}, ledger.snapshot())

	state := reduceEmittedStream(t, &client.lifecycleClient, negotiatedAnswer()).State()
	require.Len(t, state.Turns, 1)
	require.True(t, state.Turns[0].Terminal)
	require.Equal(t, lifecycle.OutcomeCancelled, state.Turns[0].Outcome)
}

// TestSettlementFailureIsNotACancelledSuccess pins that the cancelled-success
// answer is owed only to a prompt that settled. A commit the store refused is
// the prompt failing, and the host reads that failure rather than a clean cancel
// over durable state the store never took.
func TestSettlementFailureIsNotACancelledSuccess(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	outage := errors.New("session store outage")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	client.onAgentChunk = func() {
		store.fail(outage)
		cancel()
	}

	_, err := agent.Prompt(ctx, lifecyclePrompt(sessionID, "hang", "sub-2", "nonce-2"))
	require.ErrorIs(t, err, outage)
	require.NotContains(t, ledger.snapshot(), "idle",
		"no terminal boundary claims a foreground prefix the store does not hold")

	// The marker is a marker: it neither renames the failure nor hides it from a
	// caller matching on the cause.
	marked := unsettled(outage)
	require.EqualError(t, marked, outage.Error())
	require.ErrorIs(t, marked, outage)
}

// TestCloseSucceedsOverANativeFailureThatSettled pins what the completion latch
// publishes: the settlement boundary's own outcome, never the native turn's. A
// prompt whose model failed still completed its containment, its durable commit,
// its terminal idle, and its quiescence fact — it settled — so a close running
// concurrently with it has nothing to report and must not borrow the failure the
// prompt answers its own caller with.
func TestCloseSucceedsOverANativeFailureThatSettled(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, _, sessionID := settlementAgent(t, ledger)

	store.gate()

	promptErr := make(chan error, 1)

	go func() {
		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "provider-failure", "sub-native-close", "nonce-native-close"))
		promptErr <- err
	}()

	// The early adoption commit is the first through the gate; the settlement
	// commit is the second, and holding it parks the prompt inside a settlement
	// whose containment boundary already completed.
	<-store.started
	store.release <- struct{}{}
	<-store.started

	closeErr := make(chan error, 1)

	go func() {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: sessionID})
		closeErr <- err
	}()

	select {
	case resultErr := <-closeErr:
		t.Fatalf("close returned before the settlement it waits on: %v", resultErr)
	case <-time.After(50 * time.Millisecond):
	}

	store.release <- struct{}{}

	requireTurnFailure(t, <-promptErr, causeProvider, "upstream refused")
	require.NoError(t, <-closeErr, "a boundary that settled is a successful close, whatever the model did inside it")
	require.Equal(t, []string{"commit", "commit", "idle"}, ledger.snapshot())
}

// TestDeleteSucceedsOverANativeFailureThatSettled pins the same fact on delete,
// including its tombstone: the delete waits out the settlement of a natively
// failed turn, answers cleanly because that settlement succeeded, and the commit
// the settlement took before the tombstone landed is not resurrected by it.
func TestDeleteSucceedsOverANativeFailureThatSettled(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	// The terminal idle is emitted after the durable commit and before the
	// quiescence fact, so holding it parks the prompt inside a settlement whose
	// containment and commit are already done and whose latch is unpublished.
	idleStarted := make(chan struct{})
	idleRelease := make(chan struct{})
	client.idleStarted = idleStarted
	client.idleRelease = idleRelease

	promptErr := make(chan error, 1)

	go func() {
		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "provider-failure", "sub-native-delete", "nonce-native-delete"))
		promptErr <- err
	}()

	<-idleStarted

	deleted := make(chan error, 1)

	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(sessionID))
		deleted <- err
	}()

	select {
	case resultErr := <-deleted:
		t.Fatalf("delete returned before the settlement it waits on: %v", resultErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(idleRelease)

	requireTurnFailure(t, <-promptErr, causeProvider, "upstream refused")
	require.NoError(t, <-deleted, "a boundary that settled is a successful delete, whatever the model did inside it")

	after := store.replaceCount()

	main, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	require.NoError(t, err)
	require.Empty(t, main, "the tombstone is the last word on a deleted session")

	transcript, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.Empty(t, transcript)
	require.Equal(t, after, store.replaceCount(), "no write recreates the row after delete succeeds")
}

// TestCloseReportsACommitOutageBehindANativeFailure pins the other half: when a
// native failure and a settlement failure coincide, the prompt keeps the native
// failure as its primary wire shape and the close states the settlement failure.
// A latch carrying the native error instead would hide the store outage from the
// only surface that reports it.
//
// The close also retries the commit the settlement retained, so the gate sees a
// second write from it. Both the latch and the retry name the same outage, and
// the close states it either way.
func TestCloseReportsACommitOutageBehindANativeFailure(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, _, sessionID := settlementAgent(t, ledger)
	outage := errors.New("session store outage behind a native failure")

	store.gate()

	promptErr := make(chan error, 1)

	go func() {
		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "provider-failure", "sub-native-commit", "nonce-native-commit"))
		promptErr <- err
	}()

	// The adoption commit is already through the gate when the outage is armed,
	// so the settlement commit is the one and only failing write.
	<-store.started
	store.fail(outage)
	store.release <- struct{}{}
	<-store.started

	closeErr := make(chan error, 1)

	go func() {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: sessionID})
		closeErr <- err
	}()

	select {
	case resultErr := <-closeErr:
		t.Fatalf("close returned before the settlement it waits on: %v", resultErr)
	case <-time.After(50 * time.Millisecond):
	}

	store.release <- struct{}{}

	// The close's own durable rung retries the frames the settlement retained,
	// and the outage is still armed, so the retry is the second write to arrive.
	<-store.started
	store.release <- struct{}{}

	failure := <-promptErr
	requireTurnFailure(t, failure, causeProvider, "upstream refused")
	require.NotErrorIs(t, failure, outage, "the native failure is the prompt's primary, unflattened")
	require.ErrorIs(t, <-closeErr, outage)
	require.NotContains(t, ledger.snapshot(), "idle",
		"no terminal boundary claims a foreground prefix the store does not hold")

	// A close that could not commit reclaims nothing, so the session is still the
	// agent's and the host can close it again once the store is back.
	_, addressable := agent.session(sessionID)
	require.NoError(t, addressable, "a failed close left the session unaddressable")
}

// TestDeleteReportsATerminalDeliveryOutageBehindANativeFailure pins the same
// split for the last step of the order: the terminal lifecycle delivery. The
// prompt reports the native failure it was given; the delete reports the delivery
// outage that stopped the boundary from being stated.
func TestDeleteReportsATerminalDeliveryOutageBehindANativeFailure(t *testing.T) {
	ledger := &settlementLedger{}
	agent, _, client, sessionID := settlementAgent(t, ledger)
	deliveryFailure := errors.New("terminal lifecycle delivery failed behind a native failure")
	idleStarted := make(chan struct{})
	idleRelease := make(chan struct{})
	client.idleStarted = idleStarted
	client.idleRelease = idleRelease
	client.idleErr = deliveryFailure

	promptErr := make(chan error, 1)

	go func() {
		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "provider-failure", "sub-native-delivery", "nonce-native-delivery"))
		promptErr <- err
	}()

	// The idle is emitted after the durable commit, so the prompt is held on the
	// one step of the order that is still outstanding.
	<-idleStarted

	deleted := make(chan error, 1)

	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(sessionID))
		deleted <- err
	}()

	select {
	case resultErr := <-deleted:
		t.Fatalf("delete returned before terminal delivery settled: %v", resultErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(idleRelease)

	failure := <-promptErr
	requireTurnFailure(t, failure, causeProvider, "upstream refused")
	require.NotErrorIs(t, failure, deliveryFailure, "the native failure is the prompt's primary, unflattened")
	require.ErrorIs(t, <-deleted, deliveryFailure)
}

func TestCloseReportsSettlementCommitFailure(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)
	outage := errors.New("session store outage after containment")
	streamed := make(chan struct{})
	client.onAgentChunk = func() {
		store.fail(outage)
		close(streamed)
	}

	promptErr := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hang", "sub-close-failure", "nonce-close-failure"))
		promptErr <- err
	}()

	<-streamed

	closeErr := make(chan error, 1)
	go func() {
		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: sessionID})
		closeErr <- err
	}()

	require.ErrorIs(t, <-promptErr, outage)
	require.ErrorIs(t, <-closeErr, outage)
	require.NotContains(t, ledger.snapshot(), "idle")
}

func TestDeleteReportsTerminalLifecycleFailureAfterTombstone(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)
	deliveryFailure := errors.New("terminal lifecycle delivery failed")
	streamed := make(chan struct{})
	idleStarted := make(chan struct{})
	idleRelease := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }
	client.idleStarted = idleStarted
	client.idleRelease = idleRelease
	client.idleErr = deliveryFailure

	promptErr := make(chan error, 1)
	go func() {
		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hang", "sub-delete-failure", "nonce-delete-failure"))
		promptErr <- err
	}()

	<-streamed

	deleteErr := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(sessionID))
		deleteErr <- err
	}()

	<-idleStarted

	main, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	require.NoError(t, err)
	require.Empty(t, main)

	select {
	case resultErr := <-deleteErr:
		t.Fatalf("delete returned before terminal delivery settled: %v", resultErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(idleRelease)

	require.ErrorIs(t, <-promptErr, deliveryFailure)
	require.ErrorIs(t, <-deleteErr, deliveryFailure)

	after := store.replaceCount()
	main, err = store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	require.NoError(t, err)
	require.Empty(t, main)

	transcript, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.Empty(t, transcript)
	require.Equal(t, after, store.replaceCount())
}

// TestDeleteFencesALaterSettlementCommit pins the other half of the delete
// serialization: a settlement that reaches its commit after the tombstone landed
// writes nothing at all, so the frames it held die with the session rather than
// recreating it.
func TestDeleteFencesALaterSettlementCommit(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	streamed := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }

	prompt := make(chan struct{})

	go func() {
		defer close(prompt)

		// A delete interrupts the live native process, and the interrupt's own
		// noise is not what this test is about.
		_, _ = agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hang", "sub-5", "nonce-5"))
	}()

	<-streamed

	before := store.replaceCount()
	_, _ = agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(sessionID))
	<-prompt

	require.Equal(t, before, store.replaceCount(), "a fenced session writes nothing back over its tombstone")

	main, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	require.NoError(t, err)
	require.Empty(t, main)
}

// TestCloseWaitsForFullSettlement pins the completion latch: close waits on a
// prompt that is wholly settled, not on one whose native process merely exited.
// A close that returned at the native terminal would fence a stream the prompt
// is still writing to and answer before the frames the host was shown are
// durable.
func TestCloseWaitsForFullSettlement(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, client, sessionID := settlementAgent(t, ledger)

	streamed := make(chan struct{})
	client.onAgentChunk = func() { close(streamed) }

	prompt := make(chan struct{})

	go func() {
		defer close(prompt)

		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hang", "sub-3", "nonce-3"))
		require.NoError(t, err)
	}()

	<-streamed
	store.gate()

	closed := make(chan struct{})

	go func() {
		defer close(closed)

		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: sessionID})
		require.NoError(t, err)
	}()

	<-store.started

	select {
	case <-closed:
		t.Fatal("close returned while the settlement commit was still held")
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	<-closed
	<-prompt

	ledger.record("closed")
	steps := ledger.snapshot()
	require.Equal(t, []string{"commit", "commit", "idle", "closed"}, steps)
}

// TestDeleteIsNeverResurrectedByALateCommit pins that a delete's tombstone is
// the last write to its row. Replace clears the tombstone of every key it lists,
// so a settlement commit still in flight when the delete arrives would durably
// recreate a session the host was told is gone. The commit is held open across
// the whole delete, which is the exact window the race lives in.
func TestDeleteIsNeverResurrectedByALateCommit(t *testing.T) {
	ledger := &settlementLedger{}
	agent, store, _, sessionID := settlementAgent(t, ledger)

	store.gate()

	prompt := make(chan struct{})

	go func() {
		defer close(prompt)

		_, err := agent.Prompt(t.Context(), lifecyclePrompt(sessionID, "hello", "sub-4", "nonce-4"))
		require.NoError(t, err)
	}()

	// The early adoption commit is the first through the gate; the settlement
	// commit is the second, and it is the one the delete must serialize behind.
	<-store.started
	store.release <- struct{}{}
	<-store.started

	deleted := make(chan error, 1)
	go func() {
		_, err := agent.UnstableDeleteSession(t.Context(), DeleteSessionRequest(sessionID))
		deleted <- err
	}()

	select {
	case err := <-deleted:
		t.Fatalf("delete tombstoned the row while a commit was in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	store.release <- struct{}{}

	require.NoError(t, <-deleted)
	<-prompt

	after := store.replaceCount()

	main, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: SessionStoreMainSubpath})
	require.NoError(t, err)
	require.Empty(t, main, "the tombstone is the last word on a deleted session")

	transcript, err := store.Load(t.Context(), SessionKey{SessionID: string(sessionID), Subpath: transcriptSubpath})
	require.NoError(t, err)
	require.Empty(t, transcript)

	require.Equal(t, after, store.replaceCount(), "no write recreates the row after delete succeeds")
}
