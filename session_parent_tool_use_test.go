package ampacp

import (
	"context"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
