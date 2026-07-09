package ampacp

import (
	"context"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

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
