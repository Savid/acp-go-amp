//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

// TestSmokeAmpACPAgentBinaryConversation drives the compiled acp-go-amp binary
// end to end over its stdio against a deterministic fake amp, exercising the
// real command process without spending model tokens. It backs the
// test-integration-cover binary coverage path.
func TestSmokeAmpACPAgentBinaryConversation(t *testing.T) {
	requireIntegration(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := &recordingClient{}
	conn := connectLiveAgentBinary(t, ctx, client, acp.InitializeRequest{}, fakeAmpBinary(t))

	session, err := conn.NewSession(ctx, acp.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acp.McpServer{}})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	resp, err := conn.Prompt(ctx, acp.PromptRequest{
		SessionId: session.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock("smoke")},
	})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if resp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("stop reason = %q", resp.StopReason)
	}
	if !strings.Contains(client.text(), "ok") {
		t.Fatalf("client text = %q", client.text())
	}

	if _, err := conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
}
