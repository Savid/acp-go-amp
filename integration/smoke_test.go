//go:build integration

package integration

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	ampacp "github.com/savid/acp-go-amp"
	"github.com/savid/acp-go-amp/internal/amp"
)

func TestSmokeAmpVersion(t *testing.T) {
	integrationAmpPath(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := amp.NewClient(slog.Default(), amp.Options{})
	version, err := client.Version(ctx)
	if err != nil {
		t.Fatalf("amp binary present but version probe failed: %v", err)
	}
	if strings.TrimSpace(version) == "" {
		t.Fatal("empty amp version")
	}
}

func TestSmokeFakeACPLifecycleAndMCP(t *testing.T) {
	requireIntegration(t)
	if runtime.GOOS == "windows" {
		t.Skip("shell fake is POSIX-only")
	}
	path := fakeAmpBinary(t)
	agent := ampacp.NewAgent(
		ampacp.WithExecutablePath(path),
		ampacp.WithScratchDir(t.TempDir()),
		ampacp.WithEnv(map[string]string{"AMP_API_KEY": "fake"}),
		ampacp.WithSessionStore(ampacp.NewInMemorySessionStore()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cwd := t.TempDir()
	resp, err := agent.NewSession(ctx, ampacp.NewSessionRequest(cwd,
		ampacp.WithSessionMCPServers(
			ampacp.StdioMCPServer("stdio", "true", nil, nil),
			ampacp.HTTPMCPServer("http", "https://example.test/mcp", nil),
		),
	))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, sseErr := agent.NewSession(ctx, ampacp.NewSessionRequest(cwd, ampacp.WithSessionMCPServers(acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://example.test/sse"}}))); sseErr == nil {
		t.Fatal("SSE MCP accepted")
	}
	promptResp, err := agent.Prompt(ctx, ampacp.TextPromptRequest(resp.SessionId, "test-turn", "smoke"))
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if promptResp.StopReason != "end_turn" {
		t.Fatalf("prompt stop reason = %q", promptResp.StopReason)
	}
	if _, err := agent.UnstableDeleteSession(ctx, ampacp.DeleteSessionRequest(resp.SessionId)); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
}

func TestLiveACPPromptTurn(t *testing.T) {
	requireLiveTokens(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	client := &recordingClient{}
	conn := serveLiveAgentForTest(t, ctx, client)
	if _, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	session, err := conn.NewSession(ctx, acp.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acp.McpServer{}})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	resp, err := conn.Prompt(ctx, acp.PromptRequest{
		SessionId: session.SessionId,
		Prompt:    []acp.ContentBlock{acp.TextBlock("Reply with exactly: acp-go-amp-acp-ok")},
	})
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if resp.StopReason != acp.StopReasonEndTurn {
		t.Fatalf("stop reason = %q", resp.StopReason)
	}
	if !strings.Contains(client.text(), "acp-go-amp-acp-ok") {
		t.Fatalf("client text = %q", client.text())
	}
	responseMessageID, ok := ampMessageID(resp.Meta)
	if !ok {
		t.Fatalf("prompt response missing _meta.amp.messageId: %#v", resp.Meta)
	}
	messageIDs := client.ampMessageIDs()
	if len(messageIDs) == 0 || messageIDs[len(messageIDs)-1] != responseMessageID {
		t.Fatalf("terminal message identity mismatch: updates=%#v response=%q", messageIDs, responseMessageID)
	}

	if _, err := conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: session.SessionId}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
}

func TestLiveThreadTurn(t *testing.T) {
	requireLiveTokens(t)
	dir := t.TempDir()
	settings := filepath.Join(dir, "settings.json")
	if err := os.WriteFile(settings, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := amp.NewClient(slog.Default(), amp.Options{SettingsFile: settings, Mode: "medium"})
	thread, err := client.NewThread(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.DeleteThread(context.Background(), thread) }()
	turn, err := client.Continue(ctx, thread, map[string]any{
		"type": "user",
		"message": map[string]any{
			"role": "user",
			"content": []map[string]any{{
				"type": "text",
				"text": "Reply with exactly: acp-go-amp-live-ok",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var gotResult bool
	for msg := range turn.Messages() {
		if result, ok := msg.(*amp.ResultMessage); ok {
			gotResult = strings.Contains(result.Result, "acp-go-amp-live-ok")
		}
	}
	if !gotResult {
		t.Fatal("missing expected live result")
	}
	if _, err := client.ExportThread(ctx, thread); err != nil {
		t.Fatal(err)
	}
}

func TestLiveRestoreAfterLocalStateWipe(t *testing.T) {
	requireLiveTokens(t)
	apiKey := requireAmpAPIKey(t)

	root := t.TempDir()
	env, homeParent := isolatedAmpEnv(t, root, apiKey)
	store := ampacp.NewInMemorySessionStore()
	cwd := t.TempDir()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	agent := ampacp.NewAgent(
		ampacp.WithScratchDir(homeParent),
		ampacp.WithEnv(env),
		ampacp.WithSessionStore(store),
	)
	newResp, err := agent.NewSession(ctx, ampacp.NewSessionRequest(cwd))
	if err != nil {
		t.Fatal(err)
	}
	threadID := string(newResp.SessionId)
	cleanupEnv := env
	defer func() {
		if threadID != "" {
			deleteClient := amp.NewClient(slog.Default(), amp.Options{Env: cleanupEnv, Cwd: cwd})
			_ = deleteClient.DeleteThread(context.Background(), threadID)
		}
	}()

	seedResponse, promptErr := agent.Prompt(ctx, ampacp.TextPromptRequest(newResp.SessionId, "test-turn", "Reply with exactly: acp-go-amp-restore-seed"))
	if promptErr != nil {
		t.Fatalf("seed prompt: %v", promptErr)
	}
	seedMessageID, ok := ampMessageID(seedResponse.Meta)
	if !ok {
		t.Fatalf("seed response missing _meta.amp.messageId: %#v", seedResponse.Meta)
	}
	storedFrames, loadErr := store.Load(ctx, ampacp.SessionKey{SessionID: threadID, Subpath: "transcript"})
	if loadErr != nil {
		t.Fatalf("load native mirror: %v", loadErr)
	}
	for _, frame := range storedFrames {
		if strings.Contains(string(frame), `"messageId"`) || strings.Contains(string(frame), `"_meta"`) {
			t.Fatalf("wrapper identity contaminated native frame: %s", frame)
		}
	}

	_ = agent.Close()

	if removeErr := os.RemoveAll(root); removeErr != nil {
		t.Fatal(removeErr)
	}
	env, homeParent = isolatedAmpEnv(t, root, apiKey)
	cleanupEnv = env

	restored := ampacp.NewAgent(
		ampacp.WithScratchDir(homeParent),
		ampacp.WithEnv(env),
		ampacp.WithSessionStore(store),
	)
	defer func() { _ = restored.Close() }()
	if _, loadErr := restored.LoadSession(ctx, ampacp.LoadSessionRequest(newResp.SessionId, cwd)); loadErr != nil {
		t.Fatalf("load after local wipe: %v", loadErr)
	}
	resp, promptErr := restored.Prompt(ctx, ampacp.TextPromptRequest(newResp.SessionId, "test-turn", "Reply with exactly: acp-go-amp-restore-ok"))
	if promptErr != nil {
		t.Fatalf("continue after local wipe: %v", promptErr)
	}
	if resp.StopReason != "end_turn" {
		t.Fatalf("restore prompt stop reason = %q", resp.StopReason)
	}
	continuedMessageID, ok := ampMessageID(resp.Meta)
	if !ok {
		t.Fatalf("continued response missing _meta.amp.messageId: %#v", resp.Meta)
	}
	if continuedMessageID == seedMessageID {
		t.Fatalf("continued turn reused seed message identity %q", seedMessageID)
	}

	if _, err := restored.UnstableDeleteSession(ctx, ampacp.DeleteSessionRequest(newResp.SessionId)); err != nil {
		t.Fatalf("delete restored thread: %v", err)
	}
	threadID = ""
}
