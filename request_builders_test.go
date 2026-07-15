package ampacp

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestRequestBuildersAndCallForkSession(t *testing.T) {
	stdio := StdioMCPServer("stdio", "cmd", []string{"a"}, map[string]string{"E": "V"})
	http := HTTPMCPServer("http", "https://example.com", map[string]string{"H": "V"})
	sse := acp.McpServer{Sse: &acp.McpServerSseInline{Name: "sse", Url: "https://example.com/sse"}}
	acpServer := acp.McpServer{Acp: &acp.McpServerAcpInline{Name: "acp", Id: "server"}}
	meta := map[string]any{"foreign": map[string]any{"ok": true}}
	newReq := NewSessionRequest("/tmp/cwd",
		WithSessionMeta(meta),
		WithSessionMCPServers(stdio, http, sse, acpServer),
		WithSessionAdditionalDirectories("/tmp/other"),
		WithSessionOutputSchema(map[string]any{"type": "object"}),
		WithSessionAmpOptions(AmpOptions{Mode: "low", Effort: "low"}),
		WithSessionRawEvents(true),
		nil,
	)
	if newReq.Cwd != "/tmp/cwd" || len(newReq.McpServers) != 4 || len(newReq.AdditionalDirectories) != 1 {
		t.Fatalf("new request = %#v", newReq)
	}
	meta["foreign"] = "changed"
	if _, ok := newReq.Meta["foreign"].(map[string]any); !ok {
		t.Fatalf("meta was not cloned: %#v", newReq.Meta)
	}
	loadReq := LoadSessionRequest("T-1", "/tmp/cwd", WithSessionMeta(map[string]any{"x": "y"}))
	resumeReq := ResumeSessionRequest("T-1", "/tmp/cwd")
	forkReq := ForkSessionRequest("T-1", "/tmp/cwd", WithSessionMCPServers(stdio, http, sse, acpServer))
	if loadReq.SessionId != "T-1" || resumeReq.SessionId != "T-1" || len(forkReq.McpServers) != 4 {
		t.Fatalf("session builders failed: %#v %#v %#v", loadReq, resumeReq, forkReq)
	}
	if forkReq.McpServers[1].Http == nil || forkReq.McpServers[2].Sse == nil || forkReq.McpServers[3].Acp == nil {
		t.Fatalf("unstable mcp conversion = %#v", forkReq.McpServers)
	}
	if DeleteSessionRequest("T-1").SessionId != "T-1" {
		t.Fatal("delete request failed")
	}
	if PromptRequest("T-1", "turn-1", acp.TextBlock("hi")).SessionId != "T-1" || TextPromptRequest("T-1", "test-turn", "hi").SessionId != "T-1" {
		t.Fatal("prompt request failed")
	}
	if cancel := CancelRequest("T-1", "ignored"); cancel.SessionId != "T-1" || cancel.Meta != nil {
		t.Fatalf("cancel request = %#v", cancel)
	}
	if SetConfigOptionRequest("T-1", "mode", "low").ValueId == nil || SetModelRequest("T-1", "model").ValueId == nil {
		t.Fatal("set config request failed")
	}
	listReq := ListSessionsRequest(WithListSessionsCwd("/tmp/cwd"), WithListSessionsCursor("cursor"), WithListSessionsMeta(map[string]any{"m": "v"}), nil)
	if listReq.Cwd == nil || *listReq.Cwd != "/tmp/cwd" || listReq.Cursor == nil || *listReq.Cursor != "cursor" || listReq.Meta["m"] != "v" {
		t.Fatalf("list request = %#v", listReq)
	}

	successConn, cleanup := forkClientConnection(t, extensionAgent{Agent: NewAgent(), responseID: "T-child", fail: false})
	defer cleanup()
	resp, err := CallForkSession(context.Background(), successConn, acp.UnstableForkSessionRequest{SessionId: "T-1", Cwd: "/tmp/cwd"})
	if err != nil || resp.SessionId != "T-child" {
		t.Fatalf("CallForkSession success = %#v, %v", resp, err)
	}
	errorConn, cleanup := forkClientConnection(t, extensionAgent{Agent: NewAgent(), fail: true})
	defer cleanup()
	if _, err := CallForkSession(context.Background(), errorConn, acp.UnstableForkSessionRequest{SessionId: "T-1", Cwd: "/tmp/cwd"}); err == nil {
		t.Fatal("CallForkSession error succeeded")
	}
}

type extensionAgent struct {
	*Agent
	responseID acp.SessionId
	fail       bool
}

func (a extensionAgent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, error) {
	if a.fail {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: "failed"})
	}

	return acp.UnstableForkSessionResponse{SessionId: a.responseID}, nil
}

func forkClientConnection(t *testing.T, agent extensionAgent) (*acp.ClientSideConnection, func()) {
	t.Helper()
	c2aR, c2aW := io.Pipe()
	a2cR, a2cW := io.Pipe()
	_ = acp.NewAgentSideConnection(agent, a2cW, c2aR)
	conn := acp.NewClientSideConnection(&recordingClient{}, c2aW, a2cR)
	cleanup := func() {
		_ = c2aW.Close()
		_ = c2aR.Close()
		_ = a2cW.Close()
		_ = a2cR.Close()
	}

	return conn, cleanup
}

func TestConcurrencyValidationAllFields(t *testing.T) {
	for _, limits := range []ConcurrencyLimits{
		{MaxActiveSessions: -1},
		{MaxConcurrentClientCalls: -1},
	} {
		if err := validateConcurrencyLimits(limits); err == nil {
			t.Fatalf("limits accepted: %#v", limits)
		}
	}
	// Zero means "use the default"; positive values may raise either limit.
	for _, limits := range []ConcurrencyLimits{
		{},
		{MaxActiveSessions: 64, MaxConcurrentClientCalls: 32},
	} {
		if err := validateConcurrencyLimits(limits); err != nil {
			t.Fatalf("valid limits rejected: %#v: %v", limits, err)
		}
	}
}
