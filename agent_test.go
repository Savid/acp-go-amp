package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestInitializeShape(t *testing.T) {
	agent := NewAgent()
	resp, err := agent.Initialize(context.Background(), acp.InitializeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.AuthMethods) != 0 {
		t.Fatalf("auth methods = %d", len(resp.AuthMethods))
	}
	if resp.AgentCapabilities.SessionCapabilities.Fork != nil {
		t.Fatal("fork advertised")
	}
	if resp.AgentCapabilities.Meta["amp"] == nil {
		t.Fatal("amp meta missing")
	}
}

func TestForkExtensionUnsupported(t *testing.T) {
	agent := NewAgent()
	_, err := agent.HandleExtensionMethod(context.Background(), ForkSessionMethod, json.RawMessage(`{}`))
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("got %T", err)
	}
}

func TestSessionMetaStrictness(t *testing.T) {
	_, err := parseSessionMeta(map[string]any{"amp": map[string]any{"bad": true}})
	if err == nil {
		t.Fatal("expected unknown own namespace error")
	}
	meta, err := parseSessionMeta(map[string]any{
		"other": map[string]any{"ignored": true},
		"amp": map[string]any{
			"options":  map[string]any{"mode": "rush", "effort": "low"},
			"rawEvent": map[string]any{"enabled": true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if meta.options.Mode != "rush" || meta.options.Effort != "low" || !meta.rawEvent {
		t.Fatalf("bad meta: %+v", meta)
	}
}

func TestMCPConfig(t *testing.T) {
	cfg, err := mcpConfigJSON([]acp.McpServer{
		StdioMCPServer("stdio", "printf", []string{"x"}, map[string]string{"A": "B"}),
		HTTPMCPServer("http", "https://example.com/mcp", map[string]string{"H": "V"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg == "" {
		t.Fatal("empty config")
	}
	// Valid named servers forward verbatim into the --mcp-config JSON map keyed
	// by their declared names — never fabricated, rewritten, or deduplicated.
	var payload map[string]any
	if uerr := json.Unmarshal([]byte(cfg), &payload); uerr != nil {
		t.Fatalf("config not JSON: %v", uerr)
	}
	stdio, ok := payload["stdio"].(map[string]any)
	if !ok {
		t.Fatalf("stdio entry missing: %#v", payload)
	}
	if stdio["command"] != "printf" {
		t.Fatalf("stdio command = %#v", stdio["command"])
	}
	httpEntry, ok := payload["http"].(map[string]any)
	if !ok {
		t.Fatalf("http entry missing: %#v", payload)
	}
	if httpEntry["url"] != "https://example.com/mcp" {
		t.Fatalf("http url = %#v", httpEntry["url"])
	}
	if len(payload) != 2 {
		t.Fatalf("payload entries = %d, want 2", len(payload))
	}

	_, err = mcpConfigJSON([]acp.McpServer{{Sse: &acp.McpServerSseInline{Name: "s", Url: "https://example.com/sse"}}})
	if err == nil {
		t.Fatal("expected sse rejection")
	}
}

// requireInvalidParamsData asserts err is a -32602 RequestError whose data
// map exactly equals want.
func requireInvalidParamsData(t *testing.T, err error, want map[string]any) {
	t.Helper()
	var reqErr *acp.RequestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error = %T %v, want RequestError", err, err)
	}
	if reqErr.Code != -32602 {
		t.Fatalf("code = %d, want -32602 (%v)", reqErr.Code, err)
	}
	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %#v, want map", reqErr.Data)
	}
	if len(data) != len(want) {
		t.Fatalf("data = %#v, want %#v", data, want)
	}
	for k, v := range want {
		if data[k] != v {
			t.Fatalf("data = %#v, want %#v", data, want)
		}
	}
}

func TestMCPNameContract(t *testing.T) {
	// Empty stdio name at index 0.
	_, err := mcpConfigJSON([]acp.McpServer{{Stdio: &acp.McpServerStdio{Command: "c"}}})
	requireInvalidParamsData(t, err, map[string]any{"mcpServers[0].name": "required"})

	// Empty http name at index 1 reports its own index.
	_, err = mcpConfigJSON([]acp.McpServer{
		StdioMCPServer("keep", "c", nil, nil),
		{Http: &acp.McpServerHttpInline{Type: "http", Url: "https://example.com/mcp"}},
	})
	requireInvalidParamsData(t, err, map[string]any{"mcpServers[1].name": "required"})

	// Duplicate name reports the LATER offending index.
	_, err = mcpConfigJSON([]acp.McpServer{
		StdioMCPServer("dup", "c", nil, nil),
		HTTPMCPServer("dup", "https://example.com/mcp", nil),
	})
	requireInvalidParamsData(t, err, map[string]any{"mcpServers[1].name": "duplicate"})

	// Duplicate across three entries pins the second occurrence's index.
	_, err = mcpConfigJSON([]acp.McpServer{
		StdioMCPServer("a", "c", nil, nil),
		StdioMCPServer("b", "c", nil, nil),
		StdioMCPServer("a", "c", nil, nil),
	})
	requireInvalidParamsData(t, err, map[string]any{"mcpServers[2].name": "duplicate"})
}

func TestConfigOptions(t *testing.T) {
	session := &agentSession{mode: "smart", effort: "high"}
	options := session.configOptions()
	if len(options) != 2 {
		t.Fatalf("options=%d", len(options))
	}
	if options[0].Select == nil || options[0].Select.Type != "select" {
		t.Fatalf("bad mode option: %+v", options[0])
	}
}

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
