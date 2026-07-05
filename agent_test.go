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
	_, err = mcpConfigJSON([]acp.McpServer{{Sse: &acp.McpServerSseInline{Name: "s", Url: "https://example.com/sse"}}})
	if err == nil {
		t.Fatal("expected sse rejection")
	}
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
