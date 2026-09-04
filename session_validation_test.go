package ampacp

import (
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
)

func TestMCPConfig(t *testing.T) {
	cfg, err := mcpConfigJSON([]acp.McpServer{
		StdioMCPServer("stdio", "printf", []string{"x"}, map[string]string{"A": "B"}),
		HTTPMCPServer("http", "https://example.com/mcp", map[string]string{"H": "V"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if decodeErr := json.Unmarshal([]byte(cfg), &payload); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	stdio, stdioOK := payload["stdio"].(map[string]any)
	http, httpOK := payload["http"].(map[string]any)
	if len(payload) != 2 || !stdioOK || !httpOK || stdio["command"] != "printf" || http["url"] != "https://example.com/mcp" {
		t.Fatalf("MCP config = %#v", payload)
	}

	_, err = mcpConfigJSON([]acp.McpServer{{Sse: &acp.McpServerSseInline{Name: "s", Url: "https://example.com/sse"}}})
	requireInvalidParamsData(t, err, map[string]any{jsonFieldError: valUnsupported, jsonFieldField: "mcpServers[0]", jsonFieldServer: "s"})
	_, err = mcpConfigJSON([]acp.McpServer{{Acp: &acp.McpServerAcpInline{Name: "a", Id: "id"}}})
	requireInvalidParamsData(t, err, map[string]any{jsonFieldError: valUnsupported, jsonFieldField: "mcpServers[0]", jsonFieldServer: "a"})
	_, err = mcpConfigJSON([]acp.McpServer{{}})
	requireInvalidParamsData(t, err, map[string]any{jsonFieldError: valNoTransport, jsonFieldField: "mcpServers[0]"})
}

func TestMCPNameContract(t *testing.T) {
	_, err := mcpConfigJSON([]acp.McpServer{{Stdio: &acp.McpServerStdio{Command: "c"}}})
	requireInvalidParamsData(t, err, map[string]any{"mcpServers[0].name": "required"})
	_, err = mcpConfigJSON([]acp.McpServer{
		StdioMCPServer("dup", "c", nil, nil),
		HTTPMCPServer("dup", "https://example.com/mcp", nil),
	})
	requireInvalidParamsData(t, err, map[string]any{"mcpServers[1].name": "duplicate"})
	if _, err := mcpConfigJSON(nil); err != nil {
		t.Fatalf("empty MCP config: %v", err)
	}
}

func TestValidateOptionalAbsolutePath(t *testing.T) {
	if err := validateOptionalAbsolutePath("cwd", nil); err != nil {
		t.Fatal(err)
	}
	empty := ""
	if err := validateOptionalAbsolutePath("cwd", &empty); err != nil {
		t.Fatal(err)
	}
	abs := t.TempDir()
	if err := validateOptionalAbsolutePath("cwd", &abs); err != nil {
		t.Fatal(err)
	}
	rel := "relative/path"
	requireInvalidParamsData(t, validateOptionalAbsolutePath("cwd", &rel), map[string]any{jsonFieldError: valUnsupported, jsonFieldField: "cwd"})
}
