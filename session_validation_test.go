package ampacp

import (
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

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

	// SSE and ACP transports are rejected fail-closed with the uniform
	// -32602 shape {error:"unsupported", field:"mcpServers[N]", server:<name>}.
	_, sseErr := mcpConfigJSON([]acp.McpServer{{Sse: &acp.McpServerSseInline{Name: "s", Url: "https://example.com/sse"}}})
	requireInvalidParamsData(t, sseErr, map[string]any{
		jsonFieldError:  valUnsupported,
		jsonFieldField:  "mcpServers[0]",
		jsonFieldServer: "s",
	})

	_, acpErr := mcpConfigJSON([]acp.McpServer{{Acp: &acp.McpServerAcpInline{Name: "a", Id: "id"}}})
	requireInvalidParamsData(t, acpErr, map[string]any{
		jsonFieldError:  valUnsupported,
		jsonFieldField:  "mcpServers[0]",
		jsonFieldServer: "a",
	})

	// A transport-less entry is rejected fail-closed with exactly the
	// -32602 shape {error:"no_transport", field:"mcpServers[N]"}.
	_, noTransportErr := mcpConfigJSON([]acp.McpServer{{}})
	requireInvalidParamsData(t, noTransportErr, map[string]any{
		jsonFieldError: valNoTransport,
		jsonFieldField: "mcpServers[0]",
	})

	// The reported field pins the offending entry's own index.
	_, noTransportErr = mcpConfigJSON([]acp.McpServer{
		StdioMCPServer("keep", "c", nil, nil),
		{},
	})
	requireInvalidParamsData(t, noTransportErr, map[string]any{
		jsonFieldError: valNoTransport,
		jsonFieldField: "mcpServers[1]",
	})
}

// requireInvalidParamsData asserts err is a -32602 RequestError whose data
// map exactly equals want.
func requireInvalidParamsData(t *testing.T, err error, want map[string]any) {
	t.Helper()
	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)
	require.Equal(t, -32602, reqErr.Code, "want -32602")
	data, ok := reqErr.Data.(map[string]any)
	require.True(t, ok, "data must be a map")
	require.Len(t, data, len(want))
	for k, v := range want {
		require.Equal(t, v, data[k])
	}
}

func TestMCPNameContract(t *testing.T) {
	// Empty stdio name at index 0.
	_, err := mcpConfigJSON([]acp.McpServer{{Stdio: &acp.McpServerStdio{Command: "c"}}})
	requireInvalidParamsData(t, err, map[string]any{"mcpServers[0].name": "required"})

	// Whitespace-only stdio name at index 0 is rejected, not forwarded.
	_, err = mcpConfigJSON([]acp.McpServer{{Stdio: &acp.McpServerStdio{Name: "   ", Command: "c"}}})
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

func TestInvalidMCPShapes(t *testing.T) {
	if _, err := mcpConfigJSON([]acp.McpServer{{Stdio: &acp.McpServerStdio{}}}); err == nil {
		t.Fatal("empty stdio name accepted")
	}
	if _, err := mcpConfigJSON([]acp.McpServer{{Http: &acp.McpServerHttpInline{Type: "http"}}}); err == nil {
		t.Fatal("empty http name accepted")
	}
	if _, err := mcpConfigJSON([]acp.McpServer{{Acp: &acp.McpServerAcpInline{Name: "acp", Id: "id"}}}); err == nil {
		t.Fatal("acp mcp accepted")
	}
	if _, err := mcpConfigJSON([]acp.McpServer{}); err != nil {
		t.Fatalf("empty mcp failed: %v", err)
	}
}

// TestValidateOptionalAbsolutePath pins the cwd-filter validation: absent and
// empty filters pass, relative paths reject with the uniform field shape.
func TestValidateOptionalAbsolutePath(t *testing.T) {
	if err := validateOptionalAbsolutePath("cwd", nil); err != nil {
		t.Fatalf("nil path = %v", err)
	}
	empty := ""
	if err := validateOptionalAbsolutePath("cwd", &empty); err != nil {
		t.Fatalf("empty path = %v", err)
	}
	abs := "/tmp/project"
	if err := validateOptionalAbsolutePath("cwd", &abs); err != nil {
		t.Fatalf("absolute path = %v", err)
	}
	rel := "relative/path"
	err := validateOptionalAbsolutePath("cwd", &rel)
	requireInvalidParamsData(t, err, map[string]any{jsonFieldField: "cwd"})
}
