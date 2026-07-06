package ampacp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestOptionsAndAmpOptions(t *testing.T) {
	store := NewInMemorySessionStore()
	logger := slog.New(slog.DiscardHandler)
	options := applyOptions([]Option{
		WithLogger(logger),
		WithAgentName("name"),
		WithAgentName(""),
		WithAgentTitle("title"),
		WithAgentTitle(""),
		WithAgentVersion("version"),
		WithAgentVersion(""),
		WithExecutablePath("/bin/amp"),
		WithHome("/tmp/home"),
		WithDefaultModel("model"),
		WithEnv(map[string]string{"A": "B"}),
		WithTracerProvider(tracenoop.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
		WithTextMapPropagator(propagation.TraceContext{}),
		WithSessionStore(store),
		WithSessionStoreLoadTimeout(time.Second),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 2, MaxConcurrentPrompts: 3, MaxConcurrentClientCalls: 4}),
		nil,
	})
	if options.AgentName != "name" || options.AgentTitle != "title" || options.AgentVersion != "version" {
		t.Fatalf("identity options = %#v", options)
	}
	if options.Env["A"] != "B" || options.SessionStore != store || options.SessionStoreLoadTimeout != time.Second {
		t.Fatalf("common options = %#v", options)
	}
	env := map[string]string{"A": "B"}
	ampOptions := NewAmpOptions(
		WithAmpModel("model"),
		WithAmpEnv(env),
		WithAmpOutputSchema(map[string]any{"type": "object"}),
		WithAmpMode("mode"),
		WithAmpEffort("effort"),
		nil,
	)
	env["A"] = "changed"
	meta := ampOptions.Meta()
	ampMeta, ok := meta[ampMetaKey].(map[string]any)
	if !ok {
		t.Fatalf("amp meta = %#v", meta[ampMetaKey])
	}
	payload, ok := ampMeta["options"].(map[string]any)
	if !ok {
		t.Fatalf("options meta = %#v", ampMeta["options"])
	}
	if payload["model"] != "model" || payload["mode"] != "mode" || payload["effort"] != "effort" {
		t.Fatalf("amp options meta = %#v", payload)
	}
	envPayload, ok := payload["env"].(map[string]string)
	if !ok || envPayload["A"] != "B" {
		t.Fatalf("env was not cloned: %#v", payload["env"])
	}
	if cloneStringMap(nil) != nil || cloneAnyMap(nil) != nil {
		t.Fatal("nil clone changed")
	}
}

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
		WithSessionAmpOptions(AmpOptions{Mode: "rush", Effort: "low"}),
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
	if PromptRequest("T-1", acp.TextBlock("hi")).SessionId != "T-1" || TextPromptRequest("T-1", "hi").SessionId != "T-1" {
		t.Fatal("prompt request failed")
	}
	if SetConfigOptionRequest("T-1", "mode", "rush").ValueId == nil || SetModelRequest("T-1", "model").ValueId == nil {
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

func TestCloneHelpersAndDeepClone(t *testing.T) {
	if cloneMCPServers(nil) != nil {
		t.Fatal("cloneMCPServers(nil) changed")
	}
	if cloneMCPServerStdio(nil) != nil {
		t.Fatal("cloneMCPServerStdio(nil) changed")
	}
	if cloneHTTPHeaders(nil) != nil {
		t.Fatal("cloneHTTPHeaders(nil) changed")
	}
	if cloneEnvVariables(nil) != nil {
		t.Fatal("cloneEnvVariables(nil) changed")
	}
	if cloneAnySlice(nil) != nil {
		t.Fatal("cloneAnySlice(nil) changed")
	}
	if got := cloneMCPServer(acp.McpServer{}); got.Http != nil || got.Sse != nil || got.Acp != nil || got.Stdio != nil {
		t.Fatalf("cloneMCPServer(empty) = %#v", got)
	}
	if got := unstableMCPServerFromStable(acp.McpServer{}); got.Http != nil || got.Sse != nil || got.Acp != nil || got.Stdio != nil {
		t.Fatalf("unstableMCPServerFromStable(empty) = %#v", got)
	}

	// A meta map with nested map and slice must be deep-cloned end to end.
	nested := map[string]any{"list": []any{map[string]any{"deep": true}}}
	cloned := cloneAnyMap(nested)
	list, ok := cloned["list"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("nested list not cloned: %#v", cloned["list"])
	}

	inner, ok := list[0].(map[string]any)
	if !ok || inner["deep"] != true {
		t.Fatalf("nested map not cloned: %#v", list[0])
	}

	origList, _ := nested["list"].([]any)
	origInner, _ := origList[0].(map[string]any)
	origInner["deep"] = false

	clonedInner, _ := list[0].(map[string]any)
	if clonedInner["deep"] != true {
		t.Fatal("deep clone aliased nested map")
	}

	// Zero-arg WithSessionMCPServers yields a nil server slice, exercising the
	// stableMCPServers nil path; the request still carries an empty slice.
	req := NewSessionRequest("/tmp/cwd", WithSessionMCPServers())
	if req.McpServers == nil || len(req.McpServers) != 0 {
		t.Fatalf("empty mcp servers = %#v", req.McpServers)
	}

	// A fork with a bare (no-transport) server exercises the default branch of
	// the unstable conversion.
	forkReq := ForkSessionRequest("T-1", "/tmp/cwd", WithSessionMCPServers(acp.McpServer{}))
	if len(forkReq.McpServers) != 1 {
		t.Fatalf("fork servers = %#v", forkReq.McpServers)
	}
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

func TestConcurrencyValidationAllFields(t *testing.T) {
	for _, limits := range []ConcurrencyLimits{
		{MaxActiveSessions: -1},
		{MaxConcurrentPrompts: -1},
		{MaxConcurrentPrompts: 2},
		{MaxConcurrentClientCalls: -1},
	} {
		if err := validateConcurrencyLimits(limits); err == nil {
			t.Fatalf("limits accepted: %#v", limits)
		}
	}
	if err := validateConcurrencyLimits(ConcurrencyLimits{MaxConcurrentPrompts: 1}); err != nil {
		t.Fatalf("single-prompt limit rejected: %v", err)
	}
	if err := validateConcurrencyLimits(ConcurrencyLimits{}); err != nil {
		t.Fatalf("zero limits: %v", err)
	}
}
