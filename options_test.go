package ampacp

import (
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
		WithScratchDir("/tmp/scratch"),
		WithEnv(map[string]string{"A": "B"}),
		WithTracerProvider(tracenoop.NewTracerProvider()),
		WithMeterProvider(noop.NewMeterProvider()),
		WithTextMapPropagator(propagation.TraceContext{}),
		WithSessionStore(store),
		WithSessionStoreLoadTimeout(time.Second),
		WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 2, MaxConcurrentClientCalls: 4}),
		WithTurnTimeout(90 * time.Second),
		nil,
	})
	if options.AgentName != "name" || options.AgentTitle != "title" || options.AgentVersion != "version" {
		t.Fatalf("identity options = %#v", options)
	}
	if options.Env["A"] != "B" || options.SessionStore != store || options.SessionStoreLoadTimeout != time.Second {
		t.Fatalf("common options = %#v", options)
	}
	if options.ExecutablePath != "/bin/amp" || options.Home != "/tmp/home" || options.DefaultModel != "model" || options.ScratchDir != "/tmp/scratch" {
		t.Fatalf("path/home/model/scratch options = %#v", options)
	}
	if options.TurnTimeout != 90*time.Second {
		t.Fatalf("turn timeout = %s", options.TurnTimeout)
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
