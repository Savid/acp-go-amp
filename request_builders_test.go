package ampacp

import (
	"context"
	"encoding/json"
	"io"
	"strings"
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
		WithSessionAmpOptions(AmpOptions{Mode: "low"}),
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

	successConn, cleanup := forkClientConnection(t, extensionAgent{Agent: newTestAgent(), responseID: "T-child", fail: false})
	defer cleanup()
	resp, err := CallForkSession(context.Background(), successConn, acp.UnstableForkSessionRequest{SessionId: "T-1", Cwd: "/tmp/cwd"})
	if err != nil || resp.SessionId != "T-child" {
		t.Fatalf("CallForkSession success = %#v, %v", resp, err)
	}
	errorConn, cleanup := forkClientConnection(t, extensionAgent{Agent: newTestAgent(), fail: true})
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

// TestSessionMetaAccumulatesRegardlessOfOptionOrder pins that the session
// request options are commutative. Every option but this one writes into
// `_meta.amp`, so a WithSessionMeta that assigned would silently discard the
// whole vendor block whenever a host happened to list it last — and a host has
// no reason to believe option order carries meaning.
func TestSessionMetaAccumulatesRegardlessOfOptionOrder(t *testing.T) {
	host := map[string]any{"trace": map[string]any{"id": "t-1"}}

	first := NewSessionRequest("/tmp/cwd",
		WithSessionMeta(host),
		WithSessionAmpOptions(AmpOptions{Mode: "low"}),
		WithSessionRawEvents(true),
	)

	last := NewSessionRequest("/tmp/cwd",
		WithSessionAmpOptions(AmpOptions{Mode: "low"}),
		WithSessionRawEvents(true),
		WithSessionMeta(host),
	)

	if !jsonEqual(t, first.Meta, last.Meta) {
		t.Fatalf("option order changed the request:\nfirst=%s\nlast=%s", mustJSON(t, first.Meta), mustJSON(t, last.Meta))
	}

	amp, vendorPresent := last.Meta[ampMetaKey].(map[string]any)
	if !vendorPresent {
		t.Fatalf("WithSessionMeta discarded the vendor block: %s", mustJSON(t, last.Meta))
	}
	if _, optionsPresent := amp[ampOptionsKey]; !optionsPresent {
		t.Fatalf("vendor options were discarded: %s", mustJSON(t, amp))
	}
	if _, rawPresent := amp[metaRawEventKey]; !rawPresent {
		t.Fatalf("vendor raw-event toggle was discarded: %s", mustJSON(t, amp))
	}
	if trace, tracePresent := last.Meta["trace"].(map[string]any); !tracePresent || trace["id"] != "t-1" {
		t.Fatalf("host metadata was discarded: %s", mustJSON(t, last.Meta))
	}

	// Two host maps accumulate rather than the later one winning outright, and
	// nested objects merge member by member.
	merged := NewSessionRequest("/tmp/cwd",
		WithSessionMeta(map[string]any{"trace": map[string]any{"id": "t-1"}}),
		WithSessionMeta(map[string]any{"trace": map[string]any{"span": "s-1"}, "tenant": "acme"}),
	)
	trace, ok := merged.Meta["trace"].(map[string]any)
	if !ok || trace["id"] != "t-1" || trace["span"] != "s-1" || merged.Meta["tenant"] != "acme" {
		t.Fatalf("host metadata did not accumulate: %s", mustJSON(t, merged.Meta))
	}

	// The same holds on session/list.
	listed := ListSessionsRequest(
		WithListSessionsMeta(map[string]any{"trace": map[string]any{"id": "t-1"}}),
		WithListSessionsMeta(map[string]any{"trace": map[string]any{"span": "s-1"}}),
	)
	listTrace, ok := listed.Meta["trace"].(map[string]any)
	if !ok || listTrace["id"] != "t-1" || listTrace["span"] != "s-1" {
		t.Fatalf("list metadata did not accumulate: %s", mustJSON(t, listed.Meta))
	}

	// The caller's map is never captured: mutating it afterwards changes nothing.
	host["trace"] = "mutated"
	if trace, kept := last.Meta["trace"].(map[string]any); !kept || trace["id"] != "t-1" {
		t.Fatalf("the builder kept the caller's map: %s", mustJSON(t, last.Meta))
	}
}

// TestMetaBuildersRefuseAReservedFamilyLiteral pins that the two builders taking
// host metadata refuse every `acp-go.dev/*` name. The namespace is family-global
// and closed: a request carrying a host's value under one of these keys would
// speak for the family with bytes the family did not write, and merging,
// overwriting, or quietly dropping it are all answers a host cannot see.
func TestMetaBuildersRefuseAReservedFamilyLiteral(t *testing.T) {
	literals := []string{
		metaRouteKey,
		"acp-go.dev/mediaEnvelope",
		"acp-go.dev/handoff",
		"acp-go.dev/lifecycle",
	}

	if got := reservedFamilyLiterals(); len(got) != len(literals) {
		t.Fatalf("reserved literal set = %#v, want the closed four %#v", got, literals)
	}

	for _, literal := range literals {
		meta := map[string]any{literal: map[string]any{"versions": []any{1}}}

		requirePanics(t, literal, func() { WithSessionMeta(meta) })
		requirePanics(t, literal, func() { WithListSessionsMeta(meta) })
	}

	// A foreign namespace is another vendor's business and rides untouched.
	req := NewSessionRequest("/tmp/cwd", WithSessionMeta(map[string]any{"example.com/thing": true}))
	if req.Meta["example.com/thing"] != true {
		t.Fatalf("a foreign namespace was refused or dropped: %s", mustJSON(t, req.Meta))
	}
}

func requirePanics(t *testing.T, literal string, call func()) {
	t.Helper()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("reserved literal %q was accepted", literal)
		}
		message, ok := recovered.(string)
		if !ok || !strings.Contains(message, literal) {
			t.Fatalf("refusal for %q did not name it: %v", literal, recovered)
		}
	}()

	call()
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return string(encoded)
}

func jsonEqual(t *testing.T, left, right any) bool {
	t.Helper()

	return mustJSON(t, left) == mustJSON(t, right)
}
