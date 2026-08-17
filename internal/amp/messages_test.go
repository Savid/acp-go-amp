package amp

import (
	"encoding/json"
	"testing"
)

func TestParseStreamJSONMessages(t *testing.T) {
	msg, err := ParseJSONLine([]byte(`{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"tool_use","id":"TU-1","name":"Bash","input":{"cmd":"printf ok"}}],"stop_reason":"tool_use","usage":{"input_tokens":1,"cache_creation_input_tokens":2,"cache_read_input_tokens":3,"output_tokens":4,"max_tokens":10,"service_tier":"standard"}},"parent_tool_use_id":null,"session_id":"T-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	assistant, ok := msg.(*AssistantMessage)
	if !ok {
		t.Fatalf("got %T", msg)
	}
	if assistant.Usage == nil || assistant.Usage.InputTokens != 1 || assistant.Usage.CacheReadInputTokens != 3 {
		t.Fatalf("bad usage: %+v", assistant.Usage)
	}
	if assistant.RawMessage()["type"] != TypeAssistant || assistant.RawJSON() == "" {
		t.Fatalf("bad assistant raw: %#v %q", assistant.RawMessage(), assistant.RawJSON())
	}
	if tool, ok := assistant.Content[0].(ToolUseBlock); !ok || tool.Name != "Bash" {
		t.Fatalf("bad content: %#v", assistant.Content)
	}
}

func TestParseResult(t *testing.T) {
	msg, err := ParseJSONLine([]byte(`{"type":"result","subtype":"success","duration_ms":1,"is_error":false,"num_turns":1,"result":"ok","session_id":"T-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	result, ok := msg.(*ResultMessage)
	if !ok {
		t.Fatalf("got %T", msg)
	}
	if result.Result != "ok" || result.SessionID != "T-1" {
		t.Fatalf("bad result: %+v", result)
	}
	if result.RawMessage()["type"] != TypeResult || result.RawJSON() == "" {
		t.Fatalf("bad result raw: %#v %q", result.RawMessage(), result.RawJSON())
	}
}

func TestParseToolResultPreservesStructuredContent(t *testing.T) {
	msg, err := ParseJSONLine([]byte(`{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"TU-1","content":[{"type":"image","data":"aGVsbG8="}]}]},"session_id":"T-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	user, ok := msg.(*UserMessage)
	if !ok {
		t.Fatalf("message = %T", msg)
	}
	result, ok := user.Content[0].(ToolResultBlock)
	if !ok {
		t.Fatalf("content = %#v", user.Content)
	}
	items, ok := result.Content.([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("structured content = %#v", result.Content)
	}
}

func TestParseAllMessageShapesAndAccessors(t *testing.T) {
	tests := []struct {
		name string
		line string
		typ  string
	}{
		{
			name: "system init",
			line: `{"type":"system","subtype":"init","cwd":"/tmp/project","session_id":"T-1","tools":["Read",2],"mcp_servers":[{"name":"svc","status":"connected"}],"agent_mode":"medium","reasoning_effort":"high"}`,
			typ:  TypeSystem,
		},
		{
			name: "user tool result",
			line: `{"type":"user","message":{"content":[{"type":"text","text":"hi"},{"type":"tool_result","tool_use_id":"TU-1","content":"ok","is_error":true},{"type":"unknown","x":1},null]},"parent_tool_use_id":"TU-parent","session_id":"T-1"}`,
			typ:  TypeUser,
		},
		{
			name: "unknown message",
			line: `{"type":"mystery","value":1}`,
			typ:  "mystery",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg, err := ParseJSONLine([]byte(tc.line))
			if err != nil {
				t.Fatal(err)
			}
			if msg.AmpType() != tc.typ {
				t.Fatalf("AmpType = %q, want %q", msg.AmpType(), tc.typ)
			}
			if msg.RawJSON() != tc.line {
				t.Fatalf("RawJSON = %q", msg.RawJSON())
			}
			if msg.RawMessage()["type"] == nil {
				t.Fatalf("RawMessage missing type: %#v", msg.RawMessage())
			}
		})
	}
}

func TestParseHelpersCoverCoercions(t *testing.T) {
	if _, err := ParseJSONLine([]byte(`{`)); err == nil {
		t.Fatal("expected malformed json error")
	}
	raw := map[string]any{"type": TypeAssistant}
	msg, err := ParseMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if msg.RawJSON() != "" {
		t.Fatalf("RawJSON without internal key = %q", msg.RawJSON())
	}
	if boolValue("no") {
		t.Fatal("non-bool coerced true")
	}
	for _, value := range []any{int(1), int64(2), float64(3), json.Number("4"), "bad"} {
		_ = intValue(value)
	}
	blocks := parseBlocks([]any{
		map[string]any{"type": BlockTypeText, "text": "text"},
		map[string]any{"type": BlockTypeToolUse, "id": "TU-1", "name": "Run", "input": map[string]any{"cmd": "true"}},
		map[string]any{"type": BlockTypeToolResult, "tool_use_id": "TU-1", "content": "ok", "is_error": false},
		map[string]any{"type": "other"},
		"skip",
	})
	if len(blocks) != 4 {
		t.Fatalf("blocks = %#v", blocks)
	}
	for _, block := range blocks {
		if block.BlockType() == "" {
			t.Fatalf("empty block type: %#v", block)
		}
	}
}
