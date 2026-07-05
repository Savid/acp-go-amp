package amp

import "testing"

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
}
