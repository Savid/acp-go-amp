package amp

import "encoding/json"

const (
	TypeSystem    = "system"
	TypeUser      = "user"
	TypeAssistant = "assistant"
	TypeResult    = "result"

	SubtypeInit             = "init"
	SubtypeSuccess          = "success"
	SubtypeErrorExecution   = "error_during_execution"
	SubtypeErrorMaxTurns    = "error_max_turns"
	BlockTypeText           = "text"
	BlockTypeToolUse        = "tool_use"
	BlockTypeToolResult     = "tool_result"
	StopReasonEndTurn       = "end_turn"
	StopReasonToolUse       = "tool_use"
	StopReasonMaxTokens     = "max_tokens"
	rawJSONInternalKey      = "\x00raw_json"
	defaultMaxJSONLineBytes = 10 * 1024 * 1024
)

type Message interface {
	AmpType() string
	RawMessage() map[string]any
	RawJSON() string
}

type SystemMessage struct {
	Subtype         string
	Cwd             string
	SessionID       string
	Tools           []string
	MCPServers      []MCPServerStatus
	AgentMode       string
	ReasoningEffort string
	Raw             map[string]any
	RawJSONText     string
}

func (m *SystemMessage) AmpType() string            { return TypeSystem }
func (m *SystemMessage) RawMessage() map[string]any { return m.Raw }
func (m *SystemMessage) RawJSON() string            { return m.RawJSONText }

type MCPServerStatus struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type UserMessage struct {
	Content         []ContentBlock
	ParentToolUseID string
	SessionID       string
	Raw             map[string]any
	RawJSONText     string
}

func (m *UserMessage) AmpType() string            { return TypeUser }
func (m *UserMessage) RawMessage() map[string]any { return m.Raw }
func (m *UserMessage) RawJSON() string            { return m.RawJSONText }

type AssistantMessage struct {
	Content         []ContentBlock
	StopReason      string
	Usage           *Usage
	ParentToolUseID string
	SessionID       string
	Raw             map[string]any
	RawJSONText     string
}

func (m *AssistantMessage) AmpType() string            { return TypeAssistant }
func (m *AssistantMessage) RawMessage() map[string]any { return m.Raw }
func (m *AssistantMessage) RawJSON() string            { return m.RawJSONText }

type ResultMessage struct {
	Subtype           string
	DurationMS        int
	IsError           bool
	NumTurns          int
	Result            string
	Error             string
	SessionID         string
	Usage             *Usage
	PermissionDenials []string
	Raw               map[string]any
	RawJSONText       string
}

func (m *ResultMessage) AmpType() string            { return TypeResult }
func (m *ResultMessage) RawMessage() map[string]any { return m.Raw }
func (m *ResultMessage) RawJSON() string            { return m.RawJSONText }

type UnknownMessage struct {
	Type        string
	Raw         map[string]any
	RawJSONText string
}

func (m *UnknownMessage) AmpType() string            { return m.Type }
func (m *UnknownMessage) RawMessage() map[string]any { return m.Raw }
func (m *UnknownMessage) RawJSON() string            { return m.RawJSONText }

type ContentBlock interface {
	BlockType() string
}

type TextBlock struct {
	Text string
}

func (b TextBlock) BlockType() string { return BlockTypeText }

type ToolUseBlock struct {
	ID    string
	Name  string
	Input map[string]any
}

func (b ToolUseBlock) BlockType() string { return BlockTypeToolUse }

type ToolResultBlock struct {
	ToolUseID string
	Content   string
	IsError   bool
	Raw       map[string]any
}

func (b ToolResultBlock) BlockType() string { return BlockTypeToolResult }

type UnknownBlock struct {
	Type string
	Raw  map[string]any
}

func (b UnknownBlock) BlockType() string { return b.Type }

type Usage struct {
	InputTokens              int
	OutputTokens             int
	CacheCreationInputTokens int
	CacheReadInputTokens     int
	MaxTokens                int
	ServiceTier              string
}

type ThreadSummary struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Updated      string `json:"updated"`
	Tree         string `json:"tree"`
	MessageCount int    `json:"messageCount"`
}

func ParseMessage(raw map[string]any) (Message, error) {
	raw, rawJSON := splitRawJSON(raw)
	switch stringValue(raw["type"]) {
	case TypeSystem:
		return parseSystem(raw, rawJSON), nil
	case TypeUser:
		return parseUser(raw, rawJSON), nil
	case TypeAssistant:
		return parseAssistant(raw, rawJSON), nil
	case TypeResult:
		return parseResult(raw, rawJSON), nil
	default:
		return &UnknownMessage{Type: stringValue(raw["type"]), Raw: raw, RawJSONText: rawJSON}, nil
	}
}

func ParseJSONLine(line []byte) (Message, error) {
	var raw map[string]any
	if err := json.Unmarshal(line, &raw); err != nil {
		return nil, err
	}

	raw[rawJSONInternalKey] = string(line)

	return ParseMessage(raw)
}

func splitRawJSON(raw map[string]any) (map[string]any, string) {
	rawJSON := stringValue(raw[rawJSONInternalKey])
	if rawJSON == "" {
		return raw, ""
	}

	clean := make(map[string]any, len(raw)-1)
	for key, value := range raw {
		if key != rawJSONInternalKey {
			clean[key] = value
		}
	}

	return clean, rawJSON
}

func parseSystem(raw map[string]any, rawJSON string) *SystemMessage {
	var servers []MCPServerStatus
	if data, err := json.Marshal(raw["mcp_servers"]); err == nil {
		_ = json.Unmarshal(data, &servers)
	}

	return &SystemMessage{
		Subtype:         stringValue(raw["subtype"]),
		Cwd:             stringValue(raw["cwd"]),
		SessionID:       stringValue(raw["session_id"]),
		Tools:           stringSlice(raw["tools"]),
		MCPServers:      servers,
		AgentMode:       stringValue(raw["agent_mode"]),
		ReasoningEffort: stringValue(raw["reasoning_effort"]),
		Raw:             raw,
		RawJSONText:     rawJSON,
	}
}

func parseUser(raw map[string]any, rawJSON string) *UserMessage {
	message, _ := raw["message"].(map[string]any)

	return &UserMessage{
		Content:         parseBlocks(message["content"]),
		ParentToolUseID: stringValue(raw["parent_tool_use_id"]),
		SessionID:       stringValue(raw["session_id"]),
		Raw:             raw,
		RawJSONText:     rawJSON,
	}
}

func parseAssistant(raw map[string]any, rawJSON string) *AssistantMessage {
	message, _ := raw["message"].(map[string]any)

	return &AssistantMessage{
		Content:         parseBlocks(message["content"]),
		StopReason:      stringValue(message["stop_reason"]),
		Usage:           parseUsage(message["usage"]),
		ParentToolUseID: stringValue(raw["parent_tool_use_id"]),
		SessionID:       stringValue(raw["session_id"]),
		Raw:             raw,
		RawJSONText:     rawJSON,
	}
}

func parseResult(raw map[string]any, rawJSON string) *ResultMessage {
	return &ResultMessage{
		Subtype:           stringValue(raw["subtype"]),
		DurationMS:        intValue(raw["duration_ms"]),
		IsError:           boolValue(raw["is_error"]),
		NumTurns:          intValue(raw["num_turns"]),
		Result:            stringValue(raw["result"]),
		Error:             stringValue(raw["error"]),
		SessionID:         stringValue(raw["session_id"]),
		Usage:             parseUsage(raw["usage"]),
		PermissionDenials: stringSlice(raw["permission_denials"]),
		Raw:               raw,
		RawJSONText:       rawJSON,
	}
}

func parseBlocks(value any) []ContentBlock {
	values, _ := value.([]any)

	blocks := make([]ContentBlock, 0, len(values))
	for _, value := range values {
		raw, _ := value.(map[string]any)
		if raw == nil {
			continue
		}

		switch typ := stringValue(raw["type"]); typ {
		case BlockTypeText:
			blocks = append(blocks, TextBlock{Text: stringValue(raw["text"])})
		case BlockTypeToolUse:
			blocks = append(blocks, ToolUseBlock{ID: stringValue(raw["id"]), Name: stringValue(raw["name"]), Input: mapValue(raw["input"])})
		case BlockTypeToolResult:
			blocks = append(blocks, ToolResultBlock{ToolUseID: stringValue(raw["tool_use_id"]), Content: stringValue(raw["content"]), IsError: boolValue(raw["is_error"]), Raw: raw})
		default:
			blocks = append(blocks, UnknownBlock{Type: typ, Raw: raw})
		}
	}

	return blocks
}

func parseUsage(value any) *Usage {
	raw := mapValue(value)
	if raw == nil {
		return nil
	}

	return &Usage{
		InputTokens:              intValue(raw["input_tokens"]),
		OutputTokens:             intValue(raw["output_tokens"]),
		CacheCreationInputTokens: intValue(raw["cache_creation_input_tokens"]),
		CacheReadInputTokens:     intValue(raw["cache_read_input_tokens"]),
		MaxTokens:                intValue(raw["max_tokens"]),
		ServiceTier:              stringValue(raw["service_tier"]),
	}
}

func stringValue(value any) string {
	if typed, ok := value.(string); ok {
		return typed
	}

	return ""
}

func boolValue(value any) bool {
	if typed, ok := value.(bool); ok {
		return typed
	}

	return false
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		n, _ := typed.Int64()

		return int(n)
	default:
		return 0
	}
}

func mapValue(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}

	return nil
}

func stringSlice(value any) []string {
	values, _ := value.([]any)

	out := make([]string, 0, len(values))
	for _, value := range values {
		if typed, ok := value.(string); ok {
			out = append(out, typed)
		}
	}

	return out
}
