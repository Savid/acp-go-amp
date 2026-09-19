package amp

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
)

const (
	partType       = "type"
	fieldStatus    = "status"
	fieldToolUseID = "toolUseID"
	partText       = "text"
	partThinking   = "thinking"
	partToolResult = "tool_result"
)

type exportedMessage struct {
	ProtocolID json.RawMessage  `json:"protocolMessageID"` //nolint:tagliatelle // Native exports use this field spelling.
	Role       string           `json:"role"`
	Content    []map[string]any `json:"content"`
	State      map[string]any   `json:"state"`
}

// VerifyExport compares the native export to an authoritative quiet thread view.
// A partial export is never complete merely because repeated reads agree.
func VerifyExport(data []byte, view View) (bool, error) {
	messages, err := exportMessages(data, view.ThreadID)
	if err != nil {
		return false, err
	}

	messages = conversationExport(messages)
	expected := conversationView(view.Messages)

	if len(messages) != len(expected) {
		return false, nil
	}

	for i, message := range messages {
		if !matchesMessage(message, expected[i]) || !messageComplete(message) {
			return false, nil
		}
	}

	return true, nil
}

// unexposedInfo reports a native info message whose content the plugin message
// API does not expose, such as the summary Amp inserts when it compacts a
// thread. The export keeps the message; the process that observed the
// compaction omits it and a fresh attachment lists it without content. Both
// verifications therefore compare the conversation around it while the mirror
// preserves it.
func unexposedInfo(role string, content []map[string]any) bool {
	return role == roleInfo && len(pluginContent(content)) == 0
}

func conversationExport(messages []exportedMessage) []exportedMessage {
	return slices.DeleteFunc(slices.Clone(messages), func(message exportedMessage) bool {
		return unexposedInfo(message.Role, message.Content)
	})
}

func conversationView(messages []Message) []Message {
	return slices.DeleteFunc(slices.Clone(messages), func(message Message) bool {
		return unexposedInfo(message.Role, message.Content)
	})
}

// VerifyMirror requires every stored message to agree with the attached thread
// before another prompt can append to it.
func VerifyMirror(data []byte, view View) error {
	messages, err := exportMessages(data, view.ThreadID)
	if err != nil {
		return err
	}

	messages = conversationExport(messages)
	expected := conversationView(view.Messages)

	if len(messages) > len(expected) {
		return errors.New("remote thread is shorter than its mirror")
	}

	for i, message := range messages {
		if !matchesMessage(message, expected[i]) {
			return errors.New("remote thread conflicts with its mirror")
		}
	}

	return nil
}

func exportMessages(data []byte, thread string) ([]exportedMessage, error) {
	var snapshot struct {
		ID       string            `json:"id"`
		Messages []exportedMessage `json:"messages"`
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}

	if snapshot.ID != thread || snapshot.Messages == nil {
		return nil, errors.New("invalid native export")
	}

	return snapshot.Messages, nil
}

func matchesMessage(message exportedMessage, want Message) bool {
	if !bytes.Equal(message.ProtocolID, want.ID) || message.Role != want.Role {
		return false
	}

	actual := pluginContent(message.Content)
	expected := normalizePluginContent(want.Content)

	return reflect.DeepEqual(uniqueToolResults(actual), uniqueToolResults(expected))
}

func pluginContent(content []map[string]any) []map[string]any {
	result := make([]map[string]any, 0, len(content))
	for _, part := range content {
		var item map[string]any

		switch part[partType] {
		case partText:
			item = map[string]any{partType: partText, partText: part[partText]}
		case partThinking:
			item = map[string]any{partType: partThinking, partThinking: part[partThinking]}
		case "tool_use":
			item = map[string]any{partType: "tool_use", "id": part["id"], "name": part["name"], "input": part["input"]}
		case partToolResult:
			run, _ := part["run"].(map[string]any)

			item = map[string]any{partType: partToolResult, fieldToolUseID: part[fieldToolUseID], fieldStatus: run[fieldStatus]}
			if output, ok := run["result"]; ok {
				item["output"] = output
			}
		default:
			// The plugin API omits images and opaque native metadata. Preserve those
			// bytes in the export, while comparing every field the API does expose.
			continue
		}

		result = append(result, item)
	}

	return result
}

func normalizePluginContent(content []map[string]any) []map[string]any {
	result := make([]map[string]any, 0, len(content))
	for _, part := range content {
		item := make(map[string]any, len(part))
		maps.Copy(item, part)

		if item[partType] == partToolResult {
			if output, ok := item["output"].(string); ok {
				var decoded any
				if json.Unmarshal([]byte(output), &decoded) == nil {
					item["output"] = decoded
				}
			}
		}

		result = append(result, item)
	}

	return result
}

func messageComplete(message exportedMessage) bool {
	if message.Role == roleAssistant {
		switch message.State[partType] {
		case "complete", StatusError, StatusCancelled:
		default:
			return false
		}
	}

	for _, part := range message.Content {
		if part[partType] != partToolResult {
			continue
		}

		run, _ := part["run"].(map[string]any)
		switch run[fieldStatus] {
		case StatusDone, StatusError, StatusCancelled:
		default:
			return false
		}
	}

	return true
}

// Native plugin message reads can repeat the same completed tool result. Only
// identical occurrences collapse; different output for one tool remains a conflict.
func uniqueToolResults(content []map[string]any) []map[string]any {
	result := make([]map[string]any, 0, len(content))
	for _, part := range content {
		if part[partType] == partToolResult && slices.ContainsFunc(result, func(previous map[string]any) bool { return reflect.DeepEqual(previous, part) }) {
			continue
		}

		result = append(result, part)
	}

	return result
}
