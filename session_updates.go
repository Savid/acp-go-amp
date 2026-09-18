package ampacp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-core/wire"
)

const (
	fieldType         = "type"
	fieldMessage      = "message"
	fieldText         = "text"
	fieldData         = "data"
	fieldSource       = "source"
	fieldURL          = "url"
	contentImage      = "image"
	roleAssistant     = "assistant"
	roleUser          = "user"
	roleInfo          = "info"
	fieldContent      = "content"
	fieldResult       = "result"
	resultMaxTurns    = "error_max_turns"
	contentThinking   = "thinking"
	contentToolUse    = "tool_use"
	contentToolResult = "tool_result"
)

type cycleState struct {
	terminal      bool
	failed        bool
	stopReason    string
	errorMessage  string
	usage         *acp.Usage
	contextUsed   int
	contextSize   int
	serviceTier   string
	tools         map[string]*toolState
	imagesEmitted bool
}

type toolState struct {
	name     string
	terminal bool
}

func textValue(value any) string {
	text, _ := value.(string)

	return text
}

func (s *session) projectFrame(ctx context.Context, state *cycleState, frame amp.Frame) error {
	switch frame.Type {
	case roleAssistant:
		state.stopReason = frame.Message.StopReason
		state.addUsage(frame.Message.Usage, false)

		return s.projectContent(ctx, state, roleAssistant, frame.Message.Content, frame.ParentToolUseID, false)
	case roleUser:
		return s.projectContent(ctx, state, roleUser, frame.Message.Content, frame.ParentToolUseID, false)
	case fieldResult:
		state.terminal = true
		state.addUsage(frame.Usage, true)
		state.failed = frame.IsError

		state.errorMessage = frame.Error
		if frame.Subtype == resultMaxTurns {
			state.failed = false
			state.stopReason = frame.Subtype
		}
	}

	return nil
}

func (state *cycleState) addUsage(usage *amp.Usage, replace bool) {
	if usage == nil {
		return
	}

	if state.usage == nil || replace {
		state.usage = &acp.Usage{CachedReadTokens: new(0), CachedWriteTokens: new(0)}
	}

	state.usage.InputTokens += usage.InputTokens
	state.usage.OutputTokens += usage.OutputTokens
	*state.usage.CachedReadTokens += usage.CacheReadInputTokens
	*state.usage.CachedWriteTokens += usage.CacheCreationInputTokens
	state.usage.TotalTokens = state.usage.InputTokens + state.usage.OutputTokens + *state.usage.CachedReadTokens + *state.usage.CachedWriteTokens
	state.contextUsed = usage.InputTokens + usage.OutputTokens + usage.CacheReadInputTokens + usage.CacheCreationInputTokens
	state.contextSize = usage.MaxTokens
	state.serviceTier = usage.ServiceTier
}

func (s *session) emitUsage(ctx context.Context, state *cycleState) error {
	if state.usage == nil {
		return nil
	}

	update := &acp.SessionUsageUpdate{Size: state.contextSize, Used: state.contextUsed}
	if state.serviceTier != "" {
		update.Meta = map[string]any{vendor: map[string]any{"serviceTier": state.serviceTier}}
	}

	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: update})
}

func (s *session) projectContent(ctx context.Context, state *cycleState, role string, parts []map[string]any, parent string, native bool) error {
	for _, part := range parts {
		switch textValue(part[fieldType]) {
		case fieldText:
			var update acp.SessionUpdate

			switch role {
			case roleUser:
				update = acp.UpdateUserMessageText(textValue(part[fieldText]))
			case roleAssistant:
				update = acp.UpdateAgentMessageText(textValue(part[fieldText]))
			default:
				continue
			}

			if err := s.emitParent(ctx, update, parent); err != nil {
				return err
			}
		case contentThinking:
			if err := s.emitParent(ctx, acp.UpdateAgentThoughtText(textValue(part[contentThinking])), parent); err != nil {
				return err
			}
		case contentToolUse:
			id, name := textValue(part["id"]), textValue(part["name"])
			if id == "" || name == "" {
				return fmt.Errorf("native tool has no identity")
			}

			if state.tools == nil {
				state.tools = make(map[string]*toolState)
			}

			if state.tools[id] != nil {
				continue
			}

			state.tools[id] = &toolState{name: name}

			update := acp.StartToolCall(acp.ToolCallId(id), name, acp.WithStartKind(toolKind(name)), acp.WithStartStatus(acp.ToolCallStatusInProgress), acp.WithStartRawInput(part["input"]))
			if err := s.emitParent(ctx, update, parent); err != nil {
				return err
			}

			if name == "todo_write" {
				if err := s.emitPlan(ctx, part["input"]); err != nil {
					return err
				}
			}
		case contentToolResult:
			if err := s.projectToolResult(ctx, state, part, parent, native); err != nil {
				return err
			}

		case contentImage:
			if role != roleUser {
				continue
			}

			block, _, refusal := s.imageContent(part)
			if refusal != nil {
				block = acp.TextBlock(refusal.Message)
			}

			if err := s.emitParent(ctx, acp.SessionUpdate{UserMessageChunk: &acp.SessionUpdateUserMessageChunk{Content: block}}, parent); err != nil {
				return err
			}
		}
	}

	return nil
}

func toolKind(name string) acp.ToolKind {
	switch strings.ToLower(name) {
	case "bash", "shell":
		return acp.ToolKindExecute
	case "read", "read_file", "read_web_page":
		return acp.ToolKindRead
	case "create_file", "edit_file", "apply_patch", "undo_edit":
		return acp.ToolKindEdit
	case "grep", "glob", "finder", "web_search":
		return acp.ToolKindSearch
	default:
		return acp.ToolKindOther
	}
}

func (s *session) emitParent(ctx context.Context, update acp.SessionUpdate, parent string) error {
	if parent == "" {
		return s.emit(ctx, update)
	}

	if conn := s.agent.connection(); conn != nil {
		return conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Meta: map[string]any{vendor: map[string]any{"parentToolUseId": parent}}, Update: update})
	}

	return nil
}

func (s *session) emitPlan(ctx context.Context, input any) error {
	object, _ := input.(map[string]any)

	todos, _ := object["todos"].([]any)
	if todos == nil {
		return nil
	}

	entries := make([]acp.PlanEntry, 0, len(todos))
	for _, raw := range todos {
		todo, _ := raw.(map[string]any)

		content := textValue(todo[fieldContent])
		if content == "" {
			continue
		}

		status := acp.PlanEntryStatusPending

		switch textValue(todo["status"]) {
		case "in_progress":
			status = acp.PlanEntryStatusInProgress
		case "completed":
			status = acp.PlanEntryStatusCompleted
		}

		priority := acp.PlanEntryPriorityMedium

		switch textValue(todo["priority"]) {
		case "high":
			priority = acp.PlanEntryPriorityHigh
		case "low":
			priority = acp.PlanEntryPriorityLow
		}

		entries = append(entries, acp.PlanEntry{Content: content, Status: status, Priority: priority})
	}

	return s.emit(ctx, acp.SessionUpdate{Plan: &acp.SessionUpdatePlan{Entries: entries}})
}

func (s *session) emitRawEvent(ctx context.Context, data []byte) {
	if s.rawEvents == nil || !s.rawEvents.Enabled() {
		return
	}

	conn := s.agent.connection()
	if conn == nil {
		return
	}

	var value map[string]any
	if json.Unmarshal(data, &value) != nil {
		return
	}

	redactImages(value)

	notify := func(ctx context.Context, method string, params map[string]any) error {
		return conn.NotifyExtension(ctx, method, params)
	}
	if err := s.rawEvents.Emit(ctx, notify, value); err != nil {
		s.agent.observe.RecordRawMessageEmitFailure(ctx, err)
	}
}

func redacted(value any) any {
	if text, ok := value.(string); ok {
		var object any
		if json.Unmarshal([]byte(text), &object) == nil {
			return redacted(object)
		}

		return text
	}

	cloned := wire.CloneValue(value)
	redactImages(cloned)

	return cloned
}

func redactImages(value any) {
	switch typed := value.(type) {
	case map[string]any:
		if kind := textValue(typed[fieldType]); kind == contentImage || kind == "base64" {
			if data := textValue(typed["data"]); data != "" {
				typed["data"] = ""
				typed["encodedBytes"] = len(data)
			}

			if uri := textValue(typed["url"]); strings.HasPrefix(uri, "data:") {
				typed["url"] = ""
				typed["encodedBytes"] = len(uri)
			}
		}

		for key, item := range typed {
			if text, ok := item.(string); ok && (key == fieldContent || key == fieldResult) {
				var decoded any
				if json.Unmarshal([]byte(text), &decoded) == nil {
					redactImages(decoded)

					data, err := json.Marshal(decoded)
					if err == nil {
						typed[key] = string(data)
					}

					continue
				}
			}

			redactImages(item)
		}
	case []any:
		for _, item := range typed {
			redactImages(item)
		}
	}
}

func (s *session) projectToolResult(ctx context.Context, state *cycleState, part map[string]any, parent string, native bool) error {
	id := textValue(part["tool_use_id"])
	content := part[fieldContent]

	failed, _ := part["is_error"].(bool)
	if native {
		id = textValue(part["toolUseID"])

		run, _ := part["run"].(map[string]any)
		switch textValue(run["status"]) {
		case amp.StatusDone:
			content = run[fieldResult]
		case amp.StatusError:
			content = run[amp.StatusError]
			failed = true
		case amp.StatusCancelled, "rejected-by-user":
			content = run["reason"]
			failed = true
		default:
			return nil
		}
	}

	tool := state.tools[id]
	if tool == nil || tool.terminal {
		return nil
	}

	blocks, refusal := s.toolContent(state, content)

	status := acp.ToolCallStatusCompleted
	if failed || refusal != nil {
		status = acp.ToolCallStatusFailed
	}

	update := acp.UpdateToolCall(acp.ToolCallId(id), acp.WithUpdateStatus(status), acp.WithUpdateContent(blocks), acp.WithUpdateRawOutput(redacted(content)))
	if refusal != nil {
		update.ToolCallUpdate.Meta = map[string]any{vendor: map[string]any{"stage": "image_output", "reason": refusal.Reason}}
	}

	if err := s.emitParent(ctx, update, parent); err != nil {
		return err
	}

	tool.terminal = true

	return nil
}
