//nolint:goconst,wsl_v5,nlreturn,govet // compact scaffold keeps protocol mapping branches local.
package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

const (
	ForkSessionMethod = "_amp/session/fork"
	RawEventMethod    = "_amp/rawEvent"

	ampMetaKey       = "amp"
	configMode       = acp.SessionConfigId("mode")
	configEffort     = acp.SessionConfigId("effort")
	configTypeSelect = "select"

	jsonFieldError     = "error"
	jsonFieldField     = "field"
	jsonFieldMethod    = "method"
	jsonFieldSessionID = "sessionId"
)

var (
	errSessionDeleted = errors.New("session deleted")
	errSessionClosed  = errors.New("session closed")
)

type ampManifest struct {
	Format             string          `json:"format"`
	ThreadID           string          `json:"threadId"`
	Cwd                string          `json:"cwd"`
	Title              string          `json:"title,omitempty"`
	Mode               string          `json:"mode,omitempty"`
	Effort             string          `json:"effort,omitempty"`
	UpdatedAtUnixMilli int64           `json:"updatedAtUnixMilli"`
	CreatedAtUnixMilli int64           `json:"createdAtUnixMilli"`
	AdditionalDirs     []string        `json:"additionalDirectories,omitempty"`
	Meta               map[string]any  `json:"meta,omitempty"`
	NativeExport       json.RawMessage `json:"nativeExport,omitempty"`
}

type parsedSessionMeta struct {
	options  AmpOptions
	rawEvent bool
}

type agentSession struct {
	agent                 *Agent
	id                    acp.SessionId
	cwd                   string
	title                 string
	mode                  string
	effort                string
	createdUnix           int64
	updatedUnix           int64
	additionalDirectories []string
	mcpConfigJSON         string
	env                   map[string]string
	rawEvents             bool
	settingsDir           string
	settingsFile          string
	closed                bool
	poisonCause           string
	turn                  chan struct{}
	cancelMu              sync.Mutex
	activeTurn            *amp.Turn
	mu                    sync.Mutex
}

func newAgentSession(agent *Agent, id acp.SessionId, cwd string, meta parsedSessionMeta, mcpConfigJSON string, additionalDirs []string) (*agentSession, error) {
	now := time.Now().UnixMilli()
	dir, err := os.MkdirTemp(agent.settingsParent(), "acp-go-amp-settings-*")
	if err != nil {
		return nil, fmt.Errorf("create amp settings dir: %w", err)
	}
	settingsFile := filepath.Join(dir, "settings.json")
	if err := os.WriteFile(settingsFile, []byte("{}\n"), 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("write amp settings file: %w", err)
	}
	mode := meta.options.Mode
	if mode == "" {
		mode = "smart"
	}
	effort := meta.options.Effort
	if effort == "" {
		effort = "high"
	}
	return &agentSession{
		agent:                 agent,
		id:                    id,
		cwd:                   cwd,
		mode:                  mode,
		effort:                effort,
		createdUnix:           now,
		updatedUnix:           now,
		additionalDirectories: append([]string(nil), additionalDirs...),
		mcpConfigJSON:         mcpConfigJSON,
		env:                   mergeEnv(agent.options.Env, meta.options.Env),
		rawEvents:             meta.rawEvent,
		settingsDir:           dir,
		settingsFile:          settingsFile,
		turn:                  make(chan struct{}, agent.maxConcurrentPrompts()),
	}, nil
}

func (s *agentSession) client() *amp.Client {
	return amp.NewClient(s.agent.log, amp.Options{
		CLIPath:       s.agent.options.ExecutablePath,
		Cwd:           s.cwd,
		SettingsFile:  s.settingsFile,
		Env:           s.env,
		ThreadID:      string(s.id),
		Mode:          s.mode,
		Effort:        s.effort,
		MCPConfigJSON: s.mcpConfigJSON,
		MaxLineBytes:  s.agent.options.MaxJSONLineBytes,
	})
}

func (s *agentSession) acquireTurn(ctx context.Context) (func(), error) {
	select {
	case s.turn <- struct{}{}:
		return func() { <-s.turn }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, backpressureError("session_prompt")
	}
}

func (s *agentSession) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	if err := s.ready(); err != nil {
		return acp.PromptResponse{}, err
	}
	release, err := s.acquireTurn(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer release()
	if err := s.ready(); err != nil {
		return acp.PromptResponse{}, err
	}

	input, err := promptInput(params.Prompt)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	turn, err := s.client().Continue(ctx, string(s.id), input)
	if err != nil {
		return acp.PromptResponse{}, classifyNativePromptError(err)
	}
	s.cancelMu.Lock()
	s.activeTurn = turn
	s.cancelMu.Unlock()
	defer func() {
		s.cancelMu.Lock()
		if s.activeTurn == turn {
			s.activeTurn = nil
		}
		s.cancelMu.Unlock()
	}()

	var transcript []SessionStoreEntry
	var promptUsage *acp.Usage
	var terminal *amp.ResultMessage
	for {
		select {
		case msg, ok := <-turn.Messages():
			if !ok {
				if terminal == nil {
					return acp.PromptResponse{}, acp.NewInternalError(map[string]any{jsonFieldError: "amp stream ended without result"})
				}
				if terminal.IsError {
					return acp.PromptResponse{}, acp.NewInternalError(map[string]any{jsonFieldError: terminal.Error, "subtype": terminal.Subtype})
				}
				if err := s.persistAfterTurn(ctx, transcript); err != nil {
					return acp.PromptResponse{}, err
				}
				return acp.PromptResponse{
					StopReason:    acp.StopReasonEndTurn,
					Usage:         promptUsage,
					UserMessageId: params.MessageId,
				}, nil
			}
			if raw := msg.RawJSON(); raw != "" {
				transcript = append(transcript, SessionStoreEntry(raw))
			}
			if err := s.emitRawEvent(ctx, "stream-json", msg); err != nil {
				_ = s.interrupt(context.Background())
				return acp.PromptResponse{}, err
			}
			if err := s.emitMessage(ctx, msg); err != nil {
				_ = s.interrupt(context.Background())
				return acp.PromptResponse{}, err
			}
			if usage := messageUsage(msg); usage != nil {
				promptUsage = usage
			}
			if result, ok := msg.(*amp.ResultMessage); ok {
				terminal = result
				if usage := usageFromAmp(result.Usage); usage != nil {
					promptUsage = usage
				}
			}
		case err := <-turn.Errors():
			if ctx.Err() != nil {
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled, Usage: promptUsage, UserMessageId: params.MessageId}, nil
			}
			return acp.PromptResponse{}, classifyNativePromptError(err)
		case <-ctx.Done():
			_ = s.interrupt(context.Background())
			return acp.PromptResponse{StopReason: acp.StopReasonCancelled, Usage: promptUsage, UserMessageId: params.MessageId}, nil
		}
	}
}

func (s *agentSession) Cancel(ctx context.Context) error {
	return s.interrupt(ctx)
}

func (s *agentSession) interrupt(ctx context.Context) error {
	s.cancelMu.Lock()
	turn := s.activeTurn
	s.cancelMu.Unlock()
	if turn == nil {
		return nil
	}
	return turn.Interrupt(ctx, s.agent.options.NativeCancelTimeout)
}

func (s *agentSession) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	_ = s.interrupt(ctx)
	if s.settingsDir != "" {
		return os.RemoveAll(s.settingsDir)
	}
	return nil
}

func (s *agentSession) Delete(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	_ = s.interrupt(ctx)
	err := s.client().DeleteThread(ctx, string(s.id))
	if s.settingsDir != "" {
		err = errors.Join(err, os.RemoveAll(s.settingsDir))
	}
	return err
}

func (s *agentSession) ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisonCause != "" {
		return acp.NewInternalError(map[string]any{jsonFieldError: s.poisonCause})
	}
	if s.closed {
		return errSessionClosed
	}
	return nil
}

func (s *agentSession) manifest(ctx context.Context) ampManifest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ampManifest{
		Format:             SessionStoreFormat,
		ThreadID:           string(s.id),
		Cwd:                s.cwd,
		Title:              s.title,
		Mode:               s.mode,
		Effort:             s.effort,
		UpdatedAtUnixMilli: s.updatedUnix,
		CreatedAtUnixMilli: s.createdUnix,
		AdditionalDirs:     append([]string(nil), s.additionalDirectories...),
		Meta:               map[string]any{"amp": map[string]any{"mode": s.mode, "effort": s.effort}},
		NativeExport:       s.exportNative(ctx),
	}
}

func (s *agentSession) persistAfterTurn(ctx context.Context, transcript []SessionStoreEntry) error {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	s.updatedUnix = now
	s.mu.Unlock()

	if s.agent.store == nil {
		return nil
	}
	if len(transcript) > 0 {
		if err := s.agent.store.Append(ctx, SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath}, transcript); err != nil {
			return err
		}
	}
	fullTranscript, err := s.agent.store.Load(ctx, SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath})
	if err != nil {
		return err
	}
	main, err := json.Marshal(s.manifest(ctx))
	if err != nil {
		return err
	}
	return s.agent.store.Replace(ctx, SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{
		{Key: SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{main}},
		{Key: SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath}, Entries: fullTranscript},
	})
}

func (s *agentSession) exportNative(ctx context.Context) json.RawMessage {
	timeout := s.agent.options.NativeCommandTimeout
	if timeout <= 0 {
		timeout = defaultNativeCommandTimeout
	}
	exportCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	raw, err := s.client().ExportThread(exportCtx, string(s.id))
	if err != nil {
		s.agent.log.DebugContext(ctx, "amp threads export failed", slog.String(jsonFieldError, err.Error()))
		return nil
	}
	return raw
}

func (s *agentSession) emitMessage(ctx context.Context, msg amp.Message) error {
	switch typed := msg.(type) {
	case *amp.UserMessage:
		for _, block := range typed.Content {
			if text, ok := block.(amp.TextBlock); ok {
				if err := s.emitUpdate(ctx, acp.UpdateUserMessageText(text.Text)); err != nil {
					return err
				}
			}
			if result, ok := block.(amp.ToolResultBlock); ok {
				status := acp.ToolCallStatusCompleted
				if result.IsError {
					status = acp.ToolCallStatusFailed
				}
				raw := result.Content
				if err := s.emitUpdate(ctx, acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{
					SessionUpdate: "tool_call_update",
					ToolCallId:    acp.ToolCallId(result.ToolUseID),
					Status:        &status,
					RawOutput:     raw,
				}}); err != nil {
					return err
				}
			}
		}
	case *amp.AssistantMessage:
		for _, block := range typed.Content {
			switch block := block.(type) {
			case amp.TextBlock:
				if err := s.emitUpdate(ctx, acp.UpdateAgentMessageText(block.Text)); err != nil {
					return err
				}
			case amp.ToolUseBlock:
				if err := s.emitUpdate(ctx, acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{
					SessionUpdate: "tool_call",
					ToolCallId:    acp.ToolCallId(block.ID),
					Title:         block.Name,
					Status:        acp.ToolCallStatusPending,
					RawInput:      block.Input,
				}}); err != nil {
					return err
				}
			}
		}
		if typed.Usage != nil {
			return s.emitUsage(ctx, typed.Usage)
		}
	case *amp.ResultMessage:
		if typed.Usage != nil {
			return s.emitUsage(ctx, typed.Usage)
		}
	}
	return nil
}

func (s *agentSession) emitUsage(ctx context.Context, usage *amp.Usage) error {
	if usage == nil {
		return nil
	}
	used := usage.InputTokens + usage.OutputTokens + usage.CacheCreationInputTokens + usage.CacheReadInputTokens
	return s.emitUpdate(ctx, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
		SessionUpdate: "usage_update",
		Used:          used,
		Size:          usage.MaxTokens,
		Meta: map[string]any{
			ampMetaKey: map[string]any{
				"serviceTier": usage.ServiceTier,
			},
		},
	}})
}

func (s *agentSession) emitUpdate(ctx context.Context, update acp.SessionUpdate) error {
	conn := s.agent.connection()
	if conn == nil {
		return nil
	}
	return conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Update: update})
}

func (s *agentSession) emitRawEvent(ctx context.Context, source string, msg amp.Message) error {
	if !s.rawEvents {
		return nil
	}
	raw := msg.RawMessage()
	payload := map[string]any{
		"sessionId": s.id,
		"sequence":  s.agent.nextRawEventSequence(),
		"source":    source,
		"event":     raw,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(data) > rawEventMaxBytes {
		payload["event"] = map[string]any{
			"truncated": true,
			"type":      msg.AmpType(),
		}
	}
	conn := s.agent.connection()
	if conn == nil {
		return nil
	}
	return conn.NotifyExtension(ctx, RawEventMethod, payload)
}

func promptInput(blocks []acp.ContentBlock) (map[string]any, error) {
	content := make([]map[string]any, 0, len(blocks))
	for _, block := range blocks {
		switch {
		case block.Text != nil:
			content = append(content, map[string]any{"type": "text", "text": block.Text.Text})
		case block.Image != nil:
			image := map[string]any{
				"type": "image",
				"source": map[string]any{
					"type":       "base64",
					"media_type": block.Image.MimeType,
					"data":       block.Image.Data,
				},
			}
			content = append(content, image)
		case block.ResourceLink != nil:
			content = append(content, map[string]any{"type": "text", "text": resourceLinkText(block.ResourceLink)})
		case block.Resource != nil:
			resourceContent, err := embeddedResourceContent(block.Resource.Resource)
			if err != nil {
				return nil, err
			}
			content = append(content, resourceContent)
		default:
			return nil, acp.NewInvalidParams(map[string]any{jsonFieldField: "prompt", jsonFieldError: "unsupported content block"})
		}
	}
	return map[string]any{
		"type": "user",
		"message": map[string]any{
			"role":    "user",
			"content": content,
		},
	}, nil
}

func resourceLinkText(link *acp.ContentBlockResourceLink) string {
	parts := []string{"Resource link", "URI: " + link.Uri}
	if link.Name != "" {
		parts = append(parts, "Name: "+link.Name)
	}
	if link.Title != nil && *link.Title != "" {
		parts = append(parts, "Title: "+*link.Title)
	}
	if link.MimeType != nil && *link.MimeType != "" {
		parts = append(parts, "MIME: "+*link.MimeType)
	}
	if link.Description != nil && *link.Description != "" {
		parts = append(parts, "Description: "+*link.Description)
	}
	return strings.Join(parts, "\n")
}

func embeddedResourceContent(resource acp.EmbeddedResourceResource) (map[string]any, error) {
	if resource.TextResourceContents != nil {
		text := resource.TextResourceContents
		parts := []string{"Embedded resource", "URI: " + text.Uri}
		if text.MimeType != nil && *text.MimeType != "" {
			parts = append(parts, "MIME: "+*text.MimeType)
		}
		parts = append(parts, "", text.Text)
		return map[string]any{"type": "text", "text": strings.Join(parts, "\n")}, nil
	}
	if resource.BlobResourceContents != nil {
		blob := resource.BlobResourceContents
		if blob.MimeType != nil && strings.HasPrefix(*blob.MimeType, "image/") {
			return map[string]any{
				"type": "image",
				"source": map[string]any{
					"type":       "base64",
					"media_type": *blob.MimeType,
					"data":       blob.Blob,
				},
			}, nil
		}
		parts := []string{"Embedded resource", "URI: " + blob.Uri}
		if blob.MimeType != nil && *blob.MimeType != "" {
			parts = append(parts, "MIME: "+*blob.MimeType)
		}
		parts = append(parts, "", "Base64 content:", blob.Blob)
		return map[string]any{"type": "text", "text": strings.Join(parts, "\n")}, nil
	}
	return nil, acp.NewInvalidParams(map[string]any{jsonFieldField: "prompt", jsonFieldError: "unsupported embedded resource"})
}

func usageFromAmp(usage *amp.Usage) *acp.Usage {
	if usage == nil {
		return nil
	}
	total := usage.InputTokens + usage.OutputTokens + usage.CacheCreationInputTokens + usage.CacheReadInputTokens
	acpUsage := &acp.Usage{
		InputTokens:  usage.InputTokens,
		OutputTokens: usage.OutputTokens,
		TotalTokens:  total,
	}
	acpUsage.CachedReadTokens = acp.Ptr(usage.CacheReadInputTokens)
	acpUsage.CachedWriteTokens = acp.Ptr(usage.CacheCreationInputTokens)
	return acpUsage
}

func messageUsage(msg amp.Message) *acp.Usage {
	if assistant, ok := msg.(*amp.AssistantMessage); ok {
		return usageFromAmp(assistant.Usage)
	}
	return nil
}

func classifyNativePromptError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "does not exist") || strings.Contains(msg, "Thread not found") {
		return acp.NewInternalError(map[string]any{jsonFieldError: "native_state_missing", "detail": msg})
	}
	return acp.NewInternalError(map[string]any{jsonFieldError: msg})
}

func mergeEnv(base, session map[string]string) map[string]string {
	out := cloneStringMap(base)
	if out == nil {
		out = map[string]string{}
	}
	for key, value := range session {
		out[key] = value
	}
	return out
}
