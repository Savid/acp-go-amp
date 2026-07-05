//nolint:goconst,wsl_v5,nlreturn // compact scaffold keeps protocol mapping branches local.
package ampacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	writeFile         = os.WriteFile
	mkdirAll          = os.MkdirAll
	mkdirTemp         = os.MkdirTemp
)

type ampManifest struct {
	Format             string `json:"format"`
	ThreadID           string `json:"threadId"`
	Cwd                string `json:"cwd"`
	Title              string `json:"title,omitempty"`
	Mode               string `json:"mode,omitempty"`
	Effort             string `json:"effort,omitempty"`
	UpdatedAtUnixMilli int64  `json:"updatedAtUnixMilli"`
	CreatedAtUnixMilli int64  `json:"createdAtUnixMilli"`
}

type parsedSessionMeta struct {
	options       AmpOptions
	optionFields  ampOptionFields
	rawEvent      bool
	rawEventField bool
}

type ampOptionFields struct {
	env    bool
	mode   bool
	effort bool
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
	nativeMissingCause    string
	unsyncedFrames        []SessionStoreEntry
	turn                  chan struct{}
	cancelMu              sync.Mutex
	activePrompt          *promptTurnState
	persistMu             sync.Mutex
	mu                    sync.Mutex
}

type promptTurnState struct {
	mu        sync.Mutex
	turn      *amp.Turn
	cancelCtx context.CancelFunc
	cancelled chan struct{}
	once      sync.Once
}

func newPromptTurnState() *promptTurnState {
	return &promptTurnState{cancelled: make(chan struct{})}
}

func (s *promptTurnState) setTurn(turn *amp.Turn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turn = turn
}

func (s *promptTurnState) setCancelFunc(cancel context.CancelFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelCtx = cancel
}

func (s *promptTurnState) currentTurn() *amp.Turn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turn
}

func (s *promptTurnState) cancel() {
	var cancel context.CancelFunc
	s.mu.Lock()
	cancel = s.cancelCtx
	s.mu.Unlock()
	s.once.Do(func() { close(s.cancelled) })
	if cancel != nil {
		cancel()
	}
}

func (s *promptTurnState) isCancelled() bool {
	select {
	case <-s.cancelled:
		return true
	default:
		return false
	}
}

func newAgentSession(agent *Agent, id acp.SessionId, cwd string, meta parsedSessionMeta, mcpConfigJSON string, additionalDirs []string) (*agentSession, error) {
	now := time.Now().UnixMilli()
	if parent := agent.settingsParent(); parent != "" {
		if err := mkdirAll(parent, 0o700); err != nil {
			return nil, fmt.Errorf("create amp home parent: %w", err)
		}
	}
	dir, err := mkdirTemp(agent.settingsParent(), "acp-go-amp-session-*")
	if err != nil {
		return nil, fmt.Errorf("create amp settings dir: %w", err)
	}
	homeDir := filepath.Join(dir, "home")
	configDir := filepath.Join(dir, "xdg-config")
	cacheDir := filepath.Join(dir, "xdg-cache")
	dataDir := filepath.Join(dir, "xdg-data")
	stateDir := filepath.Join(dir, "xdg-state")
	for _, path := range []string{homeDir, configDir, cacheDir, dataDir, stateDir, filepath.Join(configDir, "amp")} {
		if err := mkdirAll(path, 0o700); err != nil {
			_ = os.RemoveAll(dir)
			return nil, fmt.Errorf("create amp isolated home: %w", err)
		}
	}
	settingsFile := filepath.Join(configDir, "amp", "settings.json")
	if err := writeFile(settingsFile, []byte("{}\n"), 0o600); err != nil {
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
	env := mergeEnv(agent.options.Env, meta.options.Env)
	env["HOME"] = homeDir
	env["XDG_CONFIG_HOME"] = configDir
	env["XDG_CACHE_HOME"] = cacheDir
	env["XDG_DATA_HOME"] = dataDir
	env["XDG_STATE_HOME"] = stateDir
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
		env:                   env,
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
		MaxLineBytes:  s.agent.options.runtime.maxJSONLineBytes,
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
	if err := s.ensureMirrorSynced(ctx); err != nil {
		return acp.PromptResponse{}, err
	}
	release, err := s.acquireTurn(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer release()

	input, err := promptInput(params.Prompt)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	state := newPromptTurnState()
	continueCtx, cancelContinue := context.WithCancel(ctx)
	defer cancelContinue()
	state.setCancelFunc(cancelContinue)
	s.setActivePrompt(state)
	defer s.clearActivePrompt(state)

	turn, err := s.client().Continue(continueCtx, string(s.id), input)
	if err != nil {
		if state.isCancelled() {
			return cancelledPromptResponse(nil, params.MessageId), nil
		}
		return acp.PromptResponse{}, classifyNativePromptError(err)
	}
	state.setTurn(turn)

	var transcript []SessionStoreEntry
	var promptUsage *acp.Usage
	var terminal *amp.ResultMessage
	for {
		select {
		case msg, ok := <-turn.Messages():
			if !ok {
				if terminal == nil {
					return streamEndedWithoutTerminal(ctx, state, promptUsage, params.MessageId, turn)
				}
				if terminal.IsError {
					if state.isCancelled() || isNativeCancelResult(terminal) {
						return cancelledPromptResponse(promptUsage, params.MessageId), nil
					}
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
			if err := s.validateFrameSessionID(msg, state); err != nil {
				return acp.PromptResponse{}, err
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
		case err, ok := <-turn.Errors():
			if !ok {
				continue
			}
			return promptErrorResponse(ctx, state, promptUsage, params.MessageId, err)
		case <-state.cancelled:
			_ = s.interruptState(context.Background(), state)
			return cancelledPromptResponse(promptUsage, params.MessageId), nil
		case <-ctx.Done():
			state.cancel()
			_ = s.interruptState(context.Background(), state)
			return cancelledPromptResponse(promptUsage, params.MessageId), nil
		}
	}
}

func (s *agentSession) Cancel(ctx context.Context) error {
	_ = ctx
	state := s.activePromptState()
	if state == nil {
		return nil
	}
	if state.isCancelled() {
		return nil
	}
	state.cancel()
	return s.interruptState(context.Background(), state)
}

func (s *agentSession) interrupt(ctx context.Context) error {
	state := s.activePromptState()
	if state == nil {
		return nil
	}
	return s.interruptState(ctx, state)
}

func (s *agentSession) interruptState(ctx context.Context, state *promptTurnState) error {
	_ = ctx
	if state == nil {
		return nil
	}
	turn := state.currentTurn()
	if turn == nil {
		return nil
	}
	timeout := s.agent.options.runtime.nativeCancelTimeout
	cancelCtx, cancel := context.WithTimeout(context.Background(), timeout+s.agent.options.runtime.nativeCloseTurnWait)
	defer cancel()
	return turn.Interrupt(cancelCtx, timeout)
}

func (s *agentSession) setActivePrompt(state *promptTurnState) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	s.activePrompt = state
}

func (s *agentSession) activePromptState() *promptTurnState {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	return s.activePrompt
}

func (s *agentSession) clearActivePrompt(state *promptTurnState) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.activePrompt == state {
		s.activePrompt = nil
	}
}

func (s *agentSession) poison(cause string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poisonCause = cause
	return acp.NewInternalError(map[string]any{jsonFieldError: cause})
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

func (s *agentSession) verifyContinuable(ctx context.Context) error {
	timeout := s.agent.options.runtime.nativeCommandTimeout
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if _, err := s.client().ExportThread(probeCtx, string(s.id)); err != nil {
		if isNativeMissingError(err) {
			s.mu.Lock()
			s.nativeMissingCause = err.Error()
			s.mu.Unlock()
			return nil
		}
		return acp.NewInternalError(map[string]any{jsonFieldError: err.Error()})
	}
	return nil
}

func (s *agentSession) ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.poisonCause != "" {
		return acp.NewInternalError(map[string]any{jsonFieldError: s.poisonCause})
	}
	if s.nativeMissingCause != "" {
		return acp.NewInternalError(map[string]any{jsonFieldError: "native_state_missing", "detail": s.nativeMissingCause})
	}
	if s.closed {
		return errSessionClosed
	}
	return nil
}

func (s *agentSession) manifest() ampManifest {
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
	}
}

// persistAfterTurn durably commits the manifest plus the full transcript in one
// Replace (X4: the whole load-append-Replace path is serialized per session).
// Per X2, any newly completed frames that fail to persist are retained in memory
// (mirror-unsynced) and re-committed on the next attempt so a store outage after
// a native turn success can never silently drop the turn.
func (s *agentSession) persistAfterTurn(ctx context.Context, transcript []SessionStoreEntry) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	now := time.Now().UnixMilli()
	s.mu.Lock()
	s.updatedUnix = now
	pending := append(cloneEntries(s.unsyncedFrames), cloneEntries(transcript)...)
	s.mu.Unlock()

	if s.agent.store == nil {
		return nil
	}
	fullTranscript, err := s.agent.store.Load(ctx, SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath})
	if err != nil {
		s.retainUnsynced(pending)
		return err
	}
	if len(pending) > 0 {
		fullTranscript = append(cloneEntries(fullTranscript), pending...)
	}
	main, _ := json.Marshal(s.manifest())
	if err := s.agent.store.Replace(ctx, SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath}, []SessionStoreReplacement{
		{Key: SessionKey{SessionID: string(s.id), Subpath: SessionStoreMainSubpath}, Entries: []SessionStoreEntry{main}},
		{Key: SessionKey{SessionID: string(s.id), Subpath: transcriptSubpath}, Entries: fullTranscript},
	}); err != nil {
		s.retainUnsynced(pending)
		return err
	}
	s.mu.Lock()
	s.unsyncedFrames = nil
	s.mu.Unlock()
	return nil
}

// retainUnsynced marks the mirror as unsynced by keeping the exact frames that
// failed to persist so they can be retried verbatim.
func (s *agentSession) retainUnsynced(pending []SessionStoreEntry) {
	s.mu.Lock()
	s.unsyncedFrames = pending
	s.mu.Unlock()
}

// ensureMirrorSynced blocks a prompt with a loud error whenever the local mirror
// still holds frames from a previously completed turn that failed to persist. It
// retries the durable Replace of the exact frames on each call and only unblocks
// once that retry succeeds (X2).
func (s *agentSession) ensureMirrorSynced(ctx context.Context) error {
	s.mu.Lock()
	unsynced := len(s.unsyncedFrames) > 0
	s.mu.Unlock()
	if !unsynced {
		return nil
	}
	if err := s.persistAfterTurn(ctx, nil); err != nil {
		return acp.NewInternalError(map[string]any{jsonFieldError: "mirror_unsynced", "detail": err.Error()})
	}
	return nil
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
	release, err := s.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()
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
	release, err := s.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()
	return conn.NotifyExtension(ctx, RawEventMethod, payload)
}

func (s *agentSession) validateFrameSessionID(msg amp.Message, state *promptTurnState) error {
	got := frameSessionID(msg)
	if got == "" || got == string(s.id) {
		return nil
	}
	if state != nil {
		state.cancel()
		_ = s.interruptState(context.Background(), state)
	}
	return s.poison(fmt.Sprintf("native session_id drift: got %q, want %q", got, s.id))
}

func frameSessionID(msg amp.Message) string {
	switch typed := msg.(type) {
	case *amp.SystemMessage:
		return typed.SessionID
	case *amp.UserMessage:
		return typed.SessionID
	case *amp.AssistantMessage:
		return typed.SessionID
	case *amp.ResultMessage:
		return typed.SessionID
	default:
		return ""
	}
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

type turnErrorReader interface {
	Errors() <-chan error
}

func receiveTurnError(turn turnErrorReader) error {
	select {
	case err := <-turn.Errors():
		return err
	default:
		return nil
	}
}

func streamEndedWithoutTerminal(ctx context.Context, state *promptTurnState, usage *acp.Usage, messageID *string, turn turnErrorReader) (acp.PromptResponse, error) {
	if err := receiveTurnError(turn); err != nil {
		return promptErrorResponse(ctx, state, usage, messageID, err)
	}
	if state != nil && state.isCancelled() {
		return cancelledPromptResponse(usage, messageID), nil
	}
	return acp.PromptResponse{}, acp.NewInternalError(map[string]any{jsonFieldError: "amp stream ended without result"})
}

func promptErrorResponse(ctx context.Context, state *promptTurnState, usage *acp.Usage, messageID *string, err error) (acp.PromptResponse, error) {
	if ctx.Err() != nil || (state != nil && state.isCancelled()) || isNativeCancelError(err) {
		// Native process cancellation can surface as a process error; ACP callers
		// should receive the cancellation result once their context is done.
		_ = err
		//nolint:nilerr // The native error is intentionally suppressed for caller cancellation.
		return cancelledPromptResponse(usage, messageID), nil
	}
	return acp.PromptResponse{}, classifyNativePromptError(err)
}

func cancelledPromptResponse(usage *acp.Usage, messageID *string) acp.PromptResponse {
	return acp.PromptResponse{StopReason: acp.StopReasonCancelled, Usage: usage, UserMessageId: messageID}
}

func classifyNativePromptError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if isNativeMissingError(err) {
		return acp.NewInternalError(map[string]any{jsonFieldError: "native_state_missing", "detail": msg})
	}
	return acp.NewInternalError(map[string]any{jsonFieldError: msg})
}

func isNativeMissingError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "does not exist") || strings.Contains(msg, "Thread not found")
}

func isNativeCancelResult(result *amp.ResultMessage) bool {
	return result != nil && isNativeCancelString(result.Error)
}

func isNativeCancelError(err error) bool {
	if err == nil {
		return false
	}
	return isNativeCancelString(err.Error())
}

func isNativeCancelString(value string) bool {
	return strings.Contains(value, "User cancelled (SIGINT/SIGTERM)") || strings.Contains(value, "SIGINT") || strings.Contains(value, "SIGTERM")
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
