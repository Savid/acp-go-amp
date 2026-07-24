package ampacp

import (
	"context"
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
	"github.com/savid/acp-go-amp/internal/observer"
)

const (
	// turnFailedError is the fixed data.error tag for a native turn failure.
	// A native turn failure is a JSON-RPC error, never a stop reason.
	turnFailedError = "amp_turn_failed"

	// Native-failure cause vocabulary (machine-readable class). data.message
	// carries the real native cause text.
	causeProcessExit = "process_exit"
	causeTransport   = "transport"
	causeProvider    = "provider"
	causeTimeout     = "timeout"
)

// firstNonEmpty returns the first argument whose trimmed value is non-empty.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}

// turnFailure builds the uniform native-turn-failure error: JSON-RPC -32603 with
// data {error:"amp_turn_failed", cause:<class>, message:<real native cause>}.
func turnFailure(cause, message string) error {
	return acp.NewInternalError(map[string]any{
		jsonFieldError: turnFailedError,
		"cause":        cause,
		keyMessage:     message,
	})
}

type promptTurnState struct {
	mu           sync.Mutex
	turn         *amp.Turn
	cancelCtx    context.CancelFunc
	closeErr     error
	cancelled    chan struct{}
	completed    chan struct{}
	cancelOnce   sync.Once
	completeOnce sync.Once
}

func newPromptTurnState() *promptTurnState {
	return &promptTurnState{cancelled: make(chan struct{}), completed: make(chan struct{})}
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
	s.cancelOnce.Do(func() { close(s.cancelled) })

	if cancel != nil {
		cancel()
	}
}

func (s *promptTurnState) complete(closeErr error) {
	s.completeOnce.Do(func() {
		s.mu.Lock()
		s.closeErr = closeErr
		s.mu.Unlock()
		close(s.completed)
	})
}

func (s *promptTurnState) awaitCompletion(ctx context.Context) error {
	select {
	case <-s.completed:
		s.mu.Lock()
		defer s.mu.Unlock()

		return s.closeErr
	case <-ctx.Done():
		return fmt.Errorf("%w: wait for active Amp turn cleanup: %v", amp.ErrProcessContainmentIncomplete, ctx.Err())
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

func (s *agentSession) Prompt(ctx context.Context, params acp.PromptRequest) (resp acp.PromptResponse, returnErr error) { //nolint:gocyclo // Prompt owns the complete turn state machine.
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

	input, err := promptInputWithLimits(params.Prompt, s.agent.options.ImageLimits)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	state := newPromptTurnState()

	continueCtx, cancelContinue := context.WithCancel(ctx)
	defer cancelContinue()

	state.setCancelFunc(cancelContinue)

	s.setActivePrompt(state)
	defer s.clearActivePrompt(state)
	defer state.complete(nil)

	configurationStarted := time.Now()
	mcpConfigPath, err := s.writePromptMCPConfig()
	observeRuntimeStartupStage(continueCtx, s.agent.options.RuntimeResourceHooks, RuntimeResourcePrompt, RuntimeStartupConfiguration, configurationStarted, err)

	if err != nil {
		return acp.PromptResponse{}, err
	}

	if mcpConfigPath != "" {
		defer func() { _ = os.Remove(mcpConfigPath) }()
	}

	s.agent.observe.RecordAmpProcessStart(continueCtx)
	promptClient := s.clientWithEnv(s.agent.observe.InjectTraceEnv(continueCtx, s.env), mcpConfigPath, RuntimeResourcePrompt)

	// The first prompt runs a thread-less `amp -x` execute: amp creates the
	// server-side thread only now, so a session that is never prompted never
	// owns a remote thread. Later prompts continue the adopted thread.
	nativeID := s.nativeSessionID()

	var turn *amp.Turn

	spawnStarted := time.Now()

	if nativeID == "" {
		turn, err = s.agent.options.runtime.executeThread(continueCtx, promptClient, input)
		observeRuntimeStartupStage(continueCtx, s.agent.options.RuntimeResourceHooks, RuntimeResourcePrompt, RuntimeStartupSession, spawnStarted, err)
	} else {
		turn, err = s.agent.options.runtime.continueThread(continueCtx, promptClient, nativeID, input)
		observeRuntimeStartupStage(continueCtx, s.agent.options.RuntimeResourceHooks, RuntimeResourcePrompt, RuntimeStartupSpawn, spawnStarted, err)
	}

	if err != nil {
		s.recordScratchContainment(err)

		if state.isCancelled() {
			return cancelledPromptResponse(nil, params.MessageId), nil
		}

		return acp.PromptResponse{}, classifyNativePromptError(err)
	}

	defer func() {
		closeErr := turn.Close()
		state.complete(closeErr)
		s.recordScratchContainment(closeErr)
		resp, returnErr = finalizeNativePrompt(resp, returnErr, closeErr)
	}()

	state.setTurn(turn)

	var timeoutCh <-chan time.Time

	if d := s.agent.options.TurnTimeout; d > 0 {
		ch, stop := s.agent.options.runtime.newTurnTimer(d)
		defer stop()

		timeoutCh = ch
	}

	var (
		transcript       []SessionStoreEntry
		promptUsage      *acp.Usage
		terminal         *amp.ResultMessage
		finalMessageID   string
		baseTranscriptAt = s.transcriptFrameCount()
	)

	for {
		select {
		case msg, ok := <-turn.Messages():
			if !ok {
				if terminal == nil {
					return streamEndedWithoutTerminal(ctx, state, promptUsage, params.MessageId, turn)
				}

				if terminal.IsError {
					// Cancel guard runs before all failure mapping.
					if state.isCancelled() || isNativeCancelResult(terminal) {
						return cancelledPromptResponse(promptUsage, params.MessageId), nil
					}
					// L1: fall back to result.result when result.error is empty so
					// the real provider cause is never lost.
					cause := firstNonEmpty(terminal.Error, terminal.Result)

					return acp.PromptResponse{}, turnFailure(causeProvider, cause)
				}

				if err := s.persistAfterTurn(ctx, transcript); err != nil {
					return acp.PromptResponse{}, err
				}

				return acp.PromptResponse{
					Meta:          ampMessageMeta(nil, finalMessageID),
					StopReason:    acp.StopReasonEndTurn,
					Usage:         promptUsage,
					UserMessageId: params.MessageId,
				}, nil
			}

			if err := s.validateFrameSessionID(ctx, msg, state); err != nil {
				return acp.PromptResponse{}, err
			}

			messageID := ""

			transcriptJSON, err := s.prepareMessageImageArtifacts(ctx, msg)
			if err != nil {
				_ = s.interrupt(context.Background())

				return acp.PromptResponse{}, err
			}

			if transcriptJSON != "" {
				transcript = append(transcript, SessionStoreEntry(transcriptJSON))

				messageID = assistantMessageIdentity(s.id, baseTranscriptAt+len(transcript), msg)
				if messageID != "" {
					finalMessageID = messageID
				}
			}
			// Raw events are non-authoritative debug output: an emit failure is
			// recorded on the observer hook and the turn continues. It never
			// aborts the prompt turn nor interrupts the harness.
			if err := s.emitRawEvent(ctx, "stream-json", msg); err != nil {
				s.agent.observe.RecordRawEventEmitFailure(ctx, err)
			}

			if err := s.emitMessage(ctx, msg, true, messageID); err != nil {
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

			if ctx.Err() != nil || state.isCancelled() {
				state.cancel()
				_ = s.interruptState(context.Background(), state)
			}

			return promptErrorResponse(ctx, state, promptUsage, params.MessageId, err)
		case <-timeoutCh:
			return s.resolveTurnDeadline(ctx, state, promptUsage, params.MessageId)
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

func finalizeNativePrompt(
	resp acp.PromptResponse,
	returnErr error,
	closeErr error,
) (acp.PromptResponse, error) {
	if !amp.ProcessContainmentComplete(closeErr) {
		return acp.PromptResponse{}, errors.Join(returnErr, closeErr)
	}

	return resp, returnErr
}

func (s *agentSession) writePromptMCPConfig() (string, error) {
	if s.mcpConfigJSON == "" {
		return "", nil
	}

	path := filepath.Join(s.settingsDir, "mcp.json")
	if err := os.WriteFile(path, []byte(s.mcpConfigJSON), 0o600); err != nil {
		return "", fmt.Errorf("write amp MCP config: %w", err)
	}

	return path, nil
}

// resolveTurnDeadline maps a fired WithTurnTimeout deadline to a terminal
// response. The cancel guard runs before all failure mapping, including timeout
// expiry: when a cancel and the deadline land in the same scheduling quantum the
// loop's select tie-break is random, so re-check the cancel condition here. A
// coincident cancel deterministically wins and yields the cancelled response,
// never the cause "timeout" failure. Otherwise a turn deadline is a failure, not
// a cancellation: abort the native turn and surface the uniform timeout failure.
func (s *agentSession) resolveTurnDeadline(ctx context.Context, state *promptTurnState, promptUsage *acp.Usage, messageID *string) (acp.PromptResponse, error) {
	if cancelPending(ctx, state) {
		state.cancel()
		_ = s.interruptState(context.Background(), state)

		return cancelledPromptResponse(promptUsage, messageID), nil
	}

	_ = s.interruptState(context.Background(), state)

	return acp.PromptResponse{}, turnFailure(causeTimeout, fmt.Sprintf("amp turn exceeded WithTurnTimeout of %s", s.agent.options.TurnTimeout))
}

// cancelPending reports whether the turn has an in-flight cancel: either the
// host context is done or a session/cancel closed the prompt-state signal.
func cancelPending(ctx context.Context, state *promptTurnState) bool {
	return ctx.Err() != nil || state.isCancelled()
}

// emitMessage translates one native message into session/update notifications.
// live is true for a running prompt turn and false for session/load replay; only
// a live turn reconciles the session's advertised mode from a native init
// frame, because replay restores state from the persisted manifest.
func (s *agentSession) emitMessage(ctx context.Context, msg amp.Message, live bool, messageID string) error {
	switch typed := msg.(type) {
	case *amp.SystemMessage:
		if live {
			return s.reconcileNativeConfig(ctx, typed)
		}
	case *amp.UserMessage:
		parent := parentToolUseTag(typed.ParentToolUseID)

		for _, block := range typed.Content {
			if text, ok := block.(amp.TextBlock); ok {
				if err := s.emitUpdate(ctx, tagParentToolUse(acp.UpdateUserMessageText(text.Text), parent)); err != nil {
					return err
				}
			}

			if result, ok := block.(amp.ToolResultBlock); ok {
				status := acp.ToolCallStatusCompleted
				if result.IsError {
					status = acp.ToolCallStatusFailed
				}

				content, raw, err := s.toolResultSnapshot(ctx, result)
				if err != nil {
					_ = s.emitImageToolFailure(
						ctx,
						result.ToolUseID,
						result.IsError,
						parent,
						err,
					)

					return err
				}

				if err := s.emitUpdate(ctx, tagParentToolUse(acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{
					SessionUpdate: "tool_call_update",
					ToolCallId:    acp.ToolCallId(result.ToolUseID),
					Status:        &status,
					RawOutput:     raw,
					Content:       content,
				}}, parent)); err != nil {
					return err
				}
			}
		}
	case *amp.AssistantMessage:
		parent := parentToolUseTag(typed.ParentToolUseID)

		for _, block := range typed.Content {
			switch block := block.(type) {
			case amp.TextBlock:
				update := withAmpMessageIdentity(acp.UpdateAgentMessageText(block.Text), messageID)
				if err := s.emitUpdate(ctx, tagParentToolUse(update, parent)); err != nil {
					return err
				}
			case amp.ToolUseBlock:
				if err := s.emitUpdate(ctx, tagParentToolUse(acp.SessionUpdate{ToolCall: &acp.SessionUpdateToolCall{
					SessionUpdate: "tool_call",
					ToolCallId:    acp.ToolCallId(block.ID),
					Title:         block.Name,
					Status:        acp.ToolCallStatusPending,
					RawInput:      block.Input,
				}}, parent)); err != nil {
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

func parentToolUseTag(frameID string) string {
	return frameID
}

// tagParentToolUse stamps _meta.amp.parentToolUseId onto a frame-derived
// session/update when the source frame carried a non-empty parent_tool_use_id.
// An empty id leaves the update untouched so main-agent activity stays untagged.
// Only the populated update variant is tagged, and the tag is merged into any
// existing _meta.amp block without disturbing sibling keys.
func tagParentToolUse(update acp.SessionUpdate, parentToolUseID string) acp.SessionUpdate {
	if parentToolUseID == "" {
		return update
	}

	switch {
	case update.UserMessageChunk != nil:
		update.UserMessageChunk.Meta = withParentToolUseMeta(update.UserMessageChunk.Meta, parentToolUseID)
	case update.AgentMessageChunk != nil:
		update.AgentMessageChunk.Meta = withParentToolUseMeta(update.AgentMessageChunk.Meta, parentToolUseID)
	case update.AgentThoughtChunk != nil:
		update.AgentThoughtChunk.Meta = withParentToolUseMeta(update.AgentThoughtChunk.Meta, parentToolUseID)
	case update.ToolCall != nil:
		update.ToolCall.Meta = withParentToolUseMeta(update.ToolCall.Meta, parentToolUseID)
	case update.ToolCallUpdate != nil:
		update.ToolCallUpdate.Meta = withParentToolUseMeta(update.ToolCallUpdate.Meta, parentToolUseID)
	}

	return update
}

// withParentToolUseMeta merges parentToolUseId into an update's _meta.amp block,
// preserving any existing _meta and _meta.amp keys.
func withParentToolUseMeta(meta map[string]any, parentToolUseID string) map[string]any {
	if meta == nil {
		meta = make(map[string]any, 1)
	}

	ampMeta, _ := meta[ampMetaKey].(map[string]any)
	if ampMeta == nil {
		ampMeta = make(map[string]any, 1)
	}

	ampMeta[metaParentToolUseIDKey] = parentToolUseID
	meta[ampMetaKey] = ampMeta

	return meta
}

func (s *agentSession) emitUsage(ctx context.Context, usage *amp.Usage) error {
	if usage == nil {
		return nil
	}

	used := usage.InputTokens + usage.OutputTokens + usage.CacheCreationInputTokens + usage.CacheReadInputTokens
	// Size is the true model context window. Amp's stream-json usage.max_tokens
	// is a context-window field (verified against amp docs: it reports
	// model-scale values such as 224000/968000 that vary by model, distinct from
	// the Anthropic API max_tokens output cap). It is never derived from `used`;
	// when amp omits it the field decodes to 0 (unknown), which is emitted as-is.
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
	s.agent.observe.ObserveFirstPromptUpdate(ctx)

	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	return conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Update: update})
}

// emitRawEvent emits one non-authoritative raw-event notification for a live
// native message. The sequence is per-session, starts at 1, and is strictly
// monotonic and contiguous over emitted notifications: a sequence is consumed
// only when a notification is actually sent, never on a skipped frame or a
// suppressed (connection-less) send. A frame with a nil native payload is
// skipped entirely — `event` is never null. An event whose marshalled payload
// cannot be built or exceeds the 64 KiB cap is never dropped for an admitted
// session — its `event` is replaced by the fixed truncation marker and the
// complete notification is rechecked. An impossible oversized structural
// envelope fails closed before delivery without consuming a sequence. The
// returned error is recorded by the caller and never fails the turn.
func (s *agentSession) emitRawEvent(ctx context.Context, source string, msg amp.Message) error {
	if !s.rawEvents {
		return nil
	}

	raw := msg.RawMessage()
	if raw == nil {
		return nil
	}

	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	// Raw notifications for one session are serialized across sequence
	// selection and delivery. A failed delivery leaves the committed counter
	// unchanged, so the next notification visible to the client reuses the
	// candidate instead of exposing a gap.
	s.rawEventMu.Lock()
	defer s.rawEventMu.Unlock()

	sequence := s.rawEventSeq.Load() + 1

	payload := map[string]any{
		jsonFieldSessionID:    s.id,
		rawEventFieldSequence: sequence,
		keySource:             source,
		rawEventFieldEvent:    raw,
	}

	capped, err := capRawEventPayload(payload)
	if err != nil {
		return err
	}

	if err := conn.NotifyExtension(ctx, RawEventMethod, capped); err != nil {
		return err
	}

	s.rawEventSeq.Store(sequence)

	return nil
}

func (s *agentSession) validateFrameSessionID(ctx context.Context, msg amp.Message, state *promptTurnState) error {
	got := frameSessionID(msg)
	if got == "" {
		return nil
	}

	s.mu.Lock()
	native := s.nativeID
	s.mu.Unlock()

	if native == "" {
		return s.adoptNativeSessionID(ctx, got, state)
	}

	if got == native {
		return nil
	}

	if state != nil {
		state.cancel()
		_ = s.interruptState(context.Background(), state)
	}

	return s.poison(fmt.Sprintf("native session_id drift: got %q, want %q", got, native))
}

// adoptNativeSessionID records the thread id amp minted for the session's
// first execute turn and persists the manifest immediately: waiting for turn
// end would leave a freshly created server-side thread unrecorded — and
// therefore undeletable — if the process died mid-turn. A persist failure
// does not abort the turn; the turn-end persist commits the same manifest and
// fails the prompt loudly if the store is still down.
func (s *agentSession) adoptNativeSessionID(ctx context.Context, threadID string, state *promptTurnState) error {
	if err := amp.ValidateThreadID(threadID); err != nil {
		if state != nil {
			state.cancel()
			_ = s.interruptState(context.Background(), state)
		}

		return s.poison(fmt.Sprintf("native session_id invalid: %v", err))
	}

	s.mu.Lock()
	s.nativeID = threadID
	s.mu.Unlock()

	if err := s.persistAfterTurn(ctx, nil); err != nil {
		s.agent.log.DebugContext(ctx, "persist adopted amp thread id failed", slog.String(jsonFieldSessionID, string(s.id)), slog.String(jsonFieldError, err.Error()))
	}

	return nil
}

func promptInput(blocks []acp.ContentBlock) (map[string]any, error) {
	return promptInputWithLimits(blocks, applyOptions(nil).ImageLimits)
}

func promptInputWithLimits(blocks []acp.ContentBlock, limits ImageLimits) (map[string]any, error) {
	// An empty prompt is rejected fail-closed: there is nothing to send to the
	// native harness, so accepting it would spend a turn on silence.
	if len(blocks) == 0 {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, jsonFieldField: fieldPrompt})
	}

	imageBudget := imagePromptBudget{limits: limits}

	content := make([]map[string]any, 0, len(blocks))
	for _, block := range blocks {
		switch {
		case block.Text != nil:
			content = append(content, map[string]any{keyType: valText, valText: block.Text.Text})
		case block.Image != nil:
			image, err := imageBudget.validate(block.Image.Data, block.Image.MimeType)
			if err != nil {
				return nil, err
			}

			content = append(content, map[string]any{
				keyType: valImage,
				keySource: map[string]any{
					keyType:      valBase64,
					keyMediaType: block.Image.MimeType,
					keyData:      image.base64,
				},
			})
		case block.ResourceLink != nil:
			content = append(content, map[string]any{keyType: valText, valText: resourceLinkText(block.ResourceLink)})
		case block.Resource != nil:
			resourceContent, err := embeddedResourceContent(block.Resource.Resource, &imageBudget)
			if err != nil {
				return nil, err
			}

			content = append(content, resourceContent)
		default:
			return nil, acp.NewInvalidParams(map[string]any{jsonFieldField: fieldPrompt, jsonFieldError: valUnsupported})
		}
	}

	return map[string]any{
		keyType: valUser,
		keyMessage: map[string]any{
			"role":     valUser,
			keyContent: content,
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

func embeddedResourceContent(resource acp.EmbeddedResourceResource, imageBudget *imagePromptBudget) (map[string]any, error) {
	if resource.TextResourceContents != nil {
		text := resource.TextResourceContents

		parts := []string{"Embedded resource", "URI: " + text.Uri}
		if text.MimeType != nil && *text.MimeType != "" {
			parts = append(parts, "MIME: "+*text.MimeType)
		}

		parts = append(parts, "", text.Text)

		return map[string]any{keyType: valText, valText: strings.Join(parts, "\n")}, nil
	}

	if resource.BlobResourceContents != nil {
		blob := resource.BlobResourceContents
		if blob.MimeType != nil && strings.HasPrefix(*blob.MimeType, "image/") {
			image, err := imageBudget.validate(blob.Blob, *blob.MimeType)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				keyType: valImage,
				keySource: map[string]any{
					keyType:      valBase64,
					keyMediaType: *blob.MimeType,
					keyData:      image.base64,
				},
			}, nil
		}

		parts := []string{"Embedded resource", "URI: " + blob.Uri}
		if blob.MimeType != nil && *blob.MimeType != "" {
			parts = append(parts, "MIME: "+*blob.MimeType)
		}

		parts = append(parts, "", "Base64 content:", blob.Blob)

		return map[string]any{keyType: valText, valText: strings.Join(parts, "\n")}, nil
	}

	return nil, acp.NewInvalidParams(map[string]any{jsonFieldField: fieldPrompt, jsonFieldError: "unsupported embedded resource"})
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

func promptResultForObserver(resp acp.PromptResponse, err error, model string) observer.PromptResult {
	result := observer.PromptResult{
		Err:        err,
		Model:      model,
		StopReason: string(resp.StopReason),
	}
	if resp.Usage == nil {
		return result
	}

	result.InputTokens = resp.Usage.InputTokens
	result.OutputTokens = resp.Usage.OutputTokens

	result.TotalTokens = resp.Usage.TotalTokens
	if resp.Usage.CachedReadTokens != nil {
		result.CachedReadTokens = *resp.Usage.CachedReadTokens
	}

	if resp.Usage.CachedWriteTokens != nil {
		result.CachedWriteTokens = *resp.Usage.CachedWriteTokens
	}

	if resp.Usage.ThoughtTokens != nil {
		result.ThoughtTokens = *resp.Usage.ThoughtTokens
	}

	return result
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

	return acp.PromptResponse{}, turnFailure(causeTransport, "amp stream ended without result")
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
	// A missing native thread is a wrapper-invariant condition (the server-side
	// thread no longer exists), not a turn failure, and keeps its own shape.
	if isNativeMissingError(err) {
		return acp.NewInternalError(map[string]any{jsonFieldError: "native_state_missing", keyDetail: msg})
	}

	return turnFailure(nativeFailureCause(msg), msg)
}

// nativeFailureCause classifies a native turn error into the adapter's cause
// vocabulary: a process-exit cause when the amp process died, otherwise a
// transport cause (decode/read/early-EOF). The real cause text is preserved in
// data.message either way — never a fixed placeholder.
func nativeFailureCause(msg string) string {
	if strings.Contains(msg, "amp process exited") {
		return causeProcessExit
	}

	return causeTransport
}

func isNativeMissingError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, amp.ErrProcessContainmentIncomplete) {
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
