package ampacp

import (
	"context"
	"encoding/json"
	"fmt"
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
		"message":      message,
	})
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

	s.agent.observe.RecordAmpProcessStart(continueCtx)
	promptClient := s.clientWithEnv(s.agent.observe.InjectTraceEnv(continueCtx, s.env))

	turn, err := promptClient.Continue(continueCtx, string(s.id), input)
	if err != nil {
		if state.isCancelled() {
			return cancelledPromptResponse(nil, params.MessageId), nil
		}

		return acp.PromptResponse{}, classifyNativePromptError(err)
	}

	state.setTurn(turn)

	var timeoutCh <-chan time.Time

	if d := s.agent.options.TurnTimeout; d > 0 {
		ch, stop := s.agent.options.runtime.newTurnTimer(d)
		defer stop()

		timeoutCh = ch
	}

	var (
		transcript  []SessionStoreEntry
		promptUsage *acp.Usage
		terminal    *amp.ResultMessage
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
			// Raw events are non-authoritative debug output: an emit failure is
			// recorded on the observer hook and the turn continues. It never
			// aborts the prompt turn nor interrupts the harness.
			if err := s.emitRawEvent(ctx, "stream-json", msg); err != nil {
				s.agent.observe.RecordRawEventEmitFailure(ctx, err)
			}

			if err := s.emitMessage(ctx, msg, true); err != nil {
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
// a live turn reconciles the session's advertised mode/effort from a native init
// frame, because replay restores state from the persisted manifest.
func (s *agentSession) emitMessage(ctx context.Context, msg amp.Message, live bool) error {
	switch typed := msg.(type) {
	case *amp.SystemMessage:
		if live {
			return s.reconcileNativeConfig(ctx, typed)
		}
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

	release, err := s.agent.acquireClientCall(ctx)
	if err != nil {
		return err
	}
	defer release()

	return conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Update: update})
}

// emitRawEvent emits one non-authoritative raw-event notification for a live
// native message. The sequence is per-session, starts at 1, and is strictly
// monotonic and contiguous: every event consumes exactly one sequence and
// carries exactly one notification. An event whose marshalled payload cannot be
// built or exceeds the 64 KiB cap is never dropped and never emits nothing — its
// `event` is replaced by the fixed truncation marker so the payload is always
// valid JSON. The returned error signals only that delivery to the client
// failed; the caller records it and continues (a debug toggle never fails the
// turn).
func (s *agentSession) emitRawEvent(ctx context.Context, source string, msg amp.Message) error {
	if !s.rawEvents {
		return nil
	}

	payload := map[string]any{
		"sessionId": s.id,
		"sequence":  s.nextRawEventSequence(),
		keySource:   source,
		"event":     msg.RawMessage(),
	}
	if data, err := json.Marshal(payload); err != nil {
		payload["event"] = map[string]any{
			"truncated": true,
			"reason":    reasonUnserializable,
			keyMaxBytes: rawEventMaxBytes,
		}
	} else if len(data) > rawEventMaxBytes {
		payload["event"] = map[string]any{
			"truncated": true,
			"reason":    "oversize",
			keyMaxBytes: rawEventMaxBytes,
			"sizeBytes": len(data),
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

// nextRawEventSequence returns the next per-session raw-event sequence, starting
// at 1. Each session owns its counter so concurrent sessions each see a
// contiguous 1..N stream.
func (s *agentSession) nextRawEventSequence() int64 {
	return s.rawEventSeq.Add(1)
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

func promptInput(blocks []acp.ContentBlock) (map[string]any, error) {
	content := make([]map[string]any, 0, len(blocks))
	for _, block := range blocks {
		switch {
		case block.Text != nil:
			content = append(content, map[string]any{keyType: valText, valText: block.Text.Text})
		case block.Image != nil:
			image := map[string]any{
				keyType: "image",
				keySource: map[string]any{
					keyType:      "base64",
					"media_type": block.Image.MimeType,
					"data":       block.Image.Data,
				},
			}
			content = append(content, image)
		case block.ResourceLink != nil:
			content = append(content, map[string]any{keyType: valText, valText: resourceLinkText(block.ResourceLink)})
		case block.Resource != nil:
			resourceContent, err := embeddedResourceContent(block.Resource.Resource)
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
		"message": map[string]any{
			"role":    valUser,
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

		return map[string]any{keyType: valText, valText: strings.Join(parts, "\n")}, nil
	}

	if resource.BlobResourceContents != nil {
		blob := resource.BlobResourceContents
		if blob.MimeType != nil && strings.HasPrefix(*blob.MimeType, "image/") {
			return map[string]any{
				keyType: "image",
				keySource: map[string]any{
					keyType:      "base64",
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
