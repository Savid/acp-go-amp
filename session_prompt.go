package ampacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
)

func (s *session) mapPrompt(ctx context.Context, blocks []acp.ContentBlock) ([]byte, error) {
	if len(blocks) == 0 {
		return nil, wire.Unsupported("prompt")
	}

	decoded, refusal, err := image.ValidatePrompt(ctx, blocks, image.Options{Limits: s.agent.options.ImageLimits.core(), HandoffRoot: s.agent.options.InputHandoffRoot, NativeCeiling: amp.MaxInputImageBytes, Blobs: func(string) image.BlobDisposition { return image.BlobRefuse }})
	if err != nil {
		return nil, err
	}

	if refusal != nil {
		return nil, refusal.InvalidParams()
	}

	parts := make([]map[string]any, 0, len(blocks)+len(decoded))
	appendText := func(text string) {
		if strings.TrimSpace(text) != "" {
			parts = append(parts, map[string]any{fieldType: fieldText, fieldText: text})
		}
	}

	// The gate decides which block is media; indexing its result by block
	// position keeps this walk and the gate on one classification.
	media := make(map[int]image.Decoded, len(decoded))
	for _, item := range decoded {
		media[item.Block] = item
	}

	for position, block := range blocks {
		if img, ok := media[position]; ok {
			parts = append(parts, map[string]any{fieldType: contentImage, fieldSource: map[string]any{fieldType: "base64", "media_type": img.MIME, fieldData: base64.StdEncoding.EncodeToString(img.Data)}})

			continue
		}

		switch {
		case block.Text != nil:
			if wire.AudienceIsUserOnly(block.Text.Annotations) {
				continue
			}

			appendText(block.Text.Text)
		case block.ResourceLink != nil:
			appendText(block.ResourceLink.Uri)
		case block.Resource != nil:
			if text := block.Resource.Resource.TextResourceContents; text != nil {
				appendText(wire.ContextResourceText(text.Uri, text.Text))
			}
		default:
			return nil, wire.Unsupported("prompt")
		}
	}

	if len(parts) == 0 {
		return nil, wire.Unsupported("prompt")
	}

	data, err := json.Marshal(map[string]any{fieldType: roleUser, fieldMessage: map[string]any{"role": roleUser, fieldContent: parts}})

	return append(data, '\n'), err
}

func (s *session) prompt(ctx context.Context, params acp.PromptRequest, raw json.RawMessage) (acp.PromptResponse, error) {
	submission, refusal := lifecycle.DecodePromptCorrelation(lifecycle.RetainRequestMetadata(params.Meta, raw), s.lifecycleNegotiated())
	if refusal != nil {
		return acp.PromptResponse{}, wire.ParamRefusal(refusal)
	}

	if err := s.admissionError(); err != nil {
		return acp.PromptResponse{}, err
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer release()

	// The turn is cancelled by session/cancel, by its own timeout, and by
	// close, never by the request that carried it: the SDK cancels a session's
	// previous prompt context before dispatching the next one, and a prompt
	// this session refuses must not end the turn already running. The turn is
	// installed before its content is read so a cancel that arrives during
	// that read is not lost.
	turnCtx, cancelTurn := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelTurn()

	t := &turn{cancelNative: cancelTurn, submission: submission}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()

		return acp.PromptResponse{}, wire.UnknownSession()
	}

	s.turn = t

	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.turn = nil; s.mu.Unlock() }()

	input, err := s.mapPrompt(turnCtx, params.Prompt)
	if err != nil {
		if turnCtx.Err() != nil {
			return wire.CancelledResponse(params), nil
		}

		return acp.PromptResponse{}, err
	}

	if turnCtx.Err() != nil {
		return wire.CancelledResponse(params), nil
	}

	request, err := s.continueRequest(turnCtx)
	if err != nil {
		if turnCtx.Err() != nil {
			return wire.CancelledResponse(params), nil
		}

		return acp.PromptResponse{}, err
	}

	bridgeDir, err := s.agent.scratchDir("bridge")
	if err != nil {
		s.agent.log.ErrorContext(turnCtx, "amp bridge scratch unavailable", slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))

		return acp.PromptResponse{}, wire.InternalFailure(vendor, internalClassNativeStart)
	}

	evidence, result, stderr, runErr := amp.RunAttached(turnCtx, request, bridgeDir, s.nativeID, input, func(view amp.View) error {
		if len(s.rows) != 1 {
			return errors.New("missing stored export")
		}

		return amp.VerifyMirror(s.rows[0], view)
	}, func(data []byte) error { return s.consumeFrame(turnCtx, t, data) })
	t.evidence = evidence

	s.agent.observe.RecordProcessExit(turnCtx, "exited", nil)

	return s.finishTurn(turnCtx, t, params, result, stderr, runErr)
}

func (s *session) consumeFrame(ctx context.Context, t *turn, data []byte) error {
	frame, err := amp.DecodeFrame(data)
	if err != nil {
		return err
	}

	if !t.started {
		t.started = true

		if err := s.openStream(ctx); err != nil {
			return err
		}
	}

	s.emitRawEvent(context.WithoutCancel(ctx), data)

	if frame.SessionID != "" && frame.SessionID != s.nativeID {
		s.poisonSession(ctx, "native_session_identity_drift")

		return errIdentityDrift
	}

	if t.state.terminal {
		return nil
	}

	if frame.Type == "system" && frame.Subtype == "init" {
		if frame.AgentMode != "" {
			s.mu.Lock()
			changed := s.options.Mode != frame.AgentMode
			s.options.Mode = frame.AgentMode
			s.mu.Unlock()

			if changed {
				return s.emit(context.WithoutCancel(ctx), acp.SessionUpdate{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: s.configOptions()}})
			}
		}

		return nil
	}

	if frame.Type != roleAssistant && frame.Type != roleUser && frame.Type != fieldResult {
		return nil
	}

	if frame.SessionID == "" {
		return errors.New("native message lacks session identity")
	}

	if err := s.acceptTurn(ctx, t); err != nil {
		return err
	}

	return s.projectFrame(context.WithoutCancel(ctx), &t.state, frame)
}

func (s *session) finishTurn(ctx context.Context, t *turn, params acp.PromptRequest, result process.Result, stderr string, runErr error) (acp.PromptResponse, error) {
	s.mu.Lock()
	t.settling = true
	cancelled := t.cancelled
	s.mu.Unlock()

	reason, failure := turnVerdict(t, result, stderr, runErr, cancelled)

	settleCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), nativeCommandTimeout)
	defer finish()

	committed := false
	// The native process is reaped before any export or durable publication.
	// Identity drift attempts neither, so its turn is no more durable than one
	// whose commit failed.
	if !errors.Is(runErr, errIdentityDrift) {
		rows, exportErr := s.settleExport(settleCtx, t.evidence.View)
		if exportErr == nil {
			s.rows = rows
			exportErr = s.commitMirror(settleCtx)
		}

		committed = exportErr == nil

		// A commit failure is reported only when the turn itself did not
		// already fail, so it never relabels a real native cause.
		if exportErr != nil && failure == nil {
			s.agent.log.ErrorContext(settleCtx, "session mirror commit failed", slog.String("session_id", string(s.id)), slog.String("reason", exportErr.Error()))

			failure = nativeFailure(wire.CauseTransport, "session mirror commit failed")

			if t.state.imagesEmitted {
				failure = wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Stage: image.OutputStage, Reason: image.ReasonStorageFailed, Message: "image output is no longer available from the artifact store"})
			}
		}
	}

	if err := s.emitUsage(settleCtx, &t.state); err != nil && failure == nil {
		failure = err
	}

	info := s.sessionInfo()
	if err := s.emit(settleCtx, acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{Title: info.Title, UpdatedAt: info.UpdatedAt}}); err != nil && failure == nil {
		failure = err
	}

	// A turn the store never received ends its incarnation without a terminal
	// idle: the next incarnation's snapshot states the truth.
	if !committed {
		s.fenceStream()
	}

	if err := s.idle(settleCtx, t, reason, failure); err != nil && failure == nil {
		failure = err
	}

	s.fenceStream()

	if failure != nil {
		return acp.PromptResponse{}, failure
	}

	return acp.PromptResponse{StopReason: reason, UserMessageId: params.MessageId, Usage: t.state.usage}, nil
}

// nativeFailure renders one native turn outcome; wire.TurnFailed bounds and
// sanitizes the cause text.
func nativeFailure(cause, message string) *acp.RequestError {
	return wire.TurnFailed(vendor, wire.TurnFailure{Cause: cause, Message: message})
}

func turnVerdict(t *turn, result process.Result, stderr string, runErr error, cancelled bool) (acp.StopReason, error) {
	reason := acp.StopReasonEndTurn

	var failure error

	switch {
	case cancelled && t.evidence.Receipt == nil:
		reason = acp.StopReasonCancelled
	case errors.Is(runErr, errIdentityDrift):
		failure = nativeFailure(wire.CauseTransport, errIdentityDrift.Error())
	case t.evidence.Receipt != nil && t.evidence.Receipt.Status == amp.StatusCancelled:
		reason = acp.StopReasonCancelled
	case t.state.terminal && t.state.failed && t.evidence.Receipt != nil && t.evidence.Receipt.Status == amp.StatusDone:
		failure = nativeFailure(wire.CauseTransport, "Amp stream failed after the remote turn completed")
	case t.state.terminal && t.state.failed:
		failure = nativeFailure(wire.CauseProvider, t.state.errorMessage)
	case t.evidence.Receipt != nil && t.evidence.Receipt.Status == amp.StatusError:
		failure = nativeFailure(wire.CauseProvider, "Amp ended the native turn with an error")
	case t.state.terminal && t.state.stopReason == resultMaxTurns:
		reason = acp.StopReasonMaxTurnRequests
	// An adapter-raised error is classified before the exit status, because
	// the adapter kills the child on one and a signalled child has no status.
	case runErr != nil:
		failure = nativeFailure(wire.CauseTransport, runErr.Error())
	case result.ExitCode != 0 && !t.evidence.Stopped:
		failure = nativeFailure(wire.CauseProcessExit, fmt.Sprintf("Amp process exited with status %d: %s", result.ExitCode, stderr))
	case t.evidence.Receipt == nil:
		failure = nativeFailure(wire.CauseTransport, "Amp exited without a native turn receipt")
	case !t.state.terminal:
		failure = nativeFailure(wire.CauseTransport, "Amp exited without a stream terminal result")
	case t.state.stopReason == "max_tokens":
		reason = acp.StopReasonMaxTokens
	case t.state.stopReason == "refusal":
		reason = acp.StopReasonRefusal
	}

	return reason, failure
}
