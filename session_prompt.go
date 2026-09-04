package ampacp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-amp/internal/lifecycle"
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
	mu        sync.Mutex
	turn      *amp.Turn
	cancelCtx context.CancelFunc
	// settlement records the boundary rung that failed. Close can discharge an
	// owed durable rung by retrying it; containment and delivery failures remain
	// failures until a later teardown proves its own complete boundary.
	settlement   promptSettlement
	cancelled    chan struct{}
	completed    chan struct{}
	cancelOnce   sync.Once
	completeOnce sync.Once
}

// promptSettlement separates the boundary rungs a prompt can leave owed. The
// native turn's own outcome is deliberately absent: a model failure over a
// completed boundary is the prompt response's business, not close's.
type promptSettlement struct {
	containmentErr error
	commitErr      error
	deliveryErr    error
}

const (
	promptSettlementPhaseContainment = "containment"
	promptSettlementPhaseCommit      = "commit"
	promptSettlementPhaseDelivery    = "delivery"
)

type promptTerminalDelivery struct {
	mu            sync.Mutex
	stream        *promptStream
	notifications []acp.SessionNotification
	next          int
}

func (d *promptTerminalDelivery) deliver(ctx context.Context) error {
	if d == nil {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for d.next < len(d.notifications) {
		if beforeDelivery := d.stream.session.agent.options.runtime.beforeTerminalDelivery; beforeDelivery != nil {
			beforeDelivery(d.notifications[d.next])
		}

		if err := d.stream.deliver(ctx, d.notifications[d.next]); err != nil {
			return err
		}

		d.next++
	}

	if d.stream != nil {
		d.stream.fence()
	}

	return nil
}

func (s *agentSession) retainPendingTerminal(delivery *promptTerminalDelivery) {
	if delivery == nil || delivery.stream == nil {
		return
	}

	s.mu.Lock()
	s.pendingTerminal = delivery
	s.mu.Unlock()
}

func (s *agentSession) clearPendingTerminal(expect *promptTerminalDelivery) {
	s.mu.Lock()
	if s.pendingTerminal == expect {
		s.pendingTerminal = nil
		s.promptSettlement.deliveryErr = nil
	}
	s.mu.Unlock()
}

func (s *agentSession) recordPromptSettlement(settlement promptSettlement) {
	s.mu.Lock()
	s.promptSettlement = settlement
	s.mu.Unlock()
}

func (s *agentSession) retainedPromptSettlement() promptSettlement {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.promptSettlement
}

func (s *agentSession) hasPendingTerminal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pendingTerminal != nil
}

func (s *agentSession) deliverPendingTerminal(ctx context.Context) error {
	s.mu.Lock()
	delivery := s.pendingTerminal
	s.mu.Unlock()

	if delivery == nil {
		return nil
	}

	if err := delivery.deliver(ctx); err != nil {
		return err
	}

	s.clearPendingTerminal(delivery)

	return nil
}

// dischargeDeletedUnsyncedTerminal drops a terminal claim whose preceding
// commit was fenced by a delete. The tombstone made the retained frames final:
// publishing idle for that absent durable prefix would invert the settlement
// order. A terminal whose commit did land remains pending and must still be
// retransmitted exactly.
func (s *agentSession) dischargeDeletedUnsyncedTerminal() bool {
	s.mu.Lock()
	if s.pendingTerminal == nil || !s.mirrorUnsynced {
		s.mu.Unlock()

		return false
	}

	delivery := s.pendingTerminal
	s.pendingTerminal = nil
	s.unsyncedFrames = nil
	s.mirrorUnsynced = false
	s.promptSettlement.commitErr = nil
	s.promptSettlement.deliveryErr = nil
	s.mu.Unlock()

	delivery.stream.fence()

	return true
}

func (s promptSettlement) err() error {
	return errors.Join(s.containmentErr, s.commitErr, s.deliveryErr)
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

func (s *promptTurnState) complete(settlementErr error) {
	s.completeSettlement(promptSettlement{containmentErr: settlementErr})
}

func (s *promptTurnState) completeSettlement(settlement promptSettlement) {
	s.completeOnce.Do(func() {
		s.mu.Lock()
		s.settlement = settlement
		s.mu.Unlock()
		close(s.completed)
	})
}

func (s *promptTurnState) settled() bool {
	select {
	case <-s.completed:
		return true
	default:
		return false
	}
}

func (s *promptTurnState) awaitCompletion(ctx context.Context) error {
	return s.awaitSettlement(ctx).err()
}

func (s *promptTurnState) awaitSettlement(ctx context.Context) promptSettlement {
	select {
	case <-s.completed:
		s.mu.Lock()
		defer s.mu.Unlock()

		return s.settlement
	case <-ctx.Done():
		return promptSettlement{containmentErr: fmt.Errorf("%w: wait for active Amp turn cleanup: %v", amp.ErrContainmentIncomplete, ctx.Err())}
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

// lifecycleOutcome is the truthful end of one foreground cycle: the ACP v1 stop
// reason the native harness reported and the outcome recorded on the ending
// turn. It is read from the pair the turn itself produced, so a post-acceptance
// failure the v1 response later reports as a cancellation still settles the
// lifecycle turn as failed.
type lifecycleOutcome struct {
	stopReason string
	outcome    lifecycle.Outcome
}

// lifecycleOutcomeFor derives one cycle's recorded end from the turn's own
// result. No ACP v1 stop reason names a failure, so a failed cycle records its
// outcome and states no stop reason at all rather than borrowing one.
func lifecycleOutcomeFor(resp acp.PromptResponse, err error) lifecycleOutcome {
	if err != nil {
		return lifecycleOutcome{outcome: lifecycle.OutcomeFailed}
	}

	reason := string(resp.StopReason)

	switch resp.StopReason {
	case acp.StopReasonCancelled:
		return lifecycleOutcome{stopReason: reason, outcome: lifecycle.OutcomeCancelled}
	case acp.StopReasonMaxTokens, acp.StopReasonMaxTurnRequests:
		return lifecycleOutcome{stopReason: reason, outcome: lifecycle.OutcomeLimit}
	case acp.StopReasonRefusal:
		return lifecycleOutcome{stopReason: reason, outcome: lifecycle.OutcomeRefused}
	default:
		return lifecycleOutcome{stopReason: reason, outcome: lifecycle.OutcomeSuccess}
	}
}

// promptResult is one native turn's complete outcome. Every exit path from the
// message loop fills it in, including the frames the turn already streamed, so
// one commit point covers cancel, failure, timeout, and success alike and
// durable state never diverges from what the client was shown.
type promptResult struct {
	response   acp.PromptResponse
	err        error
	transcript []SessionStoreEntry
}

func (s *agentSession) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	if err := s.ready(); err != nil {
		return acp.PromptResponse{}, err
	}

	if !amp.HasAPIKey(composeEnv(s.agent.nativeEnvironmentBase(), s.env)) {
		return acp.PromptResponse{}, missingAPIKeyError()
	}

	if err := s.ensureMirrorSynced(ctx); err != nil {
		return acp.PromptResponse{}, err
	}

	// The submission identity is read and validated before dispatch: a prompt
	// this adapter cannot correlate writes no frame to the harness.
	submission, refusal := lifecycle.DecodePromptCorrelation(params.Meta, s.agent.negotiatedLifecycle())
	if refusal != nil {
		return acp.PromptResponse{}, lifecycleParamError(refusal)
	}

	release, err := s.acquireTurn(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer release()

	input, err := promptInputWithPolicy(ctx, params.Prompt, s.agent.promptImagePolicy())
	if err != nil {
		return acp.PromptResponse{}, err
	}

	state := newPromptTurnState()

	continueCtx, cancelContinue := context.WithCancel(ctx)
	defer cancelContinue()

	state.setCancelFunc(cancelContinue)

	// Admission and closure are one linearization: a prompt published here is a
	// prompt every later close or delete waits out, and a session already closed
	// or already fenced for delete admits none. Publishing after the checks that
	// gate it would let a teardown see no active prompt and complete while this
	// one went on to launch native work against the session it just tore down.
	if admitErr := s.admitPrompt(state); admitErr != nil {
		return acp.PromptResponse{}, admitErr
	}

	var incarnation *promptStream

	defer func() {
		recovered := recover()

		if recovered != nil && !state.settled() {
			settlement := s.containPromptPanic(state)

			if incarnation != nil {
				incarnation.fence()
			}

			s.recordPromptSettlement(settlement)
			state.completeSettlement(settlement)
		}

		if recovered == nil {
			state.complete(nil)
		}

		s.clearActivePrompt(state)

		if recovered != nil {
			panic(recovered)
		}
	}()

	ctx = withCallbackProvenance(ctx, s.agent, state)
	continueCtx = withCallbackProvenance(continueCtx, s.agent, state)

	// One prompt is one contained process, so one prompt is one incarnation: the
	// snapshot opening it is the first lifecycle-bearing notification inside the
	// prompt, and it precedes acceptance.
	incarnation, err = s.openPromptStream(ctx)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	s.agent.observe.RecordAmpProcessStart(continueCtx)
	// The trace carrier is the last phase: it composes over the session
	// environment under the platform key identity, so a caller-supplied
	// traceparent spelling can never displace the propagated context.
	promptEnv := composeEnv(s.env, s.agent.observe.InjectTraceEnv(continueCtx, nil))
	promptClient := s.clientWithEnv(promptEnv, s.mcpConfigFile)

	turn, err := s.launchNativeTurn(continueCtx, promptClient, input)
	if err != nil {
		s.recordScratchContainment(err)

		// A launch that failed accepted nothing and will emit nothing more, so
		// the incarnation ends here rather than at a settlement this prompt never
		// reaches. It precedes the latch below for the same reason it does there.
		incarnation.fence()

		// A launch that started a process and could not deliver its input owns a
		// tree its cleanup did not contain. That boundary is the latch's own
		// business, so it is published there: a close or delete waiting on this
		// prompt must not read an incomplete boundary as a settled one and go on
		// to remove the scratch state a surviving tree still runs against. It
		// outranks the cancel guard — a request whose boundary broke did not end
		// cleanly, whatever the host asked for.
		if !amp.ProcessContainmentComplete(err) {
			state.completeSettlement(promptSettlement{containmentErr: err})

			return acp.PromptResponse{}, unsettled(errors.Join(classifyNativePromptError(err), err))
		}

		if state.isCancelled() {
			return cancelledPromptResponse(nil, params.MessageId), nil
		}

		return acp.PromptResponse{}, classifyNativePromptError(err)
	}

	state.setTurn(turn)

	// Acceptance is the dispatch linearization point: the native dispatcher owns
	// the frame from here, so the turn exists and must be settled.
	if err := incarnation.accept(ctx, submission); err != nil {
		return s.settlePrompt(ctx, turn, state, incarnation, promptResult{err: err})
	}

	var timeoutCh <-chan time.Time

	if d := s.agent.options.TurnTimeout; d > 0 {
		ch, stop := invokeOwnedPair(func() (<-chan time.Time, func()) {
			return s.agent.options.runtime.newTurnTimer(d)
		})

		defer func() { invokeOwned(stop) }()

		timeoutCh = ch
	}

	result := s.runPromptTurn(ctx, turn, state, params.MessageId, timeoutCh)

	return s.settlePrompt(ctx, turn, state, incarnation, result)
}

// launchNativeTurn starts the one short-lived amp process this prompt runs on.
// The first prompt runs a thread-less `amp -x` execute: amp creates the
// server-side thread only now, so a session that is never prompted never owns a
// remote thread. Later prompts continue the adopted thread.
func (s *agentSession) launchNativeTurn(ctx context.Context, client *amp.Client, input any) (*amp.Turn, error) {
	nativeID := s.nativeSessionID()

	if nativeID == "" {
		callbackCtx := withExactCallbackGeneration(ctx, "native:execute_thread")
		turn, err := invokeOwnedPair(func() (*amp.Turn, error) {
			return s.agent.options.runtime.executeThread(callbackCtx, client, input)
		})

		return turn, err
	}

	callbackCtx := withExactCallbackGeneration(ctx, "native:continue_thread")
	turn, err := invokeOwnedPair(func() (*amp.Turn, error) {
		return s.agent.options.runtime.continueThread(callbackCtx, client, nativeID, input)
	})

	return turn, err
}

// unsettledPrompt marks a failure that stopped the prompt from settling. The
// v1 response for a cancelled request is a cancelled success, but only once the
// prompt actually settled: reporting one over a commit the store never took, or
// a boundary that was never emitted, would tell the host a turn ended cleanly
// while its durable state and its lifecycle stream say otherwise.
type unsettledPromptError struct{ err error }

func (e unsettledPromptError) Error() string { return e.err.Error() }
func (e unsettledPromptError) Unwrap() error { return e.err }

// unsettled marks a settlement failure, keeping the underlying failure's exact
// wire shape for the caller that reads it.
func unsettled(err error) error { return unsettledPromptError{err: err} }

// settlePrompt closes the turn in the one order the close-fenced proof class
// binds: the native terminal is already past, so the whole-tree containment and
// vacancy proof completes first, then the durable commit, then the terminal idle,
// then the quiescence fact the completed proof produced, and only then the v1
// response. A failed commit or an incomplete boundary fails the prompt and emits
// no terminal idle, so no boundary claims a foreground prefix the store does not
// hold and the incarnation ends unsettled for the next snapshot to state.
//
// Settlement runs on a context detached from the request's. A cancelled request
// still gets its durable commit, its terminal boundary, and its fenced stream:
// the cancellation ends the native turn, never the settlement of what that turn
// already streamed.
func (s *agentSession) settlePrompt(
	ctx context.Context,
	turn *amp.Turn,
	state *promptTurnState,
	incarnation *promptStream,
	result promptResult,
) (acp.PromptResponse, error) {
	settleCtx := context.WithoutCancel(ctx)
	phase := promptSettlementPhaseContainment
	contained := false

	var terminal *promptTerminalDelivery

	var settlement promptSettlement

	defer func() {
		recovered := recover()

		if recovered != nil {
			panicErr := errAgentGoroutinePanic

			switch phase {
			case promptSettlementPhaseCommit:
				settlement.commitErr = errors.Join(settlement.commitErr, panicErr)
			case promptSettlementPhaseDelivery:
				settlement.deliveryErr = errors.Join(settlement.deliveryErr, panicErr)
			default:
				settlement.containmentErr = errors.Join(settlement.containmentErr, panicErr)
			}

			if !contained {
				panicSettlement := s.containPromptPanic(state)
				settlement.containmentErr = errors.Join(settlement.containmentErr, panicSettlement.containmentErr)
			}

			if terminal == nil {
				incarnation.fence()
			}
		}

		s.recordPromptSettlement(settlement)
		state.completeSettlement(settlement)

		if recovered != nil {
			panic(recovered)
		}
	}()

	settleCtx = withExactCallbackGeneration(settleCtx, "native:settle_turn")
	_, closeErr := invokeOwnedPair(func() (struct{}, error) {
		return struct{}{}, s.agent.options.runtime.settleTurn(turn)
	})
	s.recordScratchContainment(closeErr)
	s.recordVacancy(s.agent.options.hostAuthoritySupplied && amp.ProcessContainmentComplete(closeErr))
	contained = amp.ProcessContainmentComplete(closeErr)

	// Settlement is what the completion latch publishes: the boundary's own
	// failures, separated by rung. A native turn that failed over a boundary which
	// completed, committed, and delivered its terminal facts did settle, so a
	// concurrent close or delete succeeds while the prompt still answers the host
	// with the native failure. Conflating the two would both fail a close that
	// nothing went wrong for and hide a real settlement failure behind the native
	// error the prompt reports first.
	if !amp.ProcessContainmentComplete(closeErr) {
		settlement.containmentErr = closeErr

		incarnation.fence()

		return acp.PromptResponse{}, unsettled(errors.Join(result.err, closeErr))
	}

	terminal, closeErr = incarnation.terminalDelivery(lifecycleOutcomeFor(result.response, result.err), s.agent.options.hostAuthoritySupplied)
	if closeErr != nil {
		settlement.deliveryErr = closeErr

		incarnation.fence()

		return acp.PromptResponse{}, unsettled(firstError(result.err, closeErr))
	}

	s.retainPendingTerminal(terminal)

	phase = promptSettlementPhaseCommit

	if commitErr := s.persistAfterTurn(settleCtx, result.transcript); commitErr != nil {
		settlement.commitErr = commitErr

		return acp.PromptResponse{}, unsettled(firstError(result.err, commitErr))
	}

	phase = promptSettlementPhaseDelivery

	if deliveryErr := terminal.deliver(settleCtx); deliveryErr != nil {
		settlement.deliveryErr = deliveryErr

		return acp.PromptResponse{}, unsettled(firstError(result.err, deliveryErr))
	}

	s.clearPendingTerminal(terminal)

	if result.err != nil {
		return acp.PromptResponse{}, result.err
	}

	return result.response, nil
}

func (s *agentSession) containPromptPanic(state *promptTurnState) promptSettlement {
	turn := state.currentTurn()
	if turn == nil {
		boundaryErr := fmt.Errorf("%w: native prompt panicked before publishing its turn handle", amp.ErrContainmentIncomplete)
		s.recordScratchContainment(boundaryErr)
		s.recordVacancy(false)

		return promptSettlement{containmentErr: boundaryErr, deliveryErr: errAgentGoroutinePanic}
	}

	boundaryErr := s.settleTurnAfterPanic(turn)
	s.recordScratchContainment(boundaryErr)
	s.recordVacancy(s.agent.options.hostAuthoritySupplied && amp.ProcessContainmentComplete(boundaryErr))

	return promptSettlement{containmentErr: boundaryErr, deliveryErr: errAgentGoroutinePanic}
}

func (s *agentSession) settleTurnAfterPanic(turn *amp.Turn) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("%w: panic while containing native prompt", amp.ErrContainmentIncomplete)
		}
	}()

	return s.agent.options.runtime.settleTurn(turn)
}

// firstError keeps the first failure's exact wire shape. A store outage behind a
// native failure is retained for the next prompt to retry loudly rather than
// flattened into the error the caller is about to read.
func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// runPromptTurn consumes the native stream to its end. Every exit reports the
// frames it accumulated, so the caller's single commit point covers all of them.
func (s *agentSession) runPromptTurn(
	ctx context.Context,
	turn *amp.Turn,
	state *promptTurnState,
	messageID *string,
	timeoutCh <-chan time.Time,
) promptResult {
	var (
		transcript       []SessionStoreEntry
		promptUsage      *acp.Usage
		terminal         *amp.ResultMessage
		stopReason       string
		finalMessageID   string
		baseTranscriptAt = s.transcriptFrameCount()
	)

	for {
		select {
		case msg, ok := <-turn.Messages():
			if !ok {
				response, err := s.resolveTerminal(ctx, state, terminal, promptUsage, messageID, stopReason, finalMessageID, turn)

				return promptResult{response: response, err: err, transcript: transcript}
			}

			if err := s.validateFrameSessionID(ctx, msg, state); err != nil {
				return promptResult{err: err, transcript: transcript}
			}

			frameID := ""

			transcriptJSON, err := s.prepareMessageImageArtifacts(ctx, msg)
			if err != nil {
				_ = s.interrupt(context.Background())

				return promptResult{err: err, transcript: transcript}
			}

			if transcriptJSON != "" {
				transcript = append(transcript, SessionStoreEntry(transcriptJSON))

				frameID = assistantMessageIdentity(s.id, baseTranscriptAt+len(transcript), msg)
				if frameID != "" {
					finalMessageID = frameID
				}
			}
			// Raw events are non-authoritative debug output: an emit failure is
			// recorded on the observer hook and the turn continues. It never
			// aborts the prompt turn nor interrupts the harness.
			if err := s.emitRawEvent(ctx, "stream-json", msg); err != nil {
				s.agent.observe.RecordRawEventEmitFailure(ctx, err)
			}

			if err := s.emitMessage(ctx, msg, true, frameID); err != nil {
				_ = s.interrupt(context.Background())

				return promptResult{err: err, transcript: transcript}
			}

			if usage := messageUsage(msg); usage != nil {
				promptUsage = usage
			}

			if reason := assistantStopReason(msg); reason != "" {
				stopReason = reason
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

			response, err := promptErrorResponse(ctx, state, promptUsage, messageID, err)

			return promptResult{response: response, err: err, transcript: transcript}
		case <-timeoutCh:
			response, err := s.resolveTurnDeadline(ctx, state, promptUsage, messageID)

			return promptResult{response: response, err: err, transcript: transcript}
		case <-state.cancelled:
			_ = s.interruptState(context.Background(), state)

			return promptResult{response: cancelledPromptResponse(promptUsage, messageID), transcript: transcript}
		case <-ctx.Done():
			state.cancel()
			_ = s.interruptState(context.Background(), state)

			return promptResult{response: cancelledPromptResponse(promptUsage, messageID), transcript: transcript}
		}
	}
}

// resolveTerminal maps the native stream's end to the v1 pair. The cancel guard
// runs before every mapping, success included: a cancel and a terminal frame can
// both be ready at the same select, and a turn the host already cancelled reports
// cancelled whichever the loop happened to read. The reported stop reason is the
// native harness's own: a turn stopped by a token ceiling or a turn-request
// ceiling says so instead of claiming it ended on its own.
func (s *agentSession) resolveTerminal(
	ctx context.Context,
	state *promptTurnState,
	terminal *amp.ResultMessage,
	usage *acp.Usage,
	messageID *string,
	stopReason string,
	finalMessageID string,
	turn turnErrorReader,
) (acp.PromptResponse, error) {
	if state.isCancelled() || ctx.Err() != nil {
		//nolint:nilerr // A cancelled turn reports cancelled, not the context's own error.
		return cancelledPromptResponse(usage, messageID), nil
	}

	if terminal == nil {
		return streamEndedWithoutTerminal(ctx, state, usage, messageID, turn)
	}

	if terminal.IsError {
		if isNativeCancelResult(terminal) {
			return cancelledPromptResponse(usage, messageID), nil
		}

		if terminal.Subtype == amp.SubtypeErrorMaxTurns {
			return acp.PromptResponse{
				StopReason:    acp.StopReasonMaxTurnRequests,
				Usage:         usage,
				UserMessageId: messageID,
			}, nil
		}
		// L1: fall back to result.result when result.error is empty so the real
		// provider cause is never lost.
		return acp.PromptResponse{}, turnFailure(causeProvider, firstNonEmpty(terminal.Error, terminal.Result))
	}

	return acp.PromptResponse{
		Meta:          ampMessageMeta(nil, finalMessageID),
		StopReason:    terminalStopReason(stopReason),
		Usage:         usage,
		UserMessageId: messageID,
	}, nil
}

// terminalStopReason maps the native assistant stop reason onto the ACP v1 enum.
// Amp reports a token ceiling on the assistant frame that hit it; every other
// value means the model finished the turn it was given.
func terminalStopReason(stopReason string) acp.StopReason {
	if stopReason == amp.StopReasonMaxTokens {
		return acp.StopReasonMaxTokens
	}

	return acp.StopReasonEndTurn
}

// assistantStopReason reads the stop reason off a main-agent assistant frame.
// Delegated activity reports its own ceilings and never ends the host turn.
func assistantStopReason(msg amp.Message) string {
	assistant, ok := msg.(*amp.AssistantMessage)
	if !ok || assistant.ParentToolUseID != "" {
		return ""
	}

	return assistant.StopReason
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

	return invokeExternalResult(ctx, func() error {
		return conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: s.id, Update: update})
	})
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

	if err := invokeExternalResult(ctx, func() error {
		return conn.NotifyExtension(ctx, RawEventMethod, capped)
	}); err != nil {
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

	// The binding is recorded as soon as it is adopted, so a thread this adapter
	// created is never orphaned by a crash before the turn settles. It carries no
	// pending-turn frame — the prompt loop appends the init frame after this
	// returns, and a prompt never starts with an unsynced mirror — so the
	// adoption commit adds exactly the adopted binding to the last committed
	// state and tombstones nothing. A failure here is not fatal: the turn's own
	// settlement commits the binding again.
	if err := s.persistAfterTurn(ctx, nil); err != nil {
		s.agent.log.DebugContext(ctx, "persist adopted amp thread id failed", slog.String(jsonFieldSessionID, string(s.id)), slog.String(jsonFieldError, err.Error()))
	}

	return nil
}

// promptImagePolicy carries the inbound media policy the prompt mapper enforces:
// the decoded byte limits and the handoff read root.
type promptImagePolicy struct {
	limits ImageLimits
	// handoffRoot is the host-supplied read root for the handoff input form.
	// Empty rejects every handoff-form block.
	handoffRoot string
}

func (a *Agent) promptImagePolicy() promptImagePolicy {
	return promptImagePolicy{
		limits:      a.options.ImageLimits,
		handoffRoot: a.options.InputHandoffRoot,
	}
}

func promptInputWithPolicy(ctx context.Context, blocks []acp.ContentBlock, policy promptImagePolicy) (map[string]any, error) {
	// An empty prompt is rejected fail-closed: there is nothing to send to the
	// native harness, so accepting it would spend a turn on silence.
	if len(blocks) == 0 {
		return nil, acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, jsonFieldField: fieldPrompt})
	}

	imageBudget := imagePromptBudget{limits: policy.limits, handoffRoot: policy.handoffRoot}

	defer imageBudget.closeHandoffRoot()

	content := make([]map[string]any, 0, len(blocks))
	for _, block := range blocks {
		// Mapping runs before the turn state exists, so a disconnect or a caller
		// cancellation has no other way to stop a prompt that names many files.
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		switch {
		case block.Text != nil:
			content = append(content, map[string]any{keyType: valText, valText: block.Text.Text})
		case block.Image != nil:
			// Both input forms converge here as base64 on the native message, so a
			// handoff path is never part of the request Amp receives.
			image, err := imageBudget.validateImageBlock(ctx, block.Image)
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

		// A text resource is flattened into the prompt verbatim, so it costs the
		// thread exactly what a blob of the same length costs. It is charged to the
		// one per-prompt accumulator every other inbound payload is charged to.
		if err := imageBudget.chargeText(int64(len(text.Text))); err != nil {
			return nil, err
		}

		parts := []string{"Embedded resource", "URI: " + text.Uri}
		if text.MimeType != nil && *text.MimeType != "" {
			parts = append(parts, "MIME: "+*text.MimeType)
		}

		parts = append(parts, "", text.Text)

		return map[string]any{keyType: valText, valText: strings.Join(parts, "\n")}, nil
	}

	if resource.BlobResourceContents != nil {
		blob := resource.BlobResourceContents

		declared := ""
		if blob.MimeType != nil {
			declared = *blob.MimeType
		}

		// A declaration that claims a raster type is routed into the image gates
		// whatever its case or parameters, so image bytes cannot reach the untyped
		// channel below and bypass the native envelope.
		if declaresRasterMediaType(declared) {
			image, err := imageBudget.validate(blob.Blob, declared)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				keyType: valImage,
				keySource: map[string]any{
					keyType:      valBase64,
					keyMediaType: declared,
					keyData:      image.base64,
				},
			}, nil
		}

		// A blob that declares no raster type has no native representation on Amp,
		// so it degrades to a reference: the model is told what was attached and
		// where it lives, and the payload stays out of the prompt.
		if err := imageBudget.admitBlob(blob.Blob); err != nil {
			return nil, err
		}

		parts := []string{"Embedded resource", "URI: " + blob.Uri}
		if declared != "" {
			parts = append(parts, "MIME: "+declared)
		}

		return map[string]any{keyType: valText, valText: strings.Join(parts, "\n")}, nil
	}

	return nil, unsupportedField(fieldPrompt)
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

	if errors.Is(err, amp.ErrContainmentIncomplete) {
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
