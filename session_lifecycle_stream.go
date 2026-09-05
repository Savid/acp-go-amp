package ampacp

import (
	"context"
	"errors"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/lifecycle"
)

// A prompt-contained incarnation names three things, all derived from the
// incarnation identity so a reader can see at a glance which stream a cycle or a
// turn belongs to.
const (
	lifecycleOpenCycleSuffix = "/open"
	lifecycleCycleSuffix     = "/cycle"
	lifecycleTurnSuffix      = "/turn"
	// lifecycleBarrierPrefix labels the process-containment proof identifier with
	// the contained native root it covers.
	lifecycleBarrierPrefix = "amp-process-tree/"
)

// promptStream is one prompt's lifecycle incarnation. A prompt is one contained
// amp process, so a prompt is one whole native lifecycle source lifetime: each
// prompt opens its own stream with its own snapshot, sequence space, and
// entities, and the contained process exit fences it.
//
// A nil promptStream is the connection where the host offered nothing. Every
// method is a no-op on it, so the prompt path carries no conditional of its own.
type promptStream struct {
	session *agentSession
	client  agentClient
	stream  *lifecycle.Stream
	// openCycleID is the idle cycle the snapshot reports; cycleID is the one the
	// accepted submission runs in. They are distinct because a snapshot's
	// foreground state predates the turn.
	openCycleID string
	cycleID     string
	turnID      string
	// negotiated is the answer this connection carries. The quiescence fact a
	// boundary may state is governed by it and by that boundary's own proof,
	// never by what the opening snapshot happened to claim.
	negotiated    lifecycle.Negotiated
	authoritative bool
	// proof is the quiescence fact this configuration can state at stream open.
	proof lifecycle.QuiescenceFact
}

// openPromptStream opens the incarnation for one prompt and emits its snapshot,
// which must be the first lifecycle-bearing notification inside the prompt.
func (s *agentSession) openPromptStream(ctx context.Context) (*promptStream, error) {
	negotiated := s.agent.negotiatedLifecycle()
	if !negotiated.Present() {
		//nolint:nilnil // A connection with no answer opens no incarnation, and every promptStream method is nil-safe.
		return nil, nil
	}

	id, err := newSessionID()
	if err != nil {
		return nil, err
	}

	client := s.agent.connection()
	if client == nil {
		return nil, internalFailure(classLifecycleUnavailable)
	}

	incarnation := &promptStream{
		session:       s,
		client:        client,
		stream:        lifecycle.NewStream(id, negotiated),
		openCycleID:   id + lifecycleOpenCycleSuffix,
		cycleID:       id + lifecycleCycleSuffix,
		turnID:        id + lifecycleTurnSuffix,
		negotiated:    negotiated,
		authoritative: s.agent.options.hostAuthoritySupplied,
	}

	// The stream opens on a certified boundary only where one was actually
	// proven: the previous prompt's process was contained and its descendant set
	// enumerated empty, or this session never started a process at all. A
	// configuration that proves no class, and a session whose last boundary could
	// not state vacancy, both open on a negative fact rather than a guess.
	if incarnation.authoritative && s.vacancyProven() {
		incarnation.proof = lifecycle.QuiescenceFact{
			Quiescent: true,
			Source:    lifecycle.ProofClassProcessContainment,
		}
	}

	if err := incarnation.emit(ctx, lifecycle.SnapshotEvent(incarnation.openCycleID, incarnation.proof)); err != nil {
		return nil, err
	}

	return incarnation, nil
}

// accept records the dispatch linearization point: the native dispatcher has
// taken durable ownership of the submitted frame, and the foreground cycle the
// submission opens is running. A failure before this point emits no acceptance
// and creates neither submission nor turn.
func (p *promptStream) accept(ctx context.Context, submission lifecycle.Submission) error {
	if p == nil {
		return nil
	}

	if err := p.emit(ctx, lifecycle.AcceptedEvent(submission, p.turnID)); err != nil {
		return err
	}

	return p.emit(ctx, lifecycle.RunningEvent(p.cycleID, p.turnID))
}

// settle ends the turn and, where the configuration's proof class actually
// completed, states the quiescence fact this boundary produced. It runs after the
// containment boundary and after the durable commit: the terminal idle is emitted
// only once the foreground prefix is durable, and the quiescence fact only once
// the resumable snapshot is.
//
// The fact is governed by the answer and by the proof this boundary completed,
// never by the one the opening snapshot carried. An incarnation that opened
// unable to state a boundary still states the one it went on to prove.
func (p *promptStream) settle(ctx context.Context, outcome lifecycleOutcome, vacant bool) error {
	delivery, err := p.terminalDelivery(outcome, vacant)
	if err != nil {
		return err
	}

	return delivery.deliver(ctx)
}

func (p *promptStream) terminalDelivery(outcome lifecycleOutcome, vacant bool) (*promptTerminalDelivery, error) {
	if p == nil {
		return &promptTerminalDelivery{}, nil
	}

	notifications := make([]acp.SessionNotification, 0, 2)

	idle, err := p.prepare(lifecycle.IdleEvent(p.cycleID, p.turnID, outcome.stopReason, outcome.outcome))
	if err != nil {
		return nil, err
	}

	notifications = append(notifications, idle)

	if p.authoritative && vacant {
		quiescence, emitErr := p.prepare(lifecycle.QuiescenceEvent(lifecycle.QuiescenceFact{
			Quiescent: true,
			Source:    lifecycle.ProofClassProcessContainment,
			Watermark: p.stream.State().ReducedThrough,
			Barrier:   lifecycleBarrierPrefix + "authority",
		}))
		if emitErr != nil {
			return nil, emitErr
		}

		notifications = append(notifications, quiescence)
	}

	return &promptTerminalDelivery{stream: p, notifications: notifications}, nil
}

// fence ends this incarnation. A prompt is one contained amp process, so a prompt
// is one whole native lifecycle source lifetime: the incarnation ends where the
// prompt does, whether it settled or failed, and the fence is that end-of-emissions
// mark rather than a second claim about containment. It is recorded on the reducer
// that judges every emission, so an event attempted after it is refused at this
// adapter with the same verdict a consumer of these bytes would reach.
//
// `session/close` fences nothing of its own here: the incarnation belongs to the
// prompt, and a close waits out the settlement that already ended it.
func (p *promptStream) fence() {
	if p == nil {
		return
	}

	p.stream.Fence()
}

// emit claims the next sequence, validates the event through the same reducer the
// canonical vectors drive, and delivers it on its own identity-only carrier. An
// event this adapter cannot state truthfully fails the prompt here rather than
// reaching a consumer.
func (p *promptStream) emit(ctx context.Context, event lifecycle.Event) error {
	notification, err := p.prepare(event)
	if err != nil {
		return err
	}

	return p.deliver(ctx, notification)
}

func (p *promptStream) prepare(event lifecycle.Event) (acp.SessionNotification, error) {
	envelope, err := p.stream.Emit(event)
	if err != nil {
		// The reducer's verdict is joined rather than sent: the wire keeps the
		// closed token, and an embedding host and this adapter's log keep the
		// violation that produced it.
		return acp.SessionNotification{}, errors.Join(internalFailure(classLifecycleViolation), err)
	}

	return acp.SessionNotification{
		SessionId: p.session.id,
		Meta:      map[string]any{lifecycle.MetaKey: envelope},
		Update:    acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}},
	}, nil
}

func (p *promptStream) deliver(ctx context.Context, notification acp.SessionNotification) error {
	if p == nil || p.client == nil {
		return internalFailure(classLifecycleUnavailable)
	}

	// The envelope rides the notification's own `_meta`, beside sessionId and
	// update, and the carrier sets neither title nor updatedAt: a carrier mutates
	// no state, so it can never be coalesced away with the envelope on it.
	return invokeExternalResult(ctx, func() error { return p.client.SessionUpdate(ctx, notification) })
}
