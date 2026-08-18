package ampacp

import (
	"context"
	"strconv"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
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
	stream  *lifecycle.Stream
	// openCycleID is the idle cycle the snapshot reports; cycleID is the one the
	// accepted submission runs in. They are distinct because a snapshot's
	// foreground state predates the turn.
	openCycleID string
	cycleID     string
	turnID      string
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

	incarnation := &promptStream{
		session:     s,
		stream:      lifecycle.NewStream(id, negotiated),
		openCycleID: id + lifecycleOpenCycleSuffix,
		cycleID:     id + lifecycleCycleSuffix,
		turnID:      id + lifecycleTurnSuffix,
	}

	// Nothing owned by this session is live: the previous prompt's process was
	// contained and exited, and a session that was never prompted never started
	// one. A configuration that proves whole-tree vacancy may state that as a
	// certified boundary fencing no sequence; one that cannot states a negative
	// fact rather than a guess.
	if negotiated.AuthoritativeQuiescence {
		incarnation.proof = lifecycle.QuiescenceFact{
			Quiescent: true,
			Source:    negotiated.QuiescenceSource,
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
// completed, states the quiescence fact the completed boundary produced. It runs
// after the containment boundary and after the durable commit: the terminal idle
// is emitted only once the foreground prefix is durable, and the quiescence fact
// only once the resumable snapshot is.
func (p *promptStream) settle(ctx context.Context, outcome lifecycleOutcome, proof amp.ContainmentProof) error {
	if p == nil {
		return nil
	}

	if err := p.emit(ctx, lifecycle.IdleEvent(p.cycleID, p.turnID, outcome.stopReason, outcome.outcome)); err != nil {
		return err
	}

	if !p.proof.Quiescent || !proof.Vacant() {
		return nil
	}

	return p.emit(ctx, lifecycle.QuiescenceEvent(lifecycle.QuiescenceFact{
		Quiescent: true,
		Source:    p.proof.Source,
		Watermark: p.stream.State().ReducedThrough,
		Barrier:   lifecycleBarrierPrefix + strconv.Itoa(proof.Root),
	}))
}

// emit claims the next sequence, validates the event through the same reducer the
// canonical vectors drive, and delivers it on its own identity-only carrier. An
// event this adapter cannot state truthfully fails the prompt here rather than
// reaching a consumer.
func (p *promptStream) emit(ctx context.Context, event lifecycle.Event) error {
	envelope, err := p.stream.Emit(event)
	if err != nil {
		return acp.NewInternalError(map[string]any{
			jsonFieldError: "amp_lifecycle_violation",
			keyDetail:      err.Error(),
		})
	}

	conn := p.session.agent.connection()
	if conn == nil {
		return nil
	}

	// The envelope rides the notification's own `_meta`, beside sessionId and
	// update, and the carrier sets neither title nor updatedAt: a carrier mutates
	// no state, so it can never be coalesced away with the envelope on it.
	return conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: p.session.id,
		Meta:      map[string]any{lifecycle.MetaKey: envelope},
		Update:    acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}},
	})
}
