package ampacp

import (
	"context"
	"fmt"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
)

func (s *session) openStream(ctx context.Context) error {
	negotiated := s.lifecycleNegotiated()
	if !negotiated.Present() {
		return nil
	}

	s.lc.Fence()

	return s.lc.Open(ctx, fmt.Sprintf("%s:%d", s.id, s.agent.nextIncarnation()), negotiated, s.deliverLifecycle)
}

func (s *session) acceptTurn(ctx context.Context, t *turn) error {
	if t.accepted {
		return nil
	}

	t.accepted = true

	if !s.lifecycleNegotiated().Present() {
		return nil
	}

	return s.lc.Accept(ctx, &t.Cycle, t.submission)
}

func (s *session) idle(ctx context.Context, t *turn, reason acp.StopReason, failure error) error {
	if !t.accepted || !s.lifecycleNegotiated().Present() {
		return nil
	}

	outcome := lifecycle.OutcomeSuccess
	if reason == acp.StopReasonCancelled {
		outcome = lifecycle.OutcomeCancelled
	}

	if reason == acp.StopReasonMaxTokens || reason == acp.StopReasonMaxTurnRequests {
		outcome = lifecycle.OutcomeLimit
	}

	if reason == acp.StopReasonRefusal {
		outcome = lifecycle.OutcomeRefused
	}

	if failure != nil {
		outcome = lifecycle.OutcomeFailed
		reason = ""
	}

	return s.lc.Idle(ctx, t.Cycle, string(reason), outcome)
}

func (s *session) fenceStream() { s.lc.Fence() }

func (s *session) deliverLifecycle(ctx context.Context, envelope map[string]any) error {
	if conn := s.agent.connection(); conn != nil {
		return conn.SessionUpdate(ctx, acp.SessionNotification{Meta: map[string]any{wire.LifecycleKey: envelope}, SessionId: s.id, Update: acp.SessionUpdate{SessionInfoUpdate: &acp.SessionSessionInfoUpdate{}}})
	}

	return nil
}
