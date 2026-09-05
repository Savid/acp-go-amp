package ampacp

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

// failingLifecycleClient fails the nth session/update it is handed, so an
// emission failure can be driven at any point in the ordered stream.
type failingLifecycleClient struct {
	lifecycleClient
	failAt int
	seen   int
}

func (c *failingLifecycleClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	c.seen++
	if c.seen == c.failAt {
		return errors.New("transport refused the notification")
	}

	return c.lifecycleClient.SessionUpdate(ctx, notification)
}

// lifecycleStreamSession builds a bare session on an agent whose connection and
// negotiated answer are the ones the test needs.
func lifecycleStreamSession(t *testing.T, answer lifecycle.Negotiated, conn agentClient) *agentSession {
	t.Helper()

	agent := newTestAgent()
	require.NoError(t, agent.retainNegotiatedLifecycle(answer))

	if conn != nil {
		agent.setConnection(conn)
	}

	return &agentSession{agent: agent, id: "sess-stream"}
}

// authoritativeAnswer is the exact lifecycle capability.
func authoritativeAnswer() lifecycle.Negotiated {
	return lifecycle.Negotiated{
		Version:                 1,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	}
}

func authoritativeLifecycleStreamSession(t *testing.T, conn agentClient) *agentSession {
	t.Helper()
	session := lifecycleStreamSession(t, authoritativeAnswer(), conn)
	session.agent.options.HostAuthority = newRecordingAuthority()
	session.agent.options.hostAuthoritySupplied = true

	return session
}

// TestPromptStreamIsAbsentWithoutAnAnswer pins that a connection the host offered
// nothing on opens no incarnation, and that every method on the absent stream is
// a no-op rather than a conditional at each call site.
func TestPromptStreamIsAbsentWithoutAnAnswer(t *testing.T) {
	session := lifecycleStreamSession(t, lifecycle.Negotiated{}, nil)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.Nil(t, incarnation)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{}))
	require.NoError(t, incarnation.settle(t.Context(), lifecycleOutcome{}, false))
}

// TestPromptStreamFailsWhenItCannotMintAnIncarnation pins that a stream this
// adapter cannot identify is never opened.
func TestPromptStreamFailsWhenItCannotMintAnIncarnation(t *testing.T) {
	session := lifecycleStreamSession(t, negotiatedAnswer(), nil)

	original := randRead
	randRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }

	t.Cleanup(func() { randRead = original })

	_, err := session.openPromptStream(t.Context())
	require.ErrorContains(t, err, "no entropy")
}

// TestPromptStreamWithoutAConnectionFailsExplicitly pins that negotiated
// lifecycle state is never treated as delivered when no client owns delivery.
func TestPromptStreamWithoutAConnectionFailsExplicitly(t *testing.T) {
	session := lifecycleStreamSession(t, negotiatedAnswer(), nil)

	incarnation, err := session.openPromptStream(t.Context())
	require.Nil(t, incarnation)
	requireInternalErrorData(t, err, map[string]any{jsonFieldError: errorInternalFailure, keyClass: classLifecycleUnavailable})
}

// TestPromptStreamFailsThePromptOnAnEmissionFailure pins that a stream this
// adapter cannot deliver fails the prompt rather than continuing with a gap it
// never announced.
func TestPromptStreamFailsThePromptOnAnEmissionFailure(t *testing.T) {
	for _, failAt := range []int{1, 2, 3, 4} {
		client := &failingLifecycleClient{failAt: failAt}
		session := lifecycleStreamSession(t, negotiatedAnswer(), client)

		incarnation, err := session.openPromptStream(t.Context())
		if failAt == 1 {
			require.ErrorContains(t, err, "transport refused")

			continue
		}

		require.NoError(t, err)

		err = incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"})
		if failAt < 4 {
			require.ErrorContains(t, err, "transport refused")

			continue
		}

		require.NoError(t, err)
		require.ErrorContains(t, incarnation.settle(t.Context(),
			lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
			false), "transport refused")
	}
}

// TestPromptStreamRefusesAnEventItCannotSupport pins emit-side fail-closed: an
// event the reducer refuses never reaches a consumer, and the prompt fails with
// the violation named.
func TestPromptStreamRefusesAnEventItCannotSupport(t *testing.T) {
	client := &lifecycleClient{}
	session := lifecycleStreamSession(t, negotiatedAnswer(), client)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)

	// Settling a turn no acceptance ever opened names an entity the stream never
	// introduced.
	err = incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess}, false)

	var requestErr *acp.RequestError

	require.ErrorAs(t, err, &requestErr)

	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, errorInternalFailure, data[jsonFieldError])
	require.Equal(t, classLifecycleViolation, data[keyClass])
	require.Len(t, client.envelopes(t), 1, "a refused event is never published")
}

// TestFencedIncarnationPublishesNothingFurther pins the end-of-emissions mark a
// prompt leaves behind. A prompt is one contained amp process, so the incarnation
// ends with the prompt: an event attempted afterwards is refused at this adapter
// with the verdict a consumer of these bytes would reach, and no consumer ever
// sees it. The absent stream fences as harmlessly as it emits.
func TestFencedIncarnationPublishesNothingFurther(t *testing.T) {
	client := &lifecycleClient{}
	session := authoritativeLifecycleStreamSession(t, client)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	require.NoError(t, incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		true))
	require.True(t, incarnation.stream.Fenced(), "terminal delivery ends the incarnation")

	published := len(client.envelopes(t))

	err = incarnation.emit(t.Context(), lifecycle.RunningEvent("cyc-2", "turn-2"))

	var requestErr *acp.RequestError

	require.ErrorAs(t, err, &requestErr)

	// The violation the reducer named is logged, never sent: the wire carries the
	// closed token and its class alone.
	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{jsonFieldError: errorInternalFailure, keyClass: classLifecycleViolation}, data)
	require.Len(t, client.envelopes(t), published, "a fenced incarnation publishes nothing")

	var absent *promptStream

	absent.fence()
}

// TestAuthoritativeStreamStatesTheProvenBoundary pins the two facts a completed
// authority boundary produces.
func TestAuthoritativeStreamStatesTheProvenBoundary(t *testing.T) {
	client := &lifecycleClient{}
	session := authoritativeLifecycleStreamSession(t, client)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	require.NoError(t, incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		true))

	require.Equal(t, []string{
		"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update", "quiescence_update",
	}, client.eventTypes(t))

	envelopes := client.envelopes(t)

	opening, ok := envelopes[0]["event"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{
		"quiescent": true, "source": "process-containment", "watermark": uint64(0),
	}, opening["quiescence"])

	settled, ok := envelopes[4]["event"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, uint64(4), settled["watermark"])
	require.Equal(t, "amp-process-tree/authority", settled["barrier"])

	state := reduceEmittedStream(t, client, authoritativeAnswer()).State()
	require.True(t, state.Quiescence.Certified)
	require.Equal(t, uint64(4), state.Quiescence.Watermark)
}

// TestUnprovenVacancyStatesNoQuiescenceFact pins that a quiescence fact is only
// emitted when the authority boundary actually completed.
func TestUnprovenVacancyStatesNoQuiescenceFact(t *testing.T) {
	client := &lifecycleClient{}
	session := authoritativeLifecycleStreamSession(t, client)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	require.NoError(t, incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		false))

	require.Equal(t, []string{
		"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update",
	}, client.eventTypes(t))
}

// TestUnprovenVacancyOpensTheNextIncarnationNegative pins that the snapshot's
// quiescence claim is backed by evidence rather than by the advertisement alone:
// a session whose last boundary could not enumerate an empty descendant set
// opens its next incarnation on a negative fact.
func TestUnprovenVacancyOpensTheNextIncarnationNegative(t *testing.T) {
	client := &lifecycleClient{}
	session := authoritativeLifecycleStreamSession(t, client)
	session.recordVacancy(false)

	_, err := session.openPromptStream(t.Context())
	require.NoError(t, err)

	event, ok := client.envelopes(t)[0]["event"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{"quiescent": false}, event["quiescence"])
}

// TestRecoveredVacancyStatesTheBoundaryItProved pins that a later successful
// authority boundary can certify an incarnation that opened uncertified.
func TestRecoveredVacancyStatesTheBoundaryItProved(t *testing.T) {
	client := &lifecycleClient{}
	session := authoritativeLifecycleStreamSession(t, client)
	session.recordVacancy(false)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	require.NoError(t, incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		true))

	require.Equal(t, []string{
		"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update", "quiescence_update",
	}, client.eventTypes(t))

	state := reduceEmittedStream(t, client, authoritativeAnswer()).State()
	require.True(t, state.Quiescence.Certified)
	require.Equal(t, "amp-process-tree/authority", state.Quiescence.Barrier)
}

func TestTerminalDeliveryRefusesUnnegotiatedQuiescence(t *testing.T) {
	negotiated := lifecycle.Negotiated{Version: lifecycle.Version}
	stream := lifecycle.NewStream("stream", negotiated)
	require.NoError(t, emitLifecyclePrefix(stream))

	prompt := &promptStream{
		session:       &agentSession{id: "T-lifecycle"},
		stream:        stream,
		cycleID:       "cycle",
		turnID:        "turn",
		authoritative: true,
	}
	_, err := prompt.terminalDelivery(
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess}, true,
	)
	require.Error(t, err)
}

func emitLifecyclePrefix(stream *lifecycle.Stream) error {
	if _, err := stream.Emit(lifecycle.SnapshotEvent("open", lifecycle.QuiescenceFact{})); err != nil {
		return err
	}
	if _, err := stream.Emit(lifecycle.AcceptedEvent(lifecycle.Submission{
		SubmissionID: "submission", ClientNonce: "nonce",
	}, "turn")); err != nil {
		return err
	}
	_, err := stream.Emit(lifecycle.RunningEvent("cycle", "turn"))

	return err
}
