package ampacp

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/acp-go-sdk"
	nativeamp "github.com/savid/acp-go-amp/internal/amp"
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
	agent.retainNegotiatedLifecycle(answer)

	if conn != nil {
		agent.setConnection(conn)
	}

	return &agentSession{agent: agent, id: "sess-stream"}
}

// authoritativeAnswer is the answer a configuration whose containment enumerates
// the whole descendant tree gives.
func authoritativeAnswer() lifecycle.Negotiated {
	return lifecycle.Negotiated{
		Versions:                []int{1},
		AuthoritativeQuiescence: true,
		QuiescenceSource:        lifecycle.ProofClassProcessContainment,
		ActivityKinds:           []lifecycle.ActivityKind{},
	}
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
	require.NoError(t, incarnation.settle(t.Context(), lifecycleOutcome{}, nativeamp.ContainmentProof{}))
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
	require.ErrorContains(t, err, "lifecycle delivery unavailable")
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
			nativeamp.ContainmentProof{}), "transport refused")
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
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess}, nativeamp.ContainmentProof{})

	var requestErr *acp.RequestError

	require.ErrorAs(t, err, &requestErr)

	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "amp_lifecycle_violation", data[jsonFieldError])
	require.Len(t, client.envelopes(t), 1, "a refused event is never published")
}

func TestTerminalPreparationRefusesAnInvalidQuiescenceAfterIdle(t *testing.T) {
	client := &lifecycleClient{}
	session := lifecycleStreamSession(t, authoritativeAnswer(), client)
	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))

	// The stream reducer retains the negotiated process-containment authority.
	// Mutating only the publisher-side source makes the idle valid and the
	// following quiescence claim invalid, pinning the second preparation seam.
	incarnation.negotiated.QuiescenceSource = lifecycle.ProofClassNativeSettledBarrier
	_, err = incarnation.terminalDelivery(
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		nativeamp.ContainmentProof{Root: 42, Proven: true},
	)
	require.ErrorContains(t, err, string(lifecycle.ViolationUnnegotiatedFact))
	require.Len(t, client.envelopes(t), 3, "prepared terminal facts are not delivered before the whole pair validates")
}

// TestFencedIncarnationPublishesNothingFurther pins the end-of-emissions mark a
// prompt leaves behind. A prompt is one contained amp process, so the incarnation
// ends with the prompt: an event attempted afterwards is refused at this adapter
// with the verdict a consumer of these bytes would reach, and no consumer ever
// sees it. The absent stream fences as harmlessly as it emits.
func TestFencedIncarnationPublishesNothingFurther(t *testing.T) {
	client := &lifecycleClient{}
	session := lifecycleStreamSession(t, authoritativeAnswer(), client)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	require.NoError(t, incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		nativeamp.ContainmentProof{Root: 4242, Proven: true}))
	require.True(t, incarnation.stream.Fenced(), "terminal delivery ends the incarnation")

	published := len(client.envelopes(t))

	err = incarnation.emit(t.Context(), lifecycle.RunningEvent("cyc-2", "turn-2"))

	var requestErr *acp.RequestError

	require.ErrorAs(t, err, &requestErr)

	data, ok := requestErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "amp_lifecycle_violation", data[jsonFieldError])
	require.Contains(t, data[keyDetail], string(lifecycle.ViolationStaleStream))
	require.Len(t, client.envelopes(t), published, "a fenced incarnation publishes nothing")

	var absent *promptStream

	absent.fence()
}

// TestAuthoritativeStreamStatesTheProvenBoundary pins the two facts only a
// whole-tree proof produces: the snapshot opens on a certified boundary, and the
// settled turn is followed by the quiescence fact the completed proof produced.
func TestAuthoritativeStreamStatesTheProvenBoundary(t *testing.T) {
	client := &lifecycleClient{}
	session := lifecycleStreamSession(t, authoritativeAnswer(), client)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	require.NoError(t, incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		nativeamp.ContainmentProof{Root: 4242, Proven: true}))

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
	require.Equal(t, "amp-process-tree/4242", settled["barrier"])

	state := reduceEmittedStream(t, client, authoritativeAnswer()).State()
	require.True(t, state.Quiescence.Certified)
	require.Equal(t, uint64(4), state.Quiescence.Watermark)
}

// TestUnprovenVacancyStatesNoQuiescenceFact pins that an advertised proof class
// is still only emitted when the proof actually completed: a boundary that
// enumerated nothing states nothing.
func TestUnprovenVacancyStatesNoQuiescenceFact(t *testing.T) {
	client := &lifecycleClient{}
	session := lifecycleStreamSession(t, authoritativeAnswer(), client)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	require.NoError(t, incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		nativeamp.ContainmentProof{Root: 7, Descendants: 2, Proven: true}))

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
	session := lifecycleStreamSession(t, authoritativeAnswer(), client)
	session.recordVacancy(false)

	_, err := session.openPromptStream(t.Context())
	require.NoError(t, err)

	event, ok := client.envelopes(t)[0]["event"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{"quiescent": false}, event["quiescence"])
}

// TestRecoveredVacancyStatesTheBoundaryItProved pins that the quiescence fact is
// governed by the proof the settling boundary completed, never by the fact the
// opening snapshot carried: an incarnation that opened unable to state a
// boundary still states the one it went on to prove.
func TestRecoveredVacancyStatesTheBoundaryItProved(t *testing.T) {
	client := &lifecycleClient{}
	session := lifecycleStreamSession(t, authoritativeAnswer(), client)
	session.recordVacancy(false)

	incarnation, err := session.openPromptStream(t.Context())
	require.NoError(t, err)
	require.NoError(t, incarnation.accept(t.Context(), lifecycle.Submission{SubmissionID: "s", ClientNonce: "n"}))
	require.NoError(t, incarnation.settle(t.Context(),
		lifecycleOutcome{stopReason: "end_turn", outcome: lifecycle.OutcomeSuccess},
		nativeamp.ContainmentProof{Root: 4242, Proven: true}))

	require.Equal(t, []string{
		"lifecycle_snapshot", "prompt_accepted", "state_update", "state_update", "quiescence_update",
	}, client.eventTypes(t))

	state := reduceEmittedStream(t, client, authoritativeAnswer()).State()
	require.True(t, state.Quiescence.Certified)
	require.Equal(t, "amp-process-tree/4242", state.Quiescence.Barrier)
}
