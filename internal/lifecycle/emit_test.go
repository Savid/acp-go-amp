package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// containedConfiguration is the answer a prompt-contained incarnation whose
// containment proves whole-tree vacancy gives: no channel between prompts, no
// activity kind, and the process-containment proof class.
func containedConfiguration() Negotiated {
	return Negotiated{
		Versions:                []int{Version},
		AuthoritativeQuiescence: true,
		QuiescenceSource:        ProofClassProcessContainment,
		ActivityKinds:           []ActivityKind{},
	}
}

// emitPromptIncarnation drives the exact shape one prompt-contained prompt emits.
func emitPromptIncarnation(t *testing.T, stream *Stream) []map[string]any {
	t.Helper()

	proof := QuiescenceFact{Quiescent: true, Source: ProofClassProcessContainment}
	submission := Submission{SubmissionID: "sub-1", ClientNonce: "non-1", RunID: "run-1"}

	envelopes := make([]map[string]any, 0, 5)

	for _, event := range []Event{
		SnapshotEvent("cyc-0", proof),
		AcceptedEvent(submission, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
	} {
		envelope, err := stream.Emit(event)
		require.NoError(t, err)

		envelopes = append(envelopes, envelope)
	}

	settled := QuiescenceFact{
		Quiescent: true,
		Source:    ProofClassProcessContainment,
		Watermark: stream.State().ReducedThrough,
		Barrier:   "contained-exit-1",
	}

	envelope, err := stream.Emit(QuiescenceEvent(settled))
	require.NoError(t, err)

	return append(envelopes, envelope)
}

// TestEmittedStreamReducesThroughTheSameReducer proves the emitted bytes are
// wire-legal by the only measure that counts: decoding them from a
// session/update notification and reducing them through the reducer the family
// battery drives.
func TestEmittedStreamReducesThroughTheSameReducer(t *testing.T) {
	t.Parallel()

	negotiated := containedConfiguration()
	envelopes := emitPromptIncarnation(t, NewStream("strm-1", negotiated))
	reducer := NewReducer(Options{Negotiated: negotiated})

	for index, envelope := range envelopes {
		params, err := json.Marshal(map[string]any{
			"sessionId": "sess-1",
			"update":    map[string]any{sessionUpdateField: string(CarrierSessionInfo)},
			metaField:   map[string]any{MetaKey: envelope},
		})
		require.NoError(t, err)
		require.NoError(t, reducer.ReduceSessionUpdate(params), "envelope %d", index)
	}

	state := reducer.State()
	require.Equal(t, "strm-1", state.StreamID)
	require.Equal(t, uint64(5), state.ReducedThrough)
	require.Equal(t, []TurnRecord{{
		TurnID:       "turn-1",
		Origin:       CauseSubmission,
		Terminal:     true,
		Outcome:      OutcomeSuccess,
		SubmissionID: "sub-1",
		ClientNonce:  "non-1",
		RunID:        "run-1",
		CycleID:      "cyc-1",
		StopReason:   StopReasonEndTurn,
	}}, state.Turns)
	require.True(t, state.Quiescence.Certified)
	require.Equal(t, uint64(4), state.Quiescence.Watermark)
	require.Equal(t, "contained-exit-1", state.Quiescence.Barrier)
}

// TestEmitClaimsTheSequenceBeforeDelivery proves a refused event consumes its
// sequence: a counter that advanced only on success would make loss invisible,
// which is the exact failure contiguity exists to expose.
func TestEmitClaimsTheSequenceBeforeDelivery(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())

	_, err := stream.Emit(RunningEvent("cyc-1", "turn-1"))
	require.ErrorAs(t, err, new(*ViolationError))
	require.Equal(t, uint64(1), stream.sequence)
}

// TestSnapshotStatesAnUnprovenBoundaryAsNotQuiescent proves a configuration with
// no proof class emits a negative fact rather than a `none` sentinel or a
// present-and-empty source.
func TestSnapshotStatesAnUnprovenBoundaryAsNotQuiescent(t *testing.T) {
	t.Parallel()

	degenerate := Negotiated{Versions: []int{Version}, ActivityKinds: []ActivityKind{}}

	envelope, err := NewStream("strm-1", degenerate).Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)

	event, ok := envelope[fieldEvent].(map[string]any)
	require.True(t, ok)
	require.Equal(t, map[string]any{fieldQuiescent: false}, event[fieldQuiescence])
}

// TestAcceptanceOmitsAnAbsentRunID proves an optional handle is omitted rather
// than emitted empty: an empty opaque identifier fails closed on the reader.
func TestAcceptanceOmitsAnAbsentRunID(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())

	_, err := stream.Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)

	envelope, err := stream.Emit(AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"))
	require.NoError(t, err)

	event, ok := envelope[fieldEvent].(map[string]any)
	require.True(t, ok)
	require.NotContains(t, event, fieldRunID)
}

// emitted frames one rendered envelope as the notification a consumer decodes,
// which is the only measure of whether the emitter stated what it emitted.
func emitted(t *testing.T, envelope map[string]any) json.RawMessage {
	t.Helper()

	params, err := json.Marshal(map[string]any{
		"sessionId": "sess-1",
		"update":    map[string]any{sessionUpdateField: string(CarrierSessionInfo)},
		metaField:   map[string]any{MetaKey: envelope},
	})
	require.NoError(t, err)

	return params
}

// TestRichSnapshotIsEncodedWhole pins that a snapshot's nonterminal sets survive
// encoding. A set rendered empty over entities the reducer accepted would assert
// a vacancy the emitter never claimed, and a consumer would read the incarnation
// as holding nothing live.
func TestRichSnapshotIsEncodedWhole(t *testing.T) {
	t.Parallel()

	blocking := true
	snapshot := Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{
			State:   ForegroundRequiresAction,
			CycleID: "cyc-1",
			TurnID:  "turn-1",
			Origin:  CauseSubmission,
		},
		Activities: []ActivityUpdate{{
			ActivityID:   "act-1",
			Kind:         ActivitySubagent,
			State:        ActivityRunning,
			ToolCallID:   "tool-1",
			Cause:        CauseSubmission,
			OriginTurnID: "turn-1",
			RunID:        "run-1",
			Progress:     json.RawMessage(`{"done":1}`),
		}},
		Actions: []ActionUpdate{{
			ActionID:         "acn-1",
			Kind:             ActionPermission,
			State:            ActionPending,
			Owner:            Owner{Type: OwnerTurn, ID: "turn-1"},
			RunID:            "run-1",
			BlocksForeground: &blocking,
		}},
	}}

	envelope, err := NewStream("strm-1", richConfiguration()).Emit(snapshot)
	require.NoError(t, err)

	delivery, err := DecodeSessionUpdate(emitted(t, envelope), richConfiguration())
	require.NoError(t, err)
	require.Equal(t, snapshot, delivery.Event, "the decoded assertion is the one that was emitted")
}

// TestActivityAndActionUpdatesAreEncoded pins that every event the reducer admits
// is one the encoder can state. An event committed to the projection and lost on
// the wire would leave the emitter's own state ahead of every consumer's.
func TestActivityAndActionUpdatesAreEncoded(t *testing.T) {
	t.Parallel()

	blocking := false
	stream := NewStream("strm-1", richConfiguration())

	_, err := stream.Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)

	_, err = stream.Emit(AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"))
	require.NoError(t, err)

	_, err = stream.Emit(RunningEvent("cyc-1", "turn-1"))
	require.NoError(t, err)

	activity := Event{Type: EventActivityUpdate, Activity: &ActivityUpdate{
		ActivityID:   "act-1",
		Kind:         ActivityTask,
		State:        ActivityRunning,
		Cause:        CauseSubmission,
		OriginTurnID: "turn-1",
	}}

	envelope, err := stream.Emit(activity)
	require.NoError(t, err)

	delivery, err := DecodeSessionUpdate(emitted(t, envelope), richConfiguration())
	require.NoError(t, err)
	require.Equal(t, activity, delivery.Event)

	action := Event{Type: EventActionUpdate, Action: &ActionUpdate{
		ActionID:         "acn-1",
		Kind:             ActionElicitation,
		State:            ActionPending,
		Owner:            Owner{Type: OwnerActivity, ID: "act-1"},
		BlocksForeground: &blocking,
	}}

	envelope, err = stream.Emit(action)
	require.NoError(t, err)

	delivery, err = DecodeSessionUpdate(emitted(t, envelope), richConfiguration())
	require.NoError(t, err)
	require.Equal(t, action, delivery.Event)
}
