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
		Version:                 Version,
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

// TestFenceEndsTheIncarnationOnTheJudgingReducer proves the fence is the same
// fact on both sides of the wire. It is recorded on the reducer that judges every
// emission, so an event attempted after it is refused here with the very token a
// consumer reducing these bytes past a session_closed would report — rather than
// published on an incarnation this adapter has already finished with.
func TestFenceEndsTheIncarnationOnTheJudgingReducer(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())
	emitPromptIncarnation(t, stream)

	require.False(t, stream.Fenced(), "a stream that settled its turn is not yet ended")

	stream.Fence()
	require.True(t, stream.Fenced())

	// Fencing twice is the same fact stated twice, not a second end.
	stream.Fence()
	require.True(t, stream.Fenced())

	before := stream.State()

	envelope, err := stream.Emit(RunningEvent("cyc-2", "turn-2"))
	require.Nil(t, envelope, "a fenced incarnation hands back nothing")

	var refusal *ViolationError

	require.ErrorAs(t, err, &refusal)
	require.Equal(t, ViolationStaleStream, refusal.Kind)
	require.Equal(t, uint64(6), stream.sequence, "the refused event still consumed its sequence")
	require.Equal(t, before.ReducedThrough, stream.State().ReducedThrough,
		"nothing reaches the projection of an incarnation already ended")
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

	degenerate := Negotiated{Version: Version}

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
// encoding. A set rendered empty over entities the snapshot holds would assert a
// vacancy the emitter never claimed, and the rendered-bytes sandwich would not
// catch it — the smaller snapshot decodes and reduces cleanly. Amp's own
// snapshots open empty on both sets; this drives the sets the shared Snapshot can
// carry, over a configuration amp itself never answers.
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
			ActionID:         "act-1",
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

// TestThisAdapterConstructsNoActivityOrActionEvent pins where "amp sends no
// action" is actually written: in the constructor set. The encoder's activity and
// action arms are consumer-side — they render the shared Event the decoder
// produces — and no frame this adapter publishes reaches them. An adapter that
// grew an emitter for either form would fail here rather than quietly start
// asserting facts its negotiated answer never claimed.
func TestThisAdapterConstructsNoActivityOrActionEvent(t *testing.T) {
	t.Parallel()

	submission := Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}
	proof := QuiescenceFact{Quiescent: true, Source: ProofClassProcessContainment}

	sent := []Event{
		SnapshotEvent("cyc-0", proof),
		AcceptedEvent(submission, "turn-1"),
		RunningEvent("cyc-1", "turn-1"),
		IdleEvent("cyc-1", "turn-1", StopReasonEndTurn, OutcomeSuccess),
		QuiescenceEvent(proof),
	}

	for _, event := range sent {
		require.NotEqual(t, EventActivityUpdate, event.Type)
		require.NotEqual(t, EventActionUpdate, event.Type)
		require.Nil(t, event.Activity)
		require.Nil(t, event.Action)
	}

	// The one form carrying the sets opens with both of them empty: a
	// prompt-contained incarnation holds nothing live at its first sequence.
	require.Empty(t, sent[0].Snapshot.Activities)
	require.Empty(t, sent[0].Snapshot.Actions)
}

// TestEncoderIsTotalOverEveryShapeTheEmitterAdmits pins why the two
// consumer-side arms stay. strictShape states an invariant of the decoder's own
// Event, so it admits all six discriminants; the encoder reads the payload the
// discriminant names. An encoder narrower than the check in front of it would
// send an admitted event to the default arm and dereference a nil State, turning
// the caller defect that check exists to name into a panic.
func TestEncoderIsTotalOverEveryShapeTheEmitterAdmits(t *testing.T) {
	t.Parallel()

	for _, event := range []Event{
		{Type: EventSnapshot, Snapshot: &Snapshot{}},
		{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{}},
		{Type: EventStateUpdate, State: &StateTransition{}},
		{Type: EventActivityUpdate, Activity: &ActivityUpdate{}},
		{Type: EventActionUpdate, Action: &ActionUpdate{}},
		{Type: EventQuiescenceUpdate, Quiescence: &QuiescenceFact{}},
	} {
		require.True(t, event.strictShape(), "%s is admitted", event.Type)
		require.Equal(t, string(event.Type), encodeEvent(event)[fieldType],
			"%s renders as the form it names", event.Type)
	}
}

// TestActivityAndActionUpdatesAreEncoded pins that the consumer-side arms render
// faithfully, over a configuration amp itself never answers. A stream that
// accepted one of these and rendered it lossily would hand a consumer a smaller
// truth than the one its own projection holds.
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
		ActionID:         "act-1",
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

// TestEmitValidatesTheRenderedBytes pins that emitter self-validation runs on
// the notification the consumer will actually read, not on the struct behind it.
// The progress member below reduces perfectly well as a Go value and renders to
// bytes no consumer can decode: an emitter that judged the struct would publish
// it, and the stream would fail at every consumer instead of at its source.
func TestEmitValidatesTheRenderedBytes(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", richConfiguration())

	_, err := stream.Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)

	_, err = stream.Emit(AcceptedEvent(Submission{SubmissionID: "sub-1", ClientNonce: "non-1"}, "turn-1"))
	require.NoError(t, err)

	_, err = stream.Emit(RunningEvent("cyc-1", "turn-1"))
	require.NoError(t, err)

	before := stream.State()

	envelope, err := stream.Emit(Event{Type: EventActivityUpdate, Activity: &ActivityUpdate{
		ActivityID:   "act-1",
		Kind:         ActivityTask,
		State:        ActivityRunning,
		Cause:        CauseSubmission,
		OriginTurnID: "turn-1",
		Progress:     json.RawMessage(`{"unterminated":`),
	}})

	require.Nil(t, envelope, "an envelope this adapter cannot state is never handed back")

	var refusal *ViolationError

	require.ErrorAs(t, err, &refusal)
	require.Equal(t, ViolationMalformedEnvelope, refusal.Kind)
	require.Equal(t, uint64(4), stream.sequence, "the refused event still consumed its sequence")
	require.Equal(t, before.ReducedThrough, stream.State().ReducedThrough,
		"nothing an emission could not state reaches the projection")

	_, ok := stream.State().Activity("act-1")
	require.False(t, ok, "the activity the refused frame named was never projected")
}

// TestEmitRefusesADiscriminantWithoutItsPayload pins the caller defect the
// decoder can never produce. It is judged before the sequence is claimed: no
// frame existed to leave a gap for, and the encoder that follows reads the very
// payload the discriminant names.
func TestEmitRefusesADiscriminantWithoutItsPayload(t *testing.T) {
	t.Parallel()

	stream := NewStream("strm-1", containedConfiguration())

	for _, event := range []Event{
		{Type: EventSnapshot},
		{Type: EventPromptAccepted},
		{Type: EventStateUpdate},
		{Type: EventActivityUpdate},
		{Type: EventActionUpdate},
		{Type: EventQuiescenceUpdate},
		// The discriminant names one payload and a second rides along.
		{Type: EventSnapshot, Snapshot: &Snapshot{}, Quiescence: &QuiescenceFact{}},
		// The payload is present but the discriminant names another one.
		{Type: EventStateUpdate, Snapshot: &Snapshot{}},
		// The discriminant is outside the closed six.
		{Type: EventType("promoted"), Snapshot: &Snapshot{}},
	} {
		envelope, err := stream.Emit(event)
		require.Nil(t, envelope)

		var refusal *ViolationError

		require.ErrorAs(t, err, &refusal)
		require.Equal(t, ViolationMalformedEnvelope, refusal.Kind)
		require.Equal(t, uint64(1), refusal.Sequence, "the sequence a valid first event would claim")
	}

	require.Equal(t, uint64(0), stream.sequence, "a caller defect claims no sequence")

	// The stream is untouched: the next real event opens it at sequence one.
	envelope, err := stream.Emit(SnapshotEvent("cyc-0", QuiescenceFact{}))
	require.NoError(t, err)
	require.Equal(t, uint64(1), envelope[fieldSequence])
}
