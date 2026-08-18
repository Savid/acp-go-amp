package lifecycle

// Stream is one incarnation's ordered emitter. It claims a sequence before
// delivery is attempted, so a lost or refused event leaves a detectable gap
// rather than a silently contiguous stream, and it reduces every event through
// the same reducer the fixture battery drives, so a stream this adapter could not
// support fails at the point of emission instead of at its consumers.
//
// A Stream is not safe for concurrent use; a prompt owns its incarnation and
// emits from one goroutine.
type Stream struct {
	id       string
	reducer  *Reducer
	sequence uint64
}

// NewStream opens an incarnation identified by id. The identity names one native
// lifecycle source lifetime: it never rotates while that source survives, and it
// never outlives it.
func NewStream(id string, negotiated Negotiated) *Stream {
	return &Stream{id: id, reducer: NewReducer(Options{Negotiated: negotiated})}
}

// ID reports the incarnation this stream speaks for.
func (s *Stream) ID() string { return s.id }

// State returns the projection the emitted stream proves.
func (s *Stream) State() State { return s.reducer.State() }

// Emit claims the next sequence, reduces the event, and renders the envelope for
// the notification's `_meta`. A refused event is never rendered and its sequence
// stays consumed, which is exactly the detectable gap the ordering rule wants.
func (s *Stream) Emit(event Event) (map[string]any, error) {
	s.sequence++

	err := s.reducer.Reduce(Delivery{
		StreamID: s.id,
		Sequence: s.sequence,
		Carrier:  CarrierSessionInfo,
		Event:    event,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		fieldVersion:  Version,
		fieldStreamID: s.id,
		fieldSequence: s.sequence,
		fieldEvent:    encodeEvent(event),
	}, nil
}

// SnapshotEvent opens a stream from the whole state this adapter can state
// truthfully. A prompt-contained incarnation opens with nothing live, so the
// nonterminal sets are empty and the quiescence fact is whatever the
// configuration's proof class actually established before the prompt.
func SnapshotEvent(cycleID string, quiescence QuiescenceFact) Event {
	return Event{Type: EventSnapshot, Snapshot: &Snapshot{
		Foreground: Foreground{State: ForegroundIdle, CycleID: cycleID},
		Quiescence: quiescence,
	}}
}

// AcceptedEvent records that the native dispatcher took durable ownership of a
// submitted frame. The submission identity is echoed verbatim from the prompt's
// correlation value.
func AcceptedEvent(submission Submission, turnID string) Event {
	return Event{Type: EventPromptAccepted, PromptAccepted: &PromptAccepted{
		SubmissionID: submission.SubmissionID,
		ClientNonce:  submission.ClientNonce,
		TurnID:       turnID,
		RunID:        submission.RunID,
	}}
}

// RunningEvent opens the foreground cycle a submission caused.
func RunningEvent(cycleID, turnID string) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:   ForegroundRunning,
		CycleID: cycleID,
		TurnID:  turnID,
		Cause:   CauseSubmission,
	}}
}

// IdleEvent ends the cycle a submission caused, carrying the turn's truthful stop
// reason and recorded outcome.
func IdleEvent(cycleID, turnID, stopReason string, outcome Outcome) Event {
	return Event{Type: EventStateUpdate, State: &StateTransition{
		State:      ForegroundIdle,
		CycleID:    cycleID,
		TurnID:     turnID,
		Cause:      CauseSubmission,
		StopReason: stopReason,
		Outcome:    outcome,
	}}
}

// QuiescenceEvent states the authoritative quiescence fact a completed proof
// produced. It carries the proof class and the watermark that proof covers, never
// a guess, a heuristic, or a confidence.
func QuiescenceEvent(fact QuiescenceFact) Event {
	return Event{Type: EventQuiescenceUpdate, Quiescence: &fact}
}

// encodeEvent renders an event the reducer just admitted. A prompt-contained
// configuration proves no activity kind and holds no action awaiting an answer,
// so this adapter has no emitter that builds those two forms; the encoder still
// renders all six, because an event the reducer accepted and the encoder could
// not state would be committed to the projection and lost on the wire.
func encodeEvent(event Event) map[string]any {
	switch event.Type {
	case EventSnapshot:
		return encodeSnapshot(*event.Snapshot)
	case EventPromptAccepted:
		return withOptional(map[string]any{
			fieldType:         string(EventPromptAccepted),
			fieldSubmissionID: event.PromptAccepted.SubmissionID,
			fieldClientNonce:  event.PromptAccepted.ClientNonce,
			fieldTurnID:       event.PromptAccepted.TurnID,
		}, fieldRunID, event.PromptAccepted.RunID)
	case EventActivityUpdate:
		return map[string]any{
			fieldType:     string(EventActivityUpdate),
			fieldActivity: encodeActivity(*event.Activity),
		}
	case EventActionUpdate:
		return map[string]any{
			fieldType:   string(EventActionUpdate),
			fieldAction: encodeAction(*event.Action),
		}
	case EventQuiescenceUpdate:
		fact := encodeQuiescence(*event.Quiescence)
		fact[fieldType] = string(EventQuiescenceUpdate)

		return fact
	default:
		return encodeTransition(*event.State)
	}
}

// encodeSnapshot renders the whole-state assertion. The nonterminal sets are
// always present and carry exactly what the snapshot asserts: a set rendered
// empty over entities the reducer accepted would assert a vacancy the emitter
// never claimed.
func encodeSnapshot(snapshot Snapshot) map[string]any {
	activities := make([]any, 0, len(snapshot.Activities))
	for index := range snapshot.Activities {
		activities = append(activities, encodeActivity(snapshot.Activities[index]))
	}

	actions := make([]any, 0, len(snapshot.Actions))
	for _, action := range snapshot.Actions {
		actions = append(actions, encodeAction(action))
	}

	foreground := map[string]any{
		fieldState:   string(snapshot.Foreground.State),
		fieldCycleID: snapshot.Foreground.CycleID,
	}
	withOptional(foreground, fieldTurnID, snapshot.Foreground.TurnID)
	withOptional(foreground, fieldOrigin, string(snapshot.Foreground.Origin))

	return map[string]any{
		fieldType:       string(EventSnapshot),
		fieldForeground: foreground,
		fieldActivities: activities,
		fieldActions:    actions,
		fieldQuiescence: encodeQuiescence(snapshot.Quiescence),
	}
}

// encodeActivity renders one activity. Every member a later patch may omit is
// omitted when it has no value, so a first sight and a patch render as the
// different assertions they are.
func encodeActivity(activity ActivityUpdate) map[string]any {
	encoded := map[string]any{
		fieldActivityID: activity.ActivityID,
		fieldState:      string(activity.State),
	}
	withOptional(encoded, fieldKind, string(activity.Kind))
	withOptional(encoded, fieldParentID, activity.ParentID)
	withOptional(encoded, fieldToolCallID, activity.ToolCallID)
	withOptional(encoded, fieldCause, string(activity.Cause))
	withOptional(encoded, fieldOriginTurnID, activity.OriginTurnID)
	withOptional(encoded, fieldRunID, activity.RunID)

	if len(activity.Progress) > 0 {
		encoded[fieldProgress] = activity.Progress
	}

	return encoded
}

// encodeAction renders one action. blocksForeground is stated only when the
// update states it: rendering an absent claim as false would demote a blocking
// request to a background one.
func encodeAction(action ActionUpdate) map[string]any {
	encoded := map[string]any{
		fieldActionID: action.ActionID,
		fieldState:    string(action.State),
	}
	withOptional(encoded, fieldKind, string(action.Kind))
	withOptional(encoded, fieldRunID, action.RunID)

	if action.Owner.ID != "" {
		encoded[fieldOwner] = map[string]any{
			fieldType: string(action.Owner.Type),
			fieldID:   action.Owner.ID,
		}
	}

	if action.BlocksForeground != nil {
		encoded[fieldBlocksForeground] = *action.BlocksForeground
	}

	return encoded
}

func encodeTransition(transition StateTransition) map[string]any {
	encoded := map[string]any{
		fieldType:    string(EventStateUpdate),
		fieldState:   string(transition.State),
		fieldCycleID: transition.CycleID,
		fieldTurnID:  transition.TurnID,
		fieldCause:   string(transition.Cause),
	}
	withOptional(encoded, fieldStopReason, transition.StopReason)
	withOptional(encoded, fieldOutcome, string(transition.Outcome))

	return encoded
}

// encodeQuiescence renders a fact's members. A negative fact carries no proof at
// all: `source` is present if and only if the fact is positive, and it is never a
// `none` sentinel.
func encodeQuiescence(fact QuiescenceFact) map[string]any {
	if !fact.Quiescent {
		return map[string]any{fieldQuiescent: false}
	}

	encoded := map[string]any{
		fieldQuiescent: true,
		fieldSource:    string(fact.Source),
		fieldWatermark: fact.Watermark,
	}

	return withOptional(encoded, fieldBarrier, fact.Barrier)
}

// withOptional adds a member only when it has a value. An optional member is
// omitted rather than emitted empty, because an empty opaque identifier fails
// closed on the reading side.
func withOptional(encoded map[string]any, key, value string) map[string]any {
	if value != "" {
		encoded[key] = value
	}

	return encoded
}
