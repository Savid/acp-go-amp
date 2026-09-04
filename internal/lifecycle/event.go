package lifecycle

import "encoding/json"

// The fixed member names of the envelope, the six events, and the negotiation
// and correlation objects. They are named once so the decoder's unknown-member
// rule and the emitter's construction cannot drift apart.
const (
	fieldVersion  = "version"
	fieldStreamID = "streamId"
	fieldSequence = "sequence"
	fieldEvent    = "event"
	fieldType     = "type"

	fieldForeground = "foreground"
	fieldActivities = "activities"
	fieldActions    = "actions"
	fieldQuiescence = "quiescence"

	fieldState   = "state"
	fieldCycleID = "cycleId"
	fieldTurnID  = "turnId"
	fieldCause   = "cause"
	fieldOrigin  = "origin"

	fieldSubmissionID = "submissionId"
	fieldClientNonce  = "clientNonce"
	fieldRunID        = "runId"
	fieldStopReason   = "stopReason"
	fieldOutcome      = "outcome"

	fieldActivity     = "activity"
	fieldActivityID   = "activityId"
	fieldKind         = "kind"
	fieldParentID     = "parentId"
	fieldToolCallID   = "toolCallId"
	fieldOriginTurnID = "originTurnId"
	fieldProgress     = "progress"

	fieldAction           = "action"
	fieldActionID         = "actionId"
	fieldOwner            = "owner"
	fieldBlocksForeground = "blocksForeground"
	fieldID               = "id"

	fieldQuiescent = "quiescent"
	fieldSource    = "source"
	fieldWatermark = "watermark"
	fieldBarrier   = "barrier"

	fieldUpdatesOutsidePrompt    = "updatesOutsidePrompt"
	fieldAuthoritativeQuiescence = "authoritativeQuiescence"
	fieldQuiescenceSource        = "quiescenceSource"
	fieldActivityKinds           = "activityKinds"

	fieldSubmission = "submission"
)

// Event is one decoded lifecycle event. Exactly one payload pointer matches Type,
// and the decoder is the only producer, so a reducer never has to defend against
// a discriminant without its payload.
type Event struct {
	Type           EventType
	Snapshot       *Snapshot
	PromptAccepted *PromptAccepted
	State          *StateTransition
	Activity       *ActivityUpdate
	Action         *ActionUpdate
	Quiescence     *QuiescenceFact
}

// strictShape reports the invariant the decoder guarantees and an in-process
// caller does not: exactly one payload is present and it is the one Type names.
// The emitter checks it because the encoder reads the payload the discriminant
// names, and a value the decoder could never have produced is a caller defect
// rather than a frame.
func (e Event) strictShape() bool {
	payloads := 0

	for _, present := range []bool{
		e.Snapshot != nil, e.PromptAccepted != nil, e.State != nil,
		e.Activity != nil, e.Action != nil, e.Quiescence != nil,
	} {
		if present {
			payloads++
		}
	}

	if payloads != 1 {
		return false
	}

	return e.Type == EventSnapshot && e.Snapshot != nil ||
		e.Type == EventPromptAccepted && e.PromptAccepted != nil ||
		e.Type == EventStateUpdate && e.State != nil ||
		e.Type == EventActivityUpdate && e.Activity != nil ||
		e.Type == EventActionUpdate && e.Action != nil ||
		e.Type == EventQuiescenceUpdate && e.Quiescence != nil
}

// Snapshot opens a stream with the whole truth it can state: the foreground state
// and cycle, the complete nonterminal activity and action sets, and the current
// quiescence fact with its proof source.
type Snapshot struct {
	Foreground Foreground
	Activities []ActivityUpdate
	Actions    []ActionUpdate
	Quiescence QuiescenceFact
}

// Foreground is one foreground cycle. TurnID is empty while no turn holds it,
// and Origin names that turn's provenance exactly while it does: a resumed turn
// with no recorded origin would be a turn a consumer could not attribute. Origin
// is the snapshot's own member and is not projected — the turn record it opens
// carries it instead.
type Foreground struct {
	State   ForegroundState `json:"state"`
	CycleID string          `json:"cycleId"`
	TurnID  string          `json:"turnId,omitempty"`
	Origin  Cause           `json:"-"`
}

// PromptAccepted records that the native dispatcher took durable ownership of a
// submitted frame. Acceptance is never inferred from a running transition.
type PromptAccepted struct {
	SubmissionID string
	ClientNonce  string
	TurnID       string
	RunID        string
}

// StateTransition is one foreground state change. Only a transition that ends
// work carries a stop reason and an outcome.
type StateTransition struct {
	State      ForegroundState
	CycleID    string
	TurnID     string
	Cause      Cause
	StopReason string
	Outcome    Outcome
}

// transitionDefect reports why a transition's own shape is incomplete, or the
// empty string when it is not. Every defect it names is structural, so both the
// decoder and the reducer consult it before anything the stream holds is
// consulted: an event missing a member its state requires resolves no name and
// says nothing about a cycle, so it can report neither an unresolvable reference
// nor an inconsistent foreground.
func transitionDefect(transition StateTransition) string {
	if detail := liveForegroundDefect(transition); detail != "" {
		return detail
	}

	return endingIdleDefect(transition)
}

// liveForegroundDefect reports a transition to a live foreground state that names
// no turn. A running foreground is a turn running and a requires_action one is
// owned work blocked, so a transition to either names the turn that owns it
// whatever its cause; only a session-caused idle may omit one.
func liveForegroundDefect(transition StateTransition) string {
	if transition.State == ForegroundIdle || transition.TurnID != "" {
		return ""
	}

	return "a " + string(transition.State) + " transition names the turn that owns it"
}

// endingIdleDefect reports why an idle transition that settles a turn is
// structurally incomplete, or the empty string when it is not. An idle naming a
// turn ends it, so the outcome is always required; the stop reason is required
// with it except on a failure, where no ACP v1 stop reason names one and the v1
// error carries it instead.
func endingIdleDefect(transition StateTransition) string {
	if transition.State != ForegroundIdle || transition.TurnID == "" {
		return ""
	}

	switch {
	case transition.Outcome == "":
		return "an idle transition that ends a turn records its outcome"
	case transition.Outcome == OutcomeFailed && transition.StopReason != "":
		return "a failed outcome states no stop reason"
	case transition.Outcome != OutcomeFailed && transition.StopReason == "":
		return "an idle transition that ends a turn records its stop reason"
	default:
		return ""
	}
}

// ActivityUpdate reports one activity. A first sight carries every immutable
// identity field; a later update carries state and progress only.
type ActivityUpdate struct {
	ActivityID   string
	Kind         ActivityKind
	State        ActivityState
	ParentID     string
	ToolCallID   string
	Cause        Cause
	OriginTurnID string
	RunID        string
	// Progress is the one member whose interior this contract does not fix: an
	// opaque object a host renders and never reduces. Opaque is not exempt: it
	// takes part in the duplicate comparison the whole frame covers, and it is a
	// carried member of the restatement a terminal activity is judged by.
	Progress json.RawMessage
}

// ActionUpdate reports one permission or elicitation. Only an action blocking the
// current foreground cycle bears on requires_action.
//
// BlocksForeground is a pointer because absence and false are different facts: it
// is required on a first sight, and a later patch that omits it restates nothing.
type ActionUpdate struct {
	ActionID         string
	Kind             ActionKind
	State            ActionState
	Owner            Owner
	RunID            string
	BlocksForeground *bool
}

// QuiescenceFact is an authoritative quiescence claim with the proof that
// produced it. Watermark is the sequence the proof fences, and zero fences
// nothing.
type QuiescenceFact struct {
	Quiescent bool
	Source    ProofClass
	Watermark uint64
	Barrier   string
}

// CarrierClass reports whether an envelope rode the one carrier the extension
// permits. Every other session update carries per-entity reduction semantics a
// conformant consumer may legally coalesce, which would make the envelope
// unrecoverable.
type CarrierClass string

const (
	// CarrierUnknown is the zero value: a carrier that has not been classified
	// cannot be proven legal.
	CarrierUnknown CarrierClass = ""
	// CarrierSessionInfo names the identity-only session_info_update.
	CarrierSessionInfo CarrierClass = "session_info_update"
	// CarrierIneligible names every other carrier.
	CarrierIneligible CarrierClass = "ineligible"
)

// Delivery is one delivered lifecycle event with its ordering identity. That
// identity is (StreamID, Sequence): StreamID names the native lifecycle source
// incarnation rather than a transient connection.
type Delivery struct {
	StreamID string
	Sequence uint64
	Carrier  CarrierClass
	Event    Event
	// Frame is the whole delivered notification as a decoded value — envelope and
	// carrier together. Comparing decoded values under lifecycle value equality is
	// what distinguishes an exact retransmission from a conflicting reuse of the
	// same identity: key order and insignificant whitespace are never differences,
	// numbers are the exact values they name rather than the lexemes or the floats
	// that would round them, and nothing has to retain raw bytes for the life of a
	// session.
	Frame any
}
