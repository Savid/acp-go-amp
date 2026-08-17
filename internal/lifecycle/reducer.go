package lifecycle

import (
	"encoding/json"
	"errors"
	"reflect"
)

// DefaultRetransmissionWindow bounds how far back an exact retransmission stays
// recognizable. A duplicate identity older than the window cannot be proven
// identical, so it fails closed like any other conflicting duplicate.
const DefaultRetransmissionWindow = 1024

// Options configures a reducer.
type Options struct {
	// Negotiated are the lifecycle facts the source's configuration proved. The
	// reducer refuses anything the configuration did not advertise.
	Negotiated Negotiated
	// RetransmissionWindow overrides DefaultRetransmissionWindow.
	RetransmissionWindow int
}

// Reducer reduces one session's lifecycle stream. It validates ordering identity
// before reducing, refuses anything it cannot prove, and latches on the first
// refusal: a stream that failed closed never reduces again.
//
// One reducer follows one session across incarnations. A snapshot bearing a new
// stream identity supersedes the previous incarnation and starts a fresh
// projection; every other event on a foreign or fenced stream is stale.
//
// The one advertised fact it cannot check is `updatesOutsidePrompt`: no fact on
// the ordered stream expresses whether a prompt is in flight, so that obligation
// belongs to the emitter's own configuration and to a transport-facing consumer.
//
// A Reducer is not safe for concurrent use; the owner of the stream serializes
// reduction.
type Reducer struct {
	negotiated Negotiated
	window     int
	state      State
	base       uint64
	started    bool
	failed     *ViolationError
	frames     map[uint64]any
	// retained is the order frames were recorded in, so the oldest leaves the
	// window first.
	retained []uint64
	// lastTransition is the highest sequence carrying a transition a quiescence
	// proof must cover before it can certify a boundary.
	lastTransition uint64
	// fence is the highest watermark ever certified on this incarnation. Causal
	// work rooted at or before it is settled and never reopens.
	fence uint64
	// turnSeen and activitySeen record the sequence an identity was first seen at,
	// which is what makes late causal work mechanically detectable.
	turnSeen     map[string]uint64
	activitySeen map[string]uint64
	// blockedCycle is the cycle owing the accompanying foreground transition a
	// blocking action requires.
	blockedCycle string
}

// NewReducer builds a reducer for one session.
func NewReducer(opts Options) *Reducer {
	window := opts.RetransmissionWindow
	if window <= 0 {
		window = DefaultRetransmissionWindow
	}

	reducer := &Reducer{negotiated: opts.Negotiated, window: window}
	reducer.reset("")

	return reducer
}

// Negotiated reports the configuration the reducer validates against.
func (r *Reducer) Negotiated() Negotiated { return r.negotiated }

// State returns the projection proved so far.
func (r *Reducer) State() State { return r.state.clone() }

// Failed reports the latched refusal, if any.
func (r *Reducer) Failed() *ViolationError { return r.failed }

// Close records that the addressed session's close completed. The incarnation is
// fenced: any later event bearing its identity is stale.
func (r *Reducer) Close() { r.state.Closed = true }

// ReduceSessionUpdate decodes one session/update notification payload and reduces
// the delivery it carries. A frame carrying no envelope reports ErrNoEnvelope and
// changes nothing; every other refusal latches the stream, so a caller that stops
// here holds the projection as it stood at the moment of refusal.
func (r *Reducer) ReduceSessionUpdate(params json.RawMessage) error {
	if r.failed != nil {
		return r.failed
	}

	delivery, err := DecodeSessionUpdate(params, r.negotiated)
	if err != nil {
		var refusal *ViolationError
		if errors.As(err, &refusal) {
			r.failed = refusal
		}

		return err
	}

	return r.Reduce(delivery)
}

// Reduce validates and reduces one delivery. An exact retransmission of an
// already-reduced identity is suppressed wholesale and returns nil without
// changing the projection.
func (r *Reducer) Reduce(delivery Delivery) error {
	switch {
	case r.failed != nil:
		return r.failed
	case !r.started:
		return r.reduceFirst(delivery)
	case delivery.StreamID != r.state.StreamID:
		return r.reduceForeign(delivery)
	case r.state.Closed:
		return r.fail(delivery, ViolationStaleStream, "the stream is fenced")
	case delivery.Sequence < r.base:
		return r.fail(delivery, ViolationSequenceRegression, "below the stream's snapshot boundary")
	case delivery.Sequence <= r.state.ReducedThrough:
		return r.reduceDuplicate(delivery)
	case delivery.Sequence > r.state.ReducedThrough+1:
		return r.fail(delivery, ViolationSequenceGap, "expected the next contiguous sequence")
	case delivery.Carrier != CarrierSessionInfo:
		return r.fail(delivery, ViolationIllegalCarrier, "carrier "+string(delivery.Carrier))
	case delivery.Event.Type == EventSnapshot:
		return r.fail(delivery, ViolationStreamCycle, "a snapshot opens a stream and never appears inside one")
	}

	if err := r.apply(delivery); err != nil {
		return err
	}

	r.commit(delivery)

	return nil
}

// reduceForeign admits the next incarnation. Only its opening snapshot may arrive
// on a stream identity this reducer has not seen; a projection is per incarnation
// and adopts nothing from the one it supersedes.
func (r *Reducer) reduceForeign(delivery Delivery) error {
	if delivery.Event.Type != EventSnapshot {
		return r.fail(delivery, ViolationStaleStream, "stream is "+r.state.StreamID)
	}

	r.reset(delivery.StreamID)

	return r.reduceFirst(delivery)
}

func (r *Reducer) reset(streamID string) {
	r.state = State{StreamID: streamID}
	r.base = 0
	r.started = false
	r.frames = make(map[uint64]any)
	r.retained = nil
	r.lastTransition = 0
	r.fence = 0
	r.turnSeen = make(map[string]uint64)
	r.activitySeen = make(map[string]uint64)
	r.blockedCycle = ""
}

func (r *Reducer) reduceFirst(delivery Delivery) error {
	r.state.StreamID = delivery.StreamID
	if delivery.Carrier != CarrierSessionInfo {
		return r.fail(delivery, ViolationIllegalCarrier, "carrier "+string(delivery.Carrier))
	}

	if delivery.Event.Type != EventSnapshot {
		return r.fail(delivery, ViolationDeltaBeforeSnapshot, "first event was "+string(delivery.Event.Type))
	}

	r.started = true
	r.base = delivery.Sequence

	if err := r.applySnapshot(delivery); err != nil {
		r.started = false

		return err
	}

	r.commit(delivery)

	return nil
}

func (r *Reducer) reduceDuplicate(delivery Delivery) error {
	if recorded, known := r.frames[delivery.Sequence]; known && reflect.DeepEqual(recorded, delivery.Frame) {
		r.state.SuppressedRetransmissions++

		return nil
	}

	return r.fail(delivery, ViolationConflictingDuplicate, "the identity already delivered different content")
}

func (r *Reducer) commit(delivery Delivery) {
	r.state.ReducedThrough = delivery.Sequence
	r.frames[delivery.Sequence] = delivery.Frame
	r.retained = append(r.retained, delivery.Sequence)

	if len(r.retained) > r.window {
		delete(r.frames, r.retained[0])
		r.retained = r.retained[1:]
	}
}

func (r *Reducer) fail(delivery Delivery, kind ViolationKind, detail string) error {
	r.failed = violation(kind, delivery.StreamID, delivery.Sequence, detail)

	return r.failed
}

func (r *Reducer) apply(delivery Delivery) error {
	switch delivery.Event.Type {
	case EventPromptAccepted:
		return r.applyPromptAccepted(delivery)
	case EventStateUpdate:
		return r.applyStateUpdate(delivery)
	case EventActivityUpdate:
		return r.applyActivityUpdate(delivery)
	case EventActionUpdate:
		return r.applyActionUpdate(delivery)
	default:
		return r.applyQuiescence(delivery)
	}
}

// invalidateQuiescence revokes the certified fact. Acceptance, a live foreground
// transition, a new activity, and a pending action all invalidate it; the revoking
// sequence is the first one that did, so a stale fact can never be re-read as
// fresh.
func (r *Reducer) invalidateQuiescence(sequence uint64) {
	if !r.state.Quiescence.Certified {
		return
	}

	r.state.Quiescence = QuiescenceState{InvalidatedAt: sequence}
}
