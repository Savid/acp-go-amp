package ampacp

import (
	"context"
	"errors"

	"github.com/coder/acp-go-sdk"
)

// The closed off-prompt `-32603` vocabulary. Every internal error this adapter
// answers away from the prompt turn carries exactly one of these tokens in
// `data.error`, and the JSON-RPC `message` stays the constant "Internal error".
// The data never carries a `message` member, a Go error string, or amp's own
// output: a fact more specific than the token rides a documented closed `class`
// or `cause`, and the prose it replaced is logged or joined onto the Go error
// an embedding host receives.
//
// Amp answers four of the five family tokens. It has no shared native runtime —
// one short-lived amp process serves one prompt and exits — so no operation can
// find a runtime it needs gone and un-replaceable, and `amp_runtime_unavailable`
// is unreachable here and never emitted.
const (
	// errorInvalidOptions is the construction verdict: this agent holds an
	// option set it will not serve under. NewAgent returns no error, so the
	// verdict is delivered on `initialize` and on every session-establishing
	// entry point instead.
	errorInvalidOptions = "amp_invalid_options"
	// errorRestoreFailed is a store entry `session/load` or `session/resume`
	// found and could not restore. The entry is neither deleted nor tombstoned
	// by the failure.
	errorRestoreFailed = "amp_restore_failed"
	// errorSessionPoisoned refuses every operation but close and delete on a
	// session whose native binding this adapter can no longer trust. It always
	// carries a `cause`.
	errorSessionPoisoned = "amp_session_poisoned"
	// errorInternalFailure is every failure the tokens above do not classify.
	// It optionally carries a `class`.
	errorInternalFailure = "amp_internal_failure"

	keyCause = "cause"
	keyClass = "class"
)

// The closed `class` vocabulary for amp_internal_failure. Each token names one
// condition a host can act on; nothing here is derived from a Go or native
// string.
const (
	// classHandlerFailed is an error that reached the connection boundary
	// without a shape of its own.
	classHandlerFailed = "handler_failed"
	// classOwnershipChanged is a session, flight, use, or persistence ownership
	// check that lost its race to a concurrent close, delete, or replacement.
	classOwnershipChanged = "ownership_changed"
	// classCleanupPending is a session id whose previous residence has not
	// finished releasing its local state.
	classCleanupPending = "cleanup_pending"
	// classSessionIDFailed is a failure to mint a session id.
	classSessionIDFailed = "session_id_generation_failed"
	// classNativeStartupFailed is a failed harness validation probe.
	classNativeStartupFailed = "native_startup_failed"
	// classContainmentIncomplete is a native process tree this adapter could not
	// prove vacant.
	classContainmentIncomplete = "containment_incomplete"
	// classNativeStateMissing is a server-side amp thread that no longer exists,
	// so the session has nothing to continue.
	classNativeStateMissing = "native_state_missing"
	// classTerminalDeliveryPending is a session still owing its terminal
	// lifecycle delivery.
	classTerminalDeliveryPending = "terminal_delivery_pending"
	// classLifecycleUnavailable is a lifecycle event with no client connection
	// to deliver it on.
	classLifecycleUnavailable = "lifecycle_delivery_unavailable"
	// classLifecycleViolation is an emitted lifecycle envelope this adapter's
	// own reducer refused.
	classLifecycleViolation = "lifecycle_violation"
	// classTranscriptDrift is a durable transcript whose frame count disagrees
	// with what this session committed.
	classTranscriptDrift = "transcript_frame_drift"
	// classArtifactReplacement is a failure to build the image-artifact half of
	// a durable commit.
	classArtifactReplacement = "image_artifact_replacement_failed"
	// classMirrorUnsynced is a session holding frames the store would not take.
	classMirrorUnsynced = "mirror_unsynced"
)

// The closed `cause` vocabulary for amp_session_poisoned. Both causes name the
// same class of fault from opposite ends: the native thread identity a turn
// reported is one this session cannot bind to.
const (
	// causeNativeIDDrift is a turn reporting a thread id that is not the one the
	// session is bound to.
	causeNativeIDDrift = "native_session_id_drift"
	// causeNativeIDInvalid is a first turn reporting a thread id that is not a
	// well-formed amp thread id.
	causeNativeIDInvalid = "native_session_id_invalid"
)

// invalidOptions renders the construction verdict. field names the single
// refused option when one is at fault, and is empty when the verdict joins
// several — the reasons stay on the Go error an embedding host receives and in
// this adapter's log.
func invalidOptions(field string) *acp.RequestError {
	data := map[string]any{jsonFieldError: errorInvalidOptions}
	if field != "" {
		data[jsonFieldField] = field
	}

	return acp.NewInternalError(data)
}

// restoreFailed renders an unrestorable store entry. The entry survives: a
// failed restore never deletes or tombstones what it could not read.
func restoreFailed() *acp.RequestError {
	return acp.NewInternalError(map[string]any{jsonFieldError: errorRestoreFailed})
}

// sessionPoisoned renders a poisoned session's refusal with its documented
// cause.
func sessionPoisoned(cause string) *acp.RequestError {
	return acp.NewInternalError(map[string]any{
		jsonFieldError: errorSessionPoisoned,
		keyCause:       cause,
	})
}

// internalFailure renders an unclassified failure. class is one of the closed
// tokens above, or empty when the failure has no fact worth a token.
func internalFailure(class string) *acp.RequestError {
	data := map[string]any{jsonFieldError: errorInternalFailure}
	if class != "" {
		data[keyClass] = class
	}

	return acp.NewInternalError(data)
}

// joinNativeBoundary keeps a boundary sentinel reachable behind a wire error.
// The wire says only the closed token; an embedding host still tests the joined
// error for the containment, cancellation, and authority sentinels this package
// exports, because those decide whether local state may be reclaimed.
func joinNativeBoundary(requestErr, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		containmentIncomplete(err) ||
		errors.Is(err, ErrHostAuthorityUnavailable) || errors.Is(err, ErrNativeTreeBusy) {
		return errors.Join(requestErr, publicContainmentError(err))
	}

	return requestErr
}
