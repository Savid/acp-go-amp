package lifecycle

// applySnapshot opens the stream from a whole-state assertion. A snapshot
// introduces every identity it names — its foreground turn, its nonterminal
// activities and actions, and the turns those name as origin or owner — because
// the state it describes predates the stream: a terminal origin turn or a
// terminal parent is legitimately absent from the sets it carries.
func (r *Reducer) applySnapshot(delivery Delivery) error {
	snapshot := delivery.Event.Snapshot
	if snapshot == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the snapshot payload is missing")
	}

	if !snapshot.Foreground.State.Valid() || snapshot.Foreground.CycleID == "" {
		return r.fail(delivery, ViolationMalformedEnvelope, "the snapshot's foreground is incomplete")
	}

	foreground := snapshot.Foreground
	r.state.Foreground = &foreground

	if foreground.TurnID != "" {
		r.seeTurn(foreground.TurnID, delivery.Sequence)
	}

	if err := r.adoptSnapshotActivities(delivery, snapshot.Activities); err != nil {
		return err
	}

	for _, action := range snapshot.Actions {
		if err := r.checkActionIdentity(delivery, action); err != nil {
			return err
		}

		r.recordAction(delivery, action)

		if action.Owner.Type == OwnerTurn {
			r.seeTurn(action.Owner.ID, delivery.Sequence)
		}
	}

	return r.certifyQuiescence(delivery, snapshot.Quiescence)
}

// adoptSnapshotActivities records the asserted set before checking parentage, so
// a parent stated later in the same set is already introduced when its child is
// resolved against it.
func (r *Reducer) adoptSnapshotActivities(delivery Delivery, activities []ActivityUpdate) error {
	first := len(r.state.Activities)

	for _, activity := range activities {
		if err := r.checkActivityIdentity(delivery, activity); err != nil {
			return err
		}

		r.recordActivity(delivery, activity)
		r.seeTurn(activity.OriginTurnID, delivery.Sequence)
	}

	for offset := range activities {
		recorded := r.state.Activities[first+offset]
		if err := r.checkActivityParent(delivery, recorded.ActivityID, recorded.ParentID); err != nil {
			return err
		}
	}

	return nil
}

// applyPromptAccepted opens a prompt-origin turn. Acceptance is the dispatch
// linearization point, so it also invalidates whatever boundary was certified
// before the frame the native dispatcher just took ownership of.
func (r *Reducer) applyPromptAccepted(delivery Delivery) error {
	accepted := delivery.Event.PromptAccepted
	if accepted == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the acceptance payload is missing")
	}

	if index := r.turnIndex(accepted.TurnID); index >= 0 && r.state.Turns[index].Terminal {
		return r.fail(delivery, ViolationPostTerminalMutation, "turn "+accepted.TurnID+" is terminal")
	}

	r.state.Turns = append(r.state.Turns, TurnRecord{
		TurnID:       accepted.TurnID,
		Origin:       CauseSubmission,
		SubmissionID: accepted.SubmissionID,
		ClientNonce:  accepted.ClientNonce,
		RunID:        accepted.RunID,
	})
	r.seeTurn(accepted.TurnID, delivery.Sequence)
	r.invalidateQuiescence(delivery.Sequence)
	r.lastTransition = delivery.Sequence

	return nil
}

func (r *Reducer) applyStateUpdate(delivery Delivery) error {
	transition := delivery.Event.State
	if transition == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the transition payload is missing")
	}

	if err := r.checkBlockedCycle(delivery, *transition); err != nil {
		return err
	}

	apply := r.applyLive
	if transition.State == ForegroundIdle {
		apply = r.applyIdle
	}

	if err := apply(delivery, *transition); err != nil {
		return err
	}

	r.lastTransition = delivery.Sequence

	return nil
}

// checkBlockedCycle enforces the two halves of the blocking-action rule: a
// requires-action transition names a cycle something is actually blocking, and a
// cycle a blocking action stopped reports that state before it reports anything
// else.
func (r *Reducer) checkBlockedCycle(delivery Delivery, transition StateTransition) error {
	if transition.State == ForegroundRequiresAction {
		if !r.hasBlockingAction() {
			return r.fail(delivery, ViolationInconsistentForeground,
				"cycle "+transition.CycleID+" has no outstanding blocking action")
		}

		if r.blockedCycle == transition.CycleID {
			r.blockedCycle = ""
		}

		return nil
	}

	if r.blockedCycle == transition.CycleID {
		return r.fail(delivery, ViolationInconsistentForeground,
			"cycle "+transition.CycleID+" is blocked and reported "+string(transition.State))
	}

	return nil
}

func (r *Reducer) hasBlockingAction() bool {
	for _, action := range r.state.Actions {
		if action.BlocksForeground && !action.State.Terminal() {
			return true
		}
	}

	return false
}

// applyLive reduces a running or requires-action transition. A turn with no
// accepted submission behind it is agent-origin, and the transition's cause names
// what opened it; a submission-caused transition naming a turn the stream never
// accepted references an entity that does not exist.
func (r *Reducer) applyLive(delivery Delivery, transition StateTransition) error {
	index := r.turnIndex(transition.TurnID)

	switch {
	case index >= 0 && r.state.Turns[index].Terminal:
		return r.fail(delivery, ViolationPostTerminalMutation, "turn "+transition.TurnID+" is terminal")
	case index < 0:
		if transition.Cause == CauseSubmission && !r.turnKnown(transition.TurnID) {
			return r.fail(delivery, ViolationUnknownEntity, "turn "+transition.TurnID+" was never accepted")
		}

		r.state.Turns = append(r.state.Turns, TurnRecord{TurnID: transition.TurnID, Origin: transition.Cause})
		r.seeTurn(transition.TurnID, delivery.Sequence)

		index = len(r.state.Turns) - 1
	}

	r.state.Turns[index].CycleID = transition.CycleID
	r.state.Foreground = &Foreground{
		State:   transition.State,
		CycleID: transition.CycleID,
		TurnID:  transition.TurnID,
	}
	r.invalidateQuiescence(delivery.Sequence)

	return nil
}

// applyIdle ends a foreground cycle. An ending transition never opens a turn, so
// it must name one the stream already introduced.
func (r *Reducer) applyIdle(delivery Delivery, transition StateTransition) error {
	index := r.turnIndex(transition.TurnID)

	switch {
	case transition.TurnID == "":
		r.state.Foreground = &Foreground{State: ForegroundIdle, CycleID: transition.CycleID}

		return nil
	case index >= 0 && r.state.Turns[index].Terminal:
		return r.fail(delivery, ViolationPostTerminalMutation, "turn "+transition.TurnID+" is terminal")
	case index < 0:
		if !r.turnKnown(transition.TurnID) {
			return r.fail(delivery, ViolationUnknownEntity, "turn "+transition.TurnID+" was never opened")
		}

		r.state.Turns = append(r.state.Turns, TurnRecord{TurnID: transition.TurnID, Origin: transition.Cause})
		index = len(r.state.Turns) - 1
	}

	turn := &r.state.Turns[index]
	turn.Terminal = true
	turn.StopReason = transition.StopReason
	turn.Outcome = transition.Outcome
	turn.CycleID = transition.CycleID
	r.state.Foreground = &Foreground{State: ForegroundIdle, CycleID: transition.CycleID}

	return nil
}

func (r *Reducer) applyActivityUpdate(delivery Delivery) error {
	update := delivery.Event.Activity
	if update == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the activity payload is missing")
	}

	r.lastTransition = delivery.Sequence

	if r.activityIndex(update.ActivityID) >= 0 {
		return r.patchActivity(delivery, *update)
	}

	if err := r.checkActivityIdentity(delivery, *update); err != nil {
		return err
	}

	if err := r.checkActivityReferences(delivery, *update); err != nil {
		return err
	}

	if err := r.checkCausalFence(delivery, *update); err != nil {
		return err
	}

	if err := r.checkActivityParent(delivery, update.ActivityID, update.ParentID); err != nil {
		return err
	}

	r.recordActivity(delivery, *update)

	return nil
}

// checkActivityReferences resolves a first sight's references. A parent with no
// prior first sight or an origin turn the stream never opened would leave
// parentage, ownership, and terminal ordering unenforceable.
func (r *Reducer) checkActivityReferences(delivery Delivery, update ActivityUpdate) error {
	if !r.turnKnown(update.OriginTurnID) {
		return r.fail(delivery, ViolationUnknownEntity, "origin turn "+update.OriginTurnID+" was never opened")
	}

	if update.ParentID != "" && !r.activityKnown(update.ParentID) {
		return r.fail(delivery, ViolationUnknownEntity, "parent activity "+update.ParentID+" was never seen")
	}

	return nil
}

// checkCausalFence refuses old causal work discovered after the boundary that
// settled it. The predicate is mechanical: an activity rooted in a turn or a
// parent first seen at or before a certified watermark was fenced by that proof,
// and a settled boundary never reopens.
func (r *Reducer) checkCausalFence(delivery Delivery, update ActivityUpdate) error {
	if r.fence == 0 {
		return nil
	}

	if seen, known := r.turnSeen[update.OriginTurnID]; known && seen <= r.fence {
		return r.fail(delivery, ViolationLateCausalWork, "origin turn "+update.OriginTurnID+" is fenced")
	}

	if seen, known := r.activitySeen[update.ParentID]; known && seen <= r.fence {
		return r.fail(delivery, ViolationLateCausalWork, "parent activity "+update.ParentID+" is fenced")
	}

	return nil
}

// checkActivityIdentity validates an activity's first sight, when every immutable
// identity field must be present and the kind must be one the answer proved.
func (r *Reducer) checkActivityIdentity(delivery Delivery, update ActivityUpdate) error {
	switch {
	case update.Kind == "" || update.Cause == "" || update.OriginTurnID == "":
		return r.fail(delivery, ViolationImmutableIdentityChange,
			"activity "+update.ActivityID+" states an incomplete identity")
	case !r.negotiated.DeclaresActivityKind(update.Kind):
		return r.fail(delivery, ViolationUnnegotiatedFact, "activity kind "+string(update.Kind))
	}

	return nil
}

func (r *Reducer) recordActivity(delivery Delivery, update ActivityUpdate) {
	r.state.Activities = append(r.state.Activities, ActivityRecord(update))
	r.seeActivity(update.ActivityID, delivery.Sequence)

	if !update.State.Terminal() {
		r.invalidateQuiescence(delivery.Sequence)
	}
}

// checkActivityParent validates parentage: a parent this stream introduced must
// still be nonterminal when it gains a child.
func (r *Reducer) checkActivityParent(delivery Delivery, activityID, parentID string) error {
	index := r.activityIndex(parentID)
	if parentID == "" || index < 0 || activityID == parentID {
		return nil
	}

	if r.state.Activities[index].State.Terminal() {
		return r.fail(delivery, ViolationChildAfterParentTerminal, "parent "+parentID+" is terminal")
	}

	return nil
}

// patchActivity applies a later update, which may change only state and progress.
// A restated immutable field is permitted only with its first-sight value, and
// changes nothing.
func (r *Reducer) patchActivity(delivery Delivery, update ActivityUpdate) error {
	index := r.activityIndex(update.ActivityID)

	existing := r.state.Activities[index]
	if detail := immutableActivityConflict(existing, update); detail != "" {
		return r.fail(delivery, ViolationImmutableIdentityChange, detail)
	}

	if existing.State.Terminal() && update.State != existing.State {
		return r.fail(delivery, ViolationPostTerminalMutation, "activity "+existing.ActivityID+" is terminal")
	}

	if update.State.Terminal() {
		if err := r.checkDescendantsTerminal(delivery, existing.ActivityID); err != nil {
			return err
		}
	}

	r.state.Activities[index].State = update.State

	if update.Progress != nil {
		r.state.Activities[index].Progress = update.Progress
	}

	if !update.State.Terminal() {
		r.invalidateQuiescence(delivery.Sequence)
	}

	return nil
}

// checkDescendantsTerminal refuses a parent that would terminalize while part of
// the subtree it claims to have finished is still live.
func (r *Reducer) checkDescendantsTerminal(delivery Delivery, activityID string) error {
	for _, child := range r.state.Activities {
		if child.ParentID == activityID && !child.State.Terminal() {
			return r.fail(delivery, ViolationParentTerminalBeforeChild, "child "+child.ActivityID+" is live")
		}
	}

	for _, action := range r.state.Actions {
		if action.Owner.Type == OwnerActivity && action.Owner.ID == activityID && !action.State.Terminal() {
			return r.fail(delivery, ViolationParentTerminalBeforeChild, "action "+action.ActionID+" is unresolved")
		}
	}

	return nil
}

func immutableActivityConflict(existing ActivityRecord, update ActivityUpdate) string {
	switch {
	case update.Kind != "" && update.Kind != existing.Kind:
		return "activity " + update.ActivityID + " changed kind"
	case update.ParentID != "" && update.ParentID != existing.ParentID:
		return "activity " + update.ActivityID + " changed parent"
	case update.ToolCallID != "" && update.ToolCallID != existing.ToolCallID:
		return "activity " + update.ActivityID + " changed tool link"
	case update.Cause != "" && update.Cause != existing.Cause:
		return "activity " + update.ActivityID + " changed cause"
	case update.OriginTurnID != "" && update.OriginTurnID != existing.OriginTurnID:
		return "activity " + update.ActivityID + " changed origin turn"
	case update.RunID != "" && update.RunID != existing.RunID:
		return "activity " + update.ActivityID + " changed ownership root"
	default:
		return ""
	}
}

func (r *Reducer) applyActionUpdate(delivery Delivery) error {
	update := delivery.Event.Action
	if update == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the action payload is missing")
	}

	r.lastTransition = delivery.Sequence

	if r.actionIndex(update.ActionID) >= 0 {
		return r.patchAction(delivery, *update)
	}

	if err := r.checkActionIdentity(delivery, *update); err != nil {
		return err
	}

	if err := r.checkActionOwner(delivery, *update); err != nil {
		return err
	}

	r.recordAction(delivery, *update)

	return nil
}

func (r *Reducer) checkActionIdentity(delivery Delivery, update ActionUpdate) error {
	if update.Kind == "" || update.Owner.ID == "" {
		return r.fail(delivery, ViolationImmutableIdentityChange,
			"action "+update.ActionID+" states an incomplete identity")
	}

	return nil
}

// checkActionOwner resolves the entity an action is owned by. An action hung off
// a turn or activity the stream never introduced could never be attributed.
func (r *Reducer) checkActionOwner(delivery Delivery, update ActionUpdate) error {
	known := r.turnKnown(update.Owner.ID)
	if update.Owner.Type == OwnerActivity {
		known = r.activityKnown(update.Owner.ID)
	}

	if !known {
		return r.fail(delivery, ViolationUnknownEntity,
			"owner "+string(update.Owner.Type)+" "+update.Owner.ID+" was never opened")
	}

	return nil
}

func (r *Reducer) recordAction(delivery Delivery, update ActionUpdate) {
	r.state.Actions = append(r.state.Actions, ActionRecord(update))

	if update.State.Terminal() {
		return
	}

	r.blockForeground(update)
	r.invalidateQuiescence(delivery.Sequence)
}

// blockForeground records that a cycle owes the accompanying transition. A
// blocking action never moves the foreground by itself.
func (r *Reducer) blockForeground(update ActionUpdate) {
	if !update.BlocksForeground || r.state.Foreground == nil {
		return
	}

	if r.state.Foreground.State != ForegroundRequiresAction {
		r.blockedCycle = r.state.Foreground.CycleID
	}
}

func (r *Reducer) patchAction(delivery Delivery, update ActionUpdate) error {
	index := r.actionIndex(update.ActionID)

	existing := r.state.Actions[index]

	switch {
	case update.Kind != "" && update.Kind != existing.Kind:
		return r.fail(delivery, ViolationImmutableIdentityChange, "action "+update.ActionID+" changed kind")
	case update.Owner.ID != "" && update.Owner != existing.Owner:
		return r.fail(delivery, ViolationImmutableIdentityChange, "action "+update.ActionID+" changed owner")
	case update.RunID != "" && update.RunID != existing.RunID:
		return r.fail(delivery, ViolationImmutableIdentityChange, "action "+update.ActionID+" changed ownership root")
	case existing.State.Terminal() && update.State != existing.State:
		return r.fail(delivery, ViolationPostTerminalMutation, "action "+update.ActionID+" is terminal")
	}

	r.state.Actions[index].State = update.State

	if !update.State.Terminal() {
		r.invalidateQuiescence(delivery.Sequence)
	}

	return nil
}

func (r *Reducer) applyQuiescence(delivery Delivery) error {
	fact := delivery.Event.Quiescence
	if fact == nil {
		return r.fail(delivery, ViolationMalformedEnvelope, "the quiescence payload is missing")
	}

	return r.certifyQuiescence(delivery, *fact)
}

// certifyQuiescence installs an authoritative quiescence fact. A configuration
// that proved no class, or a fact naming a class it never advertised, asserts
// something the answer did not claim. A fact this stream contradicts — work is
// still live, or the watermark does not cover every transition reduced before
// it — proves no boundary and certifies nothing.
func (r *Reducer) certifyQuiescence(delivery Delivery, fact QuiescenceFact) error {
	if !fact.Quiescent {
		r.invalidateQuiescence(delivery.Sequence)

		return nil
	}

	if !r.negotiated.AuthoritativeQuiescence || fact.Source != r.negotiated.QuiescenceSource {
		return r.fail(delivery, ViolationUnnegotiatedFact, "quiescence proof "+string(fact.Source))
	}

	if !r.state.Vacant() || fact.Watermark < r.lastTransition {
		return nil
	}

	r.state.Quiescence = QuiescenceState{
		Certified: true,
		Source:    fact.Source,
		Watermark: fact.Watermark,
		Barrier:   fact.Barrier,
	}
	r.fence = max(r.fence, fact.Watermark)

	return nil
}

// seeTurn and seeActivity retain the sequence an identity was first seen at. Only
// the first sight counts: it is what a later watermark fences.
func (r *Reducer) seeTurn(turnID string, sequence uint64) {
	if _, known := r.turnSeen[turnID]; !known {
		r.turnSeen[turnID] = sequence
	}
}

func (r *Reducer) seeActivity(activityID string, sequence uint64) {
	if _, known := r.activitySeen[activityID]; !known {
		r.activitySeen[activityID] = sequence
	}
}

func (r *Reducer) turnKnown(turnID string) bool {
	_, known := r.turnSeen[turnID]

	return known
}

func (r *Reducer) activityKnown(activityID string) bool {
	_, known := r.activitySeen[activityID]

	return known
}

func (r *Reducer) turnIndex(turnID string) int {
	return indexOf(r.state.Turns, turnID, TurnRecord.identity)
}

func (r *Reducer) activityIndex(activityID string) int {
	return indexOf(r.state.Activities, activityID, ActivityRecord.identity)
}

func (r *Reducer) actionIndex(actionID string) int {
	return indexOf(r.state.Actions, actionID, ActionRecord.identity)
}
