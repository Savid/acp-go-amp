package ampacp

import (
	"bytes"
	"encoding/json"
	"slices"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/lifecycle"
)

// negotiateLifecycle reads the host's `acp-go.dev/lifecycle` offer and answers
// with the exact capability this connection supports. It records the answer as
// the contract for the whole connection: with no offer, the key is omitted from
// the response and no envelope, correlation read, or lifecycle fact exists on
// the connection at all.
func (a *Agent) negotiateLifecycle(meta map[string]any) (map[string]any, error) {
	offer, offered, refusal := lifecycle.DecodeOffer(meta)
	if refusal != nil {
		return nil, lifecycleParamError(refusal)
	}

	if !offered {
		if retainErr := a.retainNegotiatedLifecycle(lifecycle.Negotiated{}); retainErr != nil {
			return nil, retainErr
		}

		// An omitted key and an empty answer are the same wire fact: the response
		// carries no lifecycle member at all.
		return map[string]any{}, nil
	}

	answer := offer.Answer(a.provenLifecycleFacts())

	if retainErr := a.retainNegotiatedLifecycle(answer); retainErr != nil {
		return nil, retainErr
	}

	return map[string]any{lifecycle.MetaKey: answer.Advertisement()}, nil
}

// provenLifecycleFacts states the facts this configuration can prove. Amp has
// no activity channel between prompts. A supplied host authority additionally
// proves whole-process-tree vacancy after Wait succeeds.
func (a *Agent) provenLifecycleFacts() lifecycle.Negotiated {
	proven := lifecycle.Negotiated{
		Version:       lifecycle.Version,
		ActivityKinds: []lifecycle.ActivityKind{},
	}

	if a.options.hostAuthoritySupplied {
		proven.AuthoritativeQuiescence = true
		proven.QuiescenceSource = lifecycle.ProofClassProcessContainment
	}

	return proven
}

// retainNegotiatedLifecycle records the connection's one answer. The answer is
// the contract for the whole connection, not a value the latest `initialize`
// happens to hold: a second negotiation that would change it is refused on the
// lifecycle key rather than admitted. Withdrawing a present answer is the case
// that matters — enabling version 1 obligates the foreground stream for every
// session on the connection, and a later key-less `initialize` would cancel
// that obligation while the sessions it was published for are still live, so a
// host reducing the stream would see it stop mid-turn with no terminal event.
// Repeating the identical negotiation asserts the same contract and changes
// nothing, so it is admitted.
func (a *Agent) retainNegotiatedLifecycle(answer lifecycle.Negotiated) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.lifecycleAnswered && !sameNegotiatedLifecycle(a.lifecycle, answer) {
		return acp.NewInvalidParams(map[string]any{
			jsonFieldError: valUnsupported,
			jsonFieldField: lifecycle.MetaPath,
		})
	}

	a.lifecycle = answer
	a.lifecycleAnswered = true

	return nil
}

// sameNegotiatedLifecycle compares two answers member by member. Negotiated
// carries a slice, so it is not comparable with ==, and every member is part of
// the connection's exact answer.
func sameNegotiatedLifecycle(current, next lifecycle.Negotiated) bool {
	return current.Version == next.Version &&
		current.UpdatesOutsidePrompt == next.UpdatesOutsidePrompt &&
		current.AuthoritativeQuiescence == next.AuthoritativeQuiescence &&
		current.QuiescenceSource == next.QuiescenceSource &&
		slices.Equal(current.ActivityKinds, next.ActivityKinds)
}

// negotiatedLifecycle reports the answer this connection is bound by.
func (a *Agent) negotiatedLifecycle() lifecycle.Negotiated {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lifecycle
}

// lifecycleParamError renders a refused negotiation or correlation value. The
// lifecycle key is the one family literal this adapter validates on the request
// itself, so the rejection names the exact member path that failed and carries
// the refusal's own verdict: `missing` when an enabled connection's prompt left
// the key out, and `unsupported` for every value that is present and refused.
func lifecycleParamError(refusal *lifecycle.ParamError) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: string(refusal.Verdict),
		jsonFieldField: refusal.Field,
	})
}

// rejectLifecycleMeta refuses the lifecycle key on a surface that never carries
// it. A family literal is never foreign and never a no-op: `initialize` and
// `session/prompt` are the only inbound surfaces that read one, and every other
// surface — `session/cancel` included, where the refusal fails the cancel closed
// before the native interrupt — rejects it rather than ignoring it as another
// namespace's business.
func rejectLifecycleMeta(meta map[string]any) error {
	if _, present := meta[lifecycle.MetaKey]; !present {
		return nil
	}

	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnsupported,
		jsonFieldField: lifecycle.MetaPath,
	})
}

// rejectLifecycleConfigOptionMeta refuses the key on either variant of
// `session/set_config_option`. The variant a request chose decides where its
// `_meta` lives, and neither placement is a surface that reads the key.
func rejectLifecycleConfigOptionMeta(params acp.SetSessionConfigOptionRequest) error {
	if params.Boolean != nil {
		return rejectLifecycleMeta(params.Boolean.Meta)
	}

	if params.ValueId != nil {
		return rejectLifecycleMeta(params.ValueId.Meta)
	}

	return nil
}

// rejectLifecycleMetaParams reads the reserved key off an extension request's
// raw params. Params that do not decode at all are refused as a whole before
// the key is read, because a body this adapter cannot parse holds no readable
// `_meta` and names no method: the uniform rejection is `params`, and it
// precedes method resolution exactly as the key's own refusal does. Absent
// params carry no key and are admitted here for the method itself to judge.
func rejectLifecycleMetaParams(params json.RawMessage) error {
	if len(bytes.TrimSpace(params)) == 0 {
		return nil
	}

	var carrier struct {
		Meta map[string]any `json:"_meta"` //nolint:tagliatelle // ACP fixes this reserved field name.
	}

	if err := json.Unmarshal(params, &carrier); err != nil {
		return unsupportedField(authFieldParams)
	}

	return rejectLifecycleMeta(carrier.Meta)
}
