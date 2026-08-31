package ampacp

import (
	"encoding/json"

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
		a.retainNegotiatedLifecycle(lifecycle.Negotiated{})

		// An omitted key and an empty answer are the same wire fact: the response
		// carries no lifecycle member at all.
		return map[string]any{}, nil
	}

	answer := offer.Answer(a.provenLifecycleFacts())

	a.retainNegotiatedLifecycle(answer)

	return map[string]any{lifecycle.MetaKey: answer.Advertisement()}, nil
}

// provenLifecycleFacts returns the one lifecycle capability this adapter emits.
func (a *Agent) provenLifecycleFacts() lifecycle.Negotiated {
	return lifecycle.Negotiated{Version: lifecycle.Version}
}

func (a *Agent) retainNegotiatedLifecycle(answer lifecycle.Negotiated) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.lifecycle = answer
}

// negotiatedLifecycle reports the answer this connection is bound by.
func (a *Agent) negotiatedLifecycle() lifecycle.Negotiated {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lifecycle
}

// lifecycleParamError renders a refused negotiation or correlation value. The
// lifecycle key is the one family literal this adapter validates on the request
// itself, so the rejection names the exact member path that failed.
func lifecycleParamError(refusal *lifecycle.ParamError) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnsupported,
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

func rejectLifecycleMetaParams(params json.RawMessage) error {
	var carrier struct {
		Meta map[string]any `json:"_meta"` //nolint:tagliatelle // ACP fixes this reserved field name.
	}

	_ = json.Unmarshal(params, &carrier)

	return rejectLifecycleMeta(carrier.Meta)
}
