package ampacp

import (
	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

// WithSessionAmpOptions merges Amp-specific options into _meta.amp.options.
func WithSessionAmpOptions(options AmpOptions) wire.SessionRequestOption {
	return wire.WithSessionMetaValue(options.Meta())
}

// WithSessionRawEvents toggles raw Amp event emission for the session.
func WithSessionRawEvents(enabled bool) wire.SessionRequestOption {
	return wire.WithSessionMetaValue(map[string]any{
		vendor: map[string]any{metaRawEventKey: map[string]any{metaEnabledKey: enabled}},
	})
}

// SetModelRequest constructs a model selector update, which Amp refuses: it
// advertises no model config option.
func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return wire.SetConfigOptionRequest(sessionID, configModel, acp.SessionConfigValueId(model))
}
