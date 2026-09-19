package ampacp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

func TestSessionMetaClonesTypedEnvironment(t *testing.T) {
	env := map[string]string{"SESSION_VALUE": "original"}
	meta := map[string]any{vendor: map[string]any{"options": map[string]any{"env": env}}}
	option := wire.WithSessionMeta(meta)
	first := wire.NewSessionRequest(t.TempDir(), option)
	env["SESSION_VALUE"] = "caller changed"
	second := wire.NewSessionRequest(t.TempDir(), option)
	for _, request := range []map[string]any{first.Meta, second.Meta} {
		parsed, err := parseSessionMeta(request)
		require.Nil(t, err)
		require.Equal(t, "original", parsed.options.Env["SESSION_VALUE"], "a caller mutation reached a session request built earlier")
	}
}
