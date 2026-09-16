package ampacp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestUnsupportedOptionsAndMCP(t *testing.T) {
	t.Parallel()
	for _, option := range []Option{WithHome("/tmp/home"), WithDefaultModel("model"), WithConfiguredModels([]string{"model"})} {
		a := NewAgent(option)
		_, err := a.Initialize(t.Context(), acp.InitializeRequest{})
		require.Equal(t, "amp_invalid_options", requestErrorData(t, err)["error"])
	}
	for _, options := range []AmpOptions{NewAmpOptions(WithAmpModel("model")), NewAmpOptions(WithAmpOutputSchema(map[string]any{}))} {
		require.Error(t, ValidateAmpSessionMeta(options.Meta()))
	}
	h := newHarness(t)
	h.initialize()
	request := wire.NewSessionRequest(t.TempDir())
	require.NoError(t, json.Unmarshal([]byte(`[{"name":"forbidden","command":"no","args":[],"env":[]}]`), &request.McpServers))
	_, err := h.conn.NewSession(h.ctx(), request)
	require.Equal(t, "mcpServers", requestErrorData(t, err)["field"])
}

// Unknown and unsupported values in the owned _meta.amp namespace, including an
// empty one, are refused.
func TestSessionMetaFailsClosed(t *testing.T) {
	t.Parallel()
	for _, meta := range []map[string]any{
		{vendor: map[string]any{"options": map[string]any{"model": ""}}},
		{vendor: map[string]any{"options": map[string]any{"model": "provider/id"}}},
		{vendor: map[string]any{"options": map[string]any{"mode": ""}}},
		{vendor: map[string]any{"options": map[string]any{"unknown": "value"}}},
		{vendor: map[string]any{"unknown": map[string]any{}}},
		{vendor: "not an object"},
	} {
		require.Error(t, ValidateAmpSessionMeta(meta))
	}
}

// A session advertises exactly one config option, the mode select under the
// mode category, and refuses the model selector id.
func TestModeIsTheOnlyConfigOption(t *testing.T) {
	t.Parallel()
	const mode = "plugin-mode"

	h := newHarness(t)
	h.initialize()
	session := h.newSession(WithSessionAmpOptions(NewAmpOptions(WithAmpMode(mode))))
	require.Len(t, session.ConfigOptions, 1)
	option := session.ConfigOptions[0].Select
	require.Equal(t, acp.SessionConfigId("mode"), option.Id)
	require.Equal(t, "select", option.Type)
	require.NotNil(t, option.Category)
	require.Equal(t, acp.SessionConfigOptionCategoryMode, *option.Category)
	require.Equal(t, acp.SessionConfigValueId(mode), option.CurrentValue)
	values := make([]string, 0, 5)
	for _, value := range *option.Options.Ungrouped {
		values = append(values, string(value.Value))
	}
	// An accepted value outside the built-in menu is appended to it.
	require.Equal(t, "low medium high ultra "+mode, strings.Join(values, " "))
	_, err := h.conn.SetSessionConfigOption(h.ctx(), SetModelRequest(session.SessionId, "provider/id"))
	require.Equal(t, "configId", requestErrorData(t, err)["field"])
}

// Lines Amp writes to its stdout that are not frames, and anything on its
// stderr, never reach the ACP stream or end the turn.
func TestNativeNoiseCannotCorruptACPStdout(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "NOISE", nil)
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "hello NOISE")

	for _, update := range h.rec.snapshot() {
		encoded, marshalErr := json.Marshal(update)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(encoded), "not a json record at all")
		require.NotContains(t, string(encoded), "chatter on stderr")
	}

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, list.Sessions, 1)
}
