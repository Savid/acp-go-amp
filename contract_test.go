package ampacp

import (
	"cmp"
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

func TestInvalidOptionsVerdict(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, field string
		option      Option
	}{
		{"", "home", WithHome("/tmp/home")},
		{"", "scratchDir", WithScratchDir("relative")},
		{"", "inputHandoffRoot", WithInputHandoffRoot("relative")},
		{"", "defaultModel", WithDefaultModel("model")},
		{"", "configuredModels", WithConfiguredModels([]string{"model"})},
		{"", "env", WithEnv(map[string]string{"": "x"})},
		{"", "concurrencyLimits", WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1})},
		{"", "imageLimits", WithImageLimits(ImageLimits{MaxInputBytesPerImage: -1})},
	}

	for _, tc := range cases {
		field, option := tc.field, tc.option

		t.Run(cmp.Or(tc.name, tc.field), func(t *testing.T) {
			t.Parallel()

			agent := NewAgent(testOptions(t, option)...)
			t.Cleanup(func() { _ = agent.Close() })

			_, err := agent.Initialize(context.Background(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
			require.Equal(t, -32603, requestErrorCode(t, err))

			data := requestErrorData(t, err)
			require.Equal(t, "amp_invalid_options", data["error"])
			require.Equal(t, field, data["field"])

			_, err = agent.NewSession(context.Background(), wire.NewSessionRequest(t.TempDir()))
			require.Equal(t, "amp_invalid_options", requestErrorData(t, err)["error"])
		})
	}
}

func TestNegativeClientCallLimitReturnsOptionsError(t *testing.T) {
	t.Parallel()

	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: -1}))
	defer agent.Close()

	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.Equal(t, "amp_invalid_options", requestErrorData(t, err)["error"])
}

// A non-empty mcpServers list is refused naming the field, and the model
// option is refused before any session exists.
func TestMCPServersRefused(t *testing.T) {
	t.Parallel()

	require.Error(t, ValidateAmpSessionMeta(NewAmpOptions(WithAmpModel("model")).Meta()))

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

func TestProtocolAdmission(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	initialized := h.initialize()
	require.NotContains(t, initialized.AgentCapabilities.Meta["amp"], wire.AccountUsageCapabilityKey)

	for _, method := range []string{"_amp/anything", "_amp/accountUsage"} {
		_, err := h.conn.CallExtension(h.ctx(), method, map[string]any{})
		require.Equal(t, -32601, requestErrorCode(t, err), method)
	}

	_, err := h.conn.SetSessionMode(h.ctx(), acp.SetSessionModeRequest{SessionId: "x", ModeId: "plan"})
	require.Equal(t, -32601, requestErrorCode(t, err))

	_, err = h.conn.Authenticate(h.ctx(), acp.AuthenticateRequest{MethodId: "oauth"})
	require.Equal(t, -32602, requestErrorCode(t, err))
	require.Equal(t, "oauth", requestErrorData(t, err)["methodId"])

	_, err = h.conn.Logout(h.ctx(), acp.LogoutRequest{})
	require.Equal(t, -32601, requestErrorCode(t, err))
}

func TestDefaultExecutableResolvesFromTheBasePath(t *testing.T) {
	t.Parallel()

	bin := t.TempDir()
	require.NoError(t, os.Symlink(os.Args[0], filepath.Join(bin, vendor)))

	h := newHarness(t, WithExecutablePath(""), WithEnv(map[string]string{"PATH": bin, "ACP_GO_AMP_TEST_NATIVE": filepath.Join(t.TempDir(), "native")}))
	h.initialize()
	h.newSession()
}

func TestInitializeShape(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	resp := h.initialize(withLifecycle(), func(request *acp.InitializeRequest) {
		request.ClientCapabilities.PositionEncodings = []acp.PositionEncodingKind{acp.PositionEncodingKindUtf8}
	})

	require.Empty(t, resp.AuthMethods)
	require.True(t, resp.AgentCapabilities.LoadSession)
	require.True(t, resp.AgentCapabilities.PromptCapabilities.Image)
	require.True(t, resp.AgentCapabilities.PromptCapabilities.EmbeddedContext)
	require.False(t, resp.AgentCapabilities.PromptCapabilities.Audio)
	require.False(t, resp.AgentCapabilities.McpCapabilities.Http)
	require.Nil(t, resp.AgentCapabilities.Nes)
	require.Nil(t, resp.AgentCapabilities.Providers)
	require.Nil(t, resp.AgentCapabilities.SessionCapabilities.Fork)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.Close)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.Delete)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.List)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.Resume)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.AdditionalDirectories)
	require.Equal(t, acp.PositionEncodingKindUtf8, *resp.AgentCapabilities.PositionEncoding)

	encoded, err := json.Marshal(resp.AgentCapabilities)
	require.NoError(t, err)

	var members map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &members))
	require.Equal(t, []string{"_meta", "auth", "loadSession", "mcpCapabilities", "positionEncoding", "promptCapabilities", "sessionCapabilities"}, slices.Sorted(maps.Keys(members)))

	answer, ok := resp.Meta[wire.LifecycleKey].(map[string]any)
	require.True(t, ok)
	require.EqualValues(t, 1, answer["version"])
	require.Equal(t, false, answer["updatesOutsidePrompt"])
	require.Equal(t, []any{}, answer["activityKinds"])
	require.NotContains(t, resp.AgentCapabilities.Meta, wire.LifecycleKey)

	_, err = h.conn.UnstableForkSession(h.ctx(), acp.UnstableForkSessionRequest{SessionId: "x", Cwd: t.TempDir()})
	require.Equal(t, -32601, requestErrorCode(t, err))

	plain := newHarness(t).initialize()
	require.Nil(t, plain.Meta)
	require.Equal(t, acp.PositionEncodingKindUtf16, *plain.AgentCapabilities.PositionEncoding)
}

func TestSessionMetaStrictness(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		meta  map[string]any
		field string
	}{
		{"unknown own key", map[string]any{vendor: map[string]any{"bogus": 1}}, "_meta.amp.bogus"},
		{"unknown option", map[string]any{vendor: map[string]any{"options": map[string]any{"bogus": 1}}}, "_meta.amp.options.bogus"},
		{"model", map[string]any{vendor: map[string]any{"options": map[string]any{"model": "provider/id"}}}, "_meta.amp.options.model"},
		{"relative path dir", map[string]any{vendor: map[string]any{"options": map[string]any{"extraPathDirs": []any{"rel"}}}}, "_meta.amp.options.extraPathDirs[0]"},
		{"bad env name", map[string]any{vendor: map[string]any{"options": map[string]any{"env": map[string]any{"A=B": "x"}}}}, "_meta.amp.options.env.A=B"},
		{"lifecycle literal", map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}, `_meta["` + wire.LifecycleKey + `"]`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.initialize()

			request := wire.NewSessionRequest(t.TempDir())
			request.Meta = tc.meta

			_, err := h.conn.NewSession(h.ctx(), request)
			require.Equal(t, -32602, requestErrorCode(t, err))

			data := requestErrorData(t, err)
			require.Equal(t, "unsupported", data["error"])
			require.Equal(t, tc.field, data["field"])
		})
	}
}

func TestForeignMetaIgnored(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	session := h.newSession(wire.WithSessionMeta(map[string]any{"other": map[string]any{"x": 1}, "traceparent": "00-1-2-01"}))
	require.NotEmpty(t, session.SessionId)
}

func TestUniformRejections(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	_, err := h.conn.NewSession(h.ctx(), acp.NewSessionRequest{Cwd: "relative", McpServers: []acp.McpServer{}})
	require.Equal(t, -32602, requestErrorCode(t, err))
	require.Equal(t, "cwd", requestErrorData(t, err)["field"])

	session := h.newSession()

	_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId))
	require.Equal(t, -32602, requestErrorCode(t, err))
	require.Equal(t, "prompt", requestErrorData(t, err)["field"])

	_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.ContentBlock{Audio: &acp.ContentBlockAudio{Data: "x", MimeType: "audio/wav"}}))
	require.Equal(t, -32602, requestErrorCode(t, err))
	require.Equal(t, "prompt", requestErrorData(t, err)["field"])

	const unknown acp.SessionId = "00000000-0000-4000-8000-000000000000"

	_, err = h.conn.Prompt(h.ctx(), wire.TextPromptRequest(unknown, "hi"))
	require.Equal(t, -32602, requestErrorCode(t, err))
	require.Equal(t, "unknown session", requestErrorData(t, err)["error"])

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(unknown)))
}

func TestPromptCorrelationGate(t *testing.T) {
	t.Parallel()

	t.Run("required when negotiated", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		h.initialize(withLifecycle())
		session := h.newSession()

		_, err := h.prompt(session.SessionId, "HELLO", nil)
		data := requestErrorData(t, err)
		require.Equal(t, "missing", data["error"])
		require.Equal(t, `_meta["`+wire.LifecycleKey+`"]`, data["field"])

		_, err = h.prompt(session.SessionId, "HELLO", map[string]any{wire.LifecycleKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "", "clientNonce": "n"}}})
		data = requestErrorData(t, err)
		require.Equal(t, "unsupported", data["error"])
		require.Contains(t, data["field"], "submissionId")
	})

	t.Run("refused when omitted", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		h.initialize()
		session := h.newSession()

		_, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
		data := requestErrorData(t, err)
		require.Equal(t, "unsupported", data["error"])
		require.Equal(t, `_meta["`+wire.LifecycleKey+`"]`, data["field"])
	})

	t.Run("fraction version refused over the wire", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		h.initialize(withLifecycle())
		session := h.newSession()

		_, err := h.prompt(session.SessionId, "HELLO", map[string]any{wire.LifecycleKey: map[string]any{"version": json.Number("1.0"), "submission": map[string]any{"submissionId": "s", "clientNonce": "n"}}})
		require.Equal(t, "unsupported", requestErrorData(t, err)["error"])
	})
}

func TestActiveSessionLimit(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	h.initialize()
	h.newSession()

	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Equal(t, -32600, requestErrorCode(t, err))
	require.Equal(t, "backpressure", requestErrorData(t, err)["error"])
	require.Equal(t, "active_sessions", requestErrorData(t, err)["limit"])

	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
}

func TestClosedAgentRefusesRequests(t *testing.T) {
	t.Parallel()

	agent := NewAgent(testOptions(t)...)
	require.NoError(t, agent.Close())
	require.NoError(t, agent.Close())

	t.Run("Initialize", func(t *testing.T) {
		t.Parallel()

		_, err := agent.Initialize(t.Context(), acp.InitializeRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("Authenticate", func(t *testing.T) {
		t.Parallel()

		_, err := agent.Authenticate(t.Context(), acp.AuthenticateRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("Logout", func(t *testing.T) {
		t.Parallel()

		_, err := agent.Logout(t.Context(), acp.LogoutRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("NewSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.NewSession(t.Context(), acp.NewSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("LoadSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.LoadSession(t.Context(), acp.LoadSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("ResumeSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.ResumeSession(t.Context(), acp.ResumeSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("ListSessions", func(t *testing.T) {
		t.Parallel()

		_, err := agent.ListSessions(t.Context(), acp.ListSessionsRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("Prompt", func(t *testing.T) {
		t.Parallel()

		_, err := agent.Prompt(t.Context(), acp.PromptRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("CloseSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("UnstableDeleteSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("SetSessionConfigOption", func(t *testing.T) {
		t.Parallel()

		_, err := agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("SetSessionMode", func(t *testing.T) {
		t.Parallel()

		_, err := agent.SetSessionMode(t.Context(), acp.SetSessionModeRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("extension", func(t *testing.T) {
		t.Parallel()

		_, err := agent.HandleExtensionMethod(t.Context(), "_unknown/read", json.RawMessage(`{}`))
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("cancel", func(t *testing.T) {
		t.Parallel()

		err := agent.Cancel(t.Context(), acp.CancelNotification{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})
}

func TestInitializeLifecycleAnswer(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	resp := h.initialize(withLifecycle())

	answer, ok := resp.Meta[wire.LifecycleKey].(map[string]any)
	require.True(t, ok)
	require.EqualValues(t, 1, answer["version"])
	require.Equal(t, false, answer["updatesOutsidePrompt"])
	require.Equal(t, []any{}, answer["activityKinds"])
	require.NotContains(t, resp.AgentCapabilities.Meta, wire.LifecycleKey)
}

func TestInitializeLifecycleStrictness(t *testing.T) {
	t.Parallel()

	cases := map[string]any{
		"string version": map[string]any{"version": "1"},
		"fraction":       map[string]any{"version": json.Number("1.0")},
		"other version":  map[string]any{"version": 2},
		"unknown member": map[string]any{"version": 1, "extra": true},
		"non-object":     true,
	}

	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			meta := map[string]any{wire.LifecycleKey: value}

			_, err := h.conn.Initialize(h.ctx(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: meta})
			require.Equal(t, -32602, requestErrorCode(t, err))

			data := requestErrorData(t, err)
			require.Equal(t, "unsupported", data["error"])
			require.Contains(t, data["field"], wire.LifecycleKey)
		})
	}
}

func TestInitializeWithoutHandoffOmitsAdvertisement(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	resp := h.initialize()
	require.NotContains(t, resp.AgentCapabilities.Meta, wire.HandoffKey)
	require.Equal(t, acp.PositionEncodingKindUtf16, *resp.AgentCapabilities.PositionEncoding)
}

func TestPromptBackpressure(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	done := make(chan error, 1)

	go func() {
		_, err := h.prompt(session.SessionId, "SLOW", promptMeta(1))
		done <- err
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		return slices.Contains(lifecycleEventKinds(updates), "prompt_accepted")
	})

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, -32600, requestErrorCode(t, err))
	require.Equal(t, "backpressure", requestErrorData(t, err)["error"])
	require.Equal(t, "session_prompt", requestErrorData(t, err)["limit"])

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.NoError(t, <-done)
}
