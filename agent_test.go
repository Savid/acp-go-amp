package ampacp

import (
	"context"
	"testing"

	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestCapabilityAdvertisesTheMediaEnvelopeAndHandoffForm(t *testing.T) {
	t.Parallel()

	plain := newHarness(t).initialize().AgentCapabilities.Meta
	envelope, ok := plain[wire.MediaEnvelopeKey].(map[string]any)
	require.True(t, ok)
	require.InDelta(t, float64(amp.MaxInputImageBytes), envelope["maxBytes"], 0)
	require.InDelta(t, float64(amp.MaxImageDimension), envelope["maxDimension"], 0)
	require.Equal(t, []any{"image/png", "image/jpeg", "image/gif", "image/webp"}, envelope["imageFormats"])
	require.Empty(t, envelope["documentFormats"])
	require.NotContains(t, plain, wire.HandoffKey)

	withRoot := newHarness(t, WithInputHandoffRoot(t.TempDir())).initialize().AgentCapabilities.Meta
	require.Equal(t, map[string]any{"version": float64(1)}, withRoot[wire.HandoffKey])
}

func TestVersionProbeRunsAgainAfterACancelledRequest(t *testing.T) {
	t.Parallel()

	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := a.NewSession(cancelled, wire.NewSessionRequest(t.TempDir()))
	require.Error(t, err)

	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
}

func TestVersionFloorRefusesAnOlderNative(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithEnv(map[string]string{
		"ACP_GO_AMP_TEST_NATIVE":  t.TempDir(),
		"ACP_GO_AMP_TEST_VERSION": "0.0.1",
		"GORACE":                  "atexit_sleep_ms=0",
	}))
	h.initialize()

	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	data := requestErrorData(t, err)
	require.Equal(t, "amp_internal_failure", data["error"])
	require.Equal(t, internalClassNativeStart, data["class"])
}

func TestCancelledVersionProbeDoesNotPoisonAgent(t *testing.T) {
	agent := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = agent.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := agent.ensureExecutable(ctx)
	require.Error(t, err)
	_, err = agent.ensureExecutable(t.Context())
	require.NoError(t, err, "a request-local cancelled probe permanently poisoned the agent")
}
