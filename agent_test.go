package ampacp

import (
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
