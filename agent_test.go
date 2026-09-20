package ampacp

import (
	"context"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

func TestCloseReleasesActiveSessions(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	a := NewAgent(testOptions(t, WithMeterProvider(provider))...)
	a.attach(newRecorder(), nil)

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, a.Close())

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))

	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "acp_go_"+vendor+".session.active" {
				continue
			}

			sum, ok := metric.Data.(metricdata.Sum[int64])
			require.True(t, ok)

			for _, point := range sum.DataPoints {
				require.Zero(t, point.Value, "a closed agent reports no active sessions")
			}

			return
		}
	}

	t.Fatal("active-session metric missing")
}
