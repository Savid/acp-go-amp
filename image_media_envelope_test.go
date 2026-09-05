package ampacp

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

func initializeMeta(t *testing.T, options ...Option) map[string]any {
	t.Helper()

	resp, err := newTestAgent(options...).Initialize(t.Context(), acp.InitializeRequest{})
	require.NoError(t, err)

	return resp.AgentCapabilities.Meta
}

func TestMediaEnvelopeMarshalsDocumentFormatsAsAnEmptyArray(t *testing.T) {
	encoded, err := json.Marshal(initializeMeta(t)[metaMediaEnvelopeKey])
	require.NoError(t, err)

	require.Contains(t, string(encoded), `"documentFormats":[]`)
	require.NotContains(t, string(encoded), `"documentFormats":null`)
	require.Contains(t, string(encoded), `"imageFormats":["image/png","image/jpeg","image/gif","image/webp"]`)
}

func TestMediaEnvelopeTracksConfiguredImageLimits(t *testing.T) {
	limits := ImageLimits{MaxInputBytesPerImage: 500_000, MaxInputBytesPerPrompt: 700_000}

	envelope, ok := initializeMeta(t, WithImageLimits(limits))[metaMediaEnvelopeKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, int64(500_000), envelope[keyMaxBytes])
	require.Equal(t, int64(700_000), envelope[keyMaxPromptBytes])
}

// TestAdvertisedMaxBytesIsTheBoundTheGateEnforces walks the advertised per-image
// bound: a payload of exactly that many bytes is accepted and one byte more is
// rejected, for the native envelope and for a tighter configured limit alike.
func TestAdvertisedMaxBytesIsTheBoundTheGateEnforces(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")

	for _, test := range []struct {
		name   string
		limits ImageLimits
		want   string
	}{
		{
			name:   "native envelope binds under the policy default",
			limits: applyOptions(nil).ImageLimits,
			want:   imageErrorNativeEnvelope,
		},
		{
			name:   "configured limit binds when it is tighter",
			limits: ImageLimits{MaxInputBytesPerImage: 4096, MaxInputBytesPerPrompt: defaultImageLimitBytes},
			want:   imageErrorTooLarge,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			envelope, ok := initializeMeta(t, WithImageLimits(test.limits))[metaMediaEnvelopeKey].(map[string]any)
			require.True(t, ok)
			advertised, ok := envelope[keyMaxBytes].(int64)
			require.True(t, ok)

			atBound := make([]byte, advertised)
			copy(atBound, validPNG)

			_, err := promptInputWithPolicy(
				t.Context(),
				[]acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(atBound), imageMIMEPNG)},
				promptImagePolicy{limits: test.limits},
			)
			require.NoError(t, err, "a payload of exactly the advertised size must be accepted")

			overBound := make([]byte, advertised+1)
			copy(overBound, validPNG)

			_, err = promptInputWithPolicy(
				t.Context(),
				[]acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(overBound), imageMIMEPNG)},
				promptImagePolicy{limits: test.limits},
			)
			requireInvalidParamsData(t, err, imageSizeErrorData(0, test.want, advertised+1, advertised))
		})
	}
}

// TestAdvertisedMaxDimensionIsTheBoundTheGateEnforces walks the advertised
// per-dimension bound at its exact boundary.
func TestAdvertisedMaxDimensionIsTheBoundTheGateEnforces(t *testing.T) {
	envelope, ok := initializeMeta(t)[metaMediaEnvelopeKey].(map[string]any)
	require.True(t, ok)
	advertised, ok := envelope[keyMaxDimension].(int64)
	require.True(t, ok)

	validPNG := imageFixture(t, "valid.png")
	limits := applyOptions(nil).ImageLimits

	atBound := append([]byte(nil), validPNG...)
	binary.BigEndian.PutUint32(atBound[16:20], uint32(advertised))

	_, err := promptInputWithPolicy(
		t.Context(),
		[]acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(atBound), imageMIMEPNG)},
		promptImagePolicy{limits: limits},
	)
	require.NoError(t, err)

	overBound := append([]byte(nil), validPNG...)
	binary.BigEndian.PutUint32(overBound[16:20], uint32(advertised)+1)

	_, err = promptInputWithPolicy(
		t.Context(),
		[]acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(overBound), imageMIMEPNG)},
		promptImagePolicy{limits: limits},
	)
	requireInvalidParamsData(t, err, imageErrorData(0, imageErrorNativeEnvelope))
}

// TestAdvertisedMaxPromptBytesIsTheBoundTheGateEnforces walks the advertised
// aggregate bound: the image that crosses it reports the running total.
func TestAdvertisedMaxPromptBytesIsTheBoundTheGateEnforces(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	limits := ImageLimits{
		MaxInputBytesPerImage:  defaultImageLimitBytes,
		MaxInputBytesPerPrompt: int64(len(validPNG)*2 - 1),
	}

	envelope, ok := initializeMeta(t, WithImageLimits(limits))[metaMediaEnvelopeKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, limits.MaxInputBytesPerPrompt, envelope[keyMaxPromptBytes])

	encoded := base64.StdEncoding.EncodeToString(validPNG)
	_, err := promptInputWithPolicy(t.Context(), []acp.ContentBlock{
		acp.ImageBlock(encoded, imageMIMEPNG),
		acp.ImageBlock(encoded, imageMIMEPNG),
	}, promptImagePolicy{limits: limits})
	requireInvalidParamsData(t, err, imageSizeErrorData(
		1,
		imageErrorTooLarge,
		int64(len(validPNG)*2),
		limits.MaxInputBytesPerPrompt,
	))
}

// TestAdvertisedImageFormatsAreTheAllowlistTheGateEnforces pins that every
// advertised media type is accepted and an unadvertised one is not.
func TestAdvertisedImageFormatsAreTheAllowlistTheGateEnforces(t *testing.T) {
	envelope, ok := initializeMeta(t)[metaMediaEnvelopeKey].(map[string]any)
	require.True(t, ok)
	advertised, ok := envelope[keyImageFormats].([]string)
	require.True(t, ok)

	for _, mimeType := range advertised {
		require.True(t, isPromptImageMIME(mimeType))
	}

	require.False(t, isPromptImageMIME(imageMIMEBMP))

	_, err := promptInputWithPolicy(
		t.Context(),
		[]acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(imageFixture(t, "valid.png")), imageMIMEBMP)},
		promptImagePolicy{limits: applyOptions(nil).ImageLimits},
	)
	requireInvalidParamsData(t, err, imageErrorData(0, imageErrorInvalidMediaType))
}

func TestRelativeInputHandoffRootIsAConfigurationError(t *testing.T) {
	agent := newTestAgent(WithInputHandoffRoot("relative/handoff"))

	// The host built the agent, not the caller, so the refusal is a server fault.
	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{})
	// The construction verdict names no field: this agent refuses its whole
	// option set at once, and the reason stays on the joined Go error.
	requireInternalErrorData(t, err, map[string]any{jsonFieldError: errorInvalidOptions})
	require.ErrorContains(t, err, "input handoff root must be an absolute path")

	require.Error(t, agent.validateSessionStartOptions(AmpOptions{}))
}

func TestInputHandoffRootOption(t *testing.T) {
	root := absTestPath("srv", "handoff")
	require.Equal(t, root, applyOptions([]Option{WithInputHandoffRoot(root)}).InputHandoffRoot)
	require.Empty(t, applyOptions(nil).InputHandoffRoot)
	require.NoError(t, validateInputHandoffRoot(""))
	require.NoError(t, validateInputHandoffRoot(root))
	require.Error(t, validateInputHandoffRoot(filepath.Join("srv", "handoff")))
}

func sortedMetaKeys(meta map[string]any) []string {
	keys := make([]string, 0, len(meta))
	for key := range meta {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	return keys
}

// TestAdvertisedPerImageBoundIsTheGatesOwnResolution pins that the advertised
// `maxBytes` and the verdict a send actually gets come from one resolution. The
// advertisement is the tightest term; the term that produced it is the one that
// names the refusal, so a host at the advertised bound passes and a host one byte
// over is refused by the same term the advertisement came from.
func TestAdvertisedPerImageBoundIsTheGatesOwnResolution(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured int64
		want       int64
		verdict    string
	}{
		{
			name:       "the native envelope is the tightest term",
			configured: 0,
			want:       ampNativeMaxImageBytes,
			verdict:    imageErrorNativeEnvelope,
		},
		{
			name:       "a configured limit above the envelope never loosens it",
			configured: ampNativeMaxImageBytes * 2,
			want:       ampNativeMaxImageBytes,
			verdict:    imageErrorNativeEnvelope,
		},
		{
			name:       "a configured limit below the envelope is the bound and the verdict",
			configured: 4096,
			want:       4096,
			verdict:    imageErrorTooLarge,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := ImageLimits{MaxInputBytesPerImage: test.configured}

			advertised, ok := mediaEnvelopeMeta(limits)[keyMaxBytes].(int64)
			require.True(t, ok)
			require.Equal(t, test.want, advertised, "the advertisement is the gate's own bound")

			budget := &imagePromptBudget{limits: limits}
			require.NoError(t, budget.charge(imageField, 0, advertised),
				"a payload exactly at the advertised bound passes")

			over := &imagePromptBudget{limits: limits}
			err := over.charge(imageField, 0, advertised+1)
			requireInvalidParamsData(t, err, imageSizeErrorData(0, test.verdict, advertised+1, advertised))

			// The declared-size path a handoff block takes reads the same terms.
			handoff := &imagePromptBudget{limits: limits}
			require.Nil(t, handoff.declaredSizeVerdict(advertised))
			refusal := handoff.declaredSizeVerdict(advertised + 1)
			require.NotNil(t, refusal)
			require.Equal(t, test.verdict, refusal.value)
			require.Equal(t, advertised, refusal.maxBytes)
		})
	}
}
