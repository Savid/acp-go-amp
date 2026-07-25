package ampacp

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

const blobMIMEPDF = "application/pdf"

func blobResourceBlock(blob, mimeType string) acp.ContentBlock {
	return acp.ResourceBlock(acp.EmbeddedResourceResource{
		BlobResourceContents: &acp.BlobResourceContents{
			Blob:     blob,
			MimeType: acp.Ptr(mimeType),
			Uri:      "file:///doc.pdf",
		},
	})
}

func defaultPolicy() promptImagePolicy {
	return promptImagePolicy{limits: applyOptions(nil).ImageLimits}
}

// TestBlobResourceChannelRejectsOversizeBytes pins that a non-image blob is
// bounded by the same per-image decoded limit an image blob is: the fixture is
// larger than the policy default and must be rejected rather than forwarded. The
// verdict names the resource channel, because the block a host would have to fix
// is a resource block and there is no image block at that index to inspect.
func TestBlobResourceChannelRejectsOversizeBytes(t *testing.T) {
	oversize := make([]byte, 6_295_951)
	limits := applyOptions(nil).ImageLimits

	require.Greater(t, int64(len(oversize)), limits.MaxInputBytesPerImage)

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{blobResourceBlock(base64.StdEncoding.EncodeToString(oversize), blobMIMEPDF)},
		defaultPolicy(),
	)
	requireInvalidParamsData(t, err, resourceSizeErrorData(
		0,
		imageErrorTooLarge,
		int64(len(oversize)),
		limits.MaxInputBytesPerImage,
	))
}

// TestBlobResourceChannelReportsTheAggregateOnItsOwnChannel pins that the
// per-prompt aggregate verdict a blob triggers also names the resource channel,
// not the image contract that lent the budget.
func TestBlobResourceChannelReportsTheAggregateOnItsOwnChannel(t *testing.T) {
	document := make([]byte, 4096)

	limits := applyOptions(nil).ImageLimits
	limits.MaxInputBytesPerPrompt = int64(len(document)) - 1

	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{blobResourceBlock(base64.StdEncoding.EncodeToString(document), blobMIMEPDF)},
		promptImagePolicy{limits: limits},
	)
	requireInvalidParamsData(t, err, resourceSizeErrorData(
		0,
		imageErrorTooLarge,
		int64(len(document)),
		limits.MaxInputBytesPerPrompt,
	))
}

// TestBlobResourceChannelRejectsAnEmptyRasterBlob pins that a raster declaration
// carrying no bytes is missing_data, exactly as an empty image content block is:
// an embedded resource blob has no handoff form to fall back to.
func TestBlobResourceChannelRejectsAnEmptyRasterBlob(t *testing.T) {
	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{blobResourceBlock("", imageMIMEPNG)},
		defaultPolicy(),
	)
	requireInvalidParamsData(t, err, imageErrorData(0, imageErrorMissingData))
}

func TestBlobResourceChannelRejectsCorruptBase64(t *testing.T) {
	_, err := promptInputWithPolicy(
		[]acp.ContentBlock{blobResourceBlock("%%%", blobMIMEPDF)},
		defaultPolicy(),
	)
	requireInvalidParamsData(t, err, resourceErrorData(0, imageErrorInvalidBase64))
}

// TestBlobResourceChannelCountsTowardThePromptAggregate pins that blob bytes are
// charged to the same per-prompt budget image bytes are, and that a blob consumes
// a position in the gated-media sequence.
func TestBlobResourceChannelCountsTowardThePromptAggregate(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	document := make([]byte, 4096)

	limits := applyOptions(nil).ImageLimits
	limits.MaxInputBytesPerPrompt = int64(len(document) + len(validPNG) - 1)

	_, err := promptInputWithPolicy([]acp.ContentBlock{
		blobResourceBlock(base64.StdEncoding.EncodeToString(document), blobMIMEPDF),
		acp.ImageBlock(base64.StdEncoding.EncodeToString(validPNG), imageMIMEPNG),
	}, promptImagePolicy{limits: limits})
	requireInvalidParamsData(t, err, imageSizeErrorData(
		1,
		imageErrorTooLarge,
		int64(len(document)+len(validPNG)),
		limits.MaxInputBytesPerPrompt,
	))
}

// TestGatedMediaBlocksNeverShareAnIndex pins that the error envelope's index
// counts gated media blocks in request order: a gated blob consumes a position
// whatever its declared media type, so a later block can never report an index an
// earlier one already used.
func TestGatedMediaBlocksNeverShareAnIndex(t *testing.T) {
	document := base64.StdEncoding.EncodeToString([]byte("%PDF-1.7 body"))
	raster := base64.StdEncoding.EncodeToString(imageFixture(t, "valid.png"))

	for _, test := range []struct {
		name   string
		blocks []acp.ContentBlock
		want   int
	}{
		{
			name: "an untyped blob precedes the failing image",
			blocks: []acp.ContentBlock{
				blobResourceBlock(document, blobMIMEPDF),
				acp.ImageBlock("", imageMIMEPNG),
			},
			want: 1,
		},
		{
			name: "a raster blob precedes the failing image",
			blocks: []acp.ContentBlock{
				blobResourceBlock(raster, imageMIMEPNG),
				acp.ImageBlock("", imageMIMEPNG),
			},
			want: 1,
		},
		{
			name: "two untyped blobs precede the failing image",
			blocks: []acp.ContentBlock{
				blobResourceBlock(document, blobMIMEPDF),
				acp.TextBlock("between"),
				blobResourceBlock(document, blobMIMEPDF),
				acp.ImageBlock("", imageMIMEPNG),
			},
			want: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := promptInputWithPolicy(test.blocks, defaultPolicy())
			requireInvalidParamsData(t, err, imageErrorData(test.want, imageErrorMissingData))
		})
	}
}

// TestBlobResourceChannelKeepsItsDownstreamShape pins that gating the channel did
// not change what a conforming non-image blob becomes: prompt text carrying its
// base64.
func TestBlobResourceChannelKeepsItsDownstreamShape(t *testing.T) {
	document := base64.StdEncoding.EncodeToString([]byte("%PDF-1.7 body"))

	input, err := promptInputWithPolicy(
		[]acp.ContentBlock{blobResourceBlock(document, blobMIMEPDF)},
		defaultPolicy(),
	)
	require.NoError(t, err)

	text := promptContentText(t, input)
	require.Contains(t, text, "MIME: "+blobMIMEPDF)
	require.Contains(t, text, document)
}

// TestRasterDeclarationsNeverReachTheUntypedBlobChannel is the security pin: a
// declaration that claims a raster type is routed into the image gates whatever
// its case or parameters, so its base64 can never be inlined into the prompt text
// and around the native envelope.
func TestRasterDeclarationsNeverReachTheUntypedBlobChannel(t *testing.T) {
	validPNG := imageFixture(t, "valid.png")
	encoded := base64.StdEncoding.EncodeToString(validPNG)

	for _, declared := range []string{
		"IMAGE/PNG",
		"Image/Png",
		"  image/PNG  ",
		"image/png; charset=binary",
		"IMAGE/PNG;charset=binary",
		"image/svg+xml",
		"IMAGE/HEIC",
	} {
		t.Run(declared, func(t *testing.T) {
			input, err := promptInputWithPolicy(
				[]acp.ContentBlock{blobResourceBlock(encoded, declared)},
				defaultPolicy(),
			)
			requireInvalidParamsData(t, err, imageErrorData(0, imageErrorInvalidMediaType))
			require.Nil(t, input)

			// The prompt Amp would build carries no base64 for this input at all.
			built, marshalErr := json.Marshal(input)
			require.NoError(t, marshalErr)
			require.NotContains(t, string(built), encoded)
			require.NotContains(t, string(built), encoded[:32])
		})
	}
}

// TestUntypedBlobChannelStillCarriesBase64 is the control for the pin above: the
// channel that must never see raster bytes does still inline the bytes of a blob
// that declares no raster type, so the assertion there is meaningful.
func TestUntypedBlobChannelStillCarriesBase64(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("plain bytes"))

	input, err := promptInputWithPolicy(
		[]acp.ContentBlock{blobResourceBlock(encoded, blobMIMEPDF)},
		defaultPolicy(),
	)
	require.NoError(t, err)
	require.Contains(t, promptContentText(t, input), encoded)
}

// TestBlobResourceWithoutAMediaTypeStaysUntyped pins that an absent declaration
// is not a raster declaration and still reaches the gated untyped channel.
func TestBlobResourceWithoutAMediaTypeStaysUntyped(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("plain bytes"))

	input, err := promptInputWithPolicy([]acp.ContentBlock{
		acp.ResourceBlock(acp.EmbeddedResourceResource{
			BlobResourceContents: &acp.BlobResourceContents{Blob: encoded, Uri: "file:///doc.bin"},
		}),
	}, defaultPolicy())
	require.NoError(t, err)

	text := promptContentText(t, input)
	require.NotContains(t, text, "MIME:")
	require.Contains(t, text, encoded)
}

// TestResourceLinkCarriesNoBytesWhateverItDeclares pins that the resource-link
// channel is a naming channel: it carries no payload to normalize and no bytes to
// gate, whatever media type it declares.
func TestResourceLinkCarriesNoBytesWhateverItDeclares(t *testing.T) {
	link := acp.ResourceLinkBlock("shot", "file:///shot.png")
	link.ResourceLink.MimeType = acp.Ptr("IMAGE/PNG")

	input, err := promptInputWithPolicy([]acp.ContentBlock{link}, defaultPolicy())
	require.NoError(t, err)

	text := promptContentText(t, input)
	require.Contains(t, text, "URI: file:///shot.png")
	require.Contains(t, text, "MIME: IMAGE/PNG")
	require.NotContains(t, text, "Base64")
}

func TestNormalizedMediaTypeRouting(t *testing.T) {
	for _, test := range []struct {
		declared string
		want     string
		raster   bool
	}{
		{declared: "image/png", want: "image/png", raster: true},
		{declared: "IMAGE/PNG", want: "image/png", raster: true},
		{declared: " image/png ", want: "image/png", raster: true},
		{declared: "image/png;charset=binary", want: "image/png", raster: true},
		{declared: "IMAGE/PNG ; charset=binary", want: "image/png", raster: true},
		{declared: "application/pdf", want: "application/pdf", raster: false},
		{declared: "", want: "", raster: false},
		{declared: "text/image-like", want: "text/image-like", raster: false},
	} {
		t.Run(test.declared, func(t *testing.T) {
			require.Equal(t, test.want, normalizedMediaType(test.declared))
			require.Equal(t, test.raster, declaresRasterMediaType(test.declared))
		})
	}
}

// promptContentText joins the text of every text block in a built native prompt.
func promptContentText(t *testing.T, input map[string]any) string {
	t.Helper()

	message, ok := input[keyMessage].(map[string]any)
	require.True(t, ok)
	content, ok := message[keyContent].([]map[string]any)
	require.True(t, ok)

	parts := make([]string, 0, len(content))

	for _, block := range content {
		if block[keyType] == valText {
			text, textOK := block[valText].(string)
			require.True(t, textOK)
			parts = append(parts, text)
		}
	}

	return strings.Join(parts, "\n")
}
