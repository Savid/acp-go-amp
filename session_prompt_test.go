package ampacp

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	stdimage "image"
	"image/png"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/savid/acp-go-core/wire"
)

// rasterBytes is one decodable PNG the input gate accepts.
func rasterBytes(t *testing.T) []byte {
	t.Helper()

	var b bytes.Buffer
	require.NoError(t, png.Encode(&b, stdimage.NewRGBA(stdimage.Rect(0, 0, 1, 1))))

	return b.Bytes()
}

func promptRaster(t *testing.T) string {
	t.Helper()

	return base64.StdEncoding.EncodeToString(rasterBytes(t))
}
func TestImageInputOrder(t *testing.T) {
	s := &session{agent: NewAgent()}
	blocks := []acp.ContentBlock{acp.TextBlock("before"), acp.ImageBlock(promptRaster(t), "image/png"), acp.TextBlock("after")}
	mapped, err := s.mapPrompt(t.Context(), blocks)
	require.NoError(t, err)
	var frame struct {
		Message struct {
			Content []map[string]any `json:"content"`
		} `json:"message"`
	}
	require.NoError(t, json.Unmarshal(mapped, &frame))
	kinds := make([]string, 0, len(frame.Message.Content))
	for _, part := range frame.Message.Content {
		kind, ok := part["type"].(string)
		require.True(t, ok)
		kinds = append(kinds, kind)
	}
	require.Equal(t, []string{"text", "image", "text"}, kinds)
}

// A resource carrying both a text and a blob variant is sent as its text, with
// no image part.
func TestPromptClassifiesADualVariantResourceAsText(t *testing.T) {
	mime := "image/png"
	block := acp.ContentBlock{Resource: &acp.ContentBlockResource{Resource: acp.EmbeddedResourceResource{
		TextResourceContents: &acp.TextResourceContents{Uri: "file:///notes.txt", Text: "embedded note"},
		BlobResourceContents: &acp.BlobResourceContents{Uri: "file:///shot.png", Blob: promptRaster(t), MimeType: &mime},
	}}}

	s := &session{agent: NewAgent()}
	mapped, err := s.mapPrompt(t.Context(), []acp.ContentBlock{block})
	require.NoError(t, err)
	require.Contains(t, string(mapped), "embedded note")
}

func TestPromptReadsHandoffImagesWithoutSendingThePath(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "shot.png")
	raster := rasterBytes(t)
	require.NoError(t, os.WriteFile(path, raster, 0o600))

	digest := sha256.Sum256(raster)
	uri := "file://" + path
	block := acp.ContentBlock{Image: &acp.ContentBlockImage{
		MimeType: "image/png",
		Uri:      &uri,
		Meta: map[string]any{wire.HandoffKey: map[string]any{
			"version": 1, "digest": hex.EncodeToString(digest[:]), "sizeBytes": len(raster),
		}},
	}}

	s := &session{agent: NewAgent(WithInputHandoffRoot(root))}
	mapped, err := s.mapPrompt(t.Context(), []acp.ContentBlock{acp.TextBlock("describe"), block})
	require.NoError(t, err)
	require.Contains(t, string(mapped), base64.StdEncoding.EncodeToString(raster))
	require.NotContains(t, string(mapped), path)

	// Without a configured root the same block is refused rather than read.
	bare := &session{agent: NewAgent()}
	_, err = bare.mapPrompt(t.Context(), []acp.ContentBlock{block})
	require.Equal(t, "invalid_handoff", requestErrorData(t, err)["error"])
}

func TestPromptRefusesAnImageOverThePerImageLimit(t *testing.T) {
	raster := rasterBytes(t)
	size := int64(len(raster))
	blocks := []acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(raster), "image/png")}

	for _, tc := range []struct {
		name    string
		limit   int64
		refused bool
	}{
		{"at the limit", size, false},
		{"one byte over", size - 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &session{agent: NewAgent(WithImageLimits(ImageLimits{MaxInputBytesPerImage: tc.limit}))}

			_, err := s.mapPrompt(t.Context(), blocks)
			if !tc.refused {
				require.NoError(t, err)

				return
			}

			data := requestErrorData(t, err)
			require.Equal(t, "too_large", data["error"])
			require.Equal(t, "prompt.image", data["field"])
			require.InDelta(t, float64(size), data["sizeBytes"], 0)
			require.InDelta(t, float64(tc.limit), data["maxBytes"], 0)
		})
	}
}

// pngOfSize pads a decodable PNG with one private ancillary chunk so the whole
// file is exactly size bytes.
func pngOfSize(t *testing.T, size int64) []byte {
	t.Helper()

	base := rasterBytes(t)
	payload := int(size) - len(base) - 12
	require.Positive(t, payload)

	chunk := make([]byte, 0, payload+12)
	chunk = binary.BigEndian.AppendUint32(chunk, uint32(payload))
	body := append([]byte("prVt"), make([]byte, payload)...)
	chunk = append(chunk, body...)
	chunk = binary.BigEndian.AppendUint32(chunk, crc32.ChecksumIEEE(body))

	const ihdrEnd = 33

	padded := make([]byte, 0, int(size))
	padded = append(padded, base[:ihdrEnd]...)
	padded = append(padded, chunk...)
	padded = append(padded, base[ihdrEnd:]...)
	require.Len(t, padded, int(size))

	return padded
}

// Amp's own per-image ceiling is the limit the gate enforces when the
// configured limit sits above it.
func TestPromptRefusesAnImageOverTheNativeCeiling(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		size    int64
		refused bool
	}{
		{"at the ceiling", amp.MaxInputImageBytes, false},
		{"one byte over", amp.MaxInputImageBytes + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := &session{agent: NewAgent()}
			blocks := []acp.ContentBlock{acp.ImageBlock(base64.StdEncoding.EncodeToString(pngOfSize(t, tc.size)), "image/png")}

			_, err := s.mapPrompt(t.Context(), blocks)
			if !tc.refused {
				require.NoError(t, err)

				return
			}

			data := requestErrorData(t, err)
			require.Equal(t, "too_large", data["error"])
			require.Equal(t, "prompt.image", data["field"])
			require.InDelta(t, float64(tc.size), data["sizeBytes"], 0)
			require.InDelta(t, float64(amp.MaxInputImageBytes), data["maxBytes"], 0)
		})
	}
}
