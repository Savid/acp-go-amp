package ampacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A native image carrying inline bytes becomes an image block; one carrying
// only a remote URI becomes a resource link.
func TestImageOutputPrefersInlineBytes(t *testing.T) {
	t.Parallel()

	const uri = "https://example.invalid/shot.png"

	s := &session{agent: NewAgent()}

	block, size, failure := s.imageContent(map[string]any{
		fieldType:   contentImage,
		fieldSource: map[string]any{fieldData: promptRaster(t), "mediaType": "image/png", fieldURL: uri},
	})
	require.Nil(t, failure)
	require.Nil(t, block.ResourceLink)
	require.NotNil(t, block.Image)
	require.Positive(t, size)

	link, linkSize, failure := s.imageContent(map[string]any{
		fieldType:   contentImage,
		fieldSource: map[string]any{"mediaType": "image/png", fieldURL: uri},
	})
	require.Nil(t, failure)
	require.Nil(t, link.Image)
	require.NotNil(t, link.ResourceLink)
	require.Equal(t, uri, link.ResourceLink.Uri)
	require.Zero(t, linkSize)
}
