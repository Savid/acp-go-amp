package ampacp

import "strings"

// imageMediaTypePrefix is tested only against a normalized declaration, so a
// differently cased or parameterized raster declaration is routed into the image
// gates instead of an untyped channel.
const imageMediaTypePrefix = "image/"

// normalizedMediaType lowercases, trims, and strips parameters from a declared
// media type. It exists to route a declaration, never to accept one: the
// allowlist comparison stays on the exact declared value, so an unrecognized
// raster declaration is invalid_media_type rather than a silent acceptance.
func normalizedMediaType(declared string) string {
	essence, _, _ := strings.Cut(declared, ";")

	return strings.ToLower(strings.TrimSpace(essence))
}

// declaresRasterMediaType reports whether a declaration claims to be a raster
// image. Every inbound prefix test goes through here so a declaration cannot
// dodge the image gates on case or a trailing parameter.
func declaresRasterMediaType(declared string) bool {
	return strings.HasPrefix(normalizedMediaType(declared), imageMediaTypePrefix)
}

// admitBlob gates an embedded resource blob that carries no raster declaration.
// Its bytes reach Amp inside the prompt text, so the channel spends the same
// decoded budget an image block spends: the blob must be valid base64, must fit
// the configured per-image bound, and counts toward the per-prompt aggregate. It
// consumes a position in the gated-media sequence like any other gated block, so
// no two blocks in one prompt can report the same index.
func (b *imagePromptBudget) admitBlob(blob string) error {
	index := b.nextImageIndex()

	_, sizeBytes, err := decodePromptImage(blob, 0)
	if err != nil {
		return imagePromptError(index, imageErrorInvalidBase64)
	}

	return b.charge(index, sizeBytes)
}
