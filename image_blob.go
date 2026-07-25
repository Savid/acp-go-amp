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
// Amp maps no such media type to a native representation, so the blob degrades
// to its uri and declaration and its base64 is never carried into the prompt.
// The block still spends the media budget on the payload the host actually sent
// — the base64 length, which is what a transport would have had to carry — so
// declaring bytes as an untyped blob buys a prompt no more of them than an image
// block gets, and the advertised envelope stays a true statement of the tightest
// inbound byte gate.
//
// Its verdicts report the resource channel: the byte bounds are borrowed from
// the image contract, but the block a host would have to fix is a resource
// block. A blob that is not valid base64 is refused ahead of every gate and so
// consumes no position in the gated-media sequence.
func (b *imagePromptBudget) admitBlob(blob string) error {
	if _, _, err := decodePromptImage(blob, 0); err != nil {
		return promptMediaError(resourceField, b.nextIndex, imageErrorInvalidBase64)
	}

	index := b.nextImageIndex()

	sizeBytes := int64(len(blob))
	if maxBytes := effectiveInputBytesPerImage(b.limits); sizeBytes > maxBytes {
		return promptMediaSizeError(resourceField, index, imageErrorTooLarge, sizeBytes, maxBytes)
	}

	return b.charge(resourceField, index, sizeBytes)
}
