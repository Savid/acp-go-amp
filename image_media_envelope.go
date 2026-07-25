package ampacp

import "slices"

const (
	// metaMediaEnvelopeKey is the family-reserved _meta key carrying the adapter's
	// effective inbound media bounds.
	metaMediaEnvelopeKey = "acp-go.dev/mediaEnvelope"

	keyMaxPromptBytes  = "maxPromptBytes"
	keyMaxDimension    = "maxDimension"
	keyImageFormats    = "imageFormats"
	keyDocumentFormats = "documentFormats"
	keyVersions        = "versions"
)

// effectiveInputBytesPerImage resolves the tightest per-image decoded byte bound
// the prompt gates actually enforce. Amp's native envelope is unconditional, so
// it is the ceiling even when the configured policy limit is zero (disabled) —
// which is also why a disabled policy limit can never leave a read unbounded. A
// configured limit below the envelope wins. This is the bound the media envelope
// advertises and the bound a handoff read is capped at.
func effectiveInputBytesPerImage(limits ImageLimits) int64 {
	bound := ampNativeMaxImageBytes
	if configured := limits.MaxInputBytesPerImage; configured > 0 && configured < bound {
		bound = configured
	}

	return bound
}

// effectiveInputBytesPerPrompt resolves the configured per-prompt aggregate into
// the bound the gates actually enforce. A disabled (zero) aggregate stays
// disabled: the handoff-form block count bounds the read work instead, so
// restating "disabled" as a byte number would reject the multi-image turn the
// handoff form exists to carry. Advertisement and gate both read this, so the
// number a host is told is the number it is judged by.
func effectiveInputBytesPerPrompt(limits ImageLimits) int64 {
	return limits.MaxInputBytesPerPrompt
}

// promptImageMediaTypes is Amp's inbound image allowlist, in the order the media
// envelope advertises it. The allowlist gate and the advertisement read this one
// list so they cannot disagree.
func promptImageMediaTypes() []string {
	return []string{imageMIMEPNG, imageMIMEJPEG, imageMIMEGIF, imageMIMEWebP}
}

func isPromptImageMIME(mimeType string) bool {
	return slices.Contains(promptImageMediaTypes(), mimeType)
}

// mediaEnvelopeMeta renders the adapter's effective inbound media bounds. Every
// value is derived from the constants and limits the prompt gates read, so the
// advertisement cannot drift from what is enforced.
func mediaEnvelopeMeta(limits ImageLimits) map[string]any {
	return map[string]any{
		keyMaxBytes:       effectiveInputBytesPerImage(limits),
		keyMaxPromptBytes: effectiveInputBytesPerPrompt(limits),
		keyMaxDimension:   int64(ampNativeMaxImageDimension),
		keyImageFormats:   promptImageMediaTypes(),
		// Amp maps no media type to a native document representation: a blob that
		// declares no raster type reaches the model as prompt text, which is not
		// better than materializing the file.
		keyDocumentFormats: []string{},
	}
}

// handoffAdvertisement renders the handoff capability advertisement. It is
// emitted only when a read root is configured, so its absence tells a host that
// its option never reached this adapter.
func handoffAdvertisement() map[string]any {
	return map[string]any{keyVersions: []int64{handoffVersion}}
}
