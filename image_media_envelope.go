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

// perImageByteTerm is one term of the per-image decoded byte gate: a bound and
// the verdict a payload above it reports. The two terms answer differently
// because a host fixes them differently — a configured policy limit is the
// host's own number, and Amp's native envelope is not negotiable.
type perImageByteTerm struct {
	maxBytes int64
	value    string
}

// perImageByteTerms is the per-image decoded byte gate, in the order it is
// applied. The configured policy limit is judged first so it keeps its own
// verdict, and Amp's unconditional native envelope always follows it — which is
// why a zero-disabled policy limit can never leave a read unbounded.
//
// This is the single resolution both the gates and the media-envelope
// advertisement call, so an advertised bound and the verdict a send actually
// gets are structurally unable to disagree.
func perImageByteTerms(limits ImageLimits) []perImageByteTerm {
	terms := make([]perImageByteTerm, 0, 2)
	if configured := limits.MaxInputBytesPerImage; configured > 0 {
		terms = append(terms, perImageByteTerm{maxBytes: configured, value: imageErrorTooLarge})
	}

	return append(terms, perImageByteTerm{maxBytes: ampNativeMaxImageBytes, value: imageErrorNativeEnvelope})
}

// effectiveInputBytesPerImage folds the gate's own terms into the one bound the
// media envelope advertises and a handoff read is capped at: the tightest of
// them, because a payload has to pass every term.
func effectiveInputBytesPerImage(limits ImageLimits) int64 {
	bound := int64(0)

	for index, term := range perImageByteTerms(limits) {
		if index == 0 || term.maxBytes < bound {
			bound = term.maxBytes
		}
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
