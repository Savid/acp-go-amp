package ampacp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/coder/acp-go-sdk"
)

const (
	// imageField and resourceField name the request member a verdict belongs to,
	// not the gate that produced it. Anything routed into the image chain — an
	// image content block, or a blob whose declaration names a raster type —
	// reports the image contract; every other embedded blob reports the resource
	// channel it actually arrived on, because a host has no image block to look at
	// for that index.
	imageField    = "prompt.image"
	resourceField = "prompt.resource"

	imageErrorMissingData          = "missing_data"
	imageErrorInvalidBase64        = "invalid_base64"
	imageErrorInvalidMediaType     = "invalid_media_type"
	imageErrorMediaTypeMismatch    = "media_type_mismatch"
	imageErrorAnimatedNotSupported = "animated_not_supported"
	imageErrorInvalidDimensions    = "invalid_dimensions"
	imageErrorTooLarge             = "too_large"
	imageErrorNativeEnvelope       = "native_envelope_exceeded"
	imageErrorInvalidHandoff       = "invalid_handoff"
	imageErrorPathNotAllowed       = "path_not_allowed"
	imageErrorMissingFile          = "missing_file"
	imageErrorDigestMismatch       = "handoff_digest_mismatch"

	imageMIMEPNG  = "image/png"
	imageMIMEJPEG = "image/jpeg"
	imageMIMEGIF  = "image/gif"
	imageMIMEWebP = "image/webp"

	// Amp keeps a thread's messages in a backing store that caps a single commit
	// at 1,310,720 bytes of dirty data — 320 four-kilobyte pages — and an image
	// is stored as inline base64. base64 of 983,040 decoded bytes is exactly that
	// cap, which is why an image-bearing append above it fails with an untyped
	// internal error rather than a typed size rejection.
	//
	// This gate is 921,600 decoded bytes: 1,228,800 base64 bytes, 300 of the 320
	// available pages, leaving the rest of the commit room to land. The cap is
	// per commit rather than per thread, so a long thread has less headroom than
	// a fresh one; 300 pages is the conservative pin.
	//
	// The 4.9 MiB figure sometimes quoted for Amp images is not a published
	// limit: it is a 5,138,022.4-byte constant read out of the shipped amp CLI's
	// minified bundle, so it is a client-side pre-flight in a different
	// enforcement layer rather than a service maximum, and it is far too loose
	// to protect this path.
	ampNativeMaxImageBytes     int64  = 921_600
	ampNativeMaxImageDimension uint32 = 8000
)

var (
	errInvalidImageStructure = errors.New("invalid image structure")
	pngImageSignature        = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}
)

type validatedPromptImage struct {
	base64 string
}

type imagePromptBudget struct {
	limits ImageLimits
	// handoffRoot is the configured read root for the handoff input form. Empty
	// rejects every handoff-form block.
	handoffRoot string
	nextIndex   int
	totalBytes  int64
	// handoffBlocks counts the handoff-form blocks this prompt has asked the
	// adapter to read, which is bounded independently of the byte aggregate a
	// host may disable.
	handoffBlocks int
	// root is the opened read root, held for the life of one prompt mapping so
	// every handoff open in that prompt is relative to one kernel-checked
	// descriptor.
	root *os.Root
}

// nextImageIndex allocates the position this media block reports in the prompt's
// gated-media sequence. It is claimed only once a block reaches the shared gate
// chain — image content blocks and embedded resource blobs alike — so a block
// refused ahead of every gate stays invisible to the counter, and no two gated
// blocks in one prompt can report the same index.
func (b *imagePromptBudget) nextImageIndex() int {
	index := b.nextIndex
	b.nextIndex++

	return index
}

// validateImageBlock selects the block's input form and validates it. Non-empty
// data is the embedded form even when a handoff envelope is also present; empty
// data with handoff intent is the handoff form; empty data with neither is
// missing_data.
func (b *imagePromptBudget) validateImageBlock(ctx context.Context, block *acp.ContentBlockImage) (validatedPromptImage, error) {
	if block.Data != "" {
		return b.validateEmbedded(imageField, block.Data, block.MimeType)
	}

	if !promptHandoffIntent(block) {
		return validatedPromptImage{}, imagePromptError(b.nextIndex, imageErrorMissingData)
	}

	decoded, failure := b.handoffBytes(ctx, block)
	if failure != nil {
		return validatedPromptImage{}, imagePromptHandoffError(b.nextIndex, failure)
	}

	// Both forms deliver every byte they account for: a handoff read past the
	// declared size is rejected where it is read, so the byte the gates count is
	// the byte that arrived.
	return b.gate(imageField, b.nextImageIndex(), block.MimeType, decoded, int64(len(decoded)))
}

// validate validates an embedded base64 image carried by a resource block. The
// bytes arrived on a resource, so every verdict names that member even though a
// raster declaration is what routed them into the image chain.
func (b *imagePromptBudget) validate(data, mimeType string) (validatedPromptImage, error) {
	if data == "" {
		return validatedPromptImage{}, promptMediaError(resourceField, b.nextIndex, imageErrorMissingData)
	}

	return b.validateEmbedded(resourceField, data, mimeType)
}

func (b *imagePromptBudget) validateEmbedded(field, data, mimeType string) (validatedPromptImage, error) {
	if !isPromptImageMIME(mimeType) {
		return validatedPromptImage{}, promptMediaError(field, b.nextIndex, imageErrorInvalidMediaType)
	}

	// Retain up to the ACP transport frame cap so structural inspection sees the
	// whole decodable image. The ingress frame already bounds any payload to this
	// many decoded bytes, so retaining it is memory-safe; size verdicts below
	// still gate on the full decoded size, so a payload past the retained window
	// is rejected rather than truncated and forwarded.
	decoded, sizeBytes, err := decodePromptImage(data, maxACPImageDecodedBytes)
	if err != nil {
		return validatedPromptImage{}, promptMediaError(field, b.nextIndex, imageErrorInvalidBase64)
	}

	return b.gate(field, b.nextImageIndex(), mimeType, decoded, sizeBytes)
}

// gate runs the structural and size verdicts every accepted image passes,
// whatever transport carried its bytes. sizeBytes is the payload's full decoded
// size, which can exceed the retained prefix in decoded. The caller supplies the
// request member its channel reports, because routing is chosen by media type
// while the field follows the block the bytes arrived on.
func (b *imagePromptBudget) gate(field string, index int, mimeType string, decoded []byte, sizeBytes int64) (validatedPromptImage, error) {
	sniffedMIME := sniffPromptImageMIME(decoded)
	if sniffedMIME == "" {
		return validatedPromptImage{}, promptMediaError(field, index, imageErrorMediaTypeMismatch)
	}

	width, height, animated, err := inspectPromptImage(sniffedMIME, decoded)
	if err != nil || width == 0 || height == 0 {
		return validatedPromptImage{}, promptMediaError(field, index, imageErrorInvalidDimensions)
	}

	if animated {
		return validatedPromptImage{}, promptMediaError(field, index, imageErrorAnimatedNotSupported)
	}

	if sniffedMIME != mimeType {
		return validatedPromptImage{}, promptMediaError(field, index, imageErrorMediaTypeMismatch)
	}

	if err := b.charge(field, index, sizeBytes); err != nil {
		return validatedPromptImage{}, err
	}

	if sizeBytes > ampNativeMaxImageBytes {
		return validatedPromptImage{}, promptMediaSizeError(
			field,
			index,
			imageErrorNativeEnvelope,
			sizeBytes,
			ampNativeMaxImageBytes,
		)
	}

	if width > ampNativeMaxImageDimension || height > ampNativeMaxImageDimension {
		return validatedPromptImage{}, promptMediaError(field, index, imageErrorNativeEnvelope)
	}

	return validatedPromptImage{base64: base64.StdEncoding.EncodeToString(decoded)}, nil
}

// charge applies the configured per-image bound and adds the payload to the
// prompt's running decoded total. Both bounds are shared by every gated media
// block, so the caller supplies the field its channel reports.
func (b *imagePromptBudget) charge(field string, index int, sizeBytes int64) error {
	if maxBytes := b.limits.MaxInputBytesPerImage; maxBytes > 0 && sizeBytes > maxBytes {
		return promptMediaSizeError(field, index, imageErrorTooLarge, sizeBytes, maxBytes)
	}

	b.totalBytes += sizeBytes
	if maxBytes := effectiveInputBytesPerPrompt(b.limits); maxBytes > 0 && b.totalBytes > maxBytes {
		return promptMediaSizeError(field, index, imageErrorTooLarge, b.totalBytes, maxBytes)
	}

	return nil
}

// chargeText adds a text resource's bytes to the same per-prompt accumulator the
// media forms use. Bytes are bytes: declaring them as text rather than as a blob
// must not buy a prompt more of them than the aggregate allows. It reports at the
// position the next media block would take without consuming it, because a text
// resource carries no media the index is meant to identify.
func (b *imagePromptBudget) chargeText(sizeBytes int64) error {
	b.totalBytes += sizeBytes
	if maxBytes := effectiveInputBytesPerPrompt(b.limits); maxBytes > 0 && b.totalBytes > maxBytes {
		return promptMediaSizeError(resourceField, b.nextIndex, imageErrorTooLarge, b.totalBytes, maxBytes)
	}

	return nil
}

// promptMediaError reports a gated-media defect against the request member the
// block arrived on.
func promptMediaError(field string, index int, errorValue string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldField: field,
		jsonFieldError: errorValue,
		keyIndex:       index,
	})
}

func imagePromptError(index int, errorValue string) error {
	return promptMediaError(imageField, index, errorValue)
}

// imagePromptHandoffError reports a handoff-form defect. The bytes arrived on an
// image block, so every handoff verdict names the image contract. A byte verdict
// carries the size pair every other byte verdict carries; every other verdict
// carries a constant message, because a host cannot tell a malformed block from
// a bad deployment from the error value alone.
func imagePromptHandoffError(index int, failure *handoffError) error {
	data := map[string]any{
		jsonFieldField: imageField,
		jsonFieldError: failure.value,
		keyIndex:       index,
	}

	if failure.message != "" {
		data[keyMessage] = failure.message
	}

	if failure.sizeBytes > 0 {
		data[keySizeBytes] = failure.sizeBytes
	}

	if failure.maxBytes > 0 {
		data[keyMaxBytes] = failure.maxBytes
	}

	return acp.NewInvalidParams(data)
}

func promptMediaSizeError(field string, index int, errorValue string, sizeBytes, maxBytes int64) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldField: field,
		jsonFieldError: errorValue,
		keyIndex:       index,
		keySizeBytes:   sizeBytes,
		keyMaxBytes:    maxBytes,
	})
}

type boundedImageWriter struct {
	data  []byte
	limit int64
	size  int64
}

func (w *boundedImageWriter) Write(p []byte) (int, error) {
	w.size += int64(len(p))

	remaining := w.limit - int64(len(w.data))
	if remaining > 0 {
		retain := int64(len(p))
		if retain > remaining {
			retain = remaining
		}

		w.data = append(w.data, p[:retain]...)
	}

	return len(p), nil
}

func decodePromptImage(data string, retainLimit int64) ([]byte, int64, error) {
	decoded := &boundedImageWriter{limit: retainLimit}

	_, err := io.Copy(decoded, base64.NewDecoder(base64.StdEncoding, strings.NewReader(data)))
	if err != nil {
		return nil, 0, err
	}

	return decoded.data, decoded.size, nil
}

func sniffPromptImageMIME(data []byte) string {
	switch {
	case bytes.HasPrefix(data, pngImageSignature):
		return imageMIMEPNG
	case len(data) >= 3 && data[0] == 0xff && data[1] == 0xd8 && data[2] == 0xff:
		return imageMIMEJPEG
	case len(data) >= 6 && (string(data[:6]) == "GIF87a" || string(data[:6]) == "GIF89a"):
		return imageMIMEGIF
	case len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP":
		return imageMIMEWebP
	default:
		return ""
	}
}

func inspectPromptImage(mimeType string, data []byte) (uint32, uint32, bool, error) {
	switch mimeType {
	case imageMIMEPNG:
		return inspectPromptPNG(data)
	case imageMIMEJPEG:
		width, height, err := inspectPromptJPEG(data)

		return width, height, false, err
	case imageMIMEGIF:
		return inspectPromptGIF(data)
	case imageMIMEWebP:
		return inspectPromptWebP(data)
	default:
		return 0, 0, false, errInvalidImageStructure
	}
}

func inspectPromptPNG(data []byte) (uint32, uint32, bool, error) {
	offset := len(pngImageSignature)
	if len(data) < offset+8+13 || string(data[offset+4:offset+8]) != "IHDR" {
		return 0, 0, false, errInvalidImageStructure
	}

	if binary.BigEndian.Uint32(data[offset:offset+4]) != 13 {
		return 0, 0, false, errInvalidImageStructure
	}

	width := binary.BigEndian.Uint32(data[offset+8 : offset+12])

	height := binary.BigEndian.Uint32(data[offset+12 : offset+16])
	if width == 0 || height == 0 {
		return 0, 0, false, errInvalidImageStructure
	}

	offset += 8 + 13 + 4
	for offset+8 <= len(data) {
		lengthValue := binary.BigEndian.Uint32(data[offset : offset+4])
		chunkType := string(data[offset+4 : offset+8])

		switch chunkType {
		case "acTL":
			return width, height, true, nil
		case "IDAT":
			return width, height, false, nil
		}

		chunkEnd := uint64(offset) + 12 + uint64(lengthValue)
		if chunkEnd > uint64(len(data)) {
			return width, height, false, nil
		}

		offset = int(chunkEnd) //nolint:gosec // chunkEnd is bounded by len(data).
	}

	return width, height, false, nil
}

func inspectPromptJPEG(data []byte) (uint32, uint32, error) {
	offset := 2
	for offset < len(data) {
		for offset < len(data) && data[offset] == 0xff {
			offset++
		}

		if offset >= len(data) {
			break
		}

		marker := data[offset]
		offset++

		if marker == 0x00 {
			continue
		}

		if marker == 0xd8 || marker == 0x01 || marker >= 0xd0 && marker <= 0xd7 {
			continue
		}

		if marker == 0xd9 || marker == 0xda || offset+2 > len(data) {
			break
		}

		length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		if length < 2 || offset+length > len(data) {
			break
		}

		if isJPEGFrameMarker(marker) {
			if length < 7 {
				break
			}

			height := uint32(binary.BigEndian.Uint16(data[offset+3 : offset+5]))

			width := uint32(binary.BigEndian.Uint16(data[offset+5 : offset+7]))
			if width == 0 || height == 0 {
				break
			}

			return width, height, nil
		}

		offset += length
	}

	return 0, 0, errInvalidImageStructure
}

func isJPEGFrameMarker(marker byte) bool {
	return marker >= 0xc0 && marker <= 0xcf && marker != 0xc4 && marker != 0xc8 && marker != 0xcc
}

func inspectPromptGIF(data []byte) (uint32, uint32, bool, error) {
	if len(data) < 13 {
		return 0, 0, false, errInvalidImageStructure
	}

	width := uint32(binary.LittleEndian.Uint16(data[6:8]))

	height := uint32(binary.LittleEndian.Uint16(data[8:10]))
	if width == 0 || height == 0 {
		return 0, 0, false, errInvalidImageStructure
	}

	offset := 13
	if data[10]&0x80 != 0 {
		offset += 3 << ((data[10] & 0x07) + 1)
	}

	frames := 0

	for offset < len(data) {
		switch data[offset] {
		case 0x2c:
			frames++

			if frames > 1 {
				return width, height, true, nil
			}

			offset = skipPromptGIFImage(data, offset)
		case 0x21:
			if offset+2 > len(data) {
				return width, height, false, nil
			}

			offset = skipPromptGIFSubBlocks(data, offset+2)
		case 0x3b:
			return width, height, false, nil
		default:
			return width, height, false, nil
		}
	}

	return width, height, false, nil
}

func skipPromptGIFImage(data []byte, offset int) int {
	if offset+10 > len(data) {
		return len(data)
	}

	packed := data[offset+9]

	offset += 10
	if packed&0x80 != 0 {
		offset += 3 << ((packed & 0x07) + 1)
	}

	if offset >= len(data) {
		return len(data)
	}

	return skipPromptGIFSubBlocks(data, offset+1)
}

func skipPromptGIFSubBlocks(data []byte, offset int) int {
	for offset < len(data) {
		size := int(data[offset])
		offset++

		if size == 0 {
			return offset
		}

		if size > len(data)-offset {
			return len(data)
		}

		offset += size
	}

	return offset
}

func inspectPromptWebP(data []byte) (uint32, uint32, bool, error) {
	if len(data) < 20 {
		return 0, 0, false, errInvalidImageStructure
	}

	payload := data[20:]
	switch string(data[12:16]) {
	case "VP8X":
		if len(payload) < 10 {
			return 0, 0, false, errInvalidImageStructure
		}

		width := 1 + uint32(payload[4]) + uint32(payload[5])<<8 + uint32(payload[6])<<16
		height := 1 + uint32(payload[7]) + uint32(payload[8])<<8 + uint32(payload[9])<<16

		return width, height, payload[0]&0x02 != 0, nil
	case "VP8 ":
		if len(payload) < 10 || payload[3] != 0x9d || payload[4] != 0x01 || payload[5] != 0x2a {
			return 0, 0, false, errInvalidImageStructure
		}

		width := uint32(binary.LittleEndian.Uint16(payload[6:8]) & 0x3fff)

		height := uint32(binary.LittleEndian.Uint16(payload[8:10]) & 0x3fff)
		if width == 0 || height == 0 {
			return 0, 0, false, errInvalidImageStructure
		}

		return width, height, false, nil
	case "VP8L":
		if len(payload) < 5 || payload[0] != 0x2f {
			return 0, 0, false, errInvalidImageStructure
		}

		bits := binary.LittleEndian.Uint32(payload[1:5])
		width := (bits & 0x3fff) + 1
		height := ((bits >> 14) & 0x3fff) + 1

		return width, height, false, nil
	default:
		return 0, 0, false, errInvalidImageStructure
	}
}
