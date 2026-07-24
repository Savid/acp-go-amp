package ampacp

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"strings"

	"github.com/coder/acp-go-sdk"
)

const (
	imageField = "prompt.image"

	imageErrorMissingData          = "missing_data"
	imageErrorInvalidBase64        = "invalid_base64"
	imageErrorInvalidMediaType     = "invalid_media_type"
	imageErrorMediaTypeMismatch    = "media_type_mismatch"
	imageErrorAnimatedNotSupported = "animated_not_supported"
	imageErrorInvalidDimensions    = "invalid_dimensions"
	imageErrorTooLarge             = "too_large"
	imageErrorNativeEnvelope       = "native_envelope_exceeded"

	imageMIMEPNG  = "image/png"
	imageMIMEJPEG = "image/jpeg"
	imageMIMEGIF  = "image/gif"
	imageMIMEWebP = "image/webp"

	ampNativeMaxImageBytes     int64  = 5_138_022
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
	limits     ImageLimits
	nextIndex  int
	totalBytes int64
}

func (b *imagePromptBudget) validate(data, mimeType string) (validatedPromptImage, error) {
	index := b.nextIndex
	b.nextIndex++

	if data == "" {
		return validatedPromptImage{}, imagePromptError(index, imageErrorMissingData)
	}

	if !isPromptImageMIME(mimeType) {
		return validatedPromptImage{}, imagePromptError(index, imageErrorInvalidMediaType)
	}

	// Retain up to the ACP transport frame cap so structural inspection sees the
	// whole decodable image. The ingress frame already bounds any payload to this
	// many decoded bytes, so retaining it is memory-safe; size verdicts below
	// still gate on the full decoded size.
	decoded, sizeBytes, err := decodePromptImage(data, maxACPImageDecodedBytes)
	if err != nil {
		return validatedPromptImage{}, imagePromptError(index, imageErrorInvalidBase64)
	}

	sniffedMIME := sniffPromptImageMIME(decoded)
	if sniffedMIME == "" {
		return validatedPromptImage{}, imagePromptError(index, imageErrorMediaTypeMismatch)
	}

	width, height, animated, err := inspectPromptImage(sniffedMIME, decoded)
	if err != nil || width == 0 || height == 0 {
		return validatedPromptImage{}, imagePromptError(index, imageErrorInvalidDimensions)
	}

	if animated {
		return validatedPromptImage{}, imagePromptError(index, imageErrorAnimatedNotSupported)
	}

	if sniffedMIME != mimeType {
		return validatedPromptImage{}, imagePromptError(index, imageErrorMediaTypeMismatch)
	}

	if maxBytes := b.limits.MaxInputBytesPerImage; maxBytes > 0 && sizeBytes > maxBytes {
		return validatedPromptImage{}, imagePromptSizeError(index, imageErrorTooLarge, sizeBytes, maxBytes)
	}

	b.totalBytes += sizeBytes
	if maxBytes := b.limits.MaxInputBytesPerPrompt; maxBytes > 0 && b.totalBytes > maxBytes {
		return validatedPromptImage{}, imagePromptSizeError(index, imageErrorTooLarge, b.totalBytes, maxBytes)
	}

	if sizeBytes > ampNativeMaxImageBytes {
		return validatedPromptImage{}, imagePromptSizeError(
			index,
			imageErrorNativeEnvelope,
			sizeBytes,
			ampNativeMaxImageBytes,
		)
	}

	if width > ampNativeMaxImageDimension || height > ampNativeMaxImageDimension {
		return validatedPromptImage{}, imagePromptError(index, imageErrorNativeEnvelope)
	}

	return validatedPromptImage{base64: base64.StdEncoding.EncodeToString(decoded)}, nil
}

func imagePromptError(index int, errorValue string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldField: imageField,
		jsonFieldError: errorValue,
		keyIndex:       index,
	})
}

func imagePromptSizeError(index int, errorValue string, sizeBytes, maxBytes int64) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldField: imageField,
		jsonFieldError: errorValue,
		keyIndex:       index,
		"sizeBytes":    sizeBytes,
		"maxBytes":     maxBytes,
	})
}

func isPromptImageMIME(mimeType string) bool {
	switch mimeType {
	case imageMIMEPNG, imageMIMEJPEG, imageMIMEGIF, imageMIMEWebP:
		return true
	default:
		return false
	}
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
