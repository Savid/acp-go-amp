package ampacp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

const (
	imageOutputStage = "image_output"

	imageOutputInvalidBase64     = "invalid_base64"
	imageOutputNotRaster         = "not_a_raster"
	imageOutputMediaTypeMismatch = "media_type_mismatch"
	imageOutputMissingFile       = "missing_file"
	imageOutputPathNotAllowed    = "path_not_allowed"
	imageOutputTooLarge          = "too_large"
	imageOutputStorageFailed     = "storage_failed"
	imageMIMEBMP                 = "image/bmp"

	canonicalImageArtifactType = "_amp_image_artifact"
	maxACPImageDecodedBytes    = int64(7_864_155)
)

type preparedToolImage struct {
	contentIndex int
	identity     string
	mimeType     string
	data         []byte
	encoded      string
	uri          string
	fingerprint  string
}

func imageOutputFailure(reason, message string, sizeBytes, maxBytes int64) error {
	data := map[string]any{
		jsonFieldError: turnFailedError,
		"cause":        causeTransport,
		keyMessage:     message,
		"stage":        imageOutputStage,
		"reason":       reason,
	}
	if sizeBytes > 0 || maxBytes > 0 {
		data["sizeBytes"] = sizeBytes
		data["maxBytes"] = maxBytes
	}

	return acp.NewInternalError(data)
}

func effectiveImageOutputLimit(configured int64) int64 {
	if configured <= 0 || configured > maxACPImageDecodedBytes {
		return maxACPImageDecodedBytes
	}

	return configured
}

func (s *agentSession) prepareMessageImageArtifacts(ctx context.Context, msg amp.Message) (string, error) {
	rawJSON := msg.RawJSON()

	user, ok := msg.(*amp.UserMessage)
	if !ok {
		return rawJSON, nil
	}

	transformed := false

	for index, block := range user.Content {
		result, ok := block.(amp.ToolResultBlock)
		if !ok {
			continue
		}

		canonical, changed, err := s.prepareToolResultArtifacts(ctx, result)
		if err != nil {
			_ = s.emitImageToolFailure(
				ctx,
				result.ToolUseID,
				result.IsError,
				parentToolUseTag(user.ParentToolUseID),
				err,
			)

			return "", err
		}

		if !changed {
			continue
		}

		result.Content = canonical
		if result.Raw != nil {
			result.Raw[keyContent] = canonical
		}

		user.Content[index] = result
		transformed = true
	}

	if !transformed {
		return rawJSON, nil
	}

	sanitized, err := json.Marshal(user.Raw)
	if err != nil {
		return "", imageOutputFailure(
			imageOutputStorageFailed,
			"encode sanitized image transcript frame",
			0,
			0,
		)
	}

	return string(sanitized), nil
}

func (s *agentSession) prepareToolResultArtifacts(
	ctx context.Context,
	result amp.ToolResultBlock,
) ([]any, bool, error) {
	items, structured := structuredToolResultItems(result.Content)
	if !structured {
		return nil, false, nil
	}

	var (
		canonical  = make([]any, 0, len(items))
		images     = make([]preparedToolImage, 0)
		totalBytes int64
	)

	for itemIndex, item := range items {
		raw, ok := item.(map[string]any)
		if !ok {
			canonical = append(canonical, sanitizeImageDiagnostic(item))

			continue
		}

		switch strings.ToLower(stringField(raw, keyType)) {
		case valText:
			canonical = append(canonical, map[string]any{
				keyType: valText,
				valText: stringField(raw, valText),
			})
		case valImage:
			image, err := prepareNativeToolImage(raw, result.ToolUseID, itemIndex, s.agent.options.ImageLimits)
			if err != nil {
				return nil, false, err
			}

			image.contentIndex = len(canonical)

			if image.data != nil {
				totalBytes += int64(len(image.data))

				maxBytes := effectiveImageOutputLimit(s.agent.options.ImageLimits.MaxOutputBytesPerToolCall)
				if totalBytes > maxBytes {
					return nil, false, imageOutputFailure(
						imageOutputTooLarge,
						"image output exceeds the configured per-tool-call limit",
						totalBytes,
						maxBytes,
					)
				}
			}

			images = append(images, image)
			canonical = append(canonical, nil)
		default:
			canonical = append(canonical, sanitizeImageDiagnostic(raw))
		}
	}

	if len(images) == 0 {
		return nil, false, nil
	}

	for _, image := range images {
		artifact := storedImageArtifact{
			Identity:    image.identity,
			Fingerprint: image.fingerprint,
			MimeType:    image.mimeType,
		}
		if image.data != nil {
			artifact.Kind = imageArtifactKindEmbedded
			artifact.Data = image.encoded
		} else {
			artifact.Kind = imageArtifactKindLink
			artifact.URI = image.uri
		}

		ref, err := s.persistImageArtifact(ctx, artifact)
		if err != nil {
			return nil, false, imageOutputFailure(imageOutputStorageFailed, err.Error(), 0, 0)
		}

		canonical[image.contentIndex] = map[string]any{
			keyType:    canonicalImageArtifactType,
			"ref":      ref,
			"identity": image.identity,
		}
	}

	return canonical, true, nil
}

func structuredToolResultItems(content any) ([]any, bool) {
	switch typed := content.(type) {
	case []any:
		return typed, true
	case map[string]any:
		return []any{typed}, true
	case string:
		var decoded any
		if json.Unmarshal([]byte(typed), &decoded) != nil {
			return nil, false
		}

		switch value := decoded.(type) {
		case []any:
			return value, true
		case map[string]any:
			return []any{value}, true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func prepareNativeToolImage(
	raw map[string]any,
	toolUseID string,
	itemIndex int,
	limits ImageLimits,
) (preparedToolImage, error) {
	identity := fmt.Sprintf("tool:%s:%d", toolUseID, itemIndex)
	declaredMIME := imageMIMEField(raw)

	data, location := nativeImageDataAndLocation(raw)
	if strings.HasPrefix(data, "data:") {
		dataMIME, encoded, ok := splitImageDataURL(data)
		if !ok {
			return preparedToolImage{}, imageOutputFailure(
				imageOutputInvalidBase64,
				"image output contains an invalid data URL",
				0,
				0,
			)
		}

		if declaredMIME == "" {
			declaredMIME = dataMIME
		} else if dataMIME != "" && dataMIME != declaredMIME {
			return preparedToolImage{}, imageOutputFailure(
				imageOutputMediaTypeMismatch,
				"image output data URL media type does not match its declared media type",
				0,
				0,
			)
		}

		data = encoded
	}

	if strings.HasPrefix(location, "data:") {
		dataMIME, encoded, ok := splitImageDataURL(location)
		if !ok {
			return preparedToolImage{}, imageOutputFailure(
				imageOutputInvalidBase64,
				"image output contains an invalid data URL",
				0,
				0,
			)
		}

		if declaredMIME == "" {
			declaredMIME = dataMIME
		} else if dataMIME != "" && dataMIME != declaredMIME {
			return preparedToolImage{}, imageOutputFailure(
				imageOutputMediaTypeMismatch,
				"image output data URL media type does not match its declared media type",
				0,
				0,
			)
		}

		data = encoded
		location = ""
	}

	if data != "" {
		maxBytes := effectiveImageOutputLimit(limits.MaxOutputBytesPerImage)

		decoded, sizeBytes, err := decodePromptImage(data, maxBytes+1)
		if err != nil {
			return preparedToolImage{}, imageOutputFailure(
				imageOutputInvalidBase64,
				"image output contains invalid base64",
				0,
				0,
			)
		}

		sniffedMIME, _, _, ok := inspectOutputRaster(decoded)
		if !ok {
			return preparedToolImage{}, imageOutputFailure(
				imageOutputNotRaster,
				"image output bytes are not a raster",
				0,
				0,
			)
		}

		if declaredMIME != "" && declaredMIME != sniffedMIME {
			return preparedToolImage{}, imageOutputFailure(
				imageOutputMediaTypeMismatch,
				"image output media type does not match its bytes",
				0,
				0,
			)
		}

		if sizeBytes > maxBytes {
			return preparedToolImage{}, imageOutputFailure(
				imageOutputTooLarge,
				"image output exceeds the configured per-image limit",
				sizeBytes,
				maxBytes,
			)
		}

		fingerprint := fingerprintImageOutput(decoded)

		return preparedToolImage{
			identity:    identity,
			mimeType:    sniffedMIME,
			data:        decoded,
			encoded:     base64.StdEncoding.EncodeToString(decoded),
			fingerprint: fingerprint,
		}, nil
	}

	if location == "" {
		return preparedToolImage{}, imageOutputFailure(
			imageOutputMissingFile,
			"image output contains neither bytes nor a location",
			0,
			0,
		)
	}

	if !isRemoteImageURI(location) {
		return preparedToolImage{}, imageOutputFailure(
			imageOutputPathNotAllowed,
			"image output location is not a remote HTTP resource",
			0,
			0,
		)
	}

	return preparedToolImage{
		identity:    identity,
		mimeType:    declaredMIME,
		uri:         location,
		fingerprint: fingerprintImageOutput([]byte(location)),
	}, nil
}

func isRemoteImageURI(value string) bool {
	parsed, err := url.Parse(value)

	return err == nil &&
		(parsed.Scheme == valHTTPS || parsed.Scheme == valHTTP) &&
		parsed.Host != ""
}

func nativeImageDataAndLocation(raw map[string]any) (string, string) {
	if source, ok := raw["source"].(map[string]any); ok {
		if strings.EqualFold(stringField(source, keyType), valBase64) {
			if data := stringField(source, keyData); data != "" {
				return data, ""
			}
		}
	}

	if data := stringField(raw, keyData); data != "" {
		return data, ""
	}

	return "", recursiveStringField(raw, keyURL, "uri")
}

func imageMIMEField(raw map[string]any) string {
	names := []string{keyMIMEType, keyMediaType, "mediaType"}

	// Read the declared media type only where an image block authoritatively
	// carries it: its own top-level fields or its base64 source object. A deep
	// scan could bind an unrelated nested media type and raise a spurious
	// mismatch against the image bytes.
	for _, name := range names {
		if field := stringField(raw, name); field != "" {
			return field
		}
	}

	source, ok := raw["source"].(map[string]any)
	if !ok {
		return ""
	}

	for _, name := range names {
		if field := stringField(source, name); field != "" {
			return field
		}
	}

	return ""
}

func recursiveStringField(value any, names ...string) string {
	switch typed := value.(type) {
	case map[string]any:
		for _, name := range names {
			if field := stringField(typed, name); field != "" {
				return field
			}
		}

		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}

		sort.Strings(keys)

		for _, key := range keys {
			if field := recursiveStringField(typed[key], names...); field != "" {
				return field
			}
		}
	case []any:
		for _, child := range typed {
			if field := recursiveStringField(child, names...); field != "" {
				return field
			}
		}
	}

	return ""
}

func stringField(raw map[string]any, name string) string {
	value, _ := raw[name].(string)

	return value
}

func splitImageDataURL(value string) (string, string, bool) {
	header, encoded, ok := strings.Cut(value, ",")
	if !ok || !strings.HasPrefix(header, "data:") || !strings.HasSuffix(header, ";base64") || encoded == "" {
		return "", "", false
	}

	return strings.TrimSuffix(strings.TrimPrefix(header, "data:"), ";base64"), encoded, true
}

func fingerprintImageOutput(data []byte) string {
	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:])
}

func inspectOutputRaster(data []byte) (string, uint32, uint32, bool) {
	if mimeType := sniffPromptImageMIME(data); mimeType != "" {
		width, height, _, err := inspectPromptImage(mimeType, data)
		if err == nil && width > 0 && height > 0 {
			return mimeType, width, height, true
		}

		return "", 0, 0, false
	}

	if len(data) >= 26 && string(data[:2]) == "BM" {
		width := binary.LittleEndian.Uint32(data[18:22])

		heightRaw := binary.LittleEndian.Uint32(data[22:26])
		if width == 0 || width > ^uint32(0)>>1 || heightRaw == 0 {
			return "", 0, 0, false
		}

		height := heightRaw
		if heightRaw > ^uint32(0)>>1 {
			height = ^heightRaw + 1
		}

		return imageMIMEBMP, width, height, true
	}

	return "", 0, 0, false
}

func (s *agentSession) toolResultSnapshot(
	ctx context.Context,
	result amp.ToolResultBlock,
) ([]acp.ToolCallContent, any, error) {
	items, structured := structuredToolResultItems(result.Content)
	if !structured {
		return nil, sanitizeImageDiagnostic(result.Content), nil
	}

	var (
		content    = make([]acp.ToolCallContent, 0, len(items))
		diagnostic = make([]any, 0, len(items))
		totalBytes int64
	)

	for _, item := range items {
		raw, ok := item.(map[string]any)
		if !ok {
			diagnostic = append(diagnostic, sanitizeImageDiagnostic(item))

			continue
		}

		switch strings.ToLower(stringField(raw, keyType)) {
		case valText:
			text := stringField(raw, valText)
			content = append(content, acp.ToolContent(acp.TextBlock(text)))
			diagnostic = append(diagnostic, map[string]any{keyType: valText, valText: text})
		case canonicalImageArtifactType:
			ref := stringField(raw, "ref")

			artifact, err := s.loadImageArtifact(ctx, ref)
			if err != nil {
				return nil, nil, imageOutputFailure(imageOutputStorageFailed, err.Error(), 0, 0)
			}

			identity := stringField(raw, "identity")
			if artifact.Identity != identity {
				return nil, nil, imageOutputFailure(
					imageOutputStorageFailed,
					"image artifact identity does not match its tool content position",
					0,
					0,
				)
			}

			switch artifact.Kind {
			case imageArtifactKindEmbedded:
				maxBytes := effectiveImageOutputLimit(s.agent.options.ImageLimits.MaxOutputBytesPerImage)

				decoded, sizeBytes, err := decodePromptImage(artifact.Data, maxBytes+1)
				if err != nil {
					return nil, nil, imageOutputFailure(
						imageOutputStorageFailed,
						"stored image output is not valid base64",
						0,
						0,
					)
				}

				if sizeBytes > maxBytes {
					return nil, nil, imageOutputFailure(
						imageOutputTooLarge,
						"stored image output exceeds the configured per-image limit",
						sizeBytes,
						maxBytes,
					)
				}

				mimeType, width, height, ok := inspectOutputRaster(decoded)
				if !ok || mimeType != artifact.MimeType ||
					fingerprintImageOutput(decoded) != artifact.Fingerprint {
					return nil, nil, imageOutputFailure(
						imageOutputStorageFailed,
						"stored image output metadata does not match its bytes",
						0,
						0,
					)
				}

				totalBytes += sizeBytes

				toolMax := effectiveImageOutputLimit(s.agent.options.ImageLimits.MaxOutputBytesPerToolCall)
				if totalBytes > toolMax {
					return nil, nil, imageOutputFailure(
						imageOutputTooLarge,
						"stored image output exceeds the configured per-tool-call limit",
						totalBytes,
						toolMax,
					)
				}

				content = append(content, acp.ToolContent(acp.ImageBlock(artifact.Data, artifact.MimeType)))
				diagnostic = append(diagnostic, map[string]any{
					keyType:     valImage,
					keyMIMEType: artifact.MimeType,
					"width":     width,
					"height":    height,
					"sizeBytes": sizeBytes,
					"sha256":    artifact.Fingerprint,
				})
			case imageArtifactKindLink:
				block := acp.ResourceLinkBlock("Amp image output", artifact.URI)
				if artifact.MimeType != "" {
					block.ResourceLink.MimeType = &artifact.MimeType
				}

				content = append(content, acp.ToolContent(block))
				diagnostic = append(diagnostic, map[string]any{
					keyType:     valImage,
					keyMIMEType: artifact.MimeType,
					"location":  "redacted",
					"sha256":    artifact.Fingerprint,
				})
			}
		default:
			diagnostic = append(diagnostic, sanitizeImageDiagnostic(raw))
		}
	}

	return content, diagnostic, nil
}

func sanitizeImageDiagnostic(value any) any {
	switch typed := value.(type) {
	case []any:
		sanitized := make([]any, 0, len(typed))
		for _, item := range typed {
			sanitized = append(sanitized, sanitizeImageDiagnostic(item))
		}

		return sanitized
	case map[string]any:
		if strings.EqualFold(stringField(typed, keyType), valImage) {
			return map[string]any{
				keyType:     valImage,
				keyMIMEType: imageMIMEField(typed),
				"location":  "redacted",
			}
		}

		sanitized := make(map[string]any, len(typed))
		for key, child := range typed {
			sanitized[key] = sanitizeImageDiagnostic(child)
		}

		return sanitized
	case string:
		var decoded any
		if json.Unmarshal([]byte(typed), &decoded) == nil {
			return sanitizeImageDiagnostic(decoded)
		}

		return typed
	default:
		return value
	}
}

func (s *agentSession) emitImageToolFailure(
	ctx context.Context,
	toolUseID string,
	nativeFailed bool,
	parentToolUseID string,
	failure error,
) error {
	status := acp.ToolCallStatusFailed

	raw := map[string]any{
		"stage":   imageOutputStage,
		"failure": failure.Error(),
	}
	if nativeFailed {
		raw["nativeFailed"] = true
	}

	return s.emitUpdate(ctx, tagParentToolUse(acp.SessionUpdate{ToolCallUpdate: &acp.SessionToolCallUpdate{
		SessionUpdate: "tool_call_update",
		ToolCallId:    acp.ToolCallId(toolUseID),
		Status:        &status,
		RawOutput:     raw,
	}}, parentToolUseID))
}
