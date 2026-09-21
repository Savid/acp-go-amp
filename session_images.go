package ampacp

import (
	"encoding/json"
	"net/url"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/image"
)

func (s *session) imageContent(part map[string]any) (acp.ContentBlock, int64, *image.OutputError) {
	source, _ := part[fieldSource].(map[string]any)
	data := textValue(part[fieldData])
	mime := textValue(part["mimeType"])

	uri := textValue(part[fieldURL])
	if source != nil {
		data = textValue(source[fieldData])

		mime = textValue(source["mediaType"])
		if mime == "" {
			mime = textValue(source["media_type"])
		}

		uri = textValue(source[fieldURL])
	}

	if strings.HasPrefix(uri, "data:") {
		header, payload, ok := strings.Cut(uri, ",")
		if ok && strings.HasSuffix(header, ";base64") {
			data = payload
			mime = strings.TrimSuffix(strings.TrimPrefix(header, "data:"), ";base64")
		}
	}

	// Inline bytes are the renderable form; a remote URI becomes a link only
	// when the artifact carries no payload, which the adapter never fetches.
	if data == "" && remoteURI(uri) {
		return acp.ContentBlock{ResourceLink: &acp.ContentBlockResourceLink{Name: contentImage, Uri: uri, MimeType: &mime}}, 0, nil
	}

	output, failure := image.DecodeOutput(data, mime, s.agent.options.ImageLimits.core().EffectiveOutputPerImage())
	if failure != nil {
		return acp.ContentBlock{}, 0, failure
	}

	return acp.ImageBlock(output.Data, output.MIME), output.SizeBytes, nil
}

// remoteURI reports whether a native image points at an http endpoint.
func remoteURI(uri string) bool {
	parsed, err := url.Parse(uri)

	return err == nil && (parsed.Scheme == "https" || parsed.Scheme == "http") && parsed.Host != ""
}

func (s *session) toolContent(state *cycleState, value any) ([]acp.ToolCallContent, *image.OutputError) {
	var (
		result  []acp.ToolCallContent
		failure *image.OutputError
		total   int64
	)

	limit := s.agent.options.ImageLimits.core().EffectiveOutputPerToolCall()

	var visit func(any)

	visit = func(item any) {
		switch typed := item.(type) {
		case string:
			var decoded any
			if json.Unmarshal([]byte(typed), &decoded) == nil {
				switch decoded.(type) {
				case map[string]any, []any:
					visit(decoded)

					return
				}
			}

			if typed != "" {
				result = append(result, acp.ToolContent(acp.TextBlock(typed)))
			}
		case []any:
			for _, part := range typed {
				visit(part)
			}
		case map[string]any:
			switch textValue(typed[fieldType]) {
			case fieldText:
				visit(textValue(typed[fieldText]))
			case contentImage:
				block, size, refusal := s.imageContent(typed)
				if refusal == nil && total+size > limit {
					refusal = &image.OutputError{Reason: image.ReasonTooLarge, Message: image.GuidanceTooLarge, SizeBytes: total + size, MaxBytes: limit}
				}

				if refusal != nil {
					if failure == nil {
						failure = refusal
					}

					guidance, _ := refusal.Guidance()
					result = append(result, acp.ToolContent(acp.TextBlock(guidance)))
				} else {
					total += size
					state.imagesEmitted = state.imagesEmitted || size > 0

					result = append(result, acp.ToolContent(block))
				}
			default:
				if content, ok := typed[fieldContent]; ok {
					visit(content)
				} else {
					data, err := json.Marshal(redacted(typed))
					if err == nil {
						result = append(result, acp.ToolContent(acp.TextBlock(string(data))))
					}
				}
			}
		case nil:
		default:
			data, err := json.Marshal(typed)
			if err == nil {
				result = append(result, acp.ToolContent(acp.TextBlock(string(data))))
			}
		}
	}
	visit(value)

	return result, failure
}
