package ampacp

import (
	"github.com/coder/acp-go-sdk"
)

type parsedSessionMeta struct {
	options       AmpOptions
	optionFields  ampOptionFields
	rawEvent      bool
	rawEventField bool
}

type ampOptionFields struct {
	env  bool
	mode bool
}

func parseSessionMeta(meta map[string]any) (parsedSessionMeta, error) {
	result := parsedSessionMeta{}

	for key, value := range meta {
		switch key {
		case ampMetaKey:
			ampMeta, ok := value.(map[string]any)
			if !ok {
				return result, unsupportedField("_meta.amp")
			}

			for ampKey, ampValue := range ampMeta {
				switch ampKey {
				case ampOptionsKey:
					options, fields, err := parseAmpOptionsWithPresence(ampValue)
					if err != nil {
						return result, err
					}

					result.options = options
					result.optionFields = fields
				case metaRawEventKey:
					enabled, err := parseRawEventMeta(ampValue)
					if err != nil {
						return result, err
					}

					result.rawEvent = enabled
					result.rawEventField = true
				default:
					return result, unsupportedField("_meta.amp." + ampKey)
				}
			}
		case "traceparent", "tracestate", "baggage":
		default:
			// A session lifecycle request never carries the lifecycle extension:
			// the extension is a different thing and rides none of them, so the
			// family literal is rejected here rather than ignored as another
			// namespace's business.
			if err := rejectLifecycleMeta(map[string]any{key: value}); err != nil {
				return result, err
			}
		}
	}

	return result, nil
}

func parseAmpOptionsWithPresence(value any) (AmpOptions, ampOptionFields, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return AmpOptions{}, ampOptionFields{}, unsupportedField("_meta.amp.options")
	}

	options := AmpOptions{}
	fields := ampOptionFields{}

	for key, value := range raw {
		switch key {
		case optionModelKey:
			model, ok := value.(string)
			if !ok {
				return options, fields, unsupportedField("_meta.amp.options.model")
			}

			options.Model = model
		case optionEnvKey:
			fields.env = true
			switch env := value.(type) {
			case map[string]any:
				options.Env = map[string]string{}

				for k, v := range env {
					str, ok := v.(string)
					if !ok {
						return options, fields, unsupportedField("_meta.amp.options.env." + k)
					}

					options.Env[k] = str
				}
			case map[string]string:
				options.Env = cloneStringMap(env)
			default:
				return options, fields, unsupportedField("_meta.amp.options.env")
			}
		case metaOutputSchemaKey:
			return options, fields, unsupportedField("_meta.amp.options.outputSchema")
		case optionModeKey:
			fields.mode = true

			mode, ok := value.(string)
			if !ok {
				return options, fields, unsupportedField("_meta.amp.options.mode")
			}

			options.Mode = mode
		default:
			return options, fields, unsupportedField("_meta.amp.options." + key)
		}
	}

	return options, fields, nil
}

func parseRawEventMeta(value any) (bool, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return false, unsupportedField("_meta.amp.rawEvent")
	}

	enabled := false

	for key, value := range raw {
		switch key {
		case metaEnabledKey:
			parsed, ok := value.(bool)
			if !ok {
				return false, unsupportedField("_meta.amp.rawEvent.enabled")
			}

			enabled = parsed
		default:
			return false, unsupportedField("_meta.amp.rawEvent." + key)
		}
	}

	return enabled, nil
}

func unsupportedField(path string) error {
	return acp.NewInvalidParams(map[string]any{jsonFieldError: valUnsupported, jsonFieldField: path})
}

// activeRequestEnv drops the adapter-managed residence phase from a composed
// environment so an active request is compared on the caller-supplied values
// alone. The managed keys are already canonical, so the deletion covers every
// spelling the caller could have used.
func activeRequestEnv(env map[string]string) map[string]string {
	out := cloneStringMap(env)
	for key := range managedSessionEnv("", "", "", "", "") {
		delete(out, key)
	}

	return out
}
