package lifecycle

import (
	"bytes"
	"encoding/json"
	"strings"
)

// PreserveWireMeta restores the owned namespace after typed SDK decoding. The
// input has already passed JSON syntax validation. Refusals are retained as
// values, so construction and method-specific validation keep their precedence.
// Foreign namespaces retain the SDK's decoding behavior.
func PreserveWireMeta(params json.RawMessage, meta map[string]any) map[string]any {
	var value any

	var envelopes, occurrences int

	for name, raw := range wireMembers(params) {
		if !strings.EqualFold(name, "_meta") {
			continue
		}

		envelopes++

		for key, owned := range wireMembers(raw) {
			if key == MetaKey {
				occurrences++
				value = wireValue(owned, MetaPath)
			}
		}
	}

	if occurrences == 0 {
		return meta
	}

	if envelopes > 1 || occurrences > 1 {
		value = paramError()
	}

	if meta == nil {
		meta = make(map[string]any)
	}

	meta[MetaKey] = value

	return meta
}

// wireMembers walks a syntactically valid object without collapsing repeated
// names. Nonobjects have no members; their shape is judged by the semantic reader.
func wireMembers(raw json.RawMessage) func(func(string, json.RawMessage) bool) {
	return func(yield func(string, json.RawMessage) bool) {
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 || raw[0] != '{' {
			return
		}

		decoder := json.NewDecoder(bytes.NewReader(raw))

		_, _ = decoder.Token()
		for decoder.More() {
			name, _ := decoder.Token()

			var value json.RawMessage

			_ = decoder.Decode(&value)

			field, _ := name.(string)
			if !yield(field, value) {
				return
			}
		}
	}
}

// MaskWireMeta keeps owned values out of the SDK's float64 decoder. The caller
// restores their original bytes at the semantic boundary after typed decoding.
func MaskWireMeta(params json.RawMessage, key string) json.RawMessage {
	return RewriteWireObject(params, func(name string, raw json.RawMessage) json.RawMessage {
		if !strings.EqualFold(name, "_meta") {
			return raw
		}

		return RewriteWireObject(raw, func(name string, value json.RawMessage) json.RawMessage {
			if name == key {
				return json.RawMessage("null")
			}

			return value
		})
	})
}

// RewriteWireObject preserves object order and repeated fields when masking
// owned request metadata. Invalid JSON and nonobjects remain the SDK's input.
func RewriteWireObject(raw json.RawMessage, visit func(string, json.RawMessage) json.RawMessage) json.RawMessage {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(raw) {
		return raw
	}

	var output bytes.Buffer
	output.WriteByte('{')

	first := true
	for name, value := range wireMembers(raw) {
		if !first {
			output.WriteByte(',')
		}

		first = false
		key, _ := json.Marshal(name)
		output.Write(key)
		output.WriteByte(':')
		output.Write(visit(name, value))
	}

	output.WriteByte('}')

	return output.Bytes()
}

func wireValue(raw json.RawMessage, path string) any {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any

	_ = decoder.Decode(&value)

	fields, object := value.(map[string]any)
	if !object {
		return value
	}

	seen := make(map[string]bool)

	for name, member := range wireMembers(raw) {
		field := path + "." + name
		if seen[name] {
			return &ParamError{Field: field, Verdict: VerdictUnsupported}
		}

		seen[name] = true

		// Submission is the only nested owned object. Other values are judged
		// by their existing field validator; unknown objects need no recursion.
		if path == MetaPath && name == fieldSubmission {
			decoded := wireValue(member, field)
			if refusal, ok := decoded.(*ParamError); ok {
				return refusal
			}

			fields[name] = decoded
		}
	}

	return fields
}
