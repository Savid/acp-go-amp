package ampacp

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var errStrictJSONObject = errors.New("invalid strict JSON object")

// strictJSONObjectFields walks exactly one JSON object and retains every raw
// member value. It rejects malformed or trailing input and duplicate names,
// which encoding/json's struct and map decoders otherwise accept silently.
func strictJSONObjectFields(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))

	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errStrictJSONObject
	}

	fields := map[string]json.RawMessage{}

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, errStrictJSONObject
		}

		key, _ := keyToken.(string)
		if _, duplicate := fields[key]; duplicate {
			return nil, errStrictJSONObject
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errStrictJSONObject
		}

		fields[key] = value
	}

	if _, err := decoder.Token(); err != nil {
		return nil, errStrictJSONObject
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errStrictJSONObject
	}

	return fields, nil
}
