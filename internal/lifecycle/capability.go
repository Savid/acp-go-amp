package lifecycle

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

func (n *Negotiated) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return errors.New("lifecycle capability must be an object")
	}

	decoded := Negotiated{}
	seenVersion := false

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode lifecycle capability member: %w", err)
		}

		field, ok := token.(string)
		if !ok {
			return errors.New("lifecycle capability member name must be a string")
		}

		if field != fieldVersion {
			return fmt.Errorf("unknown lifecycle capability field %q", field)
		}

		if seenVersion {
			return errors.New("duplicate lifecycle capability field \"version\"")
		}

		seenVersion = true

		var version json.RawMessage
		if err := decoder.Decode(&version); err != nil || !bytes.Equal(bytes.TrimSpace(version), []byte("1")) {
			return errors.New("lifecycle capability version must be exact integer 1")
		}

		decoded.Version = Version
	}

	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("close lifecycle capability: %w", err)
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("lifecycle capability carries trailing input")
	}

	*n = decoded

	return nil
}
