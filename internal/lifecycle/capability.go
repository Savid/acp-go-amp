package lifecycle

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

//nolint:gocyclo // The strict decoder keeps all closed-field and lexical checks in one pass.
func (n *Negotiated) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return errors.New("lifecycle capability must be an object")
	}

	decoded := Negotiated{}
	seen := map[string]bool{}

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode lifecycle capability member: %w", err)
		}

		field, ok := token.(string)
		if !ok {
			return errors.New("lifecycle capability member name must be a string")
		}

		switch field {
		case fieldVersion, fieldUpdatesOutsidePrompt, fieldAuthoritativeQuiescence,
			fieldQuiescenceSource, fieldActivityKinds:
		default:
			return fmt.Errorf("unknown lifecycle capability field %q", field)
		}

		if seen[field] {
			return fmt.Errorf("duplicate lifecycle capability field %q", field)
		}

		seen[field] = true

		switch field {
		case fieldVersion:
			var version json.RawMessage
			if err := decoder.Decode(&version); err != nil || !bytes.Equal(bytes.TrimSpace(version), []byte("1")) {
				return errors.New("lifecycle capability version must be exact integer 1")
			}

			decoded.Version = Version
		case fieldUpdatesOutsidePrompt:
			if err := decoder.Decode(&decoded.UpdatesOutsidePrompt); err != nil {
				return errors.New("lifecycle capability updatesOutsidePrompt must be boolean")
			}
		case fieldAuthoritativeQuiescence:
			if err := decoder.Decode(&decoded.AuthoritativeQuiescence); err != nil {
				return errors.New("lifecycle capability authoritativeQuiescence must be boolean")
			}
		case fieldQuiescenceSource:
			if err := decoder.Decode(&decoded.QuiescenceSource); err != nil {
				return errors.New("lifecycle capability quiescenceSource must be a proof class")
			}
		case fieldActivityKinds:
			if err := decoder.Decode(&decoded.ActivityKinds); err != nil {
				return errors.New("lifecycle capability activityKinds must be an array")
			}
		}
	}

	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("close lifecycle capability: %w", err)
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("lifecycle capability carries trailing input")
	}

	if len(seen) == 0 {
		*n = Negotiated{}

		return nil
	}

	for _, required := range []string{
		fieldVersion,
		fieldUpdatesOutsidePrompt,
		fieldAuthoritativeQuiescence,
		fieldActivityKinds,
	} {
		if !seen[required] {
			return fmt.Errorf("lifecycle capability field %q is required", required)
		}
	}

	for index, kind := range decoded.ActivityKinds {
		if !kind.Valid() {
			return fmt.Errorf("lifecycle capability activityKinds[%d] is invalid", index)
		}

		for prior := range index {
			if decoded.ActivityKinds[prior] == kind {
				return fmt.Errorf("lifecycle capability activityKinds[%d] is duplicated", index)
			}
		}
	}

	if decoded.AuthoritativeQuiescence {
		if !seen[fieldQuiescenceSource] || decoded.QuiescenceSource != ProofClassProcessContainment {
			return errors.New("lifecycle capability quiescenceSource must be process-containment")
		}
	} else if seen[fieldQuiescenceSource] {
		return errors.New("lifecycle capability quiescenceSource requires authoritativeQuiescence")
	}

	*n = decoded

	return nil
}
