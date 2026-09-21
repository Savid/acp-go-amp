package amp

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
)

var protocolIDPattern = regexp.MustCompile(`^M-[0-9A-Za-z]{22}$`)

type importMessage struct {
	Role            string           `json:"role"`
	ID              string           `json:"messageId"`
	Content         []map[string]any `json:"content"`
	Meta            json.RawMessage  `json:"meta,omitempty"`
	UserState       json.RawMessage  `json:"userState,omitempty"`
	Usage           json.RawMessage  `json:"usage,omitempty"`
	ReadAt          json.RawMessage  `json:"readAt,omitempty"`
	State           map[string]any   `json:"state,omitempty"`
	ParentToolUseID json.RawMessage  `json:"parentToolUseId,omitempty"`
}

type importThread struct {
	ID        string          `json:"id"`
	Version   int64           `json:"v"`
	Title     string          `json:"title,omitempty"`
	AgentMode string          `json:"agentMode"`
	Messages  []importMessage `json:"messages"`
}

// PrepareImport preserves message identities and content in Amp's actor input shape.
func PrepareImport(data []byte, sourceID, destinationID, mode string) (json.RawMessage, int, error) {
	if err := ValidateThreadID(destinationID); err != nil {
		return nil, 0, err
	}

	var source struct {
		ID       string `json:"id"`
		Version  int64  `json:"v"`
		Title    string `json:"title"`
		Mode     string `json:"agentMode"`
		Messages []struct {
			importMessage
			ProtocolID string          `json:"protocolMessageID"` //nolint:tagliatelle // Native exports use this field spelling.
			NumericID  json.RawMessage `json:"messageId"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(data, &source); err != nil {
		return nil, 0, err
	}

	if source.ID != sourceID || source.Messages == nil {
		return nil, 0, errors.New("invalid import source")
	}

	if source.Mode != "" {
		mode = source.Mode
	}

	if mode == "" {
		return nil, 0, errors.New("native import mode is missing")
	}

	target := importThread{ID: destinationID, Version: source.Version, Title: source.Title, AgentMode: mode, Messages: make([]importMessage, 0, len(source.Messages))}

	seen := make(map[string]bool, len(source.Messages))
	for i := range source.Messages {
		original := &source.Messages[i]
		message := original.importMessage

		message.ID = original.ProtocolID
		if !protocolIDPattern.MatchString(message.ID) || seen[message.ID] || message.Content == nil {
			return nil, 0, errors.New("invalid import message identity or content")
		}

		seen[message.ID] = true
		switch message.Role {
		case roleUser:
			message.State = nil
		case roleAssistant:
			message.Meta = nil

			message.UserState = nil
			if message.State[partType] == StatusCancelled {
				message.State = map[string]any{partType: StatusCancelled}
			} else {
				message.State = nil
			}
		case roleInfo:
			for _, part := range message.Content {
				if part[partType] != "manual_bash_invocation" {
					return nil, 0, errors.New("native import cannot preserve the info message")
				}
			}

			message.Meta = nil
			message.UserState = nil
			message.Usage = nil
			message.State = nil
		default:
			return nil, 0, errors.New("invalid native import role")
		}

		target.Messages = append(target.Messages, message)
	}

	payload, err := json.Marshal(struct {
		Thread importThread `json:"thread"`
	}{Thread: target})

	return payload, len(target.Messages), err
}

// VerifyImported checks the history independently of the destination's thread ID and row numbers.
func VerifyImported(source []byte, sourceID string, restored []byte, destinationID string) error {
	want, err := exportMessages(source, sourceID)
	if err != nil {
		return err
	}

	have, err := exportMessages(restored, destinationID)
	if err != nil {
		return err
	}

	if len(want) != len(have) {
		return errors.New("native import changed the message count")
	}

	for i, original := range want {
		current := have[i]
		if len(original.ProtocolID) == 0 || !bytes.Equal(original.ProtocolID, current.ProtocolID) || original.Role != current.Role || !reflect.DeepEqual(original.Content, current.Content) || !reflect.DeepEqual(original.State, current.State) {
			return errors.New("native import changed message identity, content, or completion state")
		}
	}

	return nil
}
