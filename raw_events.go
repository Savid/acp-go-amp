package ampacp

import (
	"encoding/json"
	"fmt"
)

const (
	rawEventMaxBytes       = 64 * 1024
	rawEventFieldSequence  = "sequence"
	rawEventFieldEvent     = "event"
	rawEventReasonOversize = "oversize"
)

// capRawEventPayload applies the raw-event marker to the complete notification
// payload and then rechecks the fully preserved envelope. Legitimate oversized
// native events therefore emit a bounded marker; an impossible structural
// envelope fails closed before delivery and cannot consume a sequence.
func capRawEventPayload(payload map[string]any) (map[string]any, error) {
	data, err := json.Marshal(payload)
	if err == nil && len(data) <= rawEventMaxBytes {
		return payload, nil
	}

	marker := map[string]any{
		"truncated": true,
		"reason":    reasonUnserializable,
		keyMaxBytes: rawEventMaxBytes,
	}
	if err == nil {
		marker["reason"] = rawEventReasonOversize
		marker["sizeBytes"] = len(data)
	}

	payload[rawEventFieldEvent] = marker

	final, finalErr := json.Marshal(payload)
	if finalErr != nil {
		return nil, fmt.Errorf("marshal capped raw event payload: %w", finalErr)
	}

	if len(final) > rawEventMaxBytes {
		return nil, fmt.Errorf("capped raw event payload is %d bytes, exceeds %d", len(final), rawEventMaxBytes)
	}

	return payload, nil
}
