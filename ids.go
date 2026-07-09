package ampacp

import (
	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

// jsonFieldSessionID is the wire key for the ACP session identifier, which for
// amp is the native Amp thread id verbatim (amp never mints its own ids — the
// thread id returned by `amp threads new` is adopted as the session id).
const jsonFieldSessionID = "sessionId"

// frameSessionID extracts the native session id carried on a stream-json frame,
// returning "" for frame kinds that carry no id. It is the single parse point
// for the identifier the wrapper reconciles every live turn against the session
// it belongs to.
func frameSessionID(msg amp.Message) string {
	switch typed := msg.(type) {
	case *amp.SystemMessage:
		return typed.SessionID
	case *amp.UserMessage:
		return typed.SessionID
	case *amp.AssistantMessage:
		return typed.SessionID
	case *amp.ResultMessage:
		return typed.SessionID
	default:
		return ""
	}
}

// unknownSessionError is the single canonical error for any session id that
// cannot be resolved — unknown, absent from the store, or tombstoned. Keeping
// one constructor guarantees the wire shape cannot drift between call sites.
func unknownSessionError() error {
	return acp.NewInvalidParams(map[string]any{jsonFieldField: jsonFieldSessionID, jsonFieldError: "unknown session"})
}
