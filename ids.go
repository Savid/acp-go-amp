package ampacp

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

// jsonFieldSessionID is the wire key for the ACP session identifier. The
// wrapper mints its own UUID session ids at session/new; the native Amp thread
// id is created lazily by the first prompt turn and recorded in the manifest
// as nativeSessionId, so the two identifier spaces never mix.
const jsonFieldSessionID = "sessionId"

// maxSessionIDBytes bounds an externally sourced ACP session id. Wrapper-minted
// ids are 36-byte UUIDs; this defensive ceiling keeps the preserved raw-event
// envelope bounded even when a host supplies corrupt stored state.
const maxSessionIDBytes = 4 * 1024

// randRead is a seam so tests can drive the entropy-failure branch of
// newSessionID; production always reads crypto/rand.
var randRead = rand.Read

// newSessionID mints the wrapper-owned ACP session id: a lowercase UUIDv4.
// It deliberately shares no shape with Amp's T- thread ids so a session id can
// never be mistaken for (or misused as) a native thread id.
func newSessionID() (string, error) {
	var id [16]byte
	if _, err := randRead(id[:]); err != nil {
		return "", fmt.Errorf("mint session id: %w", err)
	}

	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80

	encoded := hex.EncodeToString(id[:])

	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}

// validateSessionID validates an ACP session id before a session object is
// constructed around it.
func validateSessionID(id string) error {
	switch {
	case id == "":
		return errors.New("session id is required")
	case len(id) > maxSessionIDBytes:
		return fmt.Errorf("session id exceeds %d bytes", maxSessionIDBytes)
	default:
		return nil
	}
}

// validNativeSessionID reports whether a manifest nativeSessionId is
// acceptable: empty (no thread created yet) or a well-formed Amp thread id.
func validNativeSessionID(threadID string) bool {
	return threadID == "" || amp.ValidateThreadID(threadID) == nil
}

// frameSessionID extracts the native session id carried on a stream-json frame,
// returning "" for frame kinds that carry no id. It is the single parse point
// for the identifier the wrapper reconciles every live turn against the
// session's adopted native thread id.
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
