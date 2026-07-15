package ampacp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

const (
	metaMessageIDKey          = "messageId"
	messageIdentityDomain     = "acp-go-amp/message/v1"
	messageIdentityVersionV8  = byte(0x80)
	messageIdentityVariantRFC = byte(0x80)
)

// ampMessageIdentity derives the wrapper-owned ACP message identity for one
// native assistant frame. Amp stream-json exposes no message or turn id, so the
// identity is anchored to the frame's absolute position and exact native bytes
// in the durable transcript mirror. Replaying the same mirror therefore emits
// the same UUID while even byte-identical assistant frames in later turns get
// different identities. The custom payload uses UUIDv8 because the derivation
// is wrapper-defined rather than a native UUID or an RFC namespace hash.
func ampMessageIdentity(sessionID acp.SessionId, transcriptFrame int, raw string) string {
	if transcriptFrame <= 0 {
		return ""
	}

	hash := sha256.New()
	_, _ = hash.Write([]byte(messageIdentityDomain))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(sessionID))
	_, _ = hash.Write([]byte{0})

	_, _ = hash.Write([]byte(strconv.Itoa(transcriptFrame)))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(raw))

	id := hash.Sum(nil)[:16]
	id[6] = (id[6] & 0x0f) | messageIdentityVersionV8
	id[8] = (id[8] & 0x3f) | messageIdentityVariantRFC

	encoded := hex.EncodeToString(id)

	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

// assistantMessageIdentity returns an identity only for a main-agent assistant
// frame. Delegated assistant activity is correlated through parentToolUseId and
// must never become the terminal main-agent checkpoint identity.
func assistantMessageIdentity(sessionID acp.SessionId, transcriptFrame int, msg amp.Message) string {
	assistant, ok := msg.(*amp.AssistantMessage)
	if !ok || assistant.ParentToolUseID != "" || assistant.RawJSON() == "" {
		return ""
	}

	return ampMessageIdentity(sessionID, transcriptFrame, assistant.RawJSON())
}

// terminalAssistantMessageIdentity returns the final main-agent assistant
// identity from the durable transcript. Resume intentionally emits no history,
// but a persistent host still needs this identity-only checkpoint to prove
// that the native transcript matches its committed turn.
func terminalAssistantMessageIdentity(sessionID acp.SessionId, entries []SessionStoreEntry) (string, error) {
	var terminal string

	for index, entry := range entries {
		msg, err := amp.ParseJSONLine(entry)
		if err != nil {
			return "", err
		}

		if messageID := assistantMessageIdentity(sessionID, index+1, msg); messageID != "" {
			terminal = messageID
		}
	}

	return terminal, nil
}

func (s *agentSession) emitNativeMessageIdentity(ctx context.Context, messageID string) error {
	if messageID == "" {
		return nil
	}

	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	return conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: s.id,
		Meta:      ampMessageMeta(nil, messageID),
		Update: acp.SessionUpdate{
			SessionInfoUpdate: &acp.SessionSessionInfoUpdate{},
		},
	})
}

func withAmpMessageIdentity(update acp.SessionUpdate, messageID string) acp.SessionUpdate {
	if messageID == "" || update.AgentMessageChunk == nil {
		return update
	}

	update.AgentMessageChunk.MessageId = acp.Ptr(messageID)
	update.AgentMessageChunk.Meta = ampMessageMeta(update.AgentMessageChunk.Meta, messageID)

	return update
}

func ampMessageMeta(meta map[string]any, messageID string) map[string]any {
	if messageID == "" {
		return meta
	}

	if meta == nil {
		meta = make(map[string]any, 1)
	}

	ampMeta, _ := meta[ampMetaKey].(map[string]any)
	if ampMeta == nil {
		ampMeta = make(map[string]any, 1)
	}

	ampMeta[metaMessageIDKey] = messageID
	meta[ampMetaKey] = ampMeta

	return meta
}
