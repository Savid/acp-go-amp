package amp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImportedHistoryPreservesMessageIdentityAndContent(t *testing.T) {
	t.Parallel()
	const original = "T-00000000-0000-0000-0000-000000000001"
	const destination = "T-00000000-0000-0000-0000-000000000002"
	source := []byte(`{"id":"T-00000000-0000-0000-0000-000000000001","v":3,"agentMode":"low","messages":[{"messageId":1,"protocolMessageID":"M-0000000000000000000001","role":"user","content":[{"type":"text","text":"read it"}]},{"messageId":2,"protocolMessageID":"M-0000000000000000000002","role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{"path":"proof.txt"}}],"state":{"type":"cancelled"},"usage":{"inputTokens":10}}]}`)
	payload, count, err := PrepareImport(source, original, destination, "medium")
	require.NoError(t, err)
	require.Equal(t, 2, count)
	var imported struct {
		Thread importThread `json:"thread"`
	}
	require.NoError(t, json.Unmarshal(payload, &imported))
	require.Equal(t, destination, imported.Thread.ID)
	require.Equal(t, "low", imported.Thread.AgentMode)
	require.Equal(t, "M-0000000000000000000002", imported.Thread.Messages[1].ID)
	require.Equal(t, map[string]any{"type": "cancelled"}, imported.Thread.Messages[1].State)
	input, ok := imported.Thread.Messages[1].Content[0]["input"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "proof.txt", input["path"])
	var restored map[string]any
	require.NoError(t, json.Unmarshal(source, &restored))
	restored["id"] = destination
	messages, ok := restored["messages"].([]any)
	require.True(t, ok)
	for i, raw := range messages {
		message, valid := raw.(map[string]any)
		require.True(t, valid)
		message["messageId"] = i + 100
		delete(message, "usage")
	}
	encoded, err := json.Marshal(restored)
	require.NoError(t, err)
	require.NoError(t, VerifyImported(source, original, encoded, destination))
	message, ok := messages[1].(map[string]any)
	require.True(t, ok)
	message["state"] = map[string]any{partType: StatusDone}
	encoded, err = json.Marshal(restored)
	require.NoError(t, err)
	require.Error(t, VerifyImported(source, original, encoded, destination))
}

func TestImportRefusesIncompleteAndChangedHistory(t *testing.T) {
	t.Parallel()
	const original = "T-00000000-0000-0000-0000-000000000001"
	for _, messages := range []string{
		`null`,
		`[{"messageId":1,"role":"user","content":[]}]`,
		`[{"messageId":1,"protocolMessageID":"M-0000000000000000000001","role":"info","content":[{"type":"opaque"}]}]`,
	} {
		source := []byte(`{"id":"` + original + `","messages":` + messages + `}`)
		_, _, err := PrepareImport(source, original, original, "low")
		require.Error(t, err)
	}
}
