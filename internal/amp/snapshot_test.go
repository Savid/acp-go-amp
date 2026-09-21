package amp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExportRequiresReceiptContentAndTerminalNativeState(t *testing.T) {
	t.Parallel()
	view := View{ThreadID: "thread", State: "idle", Messages: []Message{
		{ID: json.RawMessage(`"M-1"`), Role: "user", Content: []map[string]any{{"type": "text", "text": "question"}}},
		{ID: json.RawMessage(`"M-2"`), Role: "assistant", Content: []map[string]any{{"type": "text", "text": "complete answer"}}},
	}}
	for _, tc := range []struct {
		name, messages string
		complete       bool
	}{
		{"empty", `[]`, false},
		{"missing answer", `[{"messageId":1,"protocolMessageID":"M-1","role":"user","content":[{"type":"text","text":"question"}]}]`, false},
		{"same IDs partial text", `[{"messageId":1,"protocolMessageID":"M-1","role":"user","content":[{"type":"text","text":"question"}]},{"messageId":2,"protocolMessageID":"M-2","role":"assistant","state":{"type":"complete"},"content":[{"type":"text","text":"complete"}]}]`, false},
		{"same content streaming", `[{"messageId":1,"protocolMessageID":"M-1","role":"user","content":[{"type":"text","text":"question"}]},{"messageId":2,"protocolMessageID":"M-2","role":"assistant","state":{"type":"streaming"},"content":[{"type":"text","text":"complete answer"}]}]`, false},
		{"complete", `[{"messageId":1,"protocolMessageID":"M-1","role":"user","content":[{"type":"text","text":"question"}]},{"messageId":2,"protocolMessageID":"M-2","role":"assistant","state":{"type":"complete"},"content":[{"type":"text","text":"complete answer"}]}]`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			complete, err := VerifyExport([]byte(`{"id":"thread","messages":`+tc.messages+`}`), view)
			require.NoError(t, err)
			require.Equal(t, tc.complete, complete)
		})
	}
}

func TestExportComparesNativeToolResultToPluginJSONOutput(t *testing.T) {
	t.Parallel()
	view := View{ThreadID: "thread", State: "idle", Messages: []Message{{ID: json.RawMessage(`"M-1"`), Role: "user", Content: []map[string]any{{"type": "tool_result", "toolUseID": "TU-1", "status": "done", "output": `{"exitCode":0,"stdout":"proof"}`}}}}}
	data := []byte(`{"id":"thread","messages":[{"messageId":1,"protocolMessageID":"M-1","role":"user","content":[{"type":"tool_result","toolUseID":"TU-1","run":{"status":"done","result":{"exitCode":0,"stdout":"proof"}}}]}]}`)
	complete, err := VerifyExport(data, view)
	require.NoError(t, err)
	require.True(t, complete)
	view.Messages[0].Content[0]["output"] = `{"exitCode":0,"stdout":"different"}`
	complete, err = VerifyExport(data, view)
	require.NoError(t, err)
	require.False(t, complete)
}

func TestRepeatedToolViewResultsMustBeIdentical(t *testing.T) {
	t.Parallel()
	message := Message{ID: json.RawMessage(`"M-1"`), Role: "user", Content: []map[string]any{{"type": "tool_result", "toolUseID": "TU-1", "status": "done", "output": `{"output":"proof","exitCode":0}`}}}
	receipt := Receipt{ID: message.ID, Status: StatusDone, Messages: []Message{message}}
	view := View{ThreadID: "thread", State: "idle", Messages: []Message{message}}
	view.Messages[0].Content = append(view.Messages[0].Content, message.Content[0])
	require.NoError(t, containsReceipt(view, receipt))
	data := []byte(`{"id":"thread","messages":[{"messageId":1,"protocolMessageID":"M-1","role":"user","content":[{"type":"tool_result","toolUseID":"TU-1","run":{"status":"done","result":{"output":"proof","exitCode":0}}}]}]}`)
	complete, err := VerifyExport(data, view)
	require.NoError(t, err)
	require.True(t, complete)
	view.Messages[0].Content[1] = map[string]any{"type": "tool_result", "toolUseID": "TU-1", "status": "done", "output": `{"output":"changed","exitCode":0}`}
	require.Error(t, containsReceipt(view, receipt))
	complete, err = VerifyExport(data, view)
	require.NoError(t, err)
	require.False(t, complete)
}

// Native compaction inserts an info message holding only a summary part. The
// export keeps it; the observing process's plugin view omits it and a fresh
// attachment lists it without content. Both verifications compare the
// conversation around it, while an info message with exposed content still has
// to appear on both sides.
func TestVerificationIgnoresCompactionSummaries(t *testing.T) {
	t.Parallel()
	user := Message{ID: json.RawMessage(`"M-1"`), Role: "user", Content: []map[string]any{{"type": "text", "text": "question"}}}
	assistant := Message{ID: json.RawMessage(`"M-2"`), Role: "assistant", Content: []map[string]any{{"type": "text", "text": "answer"}}}
	summary := Message{ID: json.RawMessage(`"M-3"`), Role: "info", Content: []map[string]any{{"type": "summary", "summary": map[string]any{"type": "message", "summary": "earlier turns"}}}}
	listed := Message{ID: json.RawMessage(`"M-3"`), Role: "info", Content: []map[string]any{}}
	later := Message{ID: json.RawMessage(`"M-4"`), Role: "user", Content: []map[string]any{{"type": "text", "text": "again"}}}
	reply := Message{ID: json.RawMessage(`"M-5"`), Role: "assistant", Content: []map[string]any{{"type": "text", "text": "answer again"}}}
	export := []byte(`{"id":"thread","messages":[` +
		`{"messageId":1,"protocolMessageID":"M-1","role":"user","content":[{"type":"text","text":"question"}]},` +
		`{"messageId":2,"protocolMessageID":"M-2","role":"assistant","state":{"type":"complete"},"content":[{"type":"text","text":"answer"}]},` +
		`{"messageId":3,"protocolMessageID":"M-3","role":"info","content":[{"type":"summary","summary":{"type":"message","summary":"earlier turns"}}]},` +
		`{"messageId":4,"protocolMessageID":"M-4","role":"user","content":[{"type":"text","text":"again"}]},` +
		`{"messageId":5,"protocolMessageID":"M-5","role":"assistant","state":{"type":"complete"},"content":[{"type":"text","text":"answer again"}]}]}`)
	for _, tc := range []struct {
		name     string
		messages []Message
		complete bool
	}{
		{"view omits the summary", []Message{user, assistant, later, reply}, true},
		{"view lists the summary without content", []Message{user, assistant, listed, later, reply}, true},
		{"view lists the summary part", []Message{user, assistant, summary, later, reply}, true},
		{"view lacks the last reply", []Message{user, assistant, later}, false},
		{"view carries an info message with exposed text", []Message{user, assistant, {ID: json.RawMessage(`"M-9"`), Role: "info", Content: []map[string]any{{"type": "text", "text": "note"}}}, later, reply}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			view := View{ThreadID: "thread", State: "idle", Messages: tc.messages}
			complete, err := VerifyExport(export, view)
			require.NoError(t, err)
			require.Equal(t, tc.complete, complete)
			mirror := VerifyMirror(export, view)
			if tc.complete {
				require.NoError(t, mirror)
			} else {
				require.Error(t, mirror)
			}
		})
	}
}
