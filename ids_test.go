package ampacp

import (
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
	"github.com/stretchr/testify/require"
)

func TestFrameSessionID(t *testing.T) {
	t.Parallel()

	require.Equal(t, "S1", frameSessionID(&amp.SystemMessage{SessionID: "S1"}))
	require.Equal(t, "U1", frameSessionID(&amp.UserMessage{SessionID: "U1"}))
	require.Equal(t, "A1", frameSessionID(&amp.AssistantMessage{SessionID: "A1"}))
	require.Equal(t, "R1", frameSessionID(&amp.ResultMessage{SessionID: "R1"}))
	require.Equal(t, "", frameSessionID(&amp.UnknownMessage{Type: "other"}))
}

func TestUnknownSessionErrorShapeCanonical(t *testing.T) {
	t.Parallel()

	var reqErr *acp.RequestError
	require.ErrorAs(t, unknownSessionError(), &reqErr)
	require.Equal(t, -32602, reqErr.Code)

	data, ok := reqErr.Data.(map[string]any)
	require.True(t, ok)
	require.Equal(t, jsonFieldSessionID, data[jsonFieldField])
	require.Equal(t, "unknown session", data[jsonFieldError])
}
