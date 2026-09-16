package amp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateThreadID(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateThreadID("T-0123abcd-4567-89ef-0123-456789abcdef"))
	for _, id := range []string{
		"",
		"0123abcd-4567-89ef-0123-456789abcdef",
		"T-0123abcd-4567-89ef-0123-456789abcde",
		"T-0123abcd-4567-89ef-0123-456789abcdefg",
		"T-0123abcd456789ef0123456789abcdef",
		"t-0123abcd-4567-89ef-0123-456789abcdef",
		"T-0123abcz-4567-89ef-0123-456789abcdef",
	} {
		require.Error(t, ValidateThreadID(id), id)
	}
}

func TestParseThreadURL(t *testing.T) {
	t.Parallel()

	id, err := ParseThreadURL([]byte("  https://ampcode.com/threads/T-0123abcd-4567-89ef-0123-456789abcdef\n"))
	require.NoError(t, err)
	require.Equal(t, "T-0123abcd-4567-89ef-0123-456789abcdef", id)

	for _, output := range []string{
		"",
		"not a url",
		"ftp://ampcode.com/threads/T-0123abcd-4567-89ef-0123-456789abcdef",
		"https:///threads/T-0123abcd-4567-89ef-0123-456789abcdef",
		"https://ampcode.com/threads/nope",
	} {
		_, err := ParseThreadURL([]byte(output))
		require.Error(t, err, output)
	}
}

func TestDecodeFrame(t *testing.T) {
	t.Parallel()

	frame, err := DecodeFrame([]byte(`{"type":"assistant","session_id":"T-1","message":{"stop_reason":"end_turn","usage":{"input_tokens":3}}}`))
	require.NoError(t, err)
	require.Equal(t, "assistant", frame.Type)
	require.Equal(t, "T-1", frame.SessionID)
	require.Equal(t, "end_turn", frame.Message.StopReason)
	require.NotNil(t, frame.Message.Usage)
	require.Equal(t, 3, frame.Message.Usage.InputTokens)

	for _, line := range []string{
		"",
		"not json",
		`["assistant"]`,
		`{"type":""}`,
		`{"session_id":"T-1"}`,
		`{"type":"assistant"`,
	} {
		_, err := DecodeFrame([]byte(line))
		require.Error(t, err, line)
	}
}
