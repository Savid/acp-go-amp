package lifecycle

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLifecycleCapabilityStrictScalar(t *testing.T) {
	t.Parallel()

	var scalar map[string]any
	require.NoError(t, json.Unmarshal([]byte(`{"version":1}`), &scalar))
	offer, present, refusal := DecodeOffer(map[string]any{MetaKey: scalar})
	require.Nil(t, refusal)
	require.True(t, present)
	require.Equal(t, Version, offer.Version)

	var decoded Negotiated
	require.NoError(t, json.Unmarshal([]byte(`{"version":1}`), &decoded))
	require.Equal(t, Negotiated{Version: Version}, decoded)

	require.NoError(t, json.Unmarshal([]byte(`{
		"version":1,
		"updatesOutsidePrompt":false,
		"authoritativeQuiescence":false,
		"activityKinds":[]
	}`), &decoded))
	require.Equal(t, Version, decoded.Version)

	for _, test := range []struct {
		name string
		data string
	}{
		{"empty", `{}`},
		{"missing", `{"updatesOutsidePrompt":true}`},
		{"other integer", `{"version":2}`},
		{"fractional", `{"version":1.0}`},
		{"noninteger fractional", `{"version":1.5}`},
		{"string", `{"version":"1"}`},
		{"boolean", `{"version":true}`},
		{"null", `null`},
		{"array", `[]`},
		{"duplicate", `{"version":1,"version":1}`},
		{"unknown", `{"version":1,"unknown":true}`},
		{"trailing", `{"version":1} {}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var value Negotiated
			require.Error(t, json.Unmarshal([]byte(test.data), &value))
		})
	}
}
