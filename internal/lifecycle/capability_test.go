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

	for _, data := range []string{
		`{"version":1,`,
		`{"version":1`,
		`{"version":1} {}`,
	} {
		require.Error(t, decoded.UnmarshalJSON([]byte(data)))
	}
}

func TestLifecycleCapabilityValidatesEveryMember(t *testing.T) {
	t.Parallel()

	var decoded Negotiated
	require.NoError(t, json.Unmarshal([]byte(`{
		"version":1,
		"updatesOutsidePrompt":true,
		"authoritativeQuiescence":true,
		"quiescenceSource":"process-containment",
		"activityKinds":["task","subagent"]
	}`), &decoded))
	require.Equal(t, Negotiated{
		Version:                 Version,
		UpdatesOutsidePrompt:    true,
		AuthoritativeQuiescence: true,
		QuiescenceSource:        ProofClassProcessContainment,
		ActivityKinds:           []ActivityKind{ActivityTask, ActivitySubagent},
	}, decoded)

	for _, test := range []struct {
		name string
		data string
	}{
		{"truncated member", `{"version":1,`},
		{"truncated object", `{"version":1`},
		{"updates type", `{"version":1,"updatesOutsidePrompt":1}`},
		{"authority type", `{"version":1,"authoritativeQuiescence":1}`},
		{"source type", `{"version":1,"quiescenceSource":1}`},
		{"kinds type", `{"version":1,"activityKinds":1}`},
		{"invalid kind", `{"version":1,"activityKinds":["worker"]}`},
		{"duplicate kind", `{"version":1,"activityKinds":["task","task"]}`},
		{"authority missing source", `{"version":1,"authoritativeQuiescence":true}`},
		{"authority wrong source", `{"version":1,"authoritativeQuiescence":true,"quiescenceSource":"other"}`},
		{"source without authority", `{"version":1,"quiescenceSource":"process-containment"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var value Negotiated
			require.Error(t, json.Unmarshal([]byte(test.data), &value))
		})
	}
}
