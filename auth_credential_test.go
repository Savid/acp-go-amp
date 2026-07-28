package ampacp

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func TestProviderCredentialMarshalsTheApiVariantOnly(t *testing.T) {
	encoded, err := json.Marshal(ProviderCredential{Type: ProviderCredentialAPI, API: &ProviderAPICredential{Key: "k"}})
	if err != nil || string(encoded) != `{"type":"api","key":"k"}` {
		t.Fatalf("marshal = %s/%v", encoded, err)
	}

	invalid := []ProviderCredential{
		{Type: ProviderCredentialAPI},
		{Type: "oauth", API: &ProviderAPICredential{Key: "k"}},
		{Type: "wellknown"},
		{},
	}
	for _, credential := range invalid {
		if _, err := json.Marshal(credential); !errors.Is(err, errProviderCredentialInvalid) {
			t.Fatalf("%#v marshalled: %v", credential, err)
		}
	}
}

func TestProviderCredentialDecodesTheApiVariantOnly(t *testing.T) {
	var credential ProviderCredential
	if err := json.Unmarshal([]byte(`{"type":"api","key":"k","metadata":{"a":"b"}}`), &credential); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if credential.Type != ProviderCredentialAPI || credential.API.Key != "k" || credential.API.Metadata["a"] != "b" {
		t.Fatalf("credential = %#v", credential)
	}

	rejected := []string{
		`[]`,
		`{`,
		`{"key":"k"}`,
		`{"type":7}`,
		`{"type":"api","key":"k","key":"k"}`,
		`{"type":"api"}`,
		`{"type":"api","key":""}`,
		`{"type":"api","key":"k","unknown":1}`,
		`{"type":"api","key":1}`,
		`{"type":"oauth","refresh":"r","access":"a","accessExpiresAt":1}`,
		`{"type":"hermesOauth","authType":"oauth","accessToken":"t"}`,
		`{"type":"wellknown","key":"k","token":"t"}`,
		`{"type":"api","key":"k"} trailing`,
		`{"type":"api","key":"k",}`,
	}
	for _, body := range rejected {
		var decoded ProviderCredential
		if err := json.Unmarshal([]byte(body), &decoded); err == nil {
			t.Fatalf("%s decoded", body)
		}
	}

	// A caller that hands the decoder bytes directly gets the same verdict: the
	// object is walked once itself rather than trusted to an outer decoder.
	malformed := []string{`{1:2}`, `{"a":}`, `{"a":1`, `{"a":1} trailing`}
	for _, body := range malformed {
		var decoded ProviderCredential
		if err := decoded.UnmarshalJSON([]byte(body)); !errors.Is(err, errProviderCredentialInvalid) {
			t.Fatalf("%s decoded directly: %v", body, err)
		}
	}
}

func TestProviderMetadataBounds(t *testing.T) {
	if !validProviderMetadata(nil) {
		t.Fatal("empty metadata rejected")
	}

	tooManyKeys := map[string]string{}
	for i := range providerMetadataMaxKeys + 1 {
		tooManyKeys[string(rune('a'+i))] = "v"
	}

	if validProviderMetadata(tooManyKeys) {
		t.Fatal("too many metadata keys accepted")
	}

	if validProviderMetadata(map[string]string{"a": strings.Repeat("x", providerMetadataMaxValueBytes+1)}) {
		t.Fatal("an oversize metadata value accepted")
	}

	oversizeTotal := map[string]string{}
	for i := range providerMetadataMaxKeys {
		oversizeTotal[string(rune('a'+i))] = strings.Repeat("x", providerMetadataMaxValueBytes)
	}

	if validProviderMetadata(oversizeTotal) {
		t.Fatal("an oversize metadata total accepted")
	}
}

func TestCredentialRejectsAnIncompleteFlow(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	authorized := fixture.mustAuthorize("connection-1")

	params := map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: authorized.FlowID,
	}

	err := fixture.call(AuthCredentialMethod, params, nil)
	if err == nil {
		t.Fatal("a pending flow was harvested")
	}

	requireAuthCause(t, err, authCauseFlowState)

	// A flow-state refusal is one the adapter made itself, so it consumes
	// nothing: the flow is exactly as pending as it was.
	if status := fixture.status(authorized.FlowID); status.State != authStatePending {
		t.Fatalf("a flow-state refusal transitioned the flow: %#v", status)
	}

	if err := fixture.call(AuthCredentialMethod, map[string]any{authFieldSessionID: "T-unknown"}, nil); err == nil {
		t.Fatal("credential answered for an unknown session")
	}
}

func TestCredentialFailsClosedAndTerminalizesTheFlow(t *testing.T) {
	// A completed flow whose slot cannot answer fails closed rather than
	// reporting absence — flipping the native-secrets flag deletes the file after
	// migrating it, so nothing found is never proof of nothing stored — and the
	// leg failed after the flow existed, so the cause it returns and the reason
	// it terminalizes on are one verdict.
	cases := map[string]struct {
		mode    string
		arrange func(*testing.T, *authFixture) func()
		cause   string
		reason  string
	}{
		"emptySlot": {
			mode:   "login-no-secret",
			cause:  authCauseHarvestFailed,
			reason: authReasonHarvestFailed,
		},
		"unassertedStore": {
			mode: "login",
			arrange: func(t *testing.T, fixture *authFixture) func() {
				t.Helper()

				if err := os.WriteFile(fixture.session.settingsFile,
					[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); err != nil {
					t.Fatal(err)
				}

				return func() {}
			},
			cause:  authCauseNativeVeto,
			reason: authReasonNativeVeto,
		},
		"fencedLedgerEntry": {
			mode: "login",
			arrange: func(t *testing.T, fixture *authFixture) func() {
				t.Helper()

				record, _, err := fixture.broker.ledger.read(authProviderID)
				if err != nil {
					t.Fatalf("ledger read: %v", err)
				}

				record.ConnectionID = "someone-else"

				if writeErr := fixture.broker.ledger.write(record); writeErr != nil {
					t.Fatalf("ledger write: %v", writeErr)
				}

				return func() {}
			},
			cause:  authCauseHarvestFailed,
			reason: authReasonHarvestFailed,
		},
		"missingLedgerEntry": {
			mode: "login",
			arrange: func(t *testing.T, fixture *authFixture) func() {
				t.Helper()

				if err := os.Remove(fixture.broker.ledger.path(authProviderID)); err != nil {
					t.Fatal(err)
				}

				return func() {}
			},
			cause:  authCauseHarvestFailed,
			reason: authReasonHarvestFailed,
		},
		"unreadableLedger": {
			mode: "login",
			arrange: func(*testing.T, *authFixture) func() {
				original := ledgerReadFile
				ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read denied") }

				return func() { ledgerReadFile = original }
			},
			cause:  authCauseHarvestFailed,
			reason: authReasonHarvestFailed,
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			fixture := newAuthFixture(t, testCase.mode)
			authorized := fixture.mustAuthorize("connection-1")

			if err := fixture.callback(authorized.FlowID, "pasted"); err != nil {
				t.Fatalf("callback: %v", err)
			}

			restore := func() {}
			if testCase.arrange != nil {
				restore = testCase.arrange(t, fixture)
			}

			err := fixture.call(AuthCredentialMethod, map[string]any{
				authFieldSessionID:  string(fixture.session.id),
				authFieldProviderID: authProviderID,
				authFieldFlowID:     authorized.FlowID,
			}, nil)

			restore()

			if err == nil {
				t.Fatal("a slot that answers nothing was harvested")
			}

			requireAuthCause(t, err, testCase.cause)

			if status := fixture.status(authorized.FlowID); status.State != authStateFailed || status.Reason != testCase.reason {
				t.Fatalf("status after a failed harvest = %#v, want failed/%s", status, testCase.reason)
			}
		})
	}
}

func TestDisconnectReleasesTheSlotWithoutClaimingRevocation(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	authorized := fixture.mustAuthorize("connection-1")

	if err := fixture.callback(authorized.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	fixture.broker.mu.Lock()
	residence := flow.residence
	fixture.broker.mu.Unlock()

	params := map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID,
		authFieldConnectionID: "connection-1", authFieldBindingGeneration: 1,
	}
	if writeErr := fixture.call(AuthDisconnectMethod, params, nil); writeErr != nil {
		t.Fatalf("disconnect: %v", writeErr)
	}

	record, ok, err := fixture.broker.ledger.read(authProviderID)
	if err != nil || !ok || record.State != authLedgerRemoved || record.BindingGeneration != 2 {
		t.Fatalf("ledger after disconnect = %#v/%v/%v", record, ok, err)
	}

	// Amp performs no native removal and promises no provider-side revocation,
	// so the resident credential is deliberately still there.
	if _, present, readErr := nativeamp.AuthReadSecret(residence); readErr != nil || !present {
		t.Fatalf("disconnect removed native state: %v/%v", present, readErr)
	}

	// The fence has moved, so the same request no longer applies.
	err = fixture.call(AuthDisconnectMethod, params, nil)
	if err == nil {
		t.Fatal("a spent binding generation disconnected again")
	}

	requireAuthCause(t, err, authCausePolicy)
}

func TestDisconnectRejectsAddressingAndFenceFailures(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	base := func() map[string]any {
		return map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID,
			authFieldConnectionID: "connection-1", authFieldBindingGeneration: 1,
		}
	}

	for name, field := range map[string]string{
		authFieldSessionID:         authFieldSessionID,
		authFieldProviderID:        authFieldProviderID,
		authFieldConnectionID:      authFieldConnectionID,
		authFieldBindingGeneration: authFieldBindingGeneration,
	} {
		params := base()
		delete(params, name)

		err := fixture.call(AuthDisconnectMethod, params, nil)
		if err == nil {
			t.Fatalf("%s: disconnect accepted the request", name)
		}

		requireInvalidAuthField(t, err, field)
	}

	if err := fixture.call(AuthDisconnectMethod, map[string]any{"unexpected": 1}, nil); err == nil {
		t.Fatal("disconnect accepted an unknown field")
	}

	unknown := base()
	unknown[authFieldSessionID] = "T-unknown"

	if err := fixture.call(AuthDisconnectMethod, unknown, nil); err == nil {
		t.Fatal("disconnect answered for an unknown session")
	}

	// No ledger entry at all is a policy refusal, not a removal.
	err := fixture.call(AuthDisconnectMethod, base(), nil)
	if err == nil {
		t.Fatal("disconnect released a slot it never recorded")
	}

	requireAuthCause(t, err, authCausePolicy)

	fixture.mustAuthorize("connection-1")

	originalRead, originalMarshal := ledgerReadFile, ledgerMarshal

	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read denied") }
	err = fixture.call(AuthDisconnectMethod, base(), nil)
	ledgerReadFile = originalRead

	if err == nil {
		t.Fatal("disconnect answered with an unreadable ledger")
	}

	requireAuthCause(t, err, authCauseHarvestFailed)

	ledgerMarshal = func(any) ([]byte, error) { return nil, errors.New("no encoder") }
	err = fixture.call(AuthDisconnectMethod, base(), nil)
	ledgerMarshal = originalMarshal

	if err == nil {
		t.Fatal("disconnect answered without recording the new generation")
	}

	requireAuthCause(t, err, authCauseProcess)
}
