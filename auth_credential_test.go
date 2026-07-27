package ampacp

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

func TestProviderCredentialMarshalsEveryVariant(t *testing.T) {
	cases := map[string]struct {
		credential ProviderCredential
		want       string
	}{
		"oauth": {
			credential: ProviderCredential{Type: ProviderCredentialOAuth, OAuth: &ProviderOAuthCredential{
				Refresh: "r", Access: "a", AccessExpiresAt: 1, AccountID: "acct", EnterpriseURL: "https://e",
			}},
			want: `{"type":"oauth","refresh":"r","access":"a","accessExpiresAt":1,"accountId":"acct","enterpriseUrl":"https://e"}`,
		},
		"api": {
			credential: ProviderCredential{Type: ProviderCredentialAPI, API: &ProviderAPICredential{Key: "k"}},
			want:       `{"type":"api","key":"k"}`,
		},
		"hermesOauth": {
			credential: ProviderCredential{Type: ProviderCredentialHermesOAuth, HermesOAuth: &ProviderHermesOAuthCredential{
				AuthType: "oauth", AccessToken: "t",
			}},
			want: `{"type":"hermesOauth","authType":"oauth","accessToken":"t"}`,
		},
	}

	for name, testCase := range cases {
		encoded, err := json.Marshal(testCase.credential)
		if err != nil || string(encoded) != testCase.want {
			t.Fatalf("%s: marshal = %s/%v, want %s", name, encoded, err, testCase.want)
		}
	}

	invalid := []ProviderCredential{
		{Type: ProviderCredentialOAuth},
		{Type: ProviderCredentialAPI},
		{Type: ProviderCredentialHermesOAuth},
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

func TestProviderAuthBindingRoundTrips(t *testing.T) {
	binding := ProviderAuthBinding{
		ConnectionID: "connection-1", Revision: 2, BindingGeneration: 3,
		Credential: ProviderCredential{Type: ProviderCredentialAPI, API: &ProviderAPICredential{Key: "k"}},
	}

	encoded, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded ProviderAuthBinding
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.ConnectionID != binding.ConnectionID || decoded.Credential.API.Key != "k" {
		t.Fatalf("binding = %#v", decoded)
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

	if err := fixture.call(AuthCredentialMethod, map[string]any{authFieldSessionID: "T-unknown"}, nil); err == nil {
		t.Fatal("credential answered for an unknown session")
	}
}

func TestCredentialFailsClosedOnAnUnreadableOrFencedSlot(t *testing.T) {
	// A completed flow whose native store answers nothing fails closed rather
	// than reporting absence: flipping the native-secrets flag deletes the file
	// after migrating it, so nothing found is never proof of nothing stored.
	empty := newAuthFixture(t, "login-no-secret")
	authorized := empty.mustAuthorize("connection-1")

	if err := empty.callback(authorized.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	params := map[string]any{
		authFieldSessionID: string(empty.session.id), authFieldProviderID: authProviderID, authFieldFlowID: authorized.FlowID,
	}

	err := empty.call(AuthCredentialMethod, params, nil)
	if err == nil {
		t.Fatal("an empty store was harvested")
	}

	requireAuthCause(t, err, authCauseHarvestFailed)

	fixture := newAuthFixture(t, "login")
	completed := fixture.mustAuthorize("connection-1")

	if writeErr := fixture.callback(completed.FlowID, "pasted"); writeErr != nil {
		t.Fatalf("callback: %v", writeErr)
	}

	harvest := map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: completed.FlowID,
	}

	// An unasserted native store vetoes the read.
	if writeErr := os.WriteFile(fixture.session.settingsFile,
		[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	err = fixture.call(AuthCredentialMethod, harvest, nil)
	if err == nil {
		t.Fatal("credential read an unasserted native store")
	}

	requireAuthCause(t, err, authCauseNativeVeto)

	if writeErr := os.WriteFile(fixture.session.settingsFile, nativeamp.AuthSettingsDocument(), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	// A ledger entry that does not name this connection generation is not this
	// connection's to hand out.
	record, _, readErr := fixture.broker.ledger.read(authProviderID)
	if readErr != nil {
		t.Fatalf("ledger read: %v", readErr)
	}

	fenced := record
	fenced.ConnectionID = "someone-else"

	if writeErr := fixture.broker.ledger.write(fenced); writeErr != nil {
		t.Fatalf("ledger write: %v", writeErr)
	}

	err = fixture.call(AuthCredentialMethod, harvest, nil)
	if err == nil {
		t.Fatal("a mismatched fence was harvested")
	}

	requireAuthCause(t, err, authCauseHarvestFailed)

	// A ledger that answers nothing at all is the same verdict.
	if removeErr := os.Remove(fixture.broker.ledger.path(authProviderID)); removeErr != nil {
		t.Fatal(removeErr)
	}

	err = fixture.call(AuthCredentialMethod, harvest, nil)
	if err == nil {
		t.Fatal("a missing ledger entry was harvested")
	}

	requireAuthCause(t, err, authCauseHarvestFailed)

	originalRead := ledgerReadFile
	ledgerReadFile = func(string) ([]byte, error) { return nil, errors.New("read denied") }

	err = fixture.call(AuthCredentialMethod, harvest, nil)
	ledgerReadFile = originalRead

	if err == nil {
		t.Fatal("an unreadable ledger was harvested")
	}

	requireAuthCause(t, err, authCauseHarvestFailed)
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

// keystoreFixtureMarker is written by the credential-residence fixture's
// entrypoint. The matrix below needs a live Secret Service, so it runs inside
// that container and nowhere else.
const keystoreFixtureMarker = "/run/acp-go-amp-keystore/marker"

// keystoreCanary values are canary material only; the fixture never plants a
// real credential and never mounts a real home.
const (
	keystoreFileCanary  = "canary-file-store-key"
	keystoreStoreCanary = "canary-keystore-key"
)

// TestKeystoreResidenceMatrix proves the two facts amp's assert-false rests on:
// the file store stays authoritative for the read path, and a keystore item
// present under the unpartitioned name — service amp.cli.apiKey, username
// ampcode.com, keyed by hostname and nothing else — never becomes the harvest
// source.
func TestKeystoreResidenceMatrix(t *testing.T) {
	if _, err := os.Stat(keystoreFixtureMarker); err != nil {
		t.Skip("the credential-residence matrix runs inside the keystore fixture container")
	}

	dataHome := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataHome, "amp"), 0o700); err != nil {
		t.Fatal(err)
	}

	seedKeystoreCanary(t, keystoreStoreCanary)

	body := `{"apiKey@https://ampcode.com/":"` + keystoreFileCanary + `"}`
	if err := os.WriteFile(nativeamp.AuthSecretsPath(dataHome), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	secret, present, err := nativeamp.AuthReadSecret(dataHome)
	if err != nil || !present {
		t.Fatalf("the file store answered nothing: %v/%v", present, err)
	}

	if secret != keystoreFileCanary {
		t.Fatalf("the harvest read %q, want the file store's %q", secret, keystoreFileCanary)
	}

	// The settings document the wrapper writes is what keeps that true.
	settings := filepath.Join(t.TempDir(), "settings.json")
	if writeErr := os.WriteFile(settings, nativeamp.AuthSettingsDocument(), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	asserted, err := nativeamp.AuthFileStoreAsserted(settings)
	if err != nil || !asserted {
		t.Fatalf("the wrapper's settings do not assert the file store: %v/%v", asserted, err)
	}

	// Removing the file leaves the keystore item alone and the harvest empty,
	// which is the fail-closed answer rather than the keystore's value.
	if err := os.Remove(nativeamp.AuthSecretsPath(dataHome)); err != nil {
		t.Fatal(err)
	}

	if _, present, err := nativeamp.AuthReadSecret(dataHome); err != nil || present {
		t.Fatalf("the unpartitioned keystore item became the harvest source: %v/%v", present, err)
	}
}

// seedKeystoreCanary plants canary material through the platform tool rather
// than through the read path, so the assertion is not a round trip of one
// library against itself.
func seedKeystoreCanary(t *testing.T, contents string) {
	t.Helper()

	command := exec.Command("secret-tool", "store", "--label=amp-canary",
		"service", "amp.cli.apiKey", "username", nativeamp.AuthURLHost)
	command.Stdin = strings.NewReader(contents)

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("seed keystore canary: %v: %s", err, output)
	}
}
