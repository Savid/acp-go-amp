package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

				record, _, err := fixture.broker.ledger.read(authProviderID, "connection-1")
				if err != nil {
					t.Fatalf("ledger read: %v", err)
				}

				record.Revision++

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

				if err := os.Remove(fixture.broker.ledger.path(authProviderID, "connection-1")); err != nil {
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

	record, ok, err := fixture.broker.ledger.read(authProviderID, "connection-1")
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

	requireAuthCause(t, err, authCauseBindingConflict)
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

	// No ledger entry at all is a binding conflict, not a removal.
	err := fixture.call(AuthDisconnectMethod, base(), nil)
	if err == nil {
		t.Fatal("disconnect released a slot it never recorded")
	}

	requireAuthCause(t, err, authCauseBindingConflict)

	fixture.mustAuthorize("connection-1")

	// Amp reads its ledger by (providerId, connectionId), so a wrong connection
	// and an absent entry are the same read; both still refuse, and a stale
	// generation against a live entry refuses on the generation alone.
	wrongConnection := base()
	wrongConnection[authFieldConnectionID] = "connection-other"

	err = fixture.call(AuthDisconnectMethod, wrongConnection, nil)
	if err == nil {
		t.Fatal("disconnect answered for a connection it never recorded")
	}

	requireAuthCause(t, err, authCauseBindingConflict)

	staleGeneration := base()
	staleGeneration[authFieldBindingGeneration] = 99

	err = fixture.call(AuthDisconnectMethod, staleGeneration, nil)
	if err == nil {
		t.Fatal("disconnect answered against a stale binding generation")
	}

	requireAuthCause(t, err, authCauseBindingConflict)

	// Neither refusal touched the entry the live binding still names.
	live, ok, readErr := fixture.broker.ledger.read(authProviderID, "connection-1")
	if readErr != nil || !ok || live.BindingGeneration != 1 || live.State == authLedgerRemoved {
		t.Fatalf("a refused fence mutated the ledger: %#v/%v/%v", live, ok, readErr)
	}

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

// TestConcurrentCredentialLegsHarvestOnce drives two credential legs at one
// completed flow at the same time. The SDK dispatches every inbound request on
// its own goroutine, so a host retrying after a client-side timeout — or any
// second caller — puts two legs on one flowId, and the claim is what decides
// which of them reads the slot. Two answers are two live copies of one key,
// and because every field access is individually locked there is no data race
// for the detector to report.
func TestConcurrentCredentialLegsHarvestOnce(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	authorized := fixture.mustAuthorize("connection-1")

	if err := fixture.callback(authorized.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	original := authReadSecret
	t.Cleanup(func() { authReadSecret = original })

	release := make(chan struct{})

	var reads atomic.Int64

	authReadSecret = func(dataHome string) (string, bool, error) {
		reads.Add(1)
		<-release

		return original(dataHome)
	}

	params, err := json.Marshal(map[string]any{
		authFieldSessionID:  string(fixture.session.id),
		authFieldProviderID: authProviderID,
		authFieldFlowID:     authorized.FlowID,
	})
	if err != nil {
		t.Fatalf("marshal credential params: %v", err)
	}

	answered := make(chan error, 2)

	for range 2 {
		go func() {
			_, callErr := fixture.agent.HandleExtensionMethod(context.Background(), AuthCredentialMethod, params)
			answered <- callErr
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(release)

	harvested := 0

	for range 2 {
		if legErr := <-answered; legErr == nil {
			harvested++
		} else {
			requireAuthCause(t, legErr, authCauseFlowState)
		}
	}

	if harvested != 1 {
		t.Fatalf("legs that harvested = %d, want 1", harvested)
	}

	if got := reads.Load(); got != 1 {
		t.Fatalf("slot reads = %d, want 1", got)
	}
}

// TestFailHarvestKeepsARecordACauseCannotTransition pins the guard on the
// harvest's demotion. Four of the leg's causes pair with no transition at all,
// and writing the state one of those yields would put the empty string —
// outside the closed wire enum, and terminal to every reader of it — into the
// flow record and every later status answer.
func TestFailHarvestKeepsARecordACauseCannotTransition(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	authorized := fixture.mustAuthorize("connection-1")

	if err := fixture.callback(authorized.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	for _, cause := range []string{authCausePolicy, authCauseBindingConflict, authCauseFlowState, authCauseFlowCancelled} {
		requireAuthCause(t, fixture.broker.failHarvest(flow, cause), cause)

		status := fixture.status(authorized.FlowID)
		if status.State != authStateAuthenticated || status.Reason != "" {
			t.Fatalf("failHarvest(%q) moved the record to %#v", cause, status)
		}
	}
}

func TestManualAPIKeyMaterialProducesTheSameOpaqueCredentialShape(t *testing.T) {
	var logs bytes.Buffer
	fixture := newAuthFixture(t, "login", WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))))

	manual := fixture.mustAuthorizeMethod("connection-manual", authMethodAPIKey)
	if err := fixture.callbackMethod(manual.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}

	if status := fixture.status(manual.FlowID); status.State != authStateSaved || status.Reason != "" || status.ExpiresAt != 0 {
		t.Fatalf("manual status = %#v", status)
	}

	manualCredential := harvestAuthCredential(t, fixture, manual.FlowID)
	if manualCredential.Credential.Type != ProviderCredentialAPI || manualCredential.Credential.API == nil ||
		manualCredential.Credential.API.Key != manualAmpKeyCanary || manualCredential.Credential.API.Metadata != nil {
		t.Fatalf("manual credential = %#v", manualCredential)
	}

	// The flow handed out one copy and retained none.
	if err := fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: manual.FlowID,
	}, nil); err == nil {
		t.Fatal("manual credential was harvested twice")
	} else {
		requireAuthCause(t, err, authCauseFlowState)
	}

	hosted := fixture.mustAuthorize("connection-hosted")
	if err := fixture.callback(hosted.FlowID, "hosted-paste"); err != nil {
		t.Fatalf("hosted callback: %v", err)
	}

	hostedCredential := harvestAuthCredential(t, fixture, hosted.FlowID)
	if hostedCredential.Credential.Type != manualCredential.Credential.Type ||
		hostedCredential.Credential.API == nil || hostedCredential.Credential.API.Metadata != nil {
		t.Fatalf("hosted/manual credential shapes diverged: %#v/%#v", hostedCredential, manualCredential)
	}

	ledgerBytes, err := os.ReadFile(fixture.broker.ledger.path(authProviderID, "connection-manual"))
	if err != nil {
		t.Fatal(err)
	}

	settingsBytes, err := os.ReadFile(fixture.session.settingsFile)
	if err != nil {
		t.Fatal(err)
	}

	for name, contents := range map[string][]byte{
		"ledger":   ledgerBytes,
		"settings": settingsBytes,
		"logs":     logs.Bytes(),
	} {
		if bytes.Contains(contents, []byte(manualAmpKeyCanary)) {
			t.Fatalf("manual key leaked into %s", name)
		}
	}
}

func TestManualAPIKeyHarvestFailurePathsRemainFenced(t *testing.T) {
	t.Run("missing retained material", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flowResult := fixture.mustAuthorizeMethod("connection-empty", authMethodAPIKey)
		if err := fixture.callbackMethod(flowResult.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
			t.Fatal(err)
		}

		flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, flowResult.FlowID)
		if err != nil {
			t.Fatal(err)
		}
		fixture.broker.mu.Lock()
		flow.dropCredential()
		fixture.broker.mu.Unlock()

		err = fixture.call(AuthCredentialMethod, map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: flowResult.FlowID,
		}, nil)
		requireAuthCause(t, err, authCauseHarvestFailed)
	})

	t.Run("credential slot timeout", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flowResult := fixture.mustAuthorizeMethod("connection-credential-timeout", authMethodAPIKey)
		if err := fixture.callbackMethod(flowResult.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
			t.Fatalf("manual material: %v", err)
		}

		release, admitted := fixture.broker.admitSlot(context.Background(), authProviderID)
		if !admitted {
			t.Fatal("could not hold the slot gate")
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := fixture.broker.credential(ctx, fixture.rawParams(map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: flowResult.FlowID,
		}))
		release()
		requireAuthCause(t, err, authCauseTimeout)

		credential := harvestAuthCredential(t, fixture, flowResult.FlowID)
		if credential.Credential.API == nil || credential.Credential.API.Key != manualAmpKeyCanary {
			t.Fatalf("timed-out credential leg consumed material: %#v", credential)
		}
	})
}

func TestManualAPIKeyDisconnectWipesUnharvestedMaterial(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	result := fixture.mustAuthorizeMethod("connection-disconnect", authMethodAPIKey)
	if err := fixture.callbackMethod(result.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, result.FlowID)
	if err != nil {
		t.Fatal(err)
	}

	if disconnectErr := fixture.disconnect("connection-disconnect", 1); disconnectErr != nil {
		t.Fatalf("disconnect: %v", disconnectErr)
	}

	fixture.broker.mu.Lock()
	retained := len(flow.credential)
	fixture.broker.mu.Unlock()
	if retained != 0 {
		t.Fatalf("disconnected flow retained %d credential bytes", retained)
	}

	if status := fixture.status(result.FlowID); status.State != authStateCancelled || status.Reason != authReasonOwnerCancel {
		t.Fatalf("disconnected flow status = %#v", status)
	}

	err = fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: result.FlowID,
	}, nil)
	if err == nil {
		t.Fatal("disconnected manual material remained harvestable")
	}
	requireAuthCause(t, err, authCauseFlowState)
}

func TestManualAPIKeyCredentialAndDisconnectShareSlotFence(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	result := fixture.mustAuthorizeMethod("connection-raced-disconnect", authMethodAPIKey)
	if err := fixture.callbackMethod(result.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}

	originalRead := ledgerReadFile
	readConfirmed := make(chan struct{})
	releaseRead := make(chan struct{})
	var reads atomic.Int64
	ledgerReadFile = func(path string) ([]byte, error) {
		contents, err := originalRead(path)
		if reads.Add(1) == 1 {
			close(readConfirmed)
			<-releaseRead
		}

		return contents, err
	}
	t.Cleanup(func() { ledgerReadFile = originalRead })

	type credentialAnswer struct {
		result any
		err    error
	}
	credentialAnswered := make(chan credentialAnswer, 1)
	go func() {
		answer, err := fixture.broker.credential(context.Background(), fixture.rawParams(map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: result.FlowID,
		}))
		credentialAnswered <- credentialAnswer{result: answer, err: err}
	}()

	<-readConfirmed

	disconnectAnswered := make(chan error, 1)
	go func() { disconnectAnswered <- fixture.disconnect("connection-raced-disconnect", 1) }()

	select {
	case err := <-disconnectAnswered:
		t.Fatalf("disconnect crossed a credential leg holding a confirmed ledger read: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseRead)

	credential := <-credentialAnswered
	if credential.err != nil {
		t.Fatalf("credential: %v", credential.err)
	}
	resultValue, ok := credential.result.(authCredentialResult)
	if !ok || resultValue.Credential.API == nil || resultValue.Credential.API.Key != manualAmpKeyCanary {
		t.Fatalf("credential result = %#v", credential.result)
	}

	if err := <-disconnectAnswered; err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	record, ok, err := fixture.broker.ledger.read(authProviderID, "connection-raced-disconnect")
	if err != nil || !ok || record.State != authLedgerRemoved || record.BindingGeneration != 2 {
		t.Fatalf("ledger after serialized harvest/disconnect = %#v/%v/%v", record, ok, err)
	}
}

func TestManualAPIKeyCancelWinsAgainstClaimedHarvestFailure(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	result := fixture.mustAuthorizeMethod("connection-raced-cancel", authMethodAPIKey)
	if err := fixture.callbackMethod(result.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, result.FlowID)
	if err != nil {
		t.Fatal(err)
	}

	originalRead := ledgerReadFile
	readConfirmed := make(chan struct{})
	releaseRead := make(chan struct{})
	ledgerReadFile = func(path string) ([]byte, error) {
		contents, readErr := originalRead(path)
		close(readConfirmed)
		<-releaseRead

		return contents, readErr
	}
	t.Cleanup(func() { ledgerReadFile = originalRead })

	credentialAnswered := make(chan error, 1)
	go func() {
		_, credentialErr := fixture.broker.credential(context.Background(), fixture.rawParams(map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: result.FlowID,
		}))
		credentialAnswered <- credentialErr
	}()

	<-readConfirmed
	cancelErr := fixture.call(AuthCancelMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: result.FlowID,
	}, nil)
	statusAfterCancel := fixture.status(result.FlowID)
	close(releaseRead)

	if cancelErr != nil {
		t.Fatalf("cancel: %v", cancelErr)
	}
	if statusAfterCancel.State != authStateCancelled || statusAfterCancel.Reason != authReasonOwnerCancel {
		t.Fatalf("status after cancel = %#v", statusAfterCancel)
	}

	harvestErr := <-credentialAnswered
	requireAuthCause(t, harvestErr, authCauseHarvestFailed)

	if status := fixture.status(result.FlowID); status.State != authStateCancelled || status.Reason != authReasonOwnerCancel {
		t.Fatalf("losing harvest overwrote owner cancellation: %#v", status)
	}

	fixture.broker.mu.Lock()
	retained, claimed := len(flow.credential), flow.harvested
	fixture.broker.mu.Unlock()
	if retained != 0 || claimed {
		t.Fatalf("cancelled flow retained bytes/claim: %d/%v", retained, claimed)
	}
}
