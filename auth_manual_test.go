package ampacp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

const manualAmpKeyCanary = "sgamp_manual_opaque_canary"

func TestManualAPIKeyAuthorizeMintsOnlyASecretInteraction(t *testing.T) {
	fixture := newAuthFixture(t, "login", WithEnv(map[string]string{
		"AMP_URL": "https://private.amp.example",
	}))

	if err := os.WriteFile(fixture.session.settingsFile,
		[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}

	original := authStartLogin
	loginCalls := 0
	authStartLogin = func(*nativeamp.Client, context.Context) (*nativeamp.AuthLogin, error) {
		loginCalls++

		return nil, errors.New("manual authorization must not start amp login")
	}
	t.Cleanup(func() { authStartLogin = original })

	result := fixture.mustAuthorizeMethod("connection-manual", authMethodAPIKey)
	if result.Interaction != authInteractionSecret || result.URL != "" ||
		result.Message != authMethodAPIKeyMessage || result.CallbackInput != authMethodAPIKeyInput ||
		result.FlowID == "" || result.FlowExpiresAt == 0 {
		t.Fatalf("manual authorize = %#v", result)
	}

	if loginCalls != 0 {
		t.Fatalf("manual authorize started %d native logins", loginCalls)
	}

	record, ok, err := fixture.broker.ledger.read(authProviderID, "connection-manual")
	if err != nil || !ok || record.Method != authMethodAPIKey || record.State != authLedgerIntent || record.FlowID != result.FlowID {
		t.Fatalf("manual intent = %#v/%v/%v", record, ok, err)
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

func TestManualAPIKeyMaterialValidationAndMethodFence(t *testing.T) {
	invalid := []string{
		"",
		"line\nbreak",
		"carriage\rreturn",
		"control\x00byte",
		strings.Repeat("x", authMaxSecretBytes+1),
	}

	for index, value := range invalid {
		fixture := newAuthFixture(t, "login-hang")
		flow := fixture.mustAuthorizeMethod("connection-invalid", authMethodAPIKey)
		err := fixture.callbackMethod(flow.FlowID, authMethodAPIKey, value)
		if err == nil {
			t.Fatalf("invalid material %d was accepted", index)
		}
		requireInvalidAuthField(t, err, authFieldInput)

		if status := fixture.status(flow.FlowID); status.State != authStatePending {
			t.Fatalf("invalid material %d moved the flow: %#v", index, status)
		}
	}

	if err := validateAuthSecret(string([]byte{0xff, 0xfe})); err == nil {
		t.Fatal("invalid UTF-8 credential material was accepted")
	}

	fixture := newAuthFixture(t, "login-hang")
	flow := fixture.mustAuthorizeMethod("connection-fenced", authMethodAPIKey)
	if err := fixture.callbackMethod(flow.FlowID, authMethodLogin, manualAmpKeyCanary); err == nil {
		t.Fatal("manual material crossed the method fence")
	} else {
		requireInvalidAuthField(t, err, authFieldMethod)
	}
}

func TestManualAPIKeyPresentationFailsClosedOnUnsafeGuidance(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	original := pinnedAuthCatalog
	pinnedAuthCatalog = func() []authCatalogMethod {
		return []authCatalogMethod{{
			ID: authMethodAPIKey, Type: authMethodTypeAPI, Label: authMethodAPIKeyLabel,
			Message: "unsafe\nmessage", Interaction: authInteractionSecret, CallbackInput: authMethodAPIKeyInput,
		}}
	}
	t.Cleanup(func() { pinnedAuthCatalog = original })

	_, err := fixture.authorizeMethod("connection-unsafe", "request-unsafe", authMethodAPIKey)
	if err == nil {
		t.Fatal("manual authorize relayed unsafe guidance")
	}
	requireAuthCause(t, err, authCauseNativeVeto)
}

func TestManualAPIKeyMaterialFailurePathsRemainFenced(t *testing.T) {
	t.Run("already saved", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flow := fixture.mustAuthorizeMethod("connection-saved", authMethodAPIKey)
		if err := fixture.callbackMethod(flow.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
			t.Fatal(err)
		}

		err := fixture.callbackMethod(flow.FlowID, authMethodAPIKey, "second-copy")
		requireAuthCause(t, err, authCauseFlowState)
	})

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

	t.Run("lineage moved", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flow := fixture.mustAuthorizeMethod("connection-moved", authMethodAPIKey)
		record, _, err := fixture.broker.ledger.read(authProviderID, "connection-moved")
		if err != nil {
			t.Fatal(err)
		}
		record.BindingGeneration++
		if writeErr := fixture.broker.ledger.write(record); writeErr != nil {
			t.Fatal(writeErr)
		}

		err = fixture.callbackMethod(flow.FlowID, authMethodAPIKey, manualAmpKeyCanary)
		requireAuthCause(t, err, authCauseBindingConflict)
		if status := fixture.status(flow.FlowID); status.State != authStatePending {
			t.Fatalf("lineage refusal moved the flow: %#v", status)
		}
	})

	t.Run("slot timeout", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flowResult := fixture.mustAuthorizeMethod("connection-timeout", authMethodAPIKey)
		flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, flowResult.FlowID)
		if err != nil {
			t.Fatal(err)
		}

		release, admitted := fixture.broker.admitSlot(context.Background(), authProviderID)
		if !admitted {
			t.Fatal("could not hold the slot gate")
		}
		defer release()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = fixture.broker.saveCredential(ctx, flow, manualAmpKeyCanary)
		requireAuthCause(t, err, authCauseTimeout)
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

	t.Run("cancel after durable confirmation", func(t *testing.T) {
		fixture := newAuthFixture(t, "login-hang")
		flowResult := fixture.mustAuthorizeMethod("connection-race", authMethodAPIKey)

		originalRename := ledgerRename
		started := make(chan struct{})
		release := make(chan struct{})
		ledgerRename = func(oldPath string, newPath string) error {
			close(started)
			<-release

			return originalRename(oldPath, newPath)
		}
		t.Cleanup(func() { ledgerRename = originalRename })

		answered := make(chan error, 1)
		go func() {
			answered <- fixture.callbackMethod(flowResult.FlowID, authMethodAPIKey, manualAmpKeyCanary)
		}()
		<-started

		if err := fixture.call(AuthCancelMethod, map[string]any{
			authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: flowResult.FlowID,
		}, nil); err != nil {
			t.Fatalf("cancel: %v", err)
		}
		close(release)

		requireAuthCause(t, <-answered, authCauseFlowState)
		if status := fixture.status(flowResult.FlowID); status.State != authStateCancelled {
			t.Fatalf("raced cancel status = %#v", status)
		}
	})
}

func TestManualAPIKeyCancelWipesUnharvestedMaterial(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")

	pending := fixture.mustAuthorizeMethod("connection-pending", authMethodAPIKey)
	if err := fixture.call(AuthCancelMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: pending.FlowID,
	}, nil); err != nil {
		t.Fatalf("cancel pending manual flow: %v", err)
	}
	if status := fixture.status(pending.FlowID); status.State != authStateCancelled || status.Reason != authReasonOwnerCancel {
		t.Fatalf("pending cancel status = %#v", status)
	}

	saved := fixture.mustAuthorizeMethod("connection-saved", authMethodAPIKey)
	if err := fixture.callbackMethod(saved.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}
	if err := fixture.call(AuthCancelMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: saved.FlowID,
	}, nil); err != nil {
		t.Fatalf("cancel saved manual flow: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, saved.FlowID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.broker.mu.Lock()
	retained := len(flow.credential)
	fixture.broker.mu.Unlock()
	if retained != 0 {
		t.Fatalf("cancelled flow retained %d credential bytes", retained)
	}
	if status := fixture.status(saved.FlowID); status.State != authStateCancelled || status.Reason != authReasonOwnerCancel {
		t.Fatalf("saved cancel status = %#v", status)
	}
	if err := fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: saved.FlowID,
	}, nil); err == nil {
		t.Fatal("cancelled manual material remained harvestable")
	}
}

func TestManualAPIKeyLateExpiryCannotWipeSavedMaterial(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	result := fixture.mustAuthorizeMethod("connection-late-expiry", authMethodAPIKey)
	if err := fixture.callbackMethod(result.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, result.FlowID)
	if err != nil {
		t.Fatal(err)
	}

	// Model the deadline goroutine winning its timer-vs-disarm select just
	// before callback saved the material, then reaching expire after callback
	// returned. A terminal flow already has its owner; the losing expiry is a
	// true no-op and must not perform terminal cleanup on that owner's state.
	fixture.broker.expire(flow)

	if status := fixture.status(result.FlowID); status.State != authStateSaved || status.Reason != "" {
		t.Fatalf("late expiry moved the saved flow: %#v", status)
	}

	credential := harvestAuthCredential(t, fixture, result.FlowID)
	if credential.Credential.API == nil || credential.Credential.API.Key != manualAmpKeyCanary {
		t.Fatalf("late expiry wiped the saved material: %#v", credential)
	}
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

func TestManualAPIKeyRestartProofAndDisconnectFence(t *testing.T) {
	first := newAuthFixture(t, "login")
	manual := first.mustAuthorizeMethod("connection-restart", authMethodAPIKey)
	if err := first.callbackMethod(manual.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}
	harvest := harvestAuthCredential(t, first, manual.FlowID)

	if err := first.agent.Close(); err != nil {
		t.Fatalf("close first agent: %v", err)
	}

	restartedAgent := newTestAgent(
		WithExecutablePath(first.agent.options.ExecutablePath),
		WithScratchDir(testScratchDir(t)),
		WithProviderAuthRoot(first.root),
	)
	t.Cleanup(func() { _ = restartedAgent.Close() })
	restartedSession, err := newAgentSession(t.Context(), restartedAgent, "T-restarted", t.TempDir(), parsedSessionMeta{}, "", nil)
	if err != nil {
		t.Fatalf("restart session: %v", err)
	}
	restartedAgent.mu.Lock()
	restartedAgent.sessions[restartedSession.id] = restartedSession
	restartedAgent.mu.Unlock()
	restarted := &authFixture{t: t, agent: restartedAgent, broker: restartedAgent.providerAuth, session: restartedSession, root: first.root}

	var inventory authInventoryResult
	if err := restarted.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(restartedSession.id)}, &inventory); err != nil {
		t.Fatalf("restart inventory: %v", err)
	}
	if len(inventory.Entries) != 1 || inventory.Entries[0].ProviderID != authProviderID ||
		inventory.Entries[0].ConnectionID != "connection-restart" ||
		inventory.Entries[0].BindingGeneration != harvest.BindingGeneration ||
		inventory.Entries[0].ProofSource != authProofNotConfirmed {
		t.Fatalf("restart inventory = %#v", inventory.Entries)
	}

	if err := restarted.call(AuthDisconnectMethod, map[string]any{
		authFieldSessionID: string(restartedSession.id), authFieldProviderID: authProviderID,
		authFieldConnectionID: "connection-restart", authFieldBindingGeneration: harvest.BindingGeneration,
	}, nil); err != nil {
		t.Fatalf("restart disconnect: %v", err)
	}

	var after authInventoryResult
	if err := restarted.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(restartedSession.id)}, &after); err != nil {
		t.Fatalf("inventory after disconnect: %v", err)
	}
	if len(after.Entries) != 0 {
		t.Fatalf("disconnected restart inventory = %#v", after.Entries)
	}
}

func harvestAuthCredential(t *testing.T, fixture *authFixture, flowID string) authCredentialResult {
	t.Helper()

	var result authCredentialResult
	if err := fixture.call(AuthCredentialMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID, authFieldFlowID: flowID,
	}, &result); err != nil {
		t.Fatalf("credential: %v", err)
	}

	return result
}
