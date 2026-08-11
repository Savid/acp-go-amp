package ampacp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

// ProviderCredentialType selects one variant of the closed credential union.
type ProviderCredentialType string

// ProviderCredentialAPI is the only variant amp's legs reach: the account this
// surface brokers is held as one opaque key, and no other shape is ever emitted
// or accepted.
const ProviderCredentialAPI ProviderCredentialType = "api"

// credentialFieldType is the union's discriminator.
const credentialFieldType = "type"

// Metadata bounds, matched to the input bounds so native metadata is always
// reinjectable.
const (
	providerMetadataMaxKeys       = 16
	providerMetadataMaxValueBytes = 1024
	providerMetadataMaxTotalBytes = 8192
)

// ProviderAPICredential is the opaque-key variant. The Amp account key is
// opaque, long-lived, and non-rotating, so it carries no expiry, no refresh
// half, and no metadata.
type ProviderAPICredential struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ProviderCredential is the closed, flat credential union. Its one variant
// pointer is non-nil in a valid value, and the type member selects it.
type ProviderCredential struct {
	Type ProviderCredentialType
	API  *ProviderAPICredential
}

var errProviderCredentialInvalid = errors.New("provider credential is not a valid member of the closed union")

func (credential ProviderCredential) MarshalJSON() ([]byte, error) {
	if credential.Type != ProviderCredentialAPI || credential.API == nil {
		return nil, errProviderCredentialInvalid
	}

	return json.Marshal(apiCredentialEnvelope{Type: credential.Type, ProviderAPICredential: *credential.API})
}

// apiCredentialEnvelope is the flat wire object of the api variant: the
// discriminator beside the variant's own members, which is what makes an
// unknown-field rejection cover the whole object rather than a subset of it.
type apiCredentialEnvelope struct {
	Type ProviderCredentialType `json:"type"`
	ProviderAPICredential
}

// UnmarshalJSON decodes strictly: an unknown field, a duplicate field, an empty
// required string, and a variant this adapter does not accept are all rejected
// rather than partially decoded. Amp brokers one opaque account key, so the
// api variant is the only one it accepts.
func (credential *ProviderCredential) UnmarshalJSON(data []byte) error {
	fields, err := strictCredentialFields(data)
	if err != nil {
		return err
	}

	raw, ok := fields[credentialFieldType]
	if !ok {
		return errProviderCredentialInvalid
	}

	var kind string
	if err := json.Unmarshal(raw, &kind); err != nil {
		return errProviderCredentialInvalid
	}

	if ProviderCredentialType(kind) != ProviderCredentialAPI {
		return errProviderCredentialInvalid
	}

	var envelope apiCredentialEnvelope

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&envelope); err != nil {
		return errProviderCredentialInvalid
	}

	if envelope.Key == "" || !validProviderMetadata(envelope.Metadata) {
		return errProviderCredentialInvalid
	}

	*credential = ProviderCredential{Type: ProviderCredentialAPI, API: &envelope.ProviderAPICredential}

	return nil
}

// strictCredentialFields walks the object once so a duplicate key is rejected
// rather than silently winning.
func strictCredentialFields(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))

	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errProviderCredentialInvalid
	}

	fields := map[string]json.RawMessage{}

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, errProviderCredentialInvalid
		}

		key, _ := keyToken.(string)
		if _, duplicate := fields[key]; duplicate {
			return nil, errProviderCredentialInvalid
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errProviderCredentialInvalid
		}

		fields[key] = value
	}

	if _, err := decoder.Token(); err != nil {
		return nil, errProviderCredentialInvalid
	}

	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errProviderCredentialInvalid
	}

	return fields, nil
}

func validProviderMetadata(metadata map[string]string) bool {
	if len(metadata) > providerMetadataMaxKeys {
		return false
	}

	total := 0

	for key, value := range metadata {
		if len(value) > providerMetadataMaxValueBytes {
			return false
		}

		total += len(key) + len(value)
	}

	return total <= providerMetadataMaxTotalBytes
}

type authCredentialResult struct {
	ConnectionID      string             `json:"connectionId"`
	Revision          int64              `json:"revision"`
	BindingGeneration int64              `json:"bindingGeneration"`
	Credential        ProviderCredential `json:"credential"`
}

// credential harvests exactly one slot: either hosted material from the native
// residence or manual material retained by this flow. It runs once per flow —
// the key is long-lived and non-rotating, so there is no harvest cycle — and a
// slot that answers nothing fails closed rather than reporting absence.
func (p *providerAuth) credential(ctx context.Context, params json.RawMessage) (any, error) {
	session, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	release, admitted := p.admitSlot(ctx, flow.providerID)
	if !admitted {
		return nil, authFailed(authCauseTimeout, flow.providerID, flow.method.ID, flow.id)
	}

	defer release()

	if claimErr := p.claimHarvest(flow); claimErr != nil {
		return nil, claimErr
	}

	record, ok, err := p.ledger.read(flow.providerID, flow.connectionID)
	if err != nil || !ok {
		return nil, p.failHarvest(flow, authCauseHarvestFailed)
	}

	if record.Method != flow.method.ID || record.ConnectionID != flow.connectionID ||
		record.Revision != flow.revision || record.BindingGeneration != flow.bindingGeneration ||
		record.State != authLedgerConfirmed {
		return nil, p.failHarvest(flow, authCauseHarvestFailed)
	}

	if flow.method.Type == authMethodTypeAPI {
		secret, present := p.takeFlowCredential(flow)
		if !present {
			return nil, p.failHarvest(flow, authCauseHarvestFailed)
		}

		return providerCredentialResult(flow, secret), nil
	}

	if storeErr := session.authFileStore(); storeErr != nil {
		return nil, p.failHarvest(flow, authCauseNativeVeto)
	}

	p.mu.Lock()
	residence := flow.residence
	p.mu.Unlock()

	secret, present, err := authReadSecret(residence)
	if err != nil || !present {
		return nil, p.failHarvest(flow, authCauseHarvestFailed)
	}

	// The residence has served its one purpose, so the login child that owns it
	// is torn down here rather than left holding a native root for a harvest
	// that has already happened.
	p.closeLogin(flow)

	return providerCredentialResult(flow, secret), nil
}

// takeFlowCredential consumes manual material after the harvest claim has
// already made this the only credential leg that can reach it. The bytes are
// wiped before the string leaves the broker.
func (p *providerAuth) takeFlowCredential(flow *authFlow) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if flow.state != authStateSaved || len(flow.credential) == 0 {
		return "", false
	}

	secret := string(flow.credential)
	flow.dropCredential()

	return secret, true
}

func providerCredentialResult(flow *authFlow, secret string) authCredentialResult {
	return authCredentialResult{
		ConnectionID:      flow.connectionID,
		Revision:          flow.revision,
		BindingGeneration: flow.bindingGeneration,
		Credential:        ProviderCredential{Type: ProviderCredentialAPI, API: &ProviderAPICredential{Key: secret}},
	}
}

// claimHarvest admits the one harvest a completed flow allows and holds the
// claim for the whole attempt, so two legs answering the same flow cannot both
// read the slot. The state read and the claim are one critical section because
// four native reads sit between them, and a check-then-set that wide lets a
// second leg pass the check before the first has set it — handing back two live
// copies of one key with no race for the detector to find, since every
// individual field access is itself locked.
func (p *providerAuth) claimHarvest(flow *authFlow) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if flow.state != authStateAuthenticated && flow.state != authStateSaved {
		return authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	if flow.harvested {
		return authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	flow.harvested = true

	return nil
}

// failHarvest fails the leg and records a demotion only while the completion
// state its harvest claimed still owns the flow. Owner cancellation can win
// while durable I/O is in flight; that terminal state remains authoritative.
// A cause that owns no transition also leaves the record where its owner left
// it, because writing an empty state would put a value outside the wire enum
// into every later status answer. The claim is released either way:
// at-most-once governs the credential a harvest hands back, and an attempt that
// handed back nothing has nothing to be once about.
func (p *providerAuth) failHarvest(flow *authFlow, cause string) error {
	p.mu.Lock()
	flow.harvested = false

	// Harvest may demote only the completed state its claim admitted. An owner
	// can cancel a saved flow while this leg is blocked on durable I/O; that
	// terminal transition already wiped the material and wins, so the losing
	// harvest must not overwrite it with its own failure.
	if flow.state == authStateAuthenticated || flow.state == authStateSaved {
		if state, reason := authFlowTransition(cause, false); state != "" {
			flow.state = state
			flow.reason = reason
			flow.dropCredential()
		}
	}
	p.mu.Unlock()

	p.closeLogin(flow)

	return authFailed(cause, flow.providerID, flow.method.ID, flow.id)
}

// disconnect bumps the binding generation and releases the ledger slot. It
// performs no native removal and promises no Amp-side revocation: amp's own
// logout refuses while an environment key is set, and the account key stays
// valid at Amp until the owner revokes it there. There is no absence to verify,
// so none is claimed — what it ends is the lineage, and the whole read →
// compare → bump → record sequence runs under the slot gate so a login
// completion cannot write the lineage it just ended back over it.
func (p *providerAuth) disconnect(ctx context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID, authFieldProviderID, authFieldConnectionID, authFieldBindingGeneration)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	providerID, err := authRequiredString(fields, authFieldProviderID)
	if err != nil {
		return nil, err
	}

	connectionID, err := authRequiredConnectionID(fields)
	if err != nil {
		return nil, err
	}

	bindingGeneration, err := authRequiredInt64(fields, authFieldBindingGeneration)
	if err != nil {
		return nil, err
	}

	if _, sessionErr := p.authSession(sessionID); sessionErr != nil {
		return nil, sessionErr
	}

	release, admitted := p.admitSlot(ctx, providerID)
	if !admitted {
		return nil, authFailed(authCauseTimeout, providerID, "", "")
	}

	defer release()

	record, ok, err := p.ledger.read(providerID, connectionID)
	if err != nil {
		return nil, authFailed(authCauseHarvestFailed, providerID, "", "")
	}

	if !ok || record.ConnectionID != connectionID || record.BindingGeneration != bindingGeneration {
		return nil, authFailed(authCauseBindingConflict, providerID, "", "")
	}

	disconnected := record
	record.BindingGeneration++
	record.State = authLedgerRemoved
	record.UpdatedAt = authNow().UnixMilli()

	if err := p.ledger.write(record); err != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	p.fenceDisconnectedManualFlow(disconnected)

	return struct{}{}, nil
}

// fenceDisconnectedManualFlow consumes the in-memory half of a manual binding
// after its durable lineage has been removed. The caller still holds the
// provider slot, so a credential leg either harvested before the disconnect or
// observes this cancellation after it; it can never return material after the
// disconnect returned. Hosted bindings have no in-memory material to revoke
// and remain outside this adapter-owned cleanup.
func (p *providerAuth) fenceDisconnectedManualFlow(removed authLedgerRecord) {
	if removed.Method != authMethodAPIKey {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, flow := range p.byID {
		if flow.method.ID != removed.Method || flow.method.Type != authMethodTypeAPI ||
			flow.providerID != removed.ProviderID || flow.connectionID != removed.ConnectionID ||
			flow.revision != removed.Revision || flow.bindingGeneration != removed.BindingGeneration ||
			flow.state != authStateSaved || flow.harvested {
			continue
		}

		flow.dropCredential()
		flow.state = authStateCancelled
		flow.reason = authReasonOwnerCancel
		flow.stopCompleter()
		delete(p.flows, authFlowKey{sessionID: flow.sessionID, providerID: flow.providerID})
	}
}
