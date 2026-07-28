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

// credential harvests exactly one slot: the one this connection's own ledger
// entry names, in the session's own isolated data home. It runs once per flow —
// the key is long-lived and non-rotating, so there is no harvest cycle — and a
// slot that answers nothing fails the leg closed rather than reporting absence,
// because a flipped native-secrets flag deletes the file after migrating it.
func (p *providerAuth) credential(_ context.Context, params json.RawMessage) (any, error) {
	session, flow, err := p.addressedFlowLeg(params)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	state := flow.state
	harvested := flow.harvested
	p.mu.Unlock()

	if state != authStateAuthenticated && state != authStateSaved {
		return nil, authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	if harvested {
		return nil, authFailed(authCauseFlowState, flow.providerID, flow.method.ID, flow.id)
	}

	if storeErr := session.authFileStore(); storeErr != nil {
		return nil, p.fail(flow, authCauseNativeVeto, false)
	}

	record, ok, err := p.ledger.read(flow.providerID)
	if err != nil || !ok {
		return nil, p.fail(flow, authCauseHarvestFailed, false)
	}

	if record.ConnectionID != flow.connectionID || record.Revision != flow.revision || record.BindingGeneration != flow.bindingGeneration {
		return nil, p.fail(flow, authCauseHarvestFailed, false)
	}

	p.mu.Lock()
	residence := flow.residence
	p.mu.Unlock()

	secret, present, err := authReadSecret(residence)
	if err != nil || !present {
		return nil, p.fail(flow, authCauseHarvestFailed, false)
	}

	p.mu.Lock()
	flow.harvested = true
	p.mu.Unlock()

	// The residence has served its one purpose, so the login child that owns it
	// is torn down here rather than left holding a native root for a harvest
	// that has already happened.
	p.closeLogin(flow)

	return authCredentialResult{
		ConnectionID:      flow.connectionID,
		Revision:          flow.revision,
		BindingGeneration: flow.bindingGeneration,
		Credential:        ProviderCredential{Type: ProviderCredentialAPI, API: &ProviderAPICredential{Key: secret}},
	}, nil
}

// disconnect bumps the binding generation and releases the ledger slot. It
// performs no native removal and promises no Amp-side revocation: amp's own
// logout refuses while an environment key is set, and the account key stays
// valid at Amp until the owner revokes it there. There is no absence to verify,
// so none is claimed.
func (p *providerAuth) disconnect(_ context.Context, params json.RawMessage) (any, error) {
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

	connectionID, err := authRequiredString(fields, authFieldConnectionID)
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

	record, ok, err := p.ledger.read(providerID)
	if err != nil {
		return nil, authFailed(authCauseHarvestFailed, providerID, "", "")
	}

	if !ok || record.ConnectionID != connectionID || record.BindingGeneration != bindingGeneration {
		return nil, authFailed(authCausePolicy, providerID, "", "")
	}

	record.BindingGeneration++
	record.State = authLedgerRemoved
	record.UpdatedAt = authNow().UnixMilli()

	if err := p.ledger.write(record); err != nil {
		return nil, authFailed(authCauseProcess, providerID, "", "")
	}

	return struct{}{}, nil
}
