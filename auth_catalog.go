package ampacp

import (
	"context"
	"encoding/json"
	"net/url"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Method entry types.
const (
	authMethodTypeOAuth = "oauth"
	authMethodTypeAPI   = "api"
)

// The whole catalog. Both methods produce the same opaque Amp account key. The
// hosted method obtains it through `amp login`; the API method accepts material
// only after authorize has minted a secret interaction. Amp's per-provider
// model credentials remain loopback-only, entitlement-gated, and server-side,
// so none of those is brokerable or offered here.
const (
	authProviderID = "amp"

	authMethodLogin  = "login"
	authMethodAPIKey = "api-key"

	authMethodLoginLabel    = "Sign in to Amp"
	authMethodAPIKeyLabel   = "Amp API key"           //nolint:gosec // UI label, not credential material.
	authMethodAPIKeyMessage = "Paste an Amp API key." //nolint:gosec // UI guidance, not credential material.
	authMethodAPIKeyInput   = "API key"
)

// Display-field bounds. A value violating its bound is dropped, never
// truncated. Labels and authorize guidance have independent bounds because the
// catalog never carries the latter.
const (
	authMaxURLBytes     = 2048
	authMaxMessageBytes = 2048
	authMaxLabelBytes   = 256
)

// authMaxCallbackBytes bounds the value a callback submits. Amp's paste-back
// value is a base64 JSON envelope rather than a bare code, so the bound is the
// envelope's, not a code's.
const authMaxCallbackBytes = 8192

// authMaxSecretBytes bounds one manually supplied Amp key. The exact bytes are
// otherwise opaque: the adapter neither trims nor normalizes credential
// material.
const authMaxSecretBytes = 1024

// pinnedAuthCatalog is the adapter's complete account-method catalog. It is a
// function so tests can replace the whole fact without mutating a returned
// slice shared with a live generation.
var pinnedAuthCatalog = func() []authCatalogMethod {
	return []authCatalogMethod{
		{ID: authMethodLogin, Type: authMethodTypeOAuth, Label: authMethodLoginLabel},
		{
			ID:            authMethodAPIKey,
			Type:          authMethodTypeAPI,
			Label:         authMethodAPIKeyLabel,
			Message:       authMethodAPIKeyMessage,
			Interaction:   authInteractionSecret,
			CallbackInput: authMethodAPIKeyInput,
		},
	}
}

// authCatalogMethod is one entry of the current catalog.
type authCatalogMethod struct {
	ID            string
	Type          string
	Label         string
	Message       string
	Interaction   string
	CallbackInput string
}

type authMethodEntry struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Label string `json:"label"`
}

type authMethodsResult struct {
	Providers  map[string][]authMethodEntry `json:"providers"`
	Generation string                       `json:"generation"`
}

// methods enumerates the catalog and mints the generation that names this exact
// result. The catalog is pinned rather than native: amp exposes no enumeration
// route, so the account methods are adapter facts.
func (p *providerAuth) methods(_ context.Context, params json.RawMessage) (any, error) {
	fields, err := authParamFields(params, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	sessionID, err := authRequiredString(fields, authFieldSessionID)
	if err != nil {
		return nil, err
	}

	if _, sessionErr := p.authSession(sessionID); sessionErr != nil {
		return nil, sessionErr
	}

	methods, entries := buildAuthCatalog()
	if len(methods) == 0 {
		return nil, authFailed(authCauseNativeVeto, "", "", "")
	}

	generation, err := newAuthToken()
	if err != nil {
		return nil, authFailed(authCauseProcess, "", "", "")
	}

	p.mu.Lock()
	p.generation = generation
	p.catalog = methods
	p.mu.Unlock()

	return authMethodsResult{Providers: entries, Generation: generation}, nil
}

// buildAuthCatalog applies the label bound entry by entry. An invalid entry is
// omitted rather than truncated. The methods leg fails closed only when none
// survive.
func buildAuthCatalog() (map[string][]authCatalogMethod, map[string][]authMethodEntry) {
	pinned := pinnedAuthCatalog()
	methods := make([]authCatalogMethod, 0, len(pinned))
	entries := make([]authMethodEntry, 0, len(pinned))

	for _, method := range pinned {
		label, ok := authDisplayText(method.Label, authMaxLabelBytes)
		if !ok {
			continue
		}

		method.Label = label
		methods = append(methods, method)
		entries = append(entries, authMethodEntry{ID: method.ID, Type: method.Type, Label: label})
	}

	if len(methods) == 0 {
		return nil, nil
	}

	return map[string][]authCatalogMethod{authProviderID: methods},
		map[string][]authMethodEntry{authProviderID: entries}
}

// authResolveMethod fences a method id against the generation that produced it.
func (p *providerAuth) authResolveMethod(providerID string, generation string, methodID string) (authCatalogMethod, error) {
	p.mu.Lock()
	current := p.generation
	methods := p.catalog
	p.mu.Unlock()

	if current == "" || current != generation {
		return authCatalogMethod{}, invalidAuthField(authFieldMethodsGeneration)
	}

	for _, method := range methods[providerID] {
		if method.ID == methodID {
			return method, nil
		}
	}

	if _, known := methods[providerID]; !known {
		return authCatalogMethod{}, invalidAuthField(authFieldProviderID)
	}

	return authCatalogMethod{}, invalidAuthField(authFieldMethod)
}

// authDisplayText normalises a native presentation string to NFC and measures
// its bounds and categories on that normalised form, which is also the form the
// adapter relays, persists, and returns. Normalising after measuring bounds a
// string nobody sends.
func authDisplayText(value string, maxBytes int) (string, bool) {
	normalized := norm.NFC.String(value)
	if normalized == "" || len(normalized) > maxBytes || !utf8.ValidString(normalized) {
		return "", false
	}

	for _, r := range normalized {
		if !authDisplayRune(r) {
			return "", false
		}
	}

	return normalized, true
}

// authDisplayRune restricts free text to Unicode categories L, N, P, S, and Zs.
// Every C* category is rejected, which is also what excludes every
// bidirectional override and embedding character.
func authDisplayRune(r rune) bool {
	switch {
	case unicode.IsLetter(r), unicode.IsNumber(r), unicode.IsPunct(r), unicode.IsSymbol(r):
		return true
	case unicode.Is(unicode.Zs, r):
		return true
	default:
		return false
	}
}

// authDisplayURL applies the url bound: at most 2048 bytes, scheme exactly
// https, no userinfo, no fragment.
func authDisplayURL(value string) (string, bool) {
	normalized := norm.NFC.String(value)
	if normalized == "" || len(normalized) > authMaxURLBytes {
		return "", false
	}

	parsed, err := url.Parse(normalized)
	if err != nil || parsed.Scheme != valHTTPS || parsed.User != nil || parsed.Fragment != "" || parsed.Host == "" {
		return "", false
	}

	return normalized, true
}

// validateAuthInputs enforces that an authorize request answers exactly the
// prompts the catalog published. Amp publishes none — its login takes no
// operator input before the paste — so any supplied key is a caller addressing
// failure, and no prompt answer ever reaches a native call or a URL.
func validateAuthInputs(inputs map[string]string) error {
	if len(inputs) > 0 {
		return invalidAuthField(authFieldInputs)
	}

	return nil
}

// validateAuthSecret bounds manually supplied credential material without
// transforming it. Control characters, including line breaks, are rejected so
// a key can never become multiple environment or diagnostic lines.
func validateAuthSecret(value string) error {
	if value == "" || len(value) > authMaxSecretBytes || !utf8.ValidString(value) {
		return invalidAuthField(authFieldInput)
	}

	for _, char := range value {
		if char == '\n' || char == '\r' || unicode.IsControl(char) {
			return invalidAuthField(authFieldInput)
		}
	}

	return nil
}
