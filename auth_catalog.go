package ampacp

import (
	"context"
	"encoding/json"
	"net/url"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// authMethodTypeOAuth is the entry type on the wire. Amp's one login is a
// hosted OAuth paste-back, so it is the only type this catalog publishes.
const authMethodTypeOAuth = "oauth"

// The whole catalog. Amp has exactly one login and it authenticates the Amp
// account itself rather than a model provider, so the entry is the account and
// the id is the native subcommand that drives it. Amp's per-provider model
// credentials are loopback-only, entitlement-gated, and stored server-side, so
// none of them is brokerable and none is offered.
const (
	authProviderID   = "amp"
	authMethodID     = "login"
	authProviderName = "Amp account"
)

// Display-field bounds. A value violating its bound is dropped, never
// truncated. The only presentation strings that cross this surface are the
// relayed url and the message, and the message is the catalog label, so the
// label bound is what the message is measured against.
const (
	authMaxURLBytes   = 2048
	authMaxLabelBytes = 256
)

// authMaxCallbackBytes bounds the value a callback submits. Amp's paste-back
// value is a base64 JSON envelope rather than a bare code, so the bound is the
// envelope's, not a code's.
const authMaxCallbackBytes = 8192

// authCatalogName is the display name of the one entry this surface publishes.
// A name that cannot pass the label bound leaves no catalog to publish, which
// is the one way the methods leg fails closed.
var authCatalogName = authProviderName

// authCatalogMethod is one entry of the current catalog.
type authCatalogMethod struct {
	ID    string
	Type  string
	Label string
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
// route, and the one login it does expose is fixed by the CLI's own surface.
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

	methods, entries := buildAuthCatalog(authCatalogName)
	if len(methods) == 0 {
		return nil, authFailed(authCauseNativeVeto, "", "", "")
	}

	generation, err := newAuthToken()
	if err != nil {
		return nil, authFailed(authCauseProcess, "", "", "")
	}

	p.mu.Lock()
	p.generation = generation
	p.mu.Unlock()

	return authMethodsResult{Providers: entries, Generation: generation}, nil
}

// buildAuthCatalog publishes the single account entry. A label that violates
// its display bound omits the entry rather than truncating it, which for a
// one-entry catalog means no catalog can be produced at all.
func buildAuthCatalog(name string) (map[string][]authCatalogMethod, map[string][]authMethodEntry) {
	label, ok := authDisplayText(name, authMaxLabelBytes)
	if !ok {
		return nil, nil
	}

	method := authCatalogMethod{ID: authMethodID, Type: authMethodTypeOAuth, Label: label}

	return map[string][]authCatalogMethod{authProviderID: {method}},
		map[string][]authMethodEntry{authProviderID: {{ID: method.ID, Type: method.Type, Label: method.Label}}}
}

// authResolveMethod fences a method id against the generation that produced it.
func (p *providerAuth) authResolveMethod(providerID string, generation string, methodID string) (authCatalogMethod, error) {
	p.mu.Lock()
	current := p.generation
	p.mu.Unlock()

	if current == "" || current != generation {
		return authCatalogMethod{}, invalidAuthField(authFieldMethodsGeneration)
	}

	methods, _ := buildAuthCatalog(authCatalogName)

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
