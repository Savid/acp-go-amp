package ampacp

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMethodsPublishesHostedAndManualAccountMethods(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	var result authMethodsResult
	if err := fixture.call(AuthMethodsMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &result); err != nil {
		t.Fatalf("methods: %v", err)
	}

	if len(result.Providers) != 1 {
		t.Fatalf("catalog = %#v, want exactly one provider", result.Providers)
	}

	entries := result.Providers[authProviderID]
	if len(entries) != 2 {
		t.Fatalf("provider entries = %#v", entries)
	}

	want := []authMethodEntry{
		{ID: authMethodLogin, Type: authMethodTypeOAuth, Label: authMethodLoginLabel},
		{ID: authMethodAPIKey, Type: authMethodTypeAPI, Label: authMethodAPIKeyLabel},
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("entries = %#v, want %#v", entries, want)
	}

	if result.Generation == "" {
		t.Fatal("methods minted no generation")
	}

	// Amp publishes no prompts, and no field the family cut.
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}

	for _, banned := range []string{"prompts", "source", "catalogComplete", "defaultApi", "remotable"} {
		if strings.Contains(string(encoded), banned) {
			t.Fatalf("catalog carries %q: %s", banned, encoded)
		}
	}

	// A second call mints a new generation, and the previous one is spent.
	if again := fixture.generation(); again == result.Generation {
		t.Fatal("methods reused a generation token")
	}
}

func TestMethodsFailsClosedWithNoEntropy(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	original := authRandRead
	authRandRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }

	err := fixture.call(AuthMethodsMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, nil)
	authRandRead = original

	if err == nil {
		t.Fatal("methods answered with no generation")
	}

	requireAuthCause(t, err, authCauseProcess)
}

func TestBuildAuthCatalogOmitsAnEntryThatViolatesItsLabelBound(t *testing.T) {
	methods, entries := buildAuthCatalog()
	if len(methods) != 1 || len(methods[authProviderID]) != 2 || len(entries) != 1 || len(entries[authProviderID]) != 2 {
		t.Fatalf("catalog = %#v/%#v", methods, entries)
	}

	original := pinnedAuthCatalog
	t.Cleanup(func() { pinnedAuthCatalog = original })

	// A label that cannot be displayed omits only that method rather than being
	// truncated.
	for _, bad := range []string{"", "Amp\u202eaccount", strings.Repeat("x", authMaxLabelBytes+1)} {
		pinnedAuthCatalog = func() []authCatalogMethod {
			return []authCatalogMethod{
				{ID: authMethodLogin, Type: authMethodTypeOAuth, Label: bad},
				{ID: authMethodAPIKey, Type: authMethodTypeAPI, Label: authMethodAPIKeyLabel},
			}
		}

		methods, entries := buildAuthCatalog()
		if len(methods[authProviderID]) != 1 || len(entries[authProviderID]) != 1 || entries[authProviderID][0].ID != authMethodAPIKey {
			t.Fatalf("label %q produced %#v/%#v", bad, methods, entries)
		}
	}
}

func TestMethodsFailsClosedWithNoPublishableCatalog(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	original := pinnedAuthCatalog
	pinnedAuthCatalog = func() []authCatalogMethod {
		return []authCatalogMethod{{ID: authMethodLogin, Type: authMethodTypeOAuth, Label: strings.Repeat("x", authMaxLabelBytes+1)}}
	}

	err := fixture.call(AuthMethodsMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, nil)
	pinnedAuthCatalog = original

	if err == nil {
		t.Fatal("methods published a catalog it could not build")
	}

	requireAuthCause(t, err, authCauseNativeVeto)
}

func TestMethodsFailsClosedWhenNoCatalogCanBeProduced(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	broker := fixture.broker

	// Seed the resolved catalog, then exercise the generation and addressing
	// fences independently.
	fixture.generation()
	broker.mu.Lock()
	broker.generation = "spent"
	broker.mu.Unlock()

	if _, err := broker.authResolveMethod(authProviderID, "other", authMethodLogin); err == nil {
		t.Fatal("a stale generation resolved a method")
	}

	if _, err := broker.authResolveMethod("openai", "spent", authMethodLogin); err == nil {
		t.Fatal("an unknown provider resolved a method")
	}

	if _, err := broker.authResolveMethod(authProviderID, "spent", "other"); err == nil {
		t.Fatal("an unknown method id resolved")
	}

	if _, err := broker.authResolveMethod(authProviderID, "spent", authMethodLogin); err != nil {
		t.Fatalf("the pinned method did not resolve: %v", err)
	}

	broker.mu.Lock()
	broker.generation = ""
	broker.mu.Unlock()

	if _, err := broker.authResolveMethod(authProviderID, "", authMethodLogin); err == nil {
		t.Fatal("a method resolved before any generation was minted")
	}
}

func TestMethodGenerationResolvesTheExactPublishedCatalog(t *testing.T) {
	fixture := newAuthFixture(t, "login-hang")
	generation := fixture.generation()

	original := pinnedAuthCatalog
	pinnedAuthCatalog = func() []authCatalogMethod {
		return []authCatalogMethod{{ID: authMethodLogin, Type: authMethodTypeOAuth, Label: authMethodLoginLabel}}
	}
	t.Cleanup(func() { pinnedAuthCatalog = original })

	method, err := fixture.broker.authResolveMethod(authProviderID, generation, authMethodAPIKey)
	if err != nil || method.ID != authMethodAPIKey || method.Type != authMethodTypeAPI {
		t.Fatalf("published generation resolved %#v/%v", method, err)
	}
}

func TestAuthDisplayTextNormalisesFirst(t *testing.T) {
	// The composed and decomposed spellings are the same value after NFC, and
	// the normalised form is what the bound is measured on.
	normalized, ok := authDisplayText("Ampé", authMaxLabelBytes)
	if !ok || normalized != "Ampé" {
		t.Fatalf("authDisplayText = %q/%v", normalized, ok)
	}

	if _, ok := authDisplayText("Amp\x00", authMaxLabelBytes); ok {
		t.Fatal("a control character passed the category restriction")
	}

	if _, ok := authDisplayText("Amp\u200f", authMaxLabelBytes); ok {
		t.Fatal("a bidi control passed the category restriction")
	}

	if value, ok := authDisplayText("Amp account 1 - ©", authMaxLabelBytes); !ok || value != "Amp account 1 - ©" {
		t.Fatalf("authDisplayText = %q/%v", value, ok)
	}

	if _, ok := authDisplayText(string([]byte{0xff, 0xfe}), authMaxLabelBytes); ok {
		t.Fatal("invalid UTF-8 passed the bound")
	}
}

func TestAuthDisplayURLBound(t *testing.T) {
	if value, ok := authDisplayURL(fakeLoginURL); !ok || value != fakeLoginURL {
		t.Fatalf("authDisplayURL = %q/%v", value, ok)
	}

	rejected := []string{
		"",
		"http://ampcode.com/auth",
		"https://user:pass@ampcode.com/auth",
		"https://ampcode.com/auth#fragment",
		"https:///auth",
		"https://ampcode.com/" + strings.Repeat("x", authMaxURLBytes),
		"https://ampcode.com/\x7f",
	}
	for _, candidate := range rejected {
		if _, ok := authDisplayURL(candidate); ok {
			t.Fatalf("authDisplayURL accepted %q", candidate)
		}
	}
}

func TestValidateAuthInputsRejectsEveryAnswer(t *testing.T) {
	if err := validateAuthInputs(nil); err != nil {
		t.Fatalf("an empty inputs object was rejected: %v", err)
	}

	if err := validateAuthInputs(map[string]string{"account": "x"}); err == nil {
		t.Fatal("an answer to a prompt amp never published was accepted")
	}
}

// Invalid UTF-8 cannot survive JSON decoding, so the bound is asserted here
// rather than through the callback leg that is its only production caller.
func TestValidateAuthSecretRejectsInvalidUTF8(t *testing.T) {
	if err := validateAuthSecret(string([]byte{0xff, 0xfe})); err == nil {
		t.Fatal("invalid UTF-8 credential material was accepted")
	}
}
