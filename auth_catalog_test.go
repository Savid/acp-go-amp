package ampacp

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestMethodsPublishesTheSingleAccountEntry(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	var result authMethodsResult
	if err := fixture.call(AuthMethodsMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &result); err != nil {
		t.Fatalf("methods: %v", err)
	}

	if len(result.Providers) != 1 {
		t.Fatalf("catalog = %#v, want exactly one provider", result.Providers)
	}

	entries := result.Providers[authProviderID]
	if len(entries) != 1 {
		t.Fatalf("provider entries = %#v", entries)
	}

	entry := entries[0]
	if entry.ID != authMethodID || entry.Type != authMethodTypeOAuth || entry.Label != authProviderName {
		t.Fatalf("entry = %#v", entry)
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
	methods, entries := buildAuthCatalog(authProviderName)
	if len(methods) != 1 || len(entries) != 1 {
		t.Fatalf("catalog = %#v/%#v", methods, entries)
	}

	// A label that cannot be displayed omits its entry rather than being
	// truncated, which for a one-entry catalog leaves no catalog at all.
	for _, bad := range []string{"", "Amp\u202eaccount", strings.Repeat("x", authMaxLabelBytes+1)} {
		if methods, entries := buildAuthCatalog(bad); methods != nil || entries != nil {
			t.Fatalf("label %q produced a catalog", bad)
		}
	}
}

func TestMethodsFailsClosedWithNoPublishableCatalog(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	original := authCatalogName
	authCatalogName = strings.Repeat("x", authMaxLabelBytes+1)

	err := fixture.call(AuthMethodsMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, nil)
	authCatalogName = original

	if err == nil {
		t.Fatal("methods published a catalog it could not build")
	}

	requireAuthCause(t, err, authCauseNativeVeto)
}

func TestMethodsFailsClosedWhenNoCatalogCanBeProduced(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	broker := fixture.broker

	// A generation naming an empty catalog resolves no method.
	broker.mu.Lock()
	broker.generation = "spent"
	broker.mu.Unlock()

	if _, err := broker.authResolveMethod(authProviderID, "other", authMethodID); err == nil {
		t.Fatal("a stale generation resolved a method")
	}

	if _, err := broker.authResolveMethod("openai", "spent", authMethodID); err == nil {
		t.Fatal("an unknown provider resolved a method")
	}

	if _, err := broker.authResolveMethod(authProviderID, "spent", "other"); err == nil {
		t.Fatal("an unknown method id resolved")
	}

	if _, err := broker.authResolveMethod(authProviderID, "spent", authMethodID); err != nil {
		t.Fatalf("the pinned method did not resolve: %v", err)
	}

	broker.mu.Lock()
	broker.generation = ""
	broker.mu.Unlock()

	if _, err := broker.authResolveMethod(authProviderID, "", authMethodID); err == nil {
		t.Fatal("a method resolved before any generation was minted")
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
