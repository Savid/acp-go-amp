//go:build integration && (linux || darwin)

package ampacp

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	nativeamp "github.com/savid/acp-go-amp/internal/amp"
)

const (
	envRunIntegration = "ACP_GO_AMP_RUN_INTEGRATION"
	envRunKeystore    = "ACP_GO_AMP_RUN_KEYSTORE"
	// envSessionBus reaches the Secret Service. Whether the fixture exported one
	// is the whole difference between the two Linux configurations.
	envSessionBus = "DBUS_SESSION_BUS_ADDRESS"
	// keystoreFixtureMarker is written by the credential-residence fixture's
	// entrypoint. Seeding a live Secret Service is only safe inside that
	// container, so the Linux configurations run nowhere else.
	keystoreFixtureMarker = "/run/acp-go-amp-keystore/marker"
	// keystoreDarwinService is a service name this test owns end to end, so the
	// macOS third never reads, overwrites, or deletes a real login item.
	keystoreDarwinService = "acp-go-amp-residence-canary"
)

// keystoreCanary values are canary material only; nothing here plants a real
// credential and no configuration mounts a real home.
const (
	keystoreFileCanary  = "canary-file-store-key"
	keystoreStoreCanary = "canary-keystore-key"
)

// TestKeystoreResidenceMatrix proves the two facts amp's assert-false rests on:
// the file store stays authoritative for the read path, and a platform keystore
// item never becomes the harvest source. It asserts that identity in whichever
// of the tier's three configurations it was placed in — keystore-absent Linux,
// keystore-present Linux, or macOS — because a harness that ships no keystore
// provider is exactly where a pin move breaks the claim silently.
func TestKeystoreResidenceMatrix(t *testing.T) {
	requireResidenceTier(t)
	seedResidenceKeystore(t)

	dataHome := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dataHome, "amp"), 0o700); err != nil {
		t.Fatal(err)
	}

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

	// Removing the file leaves any keystore item alone and the harvest empty,
	// which is the fail-closed answer rather than the keystore's value.
	if err := os.Remove(nativeamp.AuthSecretsPath(dataHome)); err != nil {
		t.Fatal(err)
	}

	if _, present, err := nativeamp.AuthReadSecret(dataHome); err != nil || present {
		t.Fatalf("a keystore item became the harvest source: %v/%v", present, err)
	}
}

// requireResidenceTier answers to both tier gates. On Linux it additionally
// requires the fixture container: planting a canary in a developer's live
// Secret Service is not something a test may do, and the container is where the
// driver runs this binary once per Linux configuration.
func requireResidenceTier(t *testing.T) {
	t.Helper()

	if os.Getenv(envRunIntegration) != "1" || os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 and %s=1 to run the credential-residence matrix",
			envRunIntegration, envRunKeystore)
	}

	if runtime.GOOS == "darwin" {
		return
	}

	if _, err := os.Stat(keystoreFixtureMarker); err != nil {
		t.Skipf("the Linux configurations run inside the keystore fixture container: %v", err)
	}
}

// seedResidenceKeystore plants canary material through the platform tool rather
// than through the read path, so the assertion is not a round trip of one
// library against itself. The keystore-absent Linux configuration has no
// service to seed, which is that configuration rather than a caveat.
func seedResidenceKeystore(t *testing.T) {
	t.Helper()

	if runtime.GOOS == "darwin" {
		seedDarwinKeychainCanary(t)

		return
	}

	if os.Getenv(envSessionBus) == "" {
		t.Log("keystore-absent configuration: no session bus is exported, so nothing is seeded")

		return
	}

	command := exec.Command("secret-tool", "store", "--label=amp-canary",
		"service", "amp.cli.apiKey", "username", nativeamp.AuthURLHost)
	command.Stdin = strings.NewReader(keystoreStoreCanary)

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("seed keystore canary: %v: %s", err, output)
	}
}

// seedDarwinKeychainCanary writes into the real login keychain, because a
// keystore write under a scratch HOME blocks on an interactive authorization
// modal that never returns. The item carries this test's own service name and
// is deleted when the test ends.
func seedDarwinKeychainCanary(t *testing.T) {
	t.Helper()

	seed := exec.Command("security", "add-generic-password",
		"-U", "-s", keystoreDarwinService, "-a", nativeamp.AuthURLHost, "-w", keystoreStoreCanary)
	if output, err := seed.CombinedOutput(); err != nil {
		t.Fatalf("seed login keychain canary: %v: %s", err, output)
	}

	t.Cleanup(func() {
		remove := exec.Command("security", "delete-generic-password",
			"-s", keystoreDarwinService, "-a", nativeamp.AuthURLHost)
		if output, err := remove.CombinedOutput(); err != nil {
			t.Errorf("delete login keychain canary: %v: %s", err, output)
		}
	})
}
