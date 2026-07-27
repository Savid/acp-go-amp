//go:build integration

package integration

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	envRunKeystore    = "ACP_GO_AMP_RUN_KEYSTORE"
	keystoreEnvFile   = "/run/acp-go-amp-keystore/env"
	keystoreRoundTrip = "/usr/local/bin/roundtrip.sh"
	keystoreProbePath = "/usr/local/bin/residence.test"
)

func requireKeystoreTier(t *testing.T) {
	t.Helper()

	requireIntegration(t)

	if os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 to run the credential-residence tier", envRunKeystore)
	}

	// The tier fails rather than skips once its gate is set: a silently green
	// residence suite is worse than a red one.
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("%s=1 requires a container runtime: %v", envRunKeystore, err)
	}
}

// TestKeystoreProviderAuthResidence runs the credential-residence matrix
// against a live Secret Service. Amp bundles a real keystore binding behind a
// settings flag whose item is keyed by hostname and nothing else, so this tier
// proves the flag the wrapper asserts false keeps the isolated file store
// authoritative and that an item present under the unpartitioned name never
// becomes the harvest source.
func TestKeystoreProviderAuthResidence(t *testing.T) {
	requireKeystoreTier(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    filepath.Join(".", "keystore"),
				Dockerfile: "Dockerfile",
				KeepImage:  true,
			},
			// Readiness is a store/lookup round trip executed in the container.
			// A log line and a bus-name check both report ready against a
			// service that answers no lookup.
			WaitingFor: wait.ForExec([]string{keystoreRoundTrip}).WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start keystore fixture: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("terminate keystore fixture: %v", err)
		}
	})

	probe := buildResidenceProbe(t)

	if err := container.CopyFileToContainer(ctx, probe, keystoreProbePath, 0o755); err != nil {
		t.Fatalf("copy residence probe: %v", err)
	}

	code, output, err := container.Exec(ctx, []string{
		"/bin/sh", "-c",
		". " + keystoreEnvFile + "; export DBUS_SESSION_BUS_ADDRESS; exec " +
			keystoreProbePath + " -test.v -test.run '^TestKeystoreResidenceMatrix$'",
	})
	if err != nil {
		t.Fatalf("run residence matrix: %v", err)
	}

	logs, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read residence output: %v", readErr)
	}

	t.Log(string(logs))

	if code != 0 {
		t.Fatalf("residence matrix exited %d", code)
	}
}

// buildResidenceProbe compiles the package that owns the read path for the
// fixture's platform. The matrix cannot run on the host: only the container has
// a Secret Service to answer it.
func buildResidenceProbe(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "residence.test")

	command := exec.Command("go", "test", "-c", "-o", out, ".")
	command.Dir = ".."
	command.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")

	if buildOutput, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build residence probe: %v: %s", err, buildOutput)
	}

	return out
}
