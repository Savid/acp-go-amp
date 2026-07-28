//go:build integration

package integration

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	envRunKeystore    = "ACP_GO_AMP_RUN_KEYSTORE"
	keystoreEnvFile   = "/run/acp-go-amp-keystore/env"
	keystoreRoundTrip = "/usr/local/bin/roundtrip.sh"
	keystoreProbePath = "/usr/local/bin/residence.test"
)

// requireRunKeystore gates the tier on both env vars.
func requireRunKeystore(t *testing.T) {
	t.Helper()

	requireIntegration(t)

	if os.Getenv(envRunKeystore) != "1" {
		t.Skipf("set %s=1 to run the credential-residence tier", envRunKeystore)
	}
}

// requireKeystoreRuntime additionally requires a container runtime for the tests
// that need one. It fails rather than skips once the gate is set: a silently
// green residence suite is worse than a red one.
func requireKeystoreRuntime(t *testing.T) {
	t.Helper()

	requireRunKeystore(t)

	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("%s=1 requires a container runtime: %v", envRunKeystore, err)
	}
}

// TestKeystoreLinuxCredentialResidence runs the credential-residence matrix
// against a live Secret Service. Amp bundles a real keystore binding behind a
// settings flag whose item is keyed by hostname and nothing else, so this tier
// proves the flag the wrapper asserts false keeps the isolated file store
// authoritative and that an item present under the unpartitioned name never
// becomes the harvest source.
func TestKeystoreLinuxCredentialResidence(t *testing.T) {
	requireKeystoreRuntime(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container := startKeystoreFixture(ctx, t)

	probe := buildResidenceProbe(t)

	if err := container.CopyFileToContainer(ctx, probe, keystoreProbePath, 0o755); err != nil {
		t.Fatalf("copy residence probe: %v", err)
	}

	// Both Linux configurations run in this one container and differ by exactly
	// one thing: whether the session bus that reaches the Secret Service is
	// exported. Which store is authoritative is a behavioral fork, and a run
	// that exercises one side of it hides the fork.
	for _, configuration := range []struct {
		name string
		bus  bool
	}{
		{name: "keystore-absent"},
		{name: "keystore-present", bus: true},
	} {
		t.Run(configuration.name, func(t *testing.T) {
			runResidenceMatrix(ctx, t, container, configuration.bus)
		})
	}
}

// startKeystoreFixture builds and starts the Secret Service fixture and
// registers its termination.
func startKeystoreFixture(ctx context.Context, t *testing.T) testcontainers.Container {
	t.Helper()

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

	return container
}

// runResidenceMatrix executes the probe in one configuration and requires it to
// have reported a pass. An exit status alone would go green on a skip, which is
// the silent success this tier exists to prevent.
func runResidenceMatrix(ctx context.Context, t *testing.T, container testcontainers.Container, bus bool) {
	t.Helper()

	script := "export " + envRunIntegration + "=1 " + envRunKeystore + "=1; "
	if bus {
		script += ". " + keystoreEnvFile + "; export DBUS_SESSION_BUS_ADDRESS; "
	}

	script += "exec " + keystoreProbePath + " -test.v -test.run '^TestKeystoreResidenceMatrix$'"

	// The raw exec stream is frame-multiplexed: every read carries an eight-byte
	// header, so an unmultiplexed reader interleaves those bytes into the logs.
	code, output, err := container.Exec(ctx, []string{"/bin/sh", "-c", script}, tcexec.Multiplexed())
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

	if !strings.Contains(string(logs), "--- PASS: TestKeystoreResidenceMatrix") {
		t.Fatal("the residence matrix did not run in this configuration")
	}
}

// buildResidenceProbe compiles the package that owns the read path for the
// fixture's platform, under the tag that guards the matrix so it never builds
// into an ungated run.
func buildResidenceProbe(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "residence.test")

	command := exec.Command("go", "test", "-c", "-tags=integration", "-o", out, ".")
	command.Dir = ".."
	command.Env = append(os.Environ(), "GOWORK=off", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")

	if buildOutput, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build residence probe: %v: %s", err, buildOutput)
	}

	return out
}
