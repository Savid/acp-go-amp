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
	browserShimProbePath = "/usr/local/bin/browsershim.test"
	browserShimProbeCase = "TestLoginNeverExecsABrowserLauncher"
)

// TestKeystoreLinuxLoginNeverExecsABrowserLauncher runs the login path's
// non-execution proof on Linux. The native binary picks its launcher by GOOS —
// `open` on darwin, `xdg-open` on everything else — so a proof that only ever
// ran on a developer's Mac leaves the branch every Linux host takes asserted
// and never executed.
func TestKeystoreLinuxLoginNeverExecsABrowserLauncher(t *testing.T) {
	requireKeystoreRuntime(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: keystoreBaseImage(t),
			Cmd:   []string{"sleep", "infinity"},
			// The probe is a static binary that shells out, so the only
			// readiness this needs is a container that answers an exec.
			WaitingFor: wait.ForExec([]string{"/bin/sh", "-c", "exit 0"}).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start browser shim fixture: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("terminate browser shim fixture: %v", err)
		}
	})

	probe := buildBrowserShimProbe(t)

	if err := container.CopyFileToContainer(ctx, probe, browserShimProbePath, 0o755); err != nil {
		t.Fatalf("copy browser shim probe: %v", err)
	}

	// The raw exec stream is frame-multiplexed: every read carries an eight-byte
	// header, so an unmultiplexed reader interleaves those bytes into the logs.
	code, output, err := container.Exec(ctx, []string{
		browserShimProbePath,
		"-test.v",
		"-test.run", "^" + browserShimProbeCase + "$",
	}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("run browser shim probe: %v", err)
	}

	logs, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read browser shim probe output: %v", readErr)
	}

	t.Log(string(logs))

	if code != 0 {
		t.Fatalf("browser shim probe exited %d", code)
	}

	if !strings.Contains(string(logs), "--- PASS: "+browserShimProbeCase) {
		t.Fatalf("%s did not run in the container", browserShimProbeCase)
	}
}

// buildBrowserShimProbe compiles the package that owns the login path for the
// fixture's platform. The launcher names it plants are the Linux ones, so it has
// to be a Linux build to mean anything.
func buildBrowserShimProbe(t *testing.T) string {
	t.Helper()

	out := filepath.Join(t.TempDir(), "browsershim.test")

	command := exec.Command("go", "test", "-c", "-o", out, "./internal/amp")
	command.Dir = ".."
	command.Env = append(os.Environ(), "GOWORK=off", "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")

	if buildOutput, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build browser shim probe: %v: %s", err, buildOutput)
	}

	return out
}

// keystoreBaseImage reads the digest-pinned base out of the fixture Dockerfile
// so every container this package starts moves on one pin. A second literal
// reference would drift from the first the moment either is bumped.
func keystoreBaseImage(t *testing.T) string {
	t.Helper()

	dockerfile, err := os.ReadFile(filepath.Join("keystore", "Dockerfile"))
	if err != nil {
		t.Fatalf("read the keystore Dockerfile: %v", err)
	}

	for line := range strings.Lines(string(dockerfile)) {
		reference, ok := strings.CutPrefix(strings.TrimSpace(line), "FROM ")
		if !ok {
			continue
		}

		reference = strings.TrimSpace(reference)
		if !strings.Contains(reference, "@sha256:") {
			t.Fatalf("the keystore base image %q is not pinned by digest", reference)
		}

		return reference
	}

	t.Fatal("the keystore Dockerfile names no base image")

	return ""
}
