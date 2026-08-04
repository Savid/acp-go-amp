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

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/savid/acp-go-amp/internal/amp"
)

const (
	browserShimProbePath         = "/usr/local/bin/browsershim.test"
	browserShimProbeCase         = "TestLoginNeverExecsABrowserLauncher"
	installedAmpContainerPath    = "/usr/local/bin/amp"
	installedAmpProbeCase        = "TestInstalledLinuxAmpLoginExecsOnlyShimLauncher"
	installedLinuxAmpProbeEnv    = "ACP_GO_AMP_TEST_INSTALLED_LINUX_AMP"
	networklessContainerModeName = "none"
)

// TestSmokeInstalledAmpBrokeredLoginCompatibilityCanary inspects the real
// installed binary without executing `amp login`, so the canary itself cannot
// open a browser or initiate provider authorization. Darwin must report the
// boundary as incompatible; Linux must prove its account-login call reaches
// the audited bare xdg-open branch and contains no known absolute launcher.
func TestSmokeInstalledAmpBrokeredLoginCompatibilityCanary(t *testing.T) {
	path := integrationAmpPath(t)
	err := amp.CheckAuthLoginBrowserCompatibility(path)

	if runtime.GOOS == "darwin" {
		if err == nil {
			t.Fatal("Darwin brokered login was accepted without an audited headless account-login contract")
		}

		t.Logf("Darwin brokered login refused before launch: %v", err)

		return
	}

	if err != nil {
		t.Fatalf("brokered login compatibility = %v", err)
	}
}

// TestKeystoreLinuxLoginNeverExecsABrowserLauncher runs the adapter's PATH-shim
// non-execution proof on Linux so that boundary is exercised on the platform
// where brokered login remains enabled.
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

// TestKeystoreInstalledLinuxAmpLoginExecsOnlyShimLauncher copies the installed
// host Linux Amp binary into a networkless, no-GUI container and executes its
// real account-login path through the adapter. The login deployment is
// unreachable loopback, so this proves native launcher interception without
// initiating provider authorization or exposing host credentials.
func TestKeystoreInstalledLinuxAmpLoginExecsOnlyShimLauncher(t *testing.T) {
	requireKeystoreRuntime(t)
	if runtime.GOOS != "linux" {
		t.Skip("the installed host Amp binary is not a Linux executable")
	}

	ampPath, err := exec.LookPath("amp")
	if err != nil {
		t.Fatalf("the real-native Linux browser canary requires installed Amp: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:       keystoreBaseImage(t),
			Cmd:         []string{"sleep", "infinity"},
			NetworkMode: dockercontainer.NetworkMode(networklessContainerModeName),
			WaitingFor:  wait.ForExec([]string{"/bin/sh", "-c", "exit 0"}).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start installed Amp browser fixture: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("terminate installed Amp browser fixture: %v", err)
		}
	})

	probe := buildBrowserShimProbe(t)
	if err := container.CopyFileToContainer(ctx, probe, browserShimProbePath, 0o755); err != nil {
		t.Fatalf("copy installed Amp browser probe: %v", err)
	}
	if err := container.CopyFileToContainer(ctx, ampPath, installedAmpContainerPath, 0o755); err != nil {
		t.Fatalf("copy installed Amp binary: %v", err)
	}

	code, output, err := container.Exec(ctx, []string{
		"/usr/bin/env",
		installedLinuxAmpProbeEnv + "=" + installedAmpContainerPath,
		browserShimProbePath,
		"-test.v",
		"-test.run", "^" + installedAmpProbeCase + "$",
	}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("run installed Amp browser probe: %v", err)
	}

	logs, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read installed Amp browser probe output: %v", readErr)
	}
	t.Log(string(logs))

	if code != 0 {
		t.Fatalf("installed Amp browser probe exited %d", code)
	}
	if !strings.Contains(string(logs), "--- PASS: "+installedAmpProbeCase) {
		t.Fatalf("%s did not run in the container", installedAmpProbeCase)
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
