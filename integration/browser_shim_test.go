//go:build integration

package integration

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	pinnedAmpContainerPath       = "/usr/local/bin/amp"
	pinnedAmpProbeCase           = "TestInstalledAmpLoginExecsOnlyShimLauncher"
	installedAmpProbeEnv         = "ACP_GO_AMP_TEST_INSTALLED_LINUX_AMP"
	networklessContainerModeName = "none"
	pinnedLinuxAmpVersion        = "0.0.1785846794-g0de1fc"
	pinnedLinuxAmpURL            = "https://static.ampcode.com/cli/0.0.1785846794-g0de1fc/amp-linux-x64.gz"
	pinnedLinuxAmpSHA256         = "6fb797cd7be032e5f674367460ebd0cd4a770700949839c63e5ddbfd336e4ee2"
	pinnedLinuxAmpMaxBytes       = 256 << 20
)

// TestSmokeInstalledAmpBrokeredLoginSafetyCanary inspects the real
// installed binary without executing `amp login`, so the canary itself cannot
// open a browser or initiate provider authorization. Darwin and Linux must
// each prove the account-login call reaches the audited bare PATH-resolved
// branch — `open` and `xdg-open` respectively — and names no direct launcher
// for the platform.
func TestSmokeInstalledAmpBrokeredLoginSafetyCanary(t *testing.T) {
	path := integrationAmpPath(t)

	if err := amp.CheckAuthLoginBrowserSafety(path); err != nil {
		t.Fatalf("brokered login safety = %v", err)
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

// TestNativeBrowserPinnedLinuxAmpLoginExecsOnlyShimLauncher executes the verified
// Linux Amp account-login path in a networkless, no-GUI container.
func TestNativeBrowserPinnedLinuxAmpLoginExecsOnlyShimLauncher(t *testing.T) {
	requireIntegration(t)
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("%s=1 requires a container runtime: %v", envRunIntegration, err)
	}
	if runtime.GOOS != "linux" {
		t.Skip("the required CI canary runs the Linux integration binary natively")
	}
	if runtime.GOARCH != "amd64" {
		t.Fatalf("the pinned Linux Amp canary requires amd64, got %s", runtime.GOARCH)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ampPath := downloadPinnedLinuxAmp(t, ctx)

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    "native-browser",
				Dockerfile: "Dockerfile",
				KeepImage:  true,
			},
			Cmd:         []string{"sleep", "infinity"},
			NetworkMode: dockercontainer.NetworkMode(networklessContainerModeName),
			WaitingFor:  wait.ForExec([]string{"/bin/sh", "-c", "exit 0"}).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start pinned Amp browser fixture: %v", err)
	}

	t.Cleanup(func() {
		if err := container.Terminate(context.WithoutCancel(ctx)); err != nil {
			t.Errorf("terminate installed Amp browser fixture: %v", err)
		}
	})

	probe := buildBrowserShimProbe(t)
	if err := container.CopyFileToContainer(ctx, probe, browserShimProbePath, 0o755); err != nil {
		t.Fatalf("copy pinned Amp browser probe: %v", err)
	}
	if err := container.CopyFileToContainer(ctx, ampPath, pinnedAmpContainerPath, 0o755); err != nil {
		t.Fatalf("copy pinned Amp binary: %v", err)
	}

	code, output, err := container.Exec(ctx, []string{
		"/usr/bin/env",
		installedAmpProbeEnv + "=" + pinnedAmpContainerPath,
		browserShimProbePath,
		"-test.v",
		"-test.run", "^" + pinnedAmpProbeCase + "$",
	}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("run pinned Amp browser probe: %v", err)
	}

	logs, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read installed Amp browser probe output: %v", readErr)
	}
	t.Log(string(logs))

	if code != 0 {
		t.Fatalf("pinned Amp browser probe exited %d", code)
	}
	if !strings.Contains(string(logs), "--- PASS: "+pinnedAmpProbeCase) {
		t.Fatalf("%s did not run in the container", pinnedAmpProbeCase)
	}
}

func downloadPinnedLinuxAmp(t *testing.T, ctx context.Context) string {
	t.Helper()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pinnedLinuxAmpURL, nil)
	if err != nil {
		t.Fatalf("create Amp %s download request: %v", pinnedLinuxAmpVersion, err)
	}
	response, err := (&http.Client{Timeout: 5 * time.Minute}).Do(request)
	if err != nil {
		t.Fatalf("download Amp %s: %v", pinnedLinuxAmpVersion, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("download Amp %s: HTTP %s", pinnedLinuxAmpVersion, response.Status)
	}

	compressed, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatalf("open Amp %s artifact: %v", pinnedLinuxAmpVersion, err)
	}
	defer compressed.Close()

	path := filepath.Join(t.TempDir(), "amp")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755)
	if err != nil {
		t.Fatalf("create Amp %s binary: %v", pinnedLinuxAmpVersion, err)
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(compressed, pinnedLinuxAmpMaxBytes+1))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("extract Amp %s: %v", pinnedLinuxAmpVersion, errors.Join(copyErr, closeErr))
	}
	if written > pinnedLinuxAmpMaxBytes {
		t.Fatalf("Amp %s exceeds %d decompressed bytes", pinnedLinuxAmpVersion, pinnedLinuxAmpMaxBytes)
	}
	if got := fmt.Sprintf("%x", hash.Sum(nil)); got != pinnedLinuxAmpSHA256 {
		t.Fatalf("Amp %s SHA-256 = %s, want %s", pinnedLinuxAmpVersion, got, pinnedLinuxAmpSHA256)
	}

	return path
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
