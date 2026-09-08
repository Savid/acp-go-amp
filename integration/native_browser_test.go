//go:build integration && browsercanary

package integration

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	pinnedAmpContainerPath       = "/usr/local/bin/amp"
	pinnedAmpProbeCase           = "TestInstalledAmpLoginExecsOnlyShimLauncher"
	installedAmpProbeEnv         = "ACP_GO_AMP_TEST_INSTALLED_LINUX_AMP"
	networklessContainerModeName = "none"
	pinnedLinuxAmpVersion        = "0.0.1785846794-g0de1fc"
	pinnedLinuxAmpURL            = "https://static.ampcode.com/cli/0.0.1785846794-g0de1fc/amp-linux-x64.gz"
	pinnedLinuxAmpSHA256         = "6fb797cd7be032e5f674367460ebd0cd4a770700949839c63e5ddbfd336e4ee2"
	pinnedLinuxAmpMaxBytes       = 256 << 20
)

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

	probe := buildBrowserShimProbe(t, "integration,browsercanary")
	if err := container.CopyFileToContainer(ctx, probe, browserShimProbePath, 0o755); err != nil {
		t.Fatalf("copy pinned Amp browser probe: %v", err)
	}
	if err := container.CopyFileToContainer(ctx, ampPath, pinnedAmpContainerPath, 0o755); err != nil {
		t.Fatalf("copy pinned Amp binary: %v", err)
	}

	code, output, err := container.Exec(ctx, []string{
		"/usr/bin/env",
		"ACP_GO_AMP_RUN_INTEGRATION=1",
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
