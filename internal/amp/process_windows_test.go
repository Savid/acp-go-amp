//go:build windows

package amp

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const windowsContainmentHelperEnv = "GO_WANT_AMP_WINDOWS_CONTAINMENT_HELPER"

func TestWindowsJobContainsNativeDescendant(t *testing.T) {
	switch os.Getenv(windowsContainmentHelperEnv) {
	case "root":
		runWindowsContainmentRoot()
		return
	case "descendant":
		runWindowsContainmentDescendant()
		return
	}

	root := t.TempDir()
	ready := filepath.Join(root, "descendant-ready")
	sentinel := filepath.Join(root, "descendant-survived")
	native := exec.Command(os.Args[0], "-test.run=^TestWindowsJobContainsNativeDescendant$")
	native.Env = replaceWindowsTestEnv(os.Environ(), map[string]string{
		windowsContainmentHelperEnv: "root",
		"AMP_WINDOWS_READY":         ready,
		"AMP_WINDOWS_SENTINEL":      sentinel,
	})
	native.Stdout = io.Discard
	native.Stderr = io.Discard

	launch, err := prepareProcessTreeCommand(native, processLaunchOptions{})
	if err != nil {
		t.Fatalf("prepare contained command: %v", err)
	}
	tree, err := startProcessTree(launch)
	if err != nil {
		t.Fatalf("start contained command: %v", err)
	}
	t.Cleanup(func() { _ = tree.terminateAndWait(5 * time.Second) })

	waitWindowsTestPath(t, ready, 5*time.Second)
	if err := tree.terminateAndWait(5 * time.Second); err != nil {
		t.Fatalf("terminate Windows job: %v", err)
	}
	_ = native.Wait()

	time.Sleep(750 * time.Millisecond)
	if _, err := os.Stat(sentinel); err == nil {
		t.Fatal("descendant escaped the Windows Job Object")
	} else if !os.IsNotExist(err) {
		t.Fatalf("inspect descendant sentinel: %v", err)
	}
}

func runWindowsContainmentRoot() {
	descendant := exec.Command(os.Args[0], "-test.run=^TestWindowsJobContainsNativeDescendant$")
	descendant.Env = replaceWindowsTestEnv(os.Environ(), map[string]string{
		windowsContainmentHelperEnv: "descendant",
	})
	if err := descendant.Start(); err != nil {
		os.Exit(2)
	}

	select {}
}

func runWindowsContainmentDescendant() {
	if err := os.WriteFile(os.Getenv("AMP_WINDOWS_READY"), []byte("ready"), 0o600); err != nil {
		os.Exit(2)
	}
	time.Sleep(500 * time.Millisecond)
	if err := os.WriteFile(os.Getenv("AMP_WINDOWS_SENTINEL"), []byte("survived"), 0o600); err != nil {
		os.Exit(2)
	}

	select {}
}

func replaceWindowsTestEnv(base []string, replacements map[string]string) []string {
	env := make([]string, 0, len(base)+len(replacements))
	for _, entry := range base {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			if _, replaced := replacements[key]; replaced {
				continue
			}
		}

		env = append(env, entry)
	}
	for key, value := range replacements {
		env = append(env, key+"="+value)
	}

	return env
}

func waitWindowsTestPath(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("path %s did not appear", path)
}
