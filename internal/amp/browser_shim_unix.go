//go:build !windows

package amp

import (
	"fmt"
	"os"
	"path/filepath"
)

func MaterializeBrowserShim(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create browser shim directory: %w", err)
	}

	for _, name := range browserLauncherNames {
		if err := os.WriteFile(filepath.Join(dir, name), browserShimScript, 0o700); err != nil { //nolint:gosec // The shim must be executable.
			return "", fmt.Errorf("write browser shim %s: %w", name, err)
		}
	}

	return dir, nil
}
