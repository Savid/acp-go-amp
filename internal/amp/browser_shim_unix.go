//go:build !windows

package amp

import (
	"fmt"
	"os"
	"path/filepath"
)

var browserShimWriteFile = os.WriteFile

func MaterializeBrowserShim(dir string) (string, error) {
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", fmt.Errorf("create browser shim directory: %w", err)
	}

	for _, name := range browserLauncherNames {
		if err := browserShimWriteFile(filepath.Join(dir, name), browserShimScript, 0o700); err != nil {
			return "", fmt.Errorf("write browser shim %s: %w", name, err)
		}
	}

	return dir, nil
}
