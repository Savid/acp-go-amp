//go:build !windows

package amp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	browserShimMkdirTemp = os.MkdirTemp
	browserShimWriteFile = os.WriteFile
)

// newBrowserShim materialises the shim beneath parent. os.MkdirTemp creates the
// directory 0700, and every launcher name becomes an executable no-op, so a
// harness that execs one directly reaches this directory before any real
// browser launcher on PATH.
func newBrowserShim(parent string) (*browserShim, error) {
	dir, err := browserShimMkdirTemp(parent, browserShimPrefix)
	if err != nil {
		return nil, fmt.Errorf("create browser shim directory: %w", err)
	}

	for _, name := range browserLauncherNames {
		if writeErr := browserShimWriteFile(filepath.Join(dir, name), browserShimScript, 0o700); writeErr != nil {
			return nil, errors.Join(
				fmt.Errorf("write browser shim %s: %w", name, writeErr),
				os.RemoveAll(dir),
			)
		}
	}

	return &browserShim{dir: dir}, nil
}

func MaterializeBrowserShim(dir string) (string, error) {
	if err := os.Mkdir(dir, 0o700); err != nil {
		return "", fmt.Errorf("create browser shim directory: %w", err)
	}

	for _, name := range browserLauncherNames {
		if err := os.WriteFile(filepath.Join(dir, name), browserShimScript, 0o700); err != nil { //nolint:gosec // The shim must be executable.
			return "", fmt.Errorf("write browser shim %s: %w", name, err)
		}
	}

	return dir, nil
}
