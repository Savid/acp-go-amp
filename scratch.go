package ampacp

import (
	"fmt"
	"os"
)

// scratchDir is the sole scratch accessor. It creates one ephemeral directory
// for one purpose under the configured scratch parent, which is the system
// temp directory when unset, creating that parent 0700 when it is missing.
// Names carry the acp-go-amp-<purpose>- prefix so a host can sweep orphans.
func (a *Agent) scratchDir(purpose string) (string, error) {
	parent := a.options.ScratchDir
	if parent == "" {
		parent = os.TempDir()
	}

	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("create scratch parent: %w", err)
	}

	return os.MkdirTemp(parent, "acp-go-amp-"+purpose+"-")
}
