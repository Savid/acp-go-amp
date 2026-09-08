package ampacp

import (
	"fmt"
	"os"
	"path/filepath"
)

// scratchParent resolves the parent directory for all ephemeral on-disk
// materialization: dir when set, else the system temp directory. This is the
// only place in the module that consults the system temp directory.
func scratchParent(dir string) string {
	if dir != "" {
		return dir
	}

	return os.TempDir()
}

// ensureScratchParent resolves the scratch parent and creates it 0700 when
// missing.
func ensureScratchParent(dir string) (string, error) {
	parent := scratchParent(dir)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", fmt.Errorf("create scratch parent: %w", err)
	}

	return parent, nil
}

func (a *Agent) ensureScratchParent() (string, error) {
	if a.options.hostAuthoritySupplied && a.options.InputHandoffRoot != "" {
		return a.ensureManagedScratchParent()
	}

	return ensureScratchParent(a.options.ScratchDir)
}

func (a *Agent) ensureManagedScratchParent() (string, error) {
	m := &a.handoff
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.initialized {
		return m.scratch, nil
	}

	parent, err := ensureScratchParent(a.options.ScratchDir)
	if err != nil {
		return "", err
	}

	parent, err = filepath.EvalSymlinks(parent)
	if err != nil {
		return "", err
	}

	parent, err = filepath.Abs(parent)
	if err != nil {
		return "", err
	}

	m.scratch, m.initialized = parent, true
	m.failure = &handoffError{value: imageErrorPathNotAllowed, message: handoffRootUnopenableMessage}

	readPath, err := filepath.EvalSymlinks(a.options.InputHandoffRoot)
	if err != nil {
		return parent, nil
	}
	// Compare directory identities along both ancestor chains, not path spelling.
	disjoint, err := disjointDirectories(parent, readPath)
	if err != nil || !disjoint {
		return parent, nil
	}

	root, err := os.OpenRoot(readPath)
	if err != nil {
		return parent, nil
	}

	m.root, m.failure = root, nil

	return parent, nil
}
