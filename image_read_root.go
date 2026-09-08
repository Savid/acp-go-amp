package ampacp

import (
	"os"
	"path/filepath"
	"sync"
)

// managedHandoff owns one read descriptor outside the complete scratch domain.
// Initialization precedes every preparation; no later read resolves its root.
// The host keeps these directory identities disjoint for the Agent's lifetime,
// including mount aliases and replacement of native roots or their ancestors.
type managedHandoff struct {
	mu          sync.Mutex
	initialized bool
	scratch     string
	root        *os.Root
	failure     *handoffError
}

func disjointDirectories(left, right string) (bool, error) {
	leftInfo, err := os.Stat(left)
	if err != nil {
		return false, err
	}

	rightInfo, err := os.Stat(right)
	if err != nil {
		return false, err
	}

	if !leftInfo.IsDir() || !rightInfo.IsDir() {
		return false, nil
	}

	for _, pair := range []struct {
		path  string
		other os.FileInfo
	}{{left, rightInfo}, {right, leftInfo}} {
		for path := pair.path; ; path = filepath.Dir(path) {
			info, statErr := os.Stat(path)
			if statErr != nil {
				return false, statErr
			}

			if os.SameFile(info, pair.other) {
				return false, nil
			}

			if filepath.Dir(path) == path {
				break
			}
		}
	}

	return true, nil
}

func (a *Agent) managedHandoffRoot() (*os.Root, *handoffError) {
	a.handoff.mu.Lock()
	defer a.handoff.mu.Unlock()

	if a.handoff.root == nil && a.handoff.failure == nil {
		return nil, &handoffError{value: imageErrorPathNotAllowed, message: handoffRootUnopenableMessage}
	}

	return a.handoff.root, a.handoff.failure
}

func (a *Agent) closeManagedHandoff() error {
	a.handoff.mu.Lock()
	defer a.handoff.mu.Unlock()

	if a.handoff.root == nil {
		return nil
	}

	err := a.handoff.root.Close()
	a.handoff.root = nil

	return err
}
