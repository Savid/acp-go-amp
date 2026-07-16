package amp

import (
	"errors"
	"os"
	"os/exec"
)

// ErrProcessTreeNotQuiescent means the adapter could not prove that every
// descendant in a native command's containment boundary exited. Callers that
// account native roots must retain the permit when this error is present.
var ErrProcessTreeNotQuiescent = errors.New("amp process tree quiescence not proven")

// processTreeCommand owns the platform launch wrapper and any parent-side
// descriptors that establish its containment boundary. Linux uses an embedded
// subreaper supervisor, Windows uses a Job Object, and platforms without an
// unescapable turn boundary reject the launch.
type processTreeCommand struct {
	cmd       *exec.Cmd
	inherited []*os.File
	control   *os.File
	ready     *os.File
}

func (c *processTreeCommand) releaseInherited() {
	for _, file := range c.inherited {
		_ = file.Close()
	}

	c.inherited = nil
}

func (c *processTreeCommand) close() {
	if c == nil {
		return
	}

	c.releaseInherited()

	if c.control != nil {
		_ = c.control.Close()
		c.control = nil
	}

	if c.ready != nil {
		_ = c.ready.Close()
		c.ready = nil
	}
}

// ProcessTreeQuiescent reports whether err proves no native descendant remains.
// Ordinary command failures are quiescent; only the containment sentinel keeps
// the managed-root permit armed.
func ProcessTreeQuiescent(err error) bool {
	return !errors.Is(err, ErrProcessTreeNotQuiescent)
}
