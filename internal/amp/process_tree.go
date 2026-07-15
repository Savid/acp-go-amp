package amp

import "errors"

// ErrProcessTreeNotQuiescent means the adapter could not prove that every
// descendant in a native command's containment boundary exited. Callers that
// account native roots must retain the permit when this error is present.
var ErrProcessTreeNotQuiescent = errors.New("amp process tree quiescence not proven")

// ProcessTreeQuiescent reports whether err proves no native descendant remains.
// Ordinary command failures are quiescent; only the containment sentinel keeps
// the managed-root permit armed.
func ProcessTreeQuiescent(err error) bool {
	return !errors.Is(err, ErrProcessTreeNotQuiescent)
}
