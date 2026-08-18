//go:build linux

package amp

// descendantCount enumerates the contained descendant set of a supervised
// subreaper tree. That is the one configuration where every descendant is
// reparented onto a root this adapter owns, so the set is exactly enumerable.
//
// The vacancy the count reports is the subreaper's own: that root exits only
// once it has reaped every descendant it adopted, so an empty answer after the
// boundary completed confirms the discipline the root already enforced rather
// than substituting for it. Ordinary same-identity execution owns no such root
// and reports no inventory at all.
func (t *processTree) descendantCount() (int, bool) {
	if !t.supervised {
		return 0, false
	}

	descendants, err := turnSupervisorDescendants(t.pgid)
	if err != nil {
		return 0, false
	}

	return len(descendants), true
}
