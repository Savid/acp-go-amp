//go:build linux

package amp

// descendantCount enumerates the contained descendant set of a supervised
// subreaper tree. That is the one configuration where every descendant is
// reparented onto a root this adapter owns, so the set is exactly enumerable and
// an empty answer is a vacancy proof rather than an assumption. Ordinary
// same-identity execution owns no such root and reports no inventory.
func (t *processTree) descendantCount() (int, bool) {
	if t == nil || !t.supervised {
		return 0, false
	}

	descendants, err := turnSupervisorDescendants(t.pgid)
	if err != nil {
		return 0, false
	}

	return len(descendants), true
}
