package amp

// ContainmentProof is the evidence one contained native root's exit produced. It
// is an observation, never an inference: a boundary that cannot enumerate its
// descendant set reports Proven false rather than an empty one, because process
// silence and a completed wait are not vacancy.
type ContainmentProof struct {
	// Root is the contained native root the boundary covered.
	Root int
	// Descendants is the size of the contained descendant set the boundary
	// enumerated.
	Descendants int
	// Proven reports that the descendant set was actually enumerated.
	Proven bool
}

// Vacant reports a proven empty descendant set: the whole-tree vacancy the
// process-containment proof class means.
func (p ContainmentProof) Vacant() bool { return p.Proven && p.Descendants == 0 }

// ContainmentProof reports what this turn's containment boundary proved about the
// native tree it owned. It is meaningful only once Close has completed that
// boundary, which is why the prompt path awaits the boundary before reading it.
func (t *Turn) ContainmentProof() ContainmentProof {
	if t.tree == nil {
		return ContainmentProof{}
	}

	count, proven := processTreeDescendantCount(t.tree)

	return ContainmentProof{Root: t.tree.process.Pid, Descendants: count, Proven: proven}
}
