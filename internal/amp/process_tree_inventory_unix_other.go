//go:build unix && !linux

package amp

// descendantCount reports no inventory. Every whole-tree proof in this adapter is
// Linux-conditional: a platform with no subreaper root cannot enumerate what a
// native process left behind, and reporting an empty set it never enumerated
// would be exactly the guess the proof classes exclude.
func (*processTree) descendantCount() (int, bool) { return 0, false }
