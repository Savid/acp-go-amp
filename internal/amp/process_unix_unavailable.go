//go:build freebsd || openbsd

package amp

import "time"

func (t *processTree) terminateAndWait(time.Duration) error {
	return t.finish(ErrProcessContainmentIncomplete)
}

func abortUnvalidatedProcessTree(t *processTree) error {
	return t.finish(ErrProcessContainmentIncomplete)
}

func handleDarwinFastExit(launch *processTreeCommand, tree *processTree, beginWait func()) error {
	launch.abortStartGate()
	beginWait()

	return abortUnvalidatedProcessTree(tree)
}
