//go:build linux

package amp

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// realTreeSettleTimeout bounds how long a fixture waits for the kernel to
// publish a state it drove. Every wait here polls /proc, which is the same
// source the enumeration reads.
const realTreeSettleTimeout = 10 * time.Second

// startRealTree launches a real process group running script and returns its
// root pid. Nothing about the enumeration is stubbed in these fixtures: the
// advertised process-containment class reads /proc, so its evidence has to come
// from /proc too.
func startRealTree(t *testing.T, script string) int {
	t.Helper()

	cmd := exec.Command("/bin/sh", "-c", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		t.Skipf("no usable /bin/sh in this environment: %v", err)
	}

	root := cmd.Process.Pid

	t.Cleanup(func() {
		_ = syscall.Kill(-root, syscall.SIGKILL)
		_ = cmd.Wait()
	})

	return root
}

// awaitDescendants polls the real enumeration until it reports at least want
// members.
func awaitDescendants(t *testing.T, root, want int) []linuxProcessIdentity {
	t.Helper()

	deadline := time.Now().Add(realTreeSettleTimeout)

	for {
		descendants, err := linuxDescendants(root)
		if err != nil {
			t.Fatalf("enumerate descendants of %d: %v", root, err)
		}

		if len(descendants) >= want {
			return descendants
		}

		if time.Now().After(deadline) {
			t.Fatalf("descendants of %d settled at %d, want at least %d", root, len(descendants), want)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func processAlive(pid int) bool {
	_, err := os.Stat(filepath.Join(turnSupervisorProcRoot, strconv.Itoa(pid)))

	return err == nil
}

// TestDescendantEnumerationWalksARealContainedTree drives the enumeration the
// advertised process-containment class actually reads against a real tree. The
// walk descends the whole subtree rather than the root's direct children, and
// the inventory a live tree produces is not vacancy.
func TestDescendantEnumerationWalksARealContainedTree(t *testing.T) {
	root := startRealTree(t, "/bin/sh -c 'sleep 300 & wait' & sleep 300")

	descendants := awaitDescendants(t, root, 2)

	inside := make(map[int]bool, len(descendants))
	for _, descendant := range descendants {
		inside[descendant.pid] = true
	}

	deep := false

	for _, descendant := range descendants {
		if inside[descendant.parentPID] {
			deep = true
		}
	}

	if !deep {
		t.Fatalf("enumeration %v found no descendant below the root's own children", descendants)
	}

	count, available := (&processTree{pgid: root, supervised: true}).descendantCount()
	if !available || count < 2 {
		t.Fatalf("live inventory = (%d, %t), want an available count of at least 2", count, available)
	}

	proof := ContainmentProof{Root: root, Descendants: count, Proven: available}
	if proof.Vacant() {
		t.Fatal("a tree with live descendants is not vacant")
	}
}

// TestDescendantEnumerationExcludesARealEscapee drives a real escapee: a
// process whose intermediate parent exits, so the kernel reparents it away from
// the root before the root is gone. The walk from that root can never see it,
// which is exactly why the vacancy claim rests on the supervisor's own reap
// discipline and treats this count as confirmation rather than as the proof.
func TestDescendantEnumerationExcludesARealEscapee(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "escapee.pid")
	root := startRealTree(t, "( sleep 300 & echo $! > "+marker+" ) & sleep 300")

	escapee := 0
	deadline := time.Now().Add(realTreeSettleTimeout)

	for escapee == 0 {
		if raw, err := os.ReadFile(marker); err == nil {
			if pid, convErr := strconv.Atoi(string(trimLine(raw))); convErr == nil {
				escapee = pid
			}
		}

		if escapee == 0 && time.Now().After(deadline) {
			t.Fatal("the escapee never announced itself")
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Cleanup(func() { _ = syscall.Kill(escapee, syscall.SIGKILL) })

	// The escapee is reparented once its intermediate shell exits, which is what
	// takes it out of the root's subtree.
	for time.Now().Before(deadline) {
		identity, err := readLinuxProcessIdentity(escapee)
		if err != nil {
			t.Fatalf("read escapee identity: %v", err)
		}

		if identity.parentPID != 0 && !inSubtree(t, root, identity.parentPID) {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	if !processAlive(escapee) {
		t.Fatal("the escapee died before it could escape")
	}

	descendants, err := linuxDescendants(root)
	if err != nil {
		t.Fatalf("enumerate descendants of %d: %v", root, err)
	}

	for _, descendant := range descendants {
		if descendant.pid == escapee {
			t.Fatalf("the enumeration claimed a reparented escapee %d as contained", escapee)
		}
	}
}

// TestDescendantEnumerationReportsARealVacantTree drives the enumeration to the
// answer that backs a positive quiescence fact: a tree whose members are all
// gone reports an empty contained set.
func TestDescendantEnumerationReportsARealVacantTree(t *testing.T) {
	root := startRealTree(t, "/bin/sh -c 'sleep 300 & wait' & sleep 300")

	awaitDescendants(t, root, 2)

	if err := syscall.Kill(-root, syscall.SIGKILL); err != nil {
		t.Fatalf("terminate the contained group: %v", err)
	}

	deadline := time.Now().Add(realTreeSettleTimeout)

	for {
		count, available := (&processTree{pgid: root, supervised: true}).descendantCount()
		if available && count == 0 {
			proof := ContainmentProof{Root: root, Descendants: count, Proven: available}
			if !proof.Vacant() {
				t.Fatal("an empty enumerated set on a supervised root is vacancy")
			}

			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("terminated tree settled at (%d, %t), want an empty available set", count, available)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// inSubtree reports whether pid is inside root's enumerated subtree.
func inSubtree(t *testing.T, root, pid int) bool {
	t.Helper()

	descendants, err := linuxDescendants(root)
	if err != nil {
		t.Fatalf("enumerate descendants of %d: %v", root, err)
	}

	for _, descendant := range descendants {
		if descendant.pid == pid {
			return true
		}
	}

	return false
}

// trimLine drops the trailing newline a shell writes with the pid it announced.
func trimLine(raw []byte) []byte {
	for len(raw) > 0 && (raw[len(raw)-1] == '\n' || raw[len(raw)-1] == '\r') {
		raw = raw[:len(raw)-1]
	}

	return raw
}
