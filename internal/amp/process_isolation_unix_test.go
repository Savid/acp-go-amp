//go:build unix

package amp

import (
	"errors"
	"os/exec"
	"syscall"
	"testing"
)

func TestProcessIsolationUnixVerificationBranches(t *testing.T) {
	originalUID, originalGID, originalGroups := processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups
	t.Cleanup(func() {
		processIsolationGeteuid, processIsolationGetegid, processIsolationGetgroups = originalUID, originalGID, originalGroups
	})

	processIsolationGeteuid = func() int { return 11 }
	processIsolationGetegid = func() int { return 22 }
	processIsolationGetgroups = func() ([]int, error) { return nil, nil }
	policy := &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}}
	if err := verifyProcessIsolation(policy); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/usr/bin/true")
	if err := applyProcessIsolation(cmd, policy); err != nil {
		t.Fatal(err)
	}
	if cmd.SysProcAttr != nil && cmd.SysProcAttr.Credential != nil {
		t.Fatal("already isolated identity received a redundant credential")
	}

	processIsolationGetgroups = func() ([]int, error) { return nil, errors.New("groups") }
	if err := verifyProcessIsolation(policy); err == nil {
		t.Fatal("group lookup failure accepted")
	}
	processIsolationGetgroups = func() ([]int, error) { return []int{22}, nil }
	if err := verifyProcessIsolation(policy); err == nil {
		t.Fatal("primary GID repeated as a supplementary group was accepted")
	}
	processIsolationGeteuid = func() int { return 12 }
	if err := verifyProcessIsolation(policy); err == nil {
		t.Fatal("wrong identity accepted")
	}
	if err := verifyProcessIsolation(nil); err == nil {
		t.Fatal("nil policy verified")
	}
	if err := applyProcessIsolation(nil, policy); err == nil {
		t.Fatal("nil command accepted")
	}
	if err := applyProcessIsolation(exec.Command("/usr/bin/true"), nil); err == nil {
		t.Fatal("nil policy applied")
	}
	if err := applyProcessIsolation(nil, &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}); err == nil {
		t.Fatal("test-only policy bypassed nil command validation")
	}
	if err := applyProcessIsolation(exec.Command("/usr/bin/true"), &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}); err != nil {
		t.Fatal(err)
	}

	cmd = exec.Command("/usr/bin/true")
	if err := applyProcessIsolation(cmd, policy); err != nil {
		t.Fatal(err)
	}
	if credential := cmd.SysProcAttr.Credential; credential == nil || credential.Uid != policy.UID || credential.Gid != policy.GID || len(credential.Groups) != 0 {
		t.Fatalf("new process attributes carried the wrong credential: %#v", cmd.SysProcAttr)
	}

	cmd = exec.Command("/usr/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := applyProcessIsolation(cmd, policy); err != nil {
		t.Fatal(err)
	}
	credential := cmd.SysProcAttr.Credential
	if !cmd.SysProcAttr.Setpgid || credential == nil || credential.Uid != policy.UID || credential.Gid != policy.GID || len(credential.Groups) != 0 {
		t.Fatalf("credential did not preserve process attributes: %#v", cmd.SysProcAttr)
	}

	t.Setenv(envIsolationUID, "invalid")
	t.Setenv(envIsolationGID, "22")
	if err := verifyInheritedProcessIsolation(); err == nil {
		t.Fatal("invalid inherited identity accepted")
	}
	t.Setenv(envIsolationUID, "11")
	t.Setenv(envIsolationTest, "true")
	if err := verifyInheritedProcessIsolation(); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envIsolationTest, "false")
	if err := verifyInheritedProcessIsolation(); err == nil {
		t.Fatal("mismatched inherited identity accepted")
	}
}
