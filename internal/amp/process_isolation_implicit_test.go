//go:build unix

package amp

import (
	"os/exec"
	"strings"
	"testing"
)

func TestProcessIsolationOmissionAllowsOrdinaryUser(t *testing.T) {
	originalEnviron := implicitIsolationEnviron
	originalUID, originalGID := implicitIsolationUID, implicitIsolationGID
	t.Cleanup(func() {
		implicitIsolationEnviron = originalEnviron
		implicitIsolationUID, implicitIsolationGID = originalUID, originalGID
	})

	implicitIsolationEnviron = func() []string {
		return []string{
			"PATH=/usr/bin",
			"AMP_API_KEY=ambient-key",
			"GOTRACEBACK=crash",
			"AMP_DISABLE_SECRET_REDACTION=1",
			adapterSupervisorModeEnv + "=guardian",
			"=empty-key",
			"malformed-entry",
		}
	}

	for _, identity := range []struct {
		name     string
		uid, gid int
		wantUID  uint32
		wantGID  uint32
	}{
		{name: "non-root", uid: 1000, gid: 1000, wantUID: 1000, wantGID: 1000},
		{name: "root", uid: 0, gid: 0, wantUID: 0, wantGID: 0},
	} {
		t.Run(identity.name, func(t *testing.T) {
			implicitIsolationUID = func() int { return identity.uid }
			implicitIsolationGID = func() int { return identity.gid }

			isolation := ImplicitProcessIsolation()
			if !isolation.Implicit || isolation.UID != identity.wantUID || isolation.GID != identity.wantGID {
				t.Fatalf("implicit capture = %#v", isolation)
			}
			if isolation.BaseEnvironment["PATH"] != "/usr/bin" || isolation.BaseEnvironment["AMP_API_KEY"] != "ambient-key" {
				t.Fatalf("implicit base environment = %#v", isolation.BaseEnvironment)
			}
			for _, scrubbed := range []string{"GOTRACEBACK", "AMP_DISABLE_SECRET_REDACTION", adapterSupervisorModeEnv, ""} {
				if _, ok := isolation.BaseEnvironment[scrubbed]; ok {
					t.Fatalf("scrubbed key %q survived the capture", scrubbed)
				}
			}
			if len(isolation.BaseEnvironment) != 2 {
				t.Fatalf("implicit base environment = %#v", isolation.BaseEnvironment)
			}
		})
	}

	if implicitIdentityValue(-1) != ^uint32(0) {
		t.Fatal("unrepresentable identity did not fail closed")
	}
}

func TestProcessIsolationOmissionAllowsRoot(t *testing.T) {
	originalGOOS := processIsolationGOOS
	originalGeteuid, originalGetegid := processIsolationGeteuid, processIsolationGetegid
	t.Cleanup(func() {
		processIsolationGOOS = originalGOOS
		processIsolationGeteuid, processIsolationGetegid = originalGeteuid, originalGetegid
	})

	processIsolationGOOS = processIsolationLinux

	for _, identity := range []struct {
		name     string
		uid, gid int
	}{
		{name: "non-root", uid: 1000, gid: 1000},
		{name: "root", uid: 0, gid: 0},
	} {
		t.Run(identity.name, func(t *testing.T) {
			processIsolationGeteuid = func() int { return identity.uid }
			processIsolationGetegid = func() int { return identity.gid }

			implicit := &ProcessIsolation{
				UID: uint32(identity.uid), GID: uint32(identity.gid),
				BaseEnvironment: map[string]string{}, Implicit: true,
			}
			if err := validateProcessIsolation(implicit); err != nil {
				t.Fatalf("implicit current identity rejected: %v", err)
			}
			if !sharedProcessIdentity(implicit) {
				t.Fatal("implicit policy did not mark the shared-identity supervisor shape")
			}

			diverged := &ProcessIsolation{
				UID: implicit.UID + 1, GID: implicit.GID,
				BaseEnvironment: map[string]string{}, Implicit: true,
			}
			if err := validateProcessIsolation(diverged); err == nil ||
				!strings.Contains(err.Error(), "process runs as") {
				t.Fatalf("diverged implicit identity accepted: %v", err)
			}
		})
	}

	authority := &ProcessIsolation{
		UID: 1000, GID: 1000, BaseEnvironment: map[string]string{},
		Implicit: true, StandaloneOwnerID: "owner",
	}
	if err := validateProcessIsolation(authority); err == nil ||
		!strings.Contains(err.Error(), "forbids identity capabilities") {
		t.Fatalf("implicit policy with standalone owner accepted: %v", err)
	}
}

// TestApplyProcessIsolationImplicitAppliesNoCredential proves the implicit
// launch never attaches a credential change: the command it prepared stays
// exactly the command it was handed.
func TestApplyProcessIsolationImplicitAppliesNoCredential(t *testing.T) {
	implicit := &ProcessIsolation{
		UID:             uint32(processIsolationGeteuid()),
		GID:             uint32(processIsolationGetegid()),
		BaseEnvironment: map[string]string{},
		Implicit:        true,
	}

	cmd := &exec.Cmd{}
	if err := applyProcessIsolation(cmd, implicit); err != nil {
		t.Fatalf("apply implicit isolation: %v", err)
	}

	if cmd.SysProcAttr != nil {
		t.Fatalf("implicit launch attached process attributes: %#v", cmd.SysProcAttr)
	}
}

// TestSupervisorEnvironmentCarriesTheImplicitMarker proves the private
// supervisor handshake states whether the launch is the implicit
// current-identity one, and that the inherited verification then proves the
// identity without demanding the empty supplementary groups only an explicit
// credential change can produce.
func TestSupervisorEnvironmentCarriesTheImplicitMarker(t *testing.T) {
	implicit := &ProcessIsolation{
		UID: uint32(processIsolationGeteuid()), GID: uint32(processIsolationGetegid()),
		BaseEnvironment: map[string]string{}, Implicit: true,
	}

	env, err := supervisorEnvironment([]string{"KEEP=1", envIsolationImplicit + "=stale"}, implicit, "test-mode")
	if err != nil {
		t.Fatal(err)
	}

	values := environmentMap(env)
	if values[envIsolationImplicit] != "true" || values["KEEP"] != "1" {
		t.Fatalf("supervisor environment = %#v", values)
	}

	t.Setenv(envIsolationUID, values[envIsolationUID])
	t.Setenv(envIsolationGID, values[envIsolationGID])
	t.Setenv(envIsolationTest, "false")
	t.Setenv(envIsolationImplicit, "true")

	if err := verifyInheritedProcessIsolation(); err != nil {
		t.Fatalf("inherited implicit identity rejected: %v", err)
	}

	t.Setenv(envIsolationUID, "4294967294")

	if err := verifyInheritedProcessIsolation(); err == nil {
		t.Fatal("inherited implicit identity mismatch accepted")
	}
}
