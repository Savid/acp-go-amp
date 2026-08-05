//go:build linux

package amp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type supervisorTestSignal string

func (s supervisorTestSignal) String() string { return string(s) }
func (supervisorTestSignal) Signal()          {}

type supervisorWriteSeeker struct {
	writeErr error
	seekErr  error
}

func (w supervisorWriteSeeker) Write(value []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}

	return len(value), nil
}

func (w supervisorWriteSeeker) Seek(int64, int) (int64, error) {
	return 0, w.seekErr
}

func restoreTurnSupervisorSeams(t *testing.T) {
	t.Helper()
	executable := turnSupervisorExecutable
	memfd := turnSupervisorMemfd
	pipe := turnSupervisorPipe
	exit := turnSupervisorExit
	notify := turnSupervisorSignalNotify
	stop := turnSupervisorSignalStop
	input := turnSupervisorInput
	enable := turnSupervisorEnable
	command := turnSupervisorCommand
	contain := turnSupervisorContain
	processID := turnSupervisorProcessID
	signalGroup := turnSupervisorSignalGroup
	writeConfig := turnSupervisorWriteConfig
	descendants := turnSupervisorDescendants
	identity := turnSupervisorIdentity
	signalPID := turnSupervisorSignalPID
	wait4 := turnSupervisorWait4
	sleep := turnSupervisorSleep
	procRoot := turnSupervisorProcRoot
	run := turnSupervisorRun
	runLiveness := turnSupervisorRunLiveness
	openFile := turnSupervisorOpenFile
	fcntl := turnSupervisorFcntl
	prctl := turnSupervisorPrctl
	setrlimit := turnSupervisorSetrlimit
	acquireStandalone := turnSupervisorAcquireStandalone
	sealConfig := turnSupervisorSealConfig
	effectiveUID := turnSupervisorEffectiveUID
	poll := turnSupervisorPoll
	syscallKillOriginal := syscallKill
	turnSupervisorEffectiveUID = func() int { return 0 }
	t.Cleanup(func() {
		turnSupervisorExecutable = executable
		turnSupervisorMemfd = memfd
		turnSupervisorPipe = pipe
		turnSupervisorExit = exit
		turnSupervisorSignalNotify = notify
		turnSupervisorSignalStop = stop
		turnSupervisorInput = input
		turnSupervisorEnable = enable
		turnSupervisorCommand = command
		turnSupervisorContain = contain
		turnSupervisorProcessID = processID
		turnSupervisorSignalGroup = signalGroup
		turnSupervisorWriteConfig = writeConfig
		turnSupervisorDescendants = descendants
		turnSupervisorIdentity = identity
		turnSupervisorSignalPID = signalPID
		turnSupervisorWait4 = wait4
		turnSupervisorSleep = sleep
		turnSupervisorProcRoot = procRoot
		turnSupervisorRun = run
		turnSupervisorRunLiveness = runLiveness
		turnSupervisorOpenFile = openFile
		turnSupervisorFcntl = fcntl
		turnSupervisorPrctl = prctl
		turnSupervisorSetrlimit = setrlimit
		turnSupervisorAcquireStandalone = acquireStandalone
		turnSupervisorSealConfig = sealConfig
		turnSupervisorEffectiveUID = effectiveUID
		turnSupervisorPoll = poll
		syscallKill = syscallKillOriginal
	})
}

func TestTurnSupervisorRequiresDistinctTrustedRoot(t *testing.T) {
	restoreTurnSupervisorSeams(t)

	turnSupervisorEffectiveUID = func() int { return 1000 }
	if err := validateTurnSupervisorIdentity(supervisorTestIsolation()); err == nil || !strings.Contains(err.Error(), "trusted root") {
		t.Fatalf("non-root identity validation = %v", err)
	}
	if _, err := prepareProcessTreeCommand(exec.Command("/bin/true"), processLaunchOptions{Isolation: supervisorTestIsolation()}); err == nil || !strings.Contains(err.Error(), "trusted root") {
		t.Fatalf("non-root parent preparation = %v", err)
	}
	config := encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/true", Args: []string{"/bin/true"}, Isolation: *supervisorTestIsolation()})
	if err := runTurnSupervisor(config, strings.NewReader(""), io.Discard); err == nil || !strings.Contains(err.Error(), "trusted root") {
		t.Fatalf("non-root supervisor bootstrap = %v", err)
	}

	turnSupervisorEffectiveUID = func() int { return 0 }
	isolation := supervisorTestIsolation()
	isolation.UID = 0
	if err := validateTurnSupervisorIdentity(isolation); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("shared root identity validation = %v", err)
	}

	isolation.UID = 11
	if err := validateTurnSupervisorIdentity(isolation); err != nil {
		t.Fatalf("distinct trusted identity validation = %v", err)
	}
}

func TestSupervisorIdentityDispositionRejectsMixedCapabilities(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	config := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"}, Isolation: *supervisorTestIsolation(), IdentityLock: true,
	}
	err := runTurnSupervisor(encodeSupervisorConfig(t, config), strings.NewReader(""), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "must be provided together") {
		t.Fatalf("mixed identity disposition = %v", err)
	}
}

func TestSupervisorIdentityDispositionRequiresExplicitAuthorityOriginAndExactStandaloneOwner(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorEffectiveUID = func() int { return 0 }
	base := turnSupervisorConfig{
		Path: "/bin/true", Args: []string{"/bin/true"}, Isolation: *supervisorTestIsolation(),
		IdentityLock: true, AuthorityDomain: true,
	}
	owner := agentStandaloneOwner{
		Version: 1, UID: base.Isolation.UID, GID: base.Isolation.GID,
		Kind: agentStandaloneOwnerKind, Provider: agentStandaloneOwnerID, OwnerID: "sealed-handoff",
		StateRoot: agentStandaloneStateRoot{Path: "/tmp/standalone-handoff", Dev: 1, Ino: 2},
	}
	validBorrowed := base
	validBorrowed.AuthorityOrigin = turnSupervisorOriginBorrowed
	if err := validateTurnSupervisorConfig(validBorrowed); err != nil {
		t.Fatalf("valid borrowed origin: %v", err)
	}
	validStandalone := base
	validStandalone.AuthorityOrigin = turnSupervisorOriginStandalone
	validStandalone.StandaloneOwner = &owner
	if err := validateTurnSupervisorConfig(validStandalone); err != nil {
		t.Fatalf("valid standalone origin: %v", err)
	}

	for name, config := range map[string]turnSupervisorConfig{
		"missing_origin":      base,
		"unknown_origin":      func() turnSupervisorConfig { value := base; value.AuthorityOrigin = "unknown"; return value }(),
		"borrowed_with_owner": func() turnSupervisorConfig { value := validBorrowed; value.StandaloneOwner = &owner; return value }(),
		"standalone_no_owner": func() turnSupervisorConfig {
			value := base
			value.AuthorityOrigin = turnSupervisorOriginStandalone
			return value
		}(),
		"standalone_wrong_uid": func() turnSupervisorConfig {
			value := validStandalone
			wrong := owner
			wrong.UID++
			value.StandaloneOwner = &wrong
			return value
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateTurnSupervisorConfig(config); err == nil {
				t.Fatal("inconsistent authority origin was accepted")
			}
		})
	}
}

func TestTurnSupervisorProductionIdentityRejectsNonRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires a non-root process")
	}

	_, err := prepareProcessTreeCommand(exec.Command("/bin/true"), processLaunchOptions{Isolation: supervisorTestIsolation()})
	if err == nil || !strings.Contains(err.Error(), "trusted root") {
		t.Fatalf("non-root production preparation = %v", err)
	}
}

func TestTurnSupervisorBootstrapBranches(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	t.Setenv(turnSupervisorModeEnv, turnSupervisorMode)

	exitCode := -1
	turnSupervisorExit = func(code int) { exitCode = code }
	turnSupervisorInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return nil, nil, nil, errors.New("input")
	}
	turnSupervisorBootstrap()
	if exitCode != 1 {
		t.Fatalf("input failure exit = %d, want 1", exitCode)
	}

	closed := make([]bool, 3)
	turnSupervisorInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return &recordingReadCloser{Reader: strings.NewReader("config"), closed: &closed[0]},
			&recordingReadCloser{Reader: strings.NewReader("control"), closed: &closed[1]},
			&recordingWriteCloser{Writer: io.Discard, closed: &closed[2]}, nil
	}
	turnSupervisorRun = func(io.Reader, io.Reader, io.Writer) error { return nil }
	turnSupervisorBootstrap()
	if exitCode != 0 || !closed[0] || !closed[1] || !closed[2] {
		t.Fatalf("successful bootstrap = exit %d, closed %v", exitCode, closed)
	}

	closed = make([]bool, 3)
	t.Setenv(turnSupervisorModeEnv, turnSupervisorLivenessMode)
	livenessCalled := false
	turnSupervisorRunLiveness = func(io.Reader, io.Reader, io.Writer) error {
		livenessCalled = true

		return nil
	}
	turnSupervisorBootstrap()
	if exitCode != 0 || !livenessCalled || !closed[0] || !closed[1] || !closed[2] {
		t.Fatalf("successful liveness bootstrap = exit %d, called %t, closed %v", exitCode, livenessCalled, closed)
	}

	t.Setenv(turnSupervisorModeEnv, "")
	exitCode = -1
	turnSupervisorBootstrap()
	if exitCode != -1 {
		t.Fatalf("disabled bootstrap exited with %d", exitCode)
	}
}

func TestInheritedTurnSupervisorInputAndEnable(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	limitSet := false
	turnSupervisorSetrlimit = func(resource int, limit *unix.Rlimit) error {
		limitSet = resource == unix.RLIMIT_CORE && limit.Cur == 0 && limit.Max == 0

		return nil
	}
	var operations []int
	turnSupervisorPrctl = func(option int, _, _, _, _ uintptr) error {
		operations = append(operations, option)

		return nil
	}
	if err := enableTurnSupervisor(); err != nil {
		t.Fatalf("enable supervisor privileges: %v", err)
	}
	if !slices.Equal(operations, []int{unix.PR_SET_CHILD_SUBREAPER, unix.PR_SET_DUMPABLE, unix.PR_SET_NO_NEW_PRIVS}) {
		t.Fatalf("supervisor privilege operations = %v", operations)
	}
	if !limitSet {
		t.Fatal("supervisor did not disable core dumps")
	}

	turnSupervisorSetrlimit = func(int, *unix.Rlimit) error { return errors.New("setrlimit") }
	if err := enableTurnSupervisor(); err == nil || !strings.Contains(err.Error(), "setrlimit") {
		t.Fatalf("core-limit failure = %v", err)
	}
	turnSupervisorSetrlimit = func(int, *unix.Rlimit) error { return nil }

	turnSupervisorPrctl = func(int, uintptr, uintptr, uintptr, uintptr) error { return errors.New("subreaper") }
	if err := enableTurnSupervisor(); err == nil || err.Error() != "subreaper" {
		t.Fatalf("subreaper failure = %v", err)
	}
	call := 0
	turnSupervisorPrctl = func(int, uintptr, uintptr, uintptr, uintptr) error {
		call++
		if call == 3 {
			return errors.New("no-new-privs")
		}

		return nil
	}
	if err := enableTurnSupervisor(); err == nil || err.Error() != "no-new-privs" {
		t.Fatalf("no-new-privs failure = %v", err)
	}

	turnSupervisorOpenFile = func(uintptr, string) *os.File { return nil }
	if _, _, _, err := inheritedTurnSupervisorInput(); err == nil {
		t.Fatal("missing inherited descriptors succeeded")
	}

	files := make([]*os.File, 0, 3)
	writes := make([]*os.File, 0, 3)
	for range 3 {
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, read)
		writes = append(writes, write)
	}
	t.Cleanup(func() {
		for _, file := range writes {
			_ = file.Close()
		}
	})
	next := 0
	turnSupervisorOpenFile = func(uintptr, string) *os.File {
		file := files[next]
		next++

		return file
	}
	closeOnExec := 0
	turnSupervisorFcntl = func(uintptr, int, int) (int, error) { closeOnExec++; return 0, nil }
	config, control, ready, err := inheritedTurnSupervisorInput()
	if err != nil {
		t.Fatalf("inherited input: %v", err)
	}
	_ = config.Close()
	_ = control.Close()
	_ = ready.Close()
	if closeOnExec != 6 {
		t.Fatalf("close-on-exec calls = %d", closeOnExec)
	}
}

func TestTurnSupervisorNativeChildHasSecurityLimits(t *testing.T) {
	const (
		phaseEnv  = "ACP_GO_AMP_TEST_NO_NEW_PRIVS_PHASE"
		statusEnv = "ACP_GO_AMP_TEST_NO_NEW_PRIVS_STATUS"
	)
	if os.Getenv(phaseEnv) == "child" {
		restoreTurnSupervisorSeams(t)
		turnSupervisorAcquireStandalone = func(uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal) (*agentStandaloneIdentity, error) {
			return &agentStandaloneIdentity{identity: &agentIdentityLock{}, authority: &agentIdentityLock{}}, nil
		}
		status := os.Getenv(statusEnv)
		config := encodeSupervisorConfig(t, turnSupervisorConfig{
			Path: "/bin/sh",
			Args: []string{"sh", "-c", `nnp=$(awk '$1 == "NoNewPrivs:" { print $2 }' /proc/self/status); printf '%s %s\n' "$nnp" "$(ulimit -c)" > "$1"`, "limits", status},
			Env:  []string{"PATH=/usr/bin:/bin"},
		})
		controlRead, controlWrite := io.Pipe()
		defer controlRead.Close()
		defer controlWrite.Close()

		if err := runTurnSupervisor(config, controlRead, io.Discard); err != nil {
			t.Fatalf("run native proof: %v", err)
		}

		return
	}

	status := filepath.Join(t.TempDir(), "no-new-privileges")
	proof := exec.Command(os.Args[0], "-test.run=^TestTurnSupervisorNativeChildHasSecurityLimits$")
	proof.Env = append(os.Environ(), phaseEnv+"=child", statusEnv+"="+status)
	if output, err := proof.CombinedOutput(); err != nil {
		t.Fatalf("native proof process: %v\n%s", err, output)
	}
	if value, err := os.ReadFile(status); err != nil || string(value) != "1 0\n" {
		t.Fatalf("native security limits = %q, %v", value, err)
	}
}

func TestProcessIsolationActualAmpTrustedSupervisorIdentityGroupsAmbientAndContainment(t *testing.T) {
	const (
		phaseEnv     = "ACP_GO_AMP_TEST_TRUSTED_SUPERVISOR_PHASE"
		stateRootEnv = "ACP_GO_AMP_TEST_STANDALONE_STATE_ROOT"
	)
	if os.Geteuid() != 0 {
		t.Skip("trusted supervisor credential boundary requires root")
	}

	root := os.Getenv("ACP_GO_AMP_TEST_ROOT")
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "acp-go-amp-trusted-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(root) })
	}
	statusRoot := filepath.Join(root, "native")
	if os.Getenv(phaseEnv) != "child" {
		if err := os.Chmod(root, 0o711); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(statusRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(statusRoot, 65534, 65534); err != nil {
			t.Fatal(err)
		}
	}
	status := filepath.Join(statusRoot, "status")
	daemon := filepath.Join(statusRoot, "daemon.pid")

	if os.Getenv(phaseEnv) == "child" {
		script := `supervisor=$PPID
if kill -STOP "$supervisor" 2>/dev/null; then echo stop=allowed; else echo stop=blocked; fi > "$1"
if printf 'forged\n' > "/proc/$supervisor/fd/$3" 2>/dev/null; then echo forge=allowed; else echo forge=blocked; fi >> "$1"
groups=$(sed -n 's/^Groups:[[:space:]]*//p' "/proc/$$/status")
if [ -z "$groups" ]; then echo groups=empty; else echo groups="$groups"; fi >> "$1"
echo uid=$(id -u) >> "$1"
echo gid=$(id -g) >> "$1"
if [ "${ACP_GO_AMP_TEST_ACTUAL_AMBIENT+x}" = x ]; then echo ambient=leaked; else echo ambient=scrubbed; fi >> "$1"
authorityfds=none
for fd in /proc/self/fd/*; do
  target=$(readlink "$fd" 2>/dev/null || true)
  case "$target" in "$ACP_GO_AMP_TEST_AUTHORITY"*) authorityfds=leaked;; esac
done
echo authorityfds=$authorityfds >> "$1"
setsid sh -c 'trap "" INT TERM; while :; do sleep 30; done' & echo $! > "$2"
if kill -KILL "$supervisor" 2>/dev/null; then echo kill=allowed; else echo kill=blocked; fi >> "$1"`
		config := turnSupervisorConfig{
			Path: "/bin/sh",
			Args: []string{"sh", "-c", script, "probe", status, daemon, "8"},
			Env: []string{
				"PATH=/usr/bin:/bin",
				"ACP_GO_AMP_TEST_AUTHORITY=" + filepath.Join(root, "acp-go", "agent-identities"),
			},
			Isolation: ProcessIsolation{
				UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}, TestOnlyIdentityLockRoot: root,
				StandaloneOwnerID: "trusted-supervisor-e2e", StandaloneStateRoot: os.Getenv(stateRootEnv),
			},
		}
		native := exec.Command(config.Path, config.Args[1:]...)
		native.Args = append([]string(nil), config.Args...)
		native.Env = append([]string(nil), config.Env...)
		native.Stdout = os.Stdout
		native.Stderr = os.Stderr
		launch, err := prepareProcessTreeCommand(native, processLaunchOptions{Isolation: &config.Isolation})
		if err != nil {
			t.Fatalf("prepare production trusted supervisor: %v", err)
		}
		tree, err := startProcessTree(launch)
		if err != nil {
			t.Fatalf("start production trusted supervisor: %v", err)
		}
		waitCtx, cancelWait := context.WithTimeout(t.Context(), 10*time.Second)
		waitErr, completed := tree.waiter.await(waitCtx)
		cancelWait()
		if !completed {
			t.Fatal("production trusted supervisor did not exit")
		}
		containmentErr := processTreeTerminateAndWait(tree, commandWaitTimeout)
		if err = errors.Join(waitErr, containmentErr); err != nil {
			t.Fatalf("settle production trusted supervisor: %v", err)
		}
		return
	}

	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	stateRoot := createAgentStandaloneProtectedStateRoot(t, 65534, 65534)
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessIsolationActualAmpTrustedSupervisorIdentityGroupsAmbientAndContainment$")
	child.Env = append(os.Environ(), phaseEnv+"=child", "ACP_GO_AMP_TEST_ACTUAL_AMBIENT=secret")
	child.Env = append(child.Env, "ACP_GO_AMP_TEST_ROOT="+root, stateRootEnv+"="+stateRoot)
	child.Dir = root
	var output bytes.Buffer
	child.Stdout = &output
	child.Stderr = &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	process := child.Process
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = process.Signal(syscall.SIGCONT)
	}()
	err := child.Wait()
	t.Cleanup(func() {
		pidBytes, readErr := os.ReadFile(daemon)
		if readErr == nil {
			pid, _ := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			if pid > 0 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	if err != nil {
		t.Fatalf("trusted supervisor helper: %v\n%s", err, output.Bytes())
	}
	result, err := os.ReadFile(status)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "stop=blocked\nforge=blocked\ngroups=empty\nuid=65534\ngid=65534\nambient=scrubbed\nauthorityfds=none\nkill=blocked\n" {
		t.Fatalf("native attack results = %q", result)
	}
	authority, err := bootstrapAgentIdentityLockDirectory(root, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		t.Fatal(err)
	}
	marker, err := loadAgentStandaloneMarker(authority, 65534, uint32(os.Geteuid()), uint32(os.Getegid()))
	closeErr := authority.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("retained standalone marker: %v", errors.Join(err, closeErr))
	}
	if marker.State != "active" {
		t.Fatalf("standalone disposition after clean supervisor exit = %q, want active", marker.State)
	}
	if pidBytes, err := os.ReadFile(daemon); err == nil {
		pid, _ := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		if pid > 0 {
			if process, findErr := os.FindProcess(pid); findErr == nil && process.Signal(syscall.Signal(0)) == nil {
				t.Fatalf("daemonized native descendant %d survived containment", pid)
			}
		}
	}
}

func TestProcessIsolationBorrowedIdentityAdoptionAndBorrowedDomainAdoptionProductionTransport(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("borrowed authority transport requires a trusted root supervisor")
	}

	const (
		uid = uint32(65534)
		gid = uint32(65534)
	)
	t.Run("ownerless_active", func(t *testing.T) {
		fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)
		isolation := &ProcessIsolation{
			UID: uid, GID: gid, BaseEnvironment: map[string]string{"PATH": "/usr/bin:/bin"},
			TestOnlyIdentityLockRoot: fixture.root,
			IdentityLock:             fixture.identity,
			AuthorityDomain:          fixture.domain,
		}
		launch, err := prepareProcessTreeCommand(exec.Command("/bin/true"), processLaunchOptions{Isolation: isolation})
		if err != nil {
			t.Fatalf("prepare borrowed production transport: %v", err)
		}
		tree, err := startProcessTree(launch)
		if err != nil {
			t.Fatalf("start borrowed production transport: %v", err)
		}
		waitCtx, cancelWait := context.WithTimeout(t.Context(), 10*time.Second)
		waitErr, completed := tree.waiter.await(waitCtx)
		cancelWait()
		if !completed {
			t.Fatal("borrowed production transport did not exit")
		}
		containmentErr := processTreeTerminateAndWait(tree, commandWaitTimeout)
		if err = errors.Join(waitErr, containmentErr); err != nil {
			t.Fatalf("settle borrowed production transport: %v", err)
		}
	})

	t.Run("standalone_owner_refuses_native_start", func(t *testing.T) {
		fixture := createBorrowedIdentityDispositionFixture(t, uid, gid)
		ownerPath := filepath.Join(fixture.authorityPath, strconv.FormatUint(uint64(uid), 10)+".owner")
		if err := os.WriteFile(ownerPath, []byte("bound\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		statusDir := t.TempDir()
		if err := os.Chmod(statusDir, 0o755); err != nil {
			t.Fatal(err)
		}
		status := filepath.Join(statusDir, "native-status")
		if err := os.WriteFile(status, []byte("blocked\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(status, int(uid), int(gid)); err != nil {
			t.Fatal(err)
		}
		isolation := &ProcessIsolation{
			UID: uid, GID: gid, BaseEnvironment: map[string]string{"PATH": "/usr/bin:/bin"},
			TestOnlyIdentityLockRoot: fixture.root,
			IdentityLock:             fixture.identity,
			AuthorityDomain:          fixture.domain,
		}
		launch, err := prepareProcessTreeCommand(
			exec.Command("/bin/sh", "-c", `printf 'launched\n' > "$1"`, "sh", status),
			processLaunchOptions{Isolation: isolation},
		)
		if err != nil {
			t.Fatalf("prepare hostile borrowed production transport: %v", err)
		}
		if tree, startErr := startProcessTree(launch); startErr == nil {
			_ = processTreeTerminateAndWait(tree, commandWaitTimeout)
			t.Fatal("borrowed production transport accepted a standalone owner binding")
		}
		result, err := os.ReadFile(status)
		if err != nil {
			t.Fatal(err)
		}
		if string(result) != "blocked\n" {
			t.Fatalf("native command ran despite standalone owner binding: %q", result)
		}
	})
}

type recordingReadCloser struct {
	io.Reader
	closed *bool
}

func (c *recordingReadCloser) Close() error {
	*c.closed = true

	return nil
}

type recordingWriteCloser struct {
	io.Writer
	closed *bool
}

func (c *recordingWriteCloser) Close() error {
	*c.closed = true

	return nil
}

func TestPrepareTurnSupervisorBranches(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	options := processLaunchOptions{Isolation: supervisorTestIsolation()}

	if _, err := prepareProcessTreeCommand(&exec.Cmd{}, options); err == nil {
		t.Fatal("incomplete native command was accepted")
	}

	native := exec.Command("true")
	turnSupervisorMemfd = func(string, int) (int, error) { return 0, errors.New("memfd") }
	if _, err := prepareProcessTreeCommand(native, options); err == nil {
		t.Fatal("memfd failure was ignored")
	}

	turnSupervisorMemfd = unix.MemfdCreate
	turnSupervisorWriteConfig = func(io.WriteSeeker, turnSupervisorConfig) error { return errors.New("write") }
	if _, err := prepareProcessTreeCommand(native, options); err == nil {
		t.Fatal("config write failure was ignored")
	}
	turnSupervisorWriteConfig = writeTurnSupervisorConfig
	turnSupervisorSealConfig = func(uintptr, int, int) (int, error) { return 0, errors.New("seal") }
	if _, err := prepareProcessTreeCommand(native, options); err == nil {
		t.Fatal("config seal failure was ignored")
	}
	turnSupervisorSealConfig = unix.FcntlInt

	pipeCalls := 0
	turnSupervisorPipe = func() (*os.File, *os.File, error) {
		pipeCalls++
		if pipeCalls == 1 {
			return nil, nil, errors.New("control pipe")
		}

		return os.Pipe()
	}
	if _, err := prepareProcessTreeCommand(native, options); err == nil {
		t.Fatal("control pipe failure was ignored")
	}

	pipeCalls = 0
	turnSupervisorPipe = func() (*os.File, *os.File, error) {
		pipeCalls++
		if pipeCalls == 2 {
			return nil, nil, errors.New("ready pipe")
		}

		return os.Pipe()
	}
	if _, err := prepareProcessTreeCommand(native, options); err == nil {
		t.Fatal("readiness pipe failure was ignored")
	}

	pipeCalls = 0
	turnSupervisorPipe = func() (*os.File, *os.File, error) {
		pipeCalls++
		if pipeCalls == 3 {
			return nil, nil, errors.New("completion pipe")
		}

		return os.Pipe()
	}
	if _, err := prepareProcessTreeCommand(native, options); err == nil {
		t.Fatal("completion pipe failure was ignored")
	}

	turnSupervisorPipe = os.Pipe
	turnSupervisorExecutable = func() (string, error) { return "", errors.New("executable") }
	if _, err := prepareProcessTreeCommand(native, options); err == nil {
		t.Fatal("executable failure was ignored")
	}

	turnSupervisorExecutable = os.Executable
	launch, err := prepareProcessTreeCommand(native, options)
	if err != nil {
		t.Fatalf("prepare supervisor: %v", err)
	}
	if launch.cmd == nil || len(launch.inherited) != 4 || launch.control == nil || launch.ready == nil || launch.completion == nil || launch.wait == nil {
		t.Fatalf("prepared launch = %#v", launch)
	}
	launch.close()
	launch.close()
}

func TestTurnSupervisorLivenessCarriesGuardianStdio(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorExecutable = func() (string, error) { return "/bin/true", nil }
	var launched *exec.Cmd
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		launched = exec.Command(name, args...)

		return launched
	}
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer controlRead.Close()
	defer controlWrite.Close()
	completionRead, completionWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer completionRead.Close()
	defer completionWrite.Close()

	liveness, data, peer, err := startTurnSupervisorLiveness(
		turnSupervisorConfig{}, controlRead, completionWrite, &turnSupervisorAuthority{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	defer peer.Close()
	if launched == nil || liveness != launched {
		t.Fatalf("launched liveness command = %#v, want %#v", liveness, launched)
	}
	if launched.Stdin != os.Stdin || launched.Stdout != os.Stdout || launched.Stderr != os.Stderr {
		t.Fatalf("liveness stdio = (%T, %T, %T), want guardian stdio", launched.Stdin, launched.Stdout, launched.Stderr)
	}
	if err = liveness.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestTrustedSupervisorLivenessPreservesNativeStandardStreams(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("trusted supervisor standard-stream transport requires root")
	}
	restoreTurnSupervisorSeams(t)
	var stdout, stderr bytes.Buffer
	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinRead.Close()
	defer stdinWrite.Close()
	native := exec.Command("/bin/sh", "-c", `IFS= read -r value
printf 'stdout:%s\n' "$value"
printf 'stderr:%s\n' "$value" >&2`)
	native.Env = []string{"PATH=/usr/bin:/bin"}
	native.Stdin = stdinRead
	native.Stdout = &stdout
	native.Stderr = &stderr
	launch, err := prepareProcessTreeCommand(native, processLaunchOptions{Isolation: supervisorTestIsolation()})
	if err != nil {
		t.Fatal(err)
	}
	tree, err := startProcessTree(launch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if tree.control != nil {
			_ = tree.control.Close()
		}
		_ = tree.process.Kill()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = tree.waiter.await(ctx)
		cancel()
	})
	if _, err = io.WriteString(stdinWrite, "transport\n"); err != nil {
		t.Fatal(err)
	}
	if err = stdinWrite.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	waitErr, completed := tree.waiter.await(ctx)
	cancel()
	if !completed || waitErr != nil {
		t.Fatalf("await standard-stream provider path completed=%v err=%v", completed, waitErr)
	}
	if stdout.String() != "stdout:transport\n" || stderr.String() != "stderr:transport\n" {
		t.Fatalf("native standard streams stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestTurnSupervisorConfigAndReadinessBranches(t *testing.T) {
	writeErr := errors.New("write")
	if err := writeTurnSupervisorConfig(supervisorWriteSeeker{writeErr: writeErr}, turnSupervisorConfig{}); !errors.Is(err, writeErr) {
		t.Fatalf("write config error = %v", err)
	}
	seekErr := errors.New("seek")
	if err := writeTurnSupervisorConfig(supervisorWriteSeeker{seekErr: seekErr}, turnSupervisorConfig{}); !errors.Is(err, seekErr) {
		t.Fatalf("seek config error = %v", err)
	}

	if err := awaitProcessTreeReady(&processTreeCommand{}); err != nil {
		t.Fatalf("nil readiness: %v", err)
	}
	regular, err := os.CreateTemp(t.TempDir(), "ready")
	if err != nil {
		t.Fatal(err)
	}
	if err := awaitProcessTreeReady(&processTreeCommand{ready: regular}); err == nil {
		t.Fatal("regular-file readiness deadline unexpectedly succeeded")
	}

	for _, test := range []struct {
		name  string
		value string
		ok    bool
	}{
		{name: "eof"},
		{name: "invalid", value: "bad\n"},
		{name: "ready", value: "ready\n", ok: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			read, write, pipeErr := os.Pipe()
			if pipeErr != nil {
				t.Fatal(pipeErr)
			}
			if test.value != "" {
				_, _ = io.WriteString(write, test.value)
			}
			_ = write.Close()
			err := awaitProcessTreeReady(&processTreeCommand{ready: read})
			if test.ok && err != nil {
				t.Fatalf("readiness = %v", err)
			}
			if !test.ok && err == nil {
				t.Fatal("invalid readiness succeeded")
			}
		})
	}
}

func TestTurnSupervisorEnvironmentReplacesInternalMode(t *testing.T) {
	t.Setenv(turnSupervisorModeEnv, "stale")
	t.Setenv("GORACE", "halt_on_error=1 atexit_sleep_ms=1000")
	env := turnSupervisorEnvironment()
	want := []string{turnSupervisorModeEnv + "=" + turnSupervisorMode}
	if !slices.Equal(env, want) {
		t.Fatalf("supervisor environment = %#v, want %#v", env, want)
	}
}

func TestRunTurnSupervisorBranches(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorEnable = func() error { return nil }
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}
	turnSupervisorProcessID = func() int { return 99 }
	turnSupervisorAcquireStandalone = func(uint32, uint32, string, string, bool, string, <-chan struct{}, <-chan os.Signal) (*agentStandaloneIdentity, error) {
		return &agentStandaloneIdentity{identity: &agentIdentityLock{}, authority: &agentIdentityLock{}}, nil
	}

	if err := runTurnSupervisor(strings.NewReader("{"), strings.NewReader(""), io.Discard); err == nil {
		t.Fatal("malformed config succeeded")
	}
	if err := runTurnSupervisor(strings.NewReader(`{}`), strings.NewReader(""), io.Discard); err == nil {
		t.Fatal("incomplete config succeeded")
	}

	config := encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/sh", Args: []string{"sh", "-c", "exit 0"}})
	turnSupervisorEnable = func() error { return errors.New("prctl") }
	if err := runTurnSupervisor(config, strings.NewReader(""), io.Discard); err == nil {
		t.Fatal("subreaper failure succeeded")
	}
	turnSupervisorEnable = func() error { return nil }

	config = encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/missing", Args: []string{"missing"}})
	if err := runTurnSupervisor(config, strings.NewReader(""), io.Discard); err == nil {
		t.Fatal("native start failure succeeded")
	}

	contained := 0
	turnSupervisorContain = func(supervisorPID int, _ int) error {
		if supervisorPID != 99 {
			t.Errorf("supervisor PID = %d", supervisorPID)
		}
		contained++

		return nil
	}
	controlRead, controlWrite := io.Pipe()
	config = encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/sh", Args: []string{"sh", "-c", "exit 0"}})
	var ready bytes.Buffer
	if err := runTurnSupervisor(config, controlRead, &ready); err != nil {
		t.Fatalf("successful supervisor: %v", err)
	}
	_ = controlWrite.Close()
	if ready.String() != "ready\n" || contained != 1 {
		t.Fatalf("successful supervisor ready=%q contained=%d", ready.String(), contained)
	}

	controlRead, controlWrite = io.Pipe()
	turnSupervisorContain = func(int, int) error { return errors.New("wait contain") }
	config = encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/sh", Args: []string{"sh", "-c", "exit 0"}})
	if err := runTurnSupervisor(config, controlRead, io.Discard); err == nil || !strings.Contains(err.Error(), "wait contain") {
		t.Fatalf("wait containment failure = %v", err)
	}
	_ = controlWrite.Close()

	turnSupervisorContain = func(_ int, nativePID int) error {
		process, _ := os.FindProcess(nativePID)
		_ = process.Kill()

		return errors.New("control contain")
	}
	config = encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/sh", Args: []string{"sh", "-c", "while :; do sleep 1; done"}})
	if err := runTurnSupervisor(config, strings.NewReader(""), io.Discard); err == nil || !strings.Contains(err.Error(), "control contain") {
		t.Fatalf("control containment failure = %v", err)
	}

	readyErr := errors.New("ready")
	turnSupervisorContain = func(_ int, nativePID int) error {
		process, _ := os.FindProcess(nativePID)
		_ = process.Kill()

		return errors.New("contain")
	}
	config = encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/sh", Args: []string{"sh", "-c", "while :; do sleep 1; done"}})
	err := runTurnSupervisor(config, strings.NewReader(""), supervisorWriteSeeker{writeErr: readyErr})
	if !errors.Is(err, readyErr) || !strings.Contains(err.Error(), "contain") {
		t.Fatalf("readiness failure = %v", err)
	}

	turnSupervisorContain = func(_ int, nativePID int) error {
		process, _ := os.FindProcess(nativePID)
		_ = process.Kill()

		return nil
	}
	config = encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/sh", Args: []string{"sh", "-c", "while :; do sleep 1; done"}})
	err = runTurnSupervisor(config, strings.NewReader(""), io.Discard)
	if err == nil {
		t.Fatal("control containment did not preserve native exit")
	}

	controlRead, controlWrite = io.Pipe()
	turnSupervisorSignalNotify = func(signals chan<- os.Signal, _ ...os.Signal) {
		signals <- supervisorTestSignal("foreign")
		signals <- syscall.SIGINT
	}
	signalled := 0
	turnSupervisorSignalGroup = func(pid int, signal syscall.Signal) error {
		if signal == syscall.SIGINT {
			signalled++
		}

		return syscall.Kill(-pid, signal)
	}
	config = encodeSupervisorConfig(t, turnSupervisorConfig{Path: "/bin/sh", Args: []string{"sh", "-c", "while :; do sleep 1; done"}})
	_ = runTurnSupervisor(config, controlRead, io.Discard)
	_ = controlWrite.Close()
	if signalled != 1 {
		t.Fatalf("forwarded signals = %d", signalled)
	}
}

func TestTurnSupervisorGuardianSIGKILLPreReadinessRefusesNativeLaunch(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}

	peerRead, peerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer peerRead.Close()
	guardian := exec.Command("/bin/sleep", "30")
	guardian.ExtraFiles = []*os.File{peerWrite}
	if err = guardian.Start(); err != nil {
		t.Fatal(err)
	}
	_ = peerWrite.Close()
	if err = guardian.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = guardian.Wait(); err == nil {
		t.Fatal("SIGKILLed guardian exited successfully")
	}

	contained := 0
	turnSupervisorContain = func(supervisorPID, nativePID int) error {
		contained++
		if supervisorPID != os.Getpid() || nativePID != 0 {
			t.Fatalf("pre-launch containment pid pair = %d, %d", supervisorPID, nativePID)
		}

		return containLinuxSupervisorDescendants(supervisorPID, nativePID)
	}
	marker := filepath.Join(t.TempDir(), "native-launched")
	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", "touch \"$1\"", "probe", marker},
	})
	var ready, completion bytes.Buffer
	err = runTurnSupervisorNative(
		config, []io.Reader{strings.NewReader("control")}, peerRead,
		&ready, &completion, 6, 7, true,
	)
	if err == nil || !strings.Contains(err.Error(), "guardian exited before native launch") {
		t.Fatalf("pre-readiness guardian death = %v", err)
	}
	if contained != 1 || ready.String() != "done\n" || completion.String() != "complete\n" {
		t.Fatalf("pre-readiness proof contained=%d ready=%q completion=%q", contained, ready.String(), completion.String())
	}
	if _, err = os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native launch marker exists after guardian death: %v", err)
	}
}

func TestTurnSupervisorGuardianSIGKILLBeforeNativeLaunchRefusesStartAndCompletesAfterECHILD(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorSignalNotify = func(chan<- os.Signal, ...os.Signal) {}
	turnSupervisorSignalStop = func(chan<- os.Signal) {}

	peerRead, peerWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer peerRead.Close()
	guardian := exec.Command("/bin/sleep", "30")
	guardian.ExtraFiles = []*os.File{peerWrite}
	if err = guardian.Start(); err != nil {
		t.Fatal(err)
	}
	_ = peerWrite.Close()
	guardianWaited := false

	setupEntered := make(chan struct{})
	continueSetup := make(chan struct{})
	setupReleased := false
	defer func() {
		if !setupReleased {
			close(continueSetup)
		}
		if !guardianWaited {
			_ = guardian.Process.Kill()
			_ = guardian.Wait()
		}
	}()
	turnSupervisorEnable = func() error {
		close(setupEntered)
		<-continueSetup

		return nil
	}
	contained := 0
	containedSupervisorPID := 0
	containedNativePID := -1
	echild := false
	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) {
		echild = true

		return -1, unix.ECHILD
	}
	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) { return nil, nil }
	turnSupervisorContain = func(supervisorPID, nativePID int) error {
		contained++
		containedSupervisorPID = supervisorPID
		containedNativePID = nativePID

		return awaitLinuxSupervisorContainment(supervisorPID, nativePID)
	}
	marker := filepath.Join(t.TempDir(), "native-launched")
	var native *exec.Cmd
	turnSupervisorCommand = func(name string, args ...string) *exec.Cmd {
		native = exec.Command(name, args...)

		return native
	}
	config := encodeSupervisorConfig(t, turnSupervisorConfig{
		Path: "/bin/sh", Args: []string{"sh", "-c", "touch \"$1\"", "probe", marker},
	})
	var ready, completion bytes.Buffer
	result := make(chan error, 1)
	go func() {
		result <- runTurnSupervisorNative(
			config, []io.Reader{strings.NewReader("control")}, peerRead,
			&ready, &completion, 6, 7, true,
		)
	}()
	select {
	case <-setupEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("native setup did not pass the early guardian check")
	}
	if err = guardian.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = guardian.Wait(); err == nil {
		t.Fatal("SIGKILLed guardian exited successfully")
	}
	guardianWaited = true
	close(continueSetup)
	setupReleased = true
	select {
	case err = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("late guardian fence did not complete")
	}
	if err == nil || !strings.Contains(err.Error(), "guardian exited before native launch") {
		t.Fatalf("late guardian death = %v", err)
	}
	if native == nil || native.Process != nil {
		t.Fatalf("native command started after guardian death: %#v", native)
	}
	if contained != 1 || containedSupervisorPID != os.Getpid() || containedNativePID != 0 || !echild ||
		ready.String() != "done\n" || completion.String() != "complete\n" {
		t.Fatalf(
			"late guardian proof contained=%d pids=(%d,%d) ECHILD=%t ready=%q completion=%q",
			contained, containedSupervisorPID, containedNativePID, echild, ready.String(), completion.String(),
		)
	}
	if _, err = os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native launch marker exists after late guardian death: %v", err)
	}
}

type supervisorPeerDeathFixture struct {
	tree           *processTree
	guardianPID    int
	livenessPID    int
	descendantPIDs []int
	authorityRoot  string
	uid            uint32
}

func TestProcessIsolationSupervisorGuardianSIGKILLRetainsAuthorityThroughECHILD(t *testing.T) {
	fixture := startSupervisorPeerDeathFixture(t, 64131, 64132, "guardian-death")
	exerciseSupervisorPeerDeath(t, fixture, fixture.livenessPID, fixture.guardianPID)
}

func TestProcessIsolationSupervisorLivenessSIGKILLRetainsAuthorityThroughECHILD(t *testing.T) {
	fixture := startSupervisorPeerDeathFixture(t, 64141, 64142, "liveness-death")
	exerciseSupervisorPeerDeath(t, fixture, fixture.guardianPID, fixture.livenessPID)
}

func startSupervisorPeerDeathFixture(
	t *testing.T,
	uid uint32,
	gid uint32,
	ownerID string,
) *supervisorPeerDeathFixture {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("dual trusted supervisor containment requires root")
	}
	setsid, err := exec.LookPath("setsid")
	if err != nil {
		t.Skip("setsid is unavailable")
	}

	root := t.TempDir()
	state := filepath.Join(root, "processes")
	if err = os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(root, "leaf.sh")
	double := filepath.Join(root, "double.sh")
	nativeScript := filepath.Join(root, "native.sh")
	if err = os.WriteFile(leaf, []byte("#!/bin/sh\ntrap '' INT TERM HUP\necho $$ > \"$1\"\nwhile :; do sleep 30; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(double, []byte("#!/bin/sh\n\"$1\" \"$2\" &\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	nativeBody := `#!/bin/sh
set -eu
echo $$ > "$AMP_TEST_NATIVE_PID"
"$AMP_TEST_LEAF" "$AMP_TEST_ORDINARY_PID" &
"$AMP_TEST_SETSID" "$AMP_TEST_LEAF" "$AMP_TEST_SESSION_PID" &
"$AMP_TEST_SETSID" "$AMP_TEST_DOUBLE" "$AMP_TEST_LEAF" "$AMP_TEST_DOUBLE_PID" &
while [ ! -s "$AMP_TEST_ORDINARY_PID" ] || [ ! -s "$AMP_TEST_SESSION_PID" ] || [ ! -s "$AMP_TEST_DOUBLE_PID" ]; do
  sleep 0.01
done
while :; do sleep 30; done
`
	if err = os.WriteFile(nativeScript, []byte(nativeBody), 0o700); err != nil {
		t.Fatal(err)
	}

	pidPaths := []string{
		filepath.Join(state, "native.pid"),
		filepath.Join(state, "ordinary.pid"),
		filepath.Join(state, "session.pid"),
		filepath.Join(state, "double.pid"),
	}
	native := exec.Command(nativeScript)
	native.Env = []string{
		"PATH=/usr/bin:/bin",
		"AMP_TEST_NATIVE_PID=" + pidPaths[0],
		"AMP_TEST_ORDINARY_PID=" + pidPaths[1],
		"AMP_TEST_SESSION_PID=" + pidPaths[2],
		"AMP_TEST_DOUBLE_PID=" + pidPaths[3],
		"AMP_TEST_LEAF=" + leaf,
		"AMP_TEST_DOUBLE=" + double,
		"AMP_TEST_SETSID=" + setsid,
	}
	identityRoot := t.TempDir()
	isolation := &ProcessIsolation{
		UID: uid, GID: gid, BaseEnvironment: map[string]string{},
		TestOnlyNoCredential: true, TestOnlyIdentityLockRoot: identityRoot,
		StandaloneOwnerID: ownerID, StandaloneStateRoot: createAgentStandaloneProtectedStateRoot(t, uid, gid),
	}
	launch, err := prepareProcessTreeCommand(native, processLaunchOptions{Isolation: isolation})
	if err != nil {
		t.Fatal(err)
	}
	tree, err := startProcessTree(launch)
	if err != nil {
		t.Fatalf("start dual trusted supervisor fixture: %v", err)
	}
	fixture := &supervisorPeerDeathFixture{
		tree: tree, guardianPID: tree.process.Pid,
		authorityRoot: filepath.Join(identityRoot, "acp-go", "agent-identities"), uid: uid,
	}
	for _, path := range pidPaths {
		fixture.descendantPIDs = append(fixture.descendantPIDs, awaitSupervisorPIDFile(t, path))
	}
	identity, err := readLinuxProcessIdentity(fixture.descendantPIDs[0])
	if err != nil {
		t.Fatalf("read native parent identity: %v", err)
	}
	fixture.livenessPID = identity.parentPID
	if fixture.livenessPID <= 0 || fixture.livenessPID == fixture.guardianPID {
		t.Fatalf("invalid guardian/liveness topology guardian=%d liveness=%d", fixture.guardianPID, fixture.livenessPID)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(fixture.guardianPID, syscall.SIGCONT)
		_ = syscall.Kill(fixture.livenessPID, syscall.SIGCONT)
		if fixture.tree.control != nil {
			_ = fixture.tree.control.Close()
		}
		for _, pid := range append(fixture.descendantPIDs, fixture.guardianPID, fixture.livenessPID) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = fixture.tree.waiter.await(ctx)
		cancel()
	})

	return fixture
}

func exerciseSupervisorPeerDeath(t *testing.T, fixture *supervisorPeerDeathFixture, survivorPID, victimPID int) {
	t.Helper()
	if err := syscall.Kill(survivorPID, syscall.SIGSTOP); err != nil {
		t.Fatalf("stop surviving trusted supervisor %d: %v", survivorPID, err)
	}
	awaitSupervisorProcessState(t, survivorPID, 'T')
	if err := syscall.Kill(victimPID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill trusted supervisor peer %d: %v", victimPID, err)
	}
	assertSupervisorAuthorityLocks(t, fixture.authorityRoot, fixture.uid, false)

	if err := syscall.Kill(survivorPID, syscall.SIGCONT); err != nil {
		t.Fatalf("resume surviving trusted supervisor %d: %v", survivorPID, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	waitErr, completed := fixture.tree.waiter.await(ctx)
	cancel()
	if !completed {
		t.Fatalf("await dual trusted supervisor completion: %v", waitErr)
	}
	if errors.Is(waitErr, ErrProcessContainmentIncomplete) {
		t.Fatalf("dual trusted supervisor lost containment proof: %v", waitErr)
	}
	for _, pid := range fixture.descendantPIDs {
		awaitSupervisorProcessGone(t, pid)
	}
	assertSupervisorAuthorityLocks(t, fixture.authorityRoot, fixture.uid, true)
}

func awaitSupervisorPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		payload, err := os.ReadFile(path)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(payload)))
			if parseErr == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("supervised process pid file %q was not published", path)

	return 0
}

func awaitSupervisorProcessState(t *testing.T, pid int, want byte) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		identity, err := readLinuxProcessIdentity(pid)
		if err == nil && identity.state == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("supervised process %d did not enter state %q", pid, want)
}

func awaitSupervisorProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for processPIDAlive(pid) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if processPIDAlive(pid) {
		t.Fatalf("supervised descendant %d survived ECHILD containment proof", pid)
	}
}

func assertSupervisorAuthorityLocks(t *testing.T, authorityRoot string, uid uint32, available bool) {
	t.Helper()
	for _, name := range []string{strconv.FormatUint(uint64(uid), 10) + ".lock", "domain.lock"} {
		path := filepath.Join(authorityRoot, name)
		deadline := time.Now().Add(5 * time.Second)
		for {
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("open authority contender %q: %v", name, err)
			}
			lockErr := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			if lockErr == nil {
				_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
			}
			_ = file.Close()
			if !available {
				if !errors.Is(lockErr, unix.EWOULDBLOCK) && !errors.Is(lockErr, unix.EAGAIN) {
					t.Fatalf("authority lock %q was not retained by frozen survivor: %v", name, lockErr)
				}
				break
			}
			if lockErr == nil {
				break
			}
			if !errors.Is(lockErr, unix.EWOULDBLOCK) && !errors.Is(lockErr, unix.EAGAIN) {
				t.Fatalf("reacquire authority lock %q: %v", name, lockErr)
			}
			if time.Now().After(deadline) {
				t.Fatalf("authority lock %q remained held after ECHILD", name)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func encodeSupervisorConfig(t *testing.T, config turnSupervisorConfig) io.Reader {
	t.Helper()
	if config.Isolation.UID == 0 {
		config.Isolation = *supervisorTestIsolation()
	}
	var buffer bytes.Buffer
	if err := json.NewEncoder(&buffer).Encode(config); err != nil {
		t.Fatal(err)
	}

	return bytes.NewReader(buffer.Bytes())
}

// runTurnSupervisor is a unit-only single-process seam for exercising the
// liveness native core's branch behavior. Production-path security tests must
// enter through prepareProcessTreeCommand and startProcessTree so they cover
// guardian launch, authority handoff, and liveness adoption.
func runTurnSupervisor(configInput io.Reader, controlInput io.Reader, readyOutput io.Writer) error {
	return runTurnSupervisorNative(
		configInput, []io.Reader{controlInput}, nil, readyOutput, nil, 6, 7, false,
	)
}

func supervisorTestIsolation() *ProcessIsolation {
	return &ProcessIsolation{UID: 11, GID: 22, BaseEnvironment: map[string]string{}, TestOnlyNoCredential: true}
}

func TestContainLinuxSupervisorDescendantsBranches(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	turnSupervisorSignalGroup = func(int, syscall.Signal) error { return errors.New("ignored") }
	waitCalls := 0
	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) {
		waitCalls++
		if waitCalls == 1 {
			return 0, nil
		}

		return -1, unix.ECHILD
	}
	retryCalls := 0
	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) {
		retryCalls++

		return nil, errors.New("retry")
	}
	turnSupervisorSleep = func(time.Duration) {}
	if err := awaitLinuxSupervisorContainment(1, 2); err != nil || retryCalls != 1 || waitCalls != 2 {
		t.Fatalf("await containment = %v after descendants=%d waits=%d", err, retryCalls, waitCalls)
	}

	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) { return 0, nil }
	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) { return nil, errors.New("list") }
	if err := containLinuxSupervisorDescendants(1, 2); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("list failure = %v", err)
	}

	waitCalls = 0
	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) {
		waitCalls++
		if waitCalls == 1 {
			return 0, nil
		}

		return -1, unix.ECHILD
	}
	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) { return nil, nil }
	if err := containLinuxSupervisorDescendants(1, 2); err != nil {
		t.Fatalf("empty tree: %v", err)
	}
	if waitCalls != 2 {
		t.Fatalf("empty snapshot was accepted without ECHILD after %d waits", waitCalls)
	}

	waitCalls = 0
	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) {
		waitCalls++
		if waitCalls == 1 {
			return -1, unix.EINTR
		}

		return -1, unix.ECHILD
	}
	if err := containLinuxSupervisorDescendants(1, 2); err != nil || waitCalls != 2 {
		t.Fatalf("interrupted wait = %v after %d calls", err, waitCalls)
	}

	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) {
		return -1, unix.EPERM
	}
	if err := containLinuxSupervisorDescendants(1, 2); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("wait failure = %v", err)
	}

	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) {
		return -1, nil
	}
	if err := containLinuxSupervisorDescendants(1, 2); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("invalid wait result = %v", err)
	}

	descendant := linuxProcessIdentity{pid: 3, state: 'S', startTime: "1"}
	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) { return 0, nil }
	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) { return []linuxProcessIdentity{descendant}, nil }
	turnSupervisorSignalPID = func(linuxProcessIdentity, syscall.Signal) error { return errors.New("kill") }
	if err := containLinuxSupervisorDescendants(1, 2); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("kill failure = %v", err)
	}

	calls := 0
	turnSupervisorDescendants = func(int) ([]linuxProcessIdentity, error) {
		calls++

		return []linuxProcessIdentity{{pid: 2, state: 'Z'}, descendant}, nil
	}
	signals := 0
	turnSupervisorSignalPID = func(linuxProcessIdentity, syscall.Signal) error {
		signals++

		return nil
	}
	waits := 0
	turnSupervisorWait4 = func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error) {
		waits++
		switch waits {
		case 1:
			return 0, nil
		case 2:
			return descendant.pid, nil
		default:
			return -1, unix.ECHILD
		}
	}
	turnSupervisorSleep = func(time.Duration) {}
	if err := containLinuxSupervisorDescendants(1, 2); err != nil {
		t.Fatalf("contain descendants: %v", err)
	}
	if signals != 1 || waits != 3 || calls != 1 {
		t.Fatalf("containment signals=%d waits=%d snapshots=%d", signals, waits, calls)
	}
}

func TestLinuxProcessInventoryAndIdentityBranches(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	root := t.TempDir()
	turnSupervisorProcRoot = root

	if _, err := readLinuxProcessIdentity(1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing identity = %v", err)
	}
	writeProcStat(t, root, 1, "malformed")
	if _, err := readLinuxProcessIdentity(1); err == nil {
		t.Fatal("malformed comm succeeded")
	}
	writeProcStat(t, root, 1, "1 (cmd) S")
	if _, err := readLinuxProcessIdentity(1); err == nil {
		t.Fatal("incomplete stat succeeded")
	}
	writeProcStat(t, root, 1, procStatLine(1, "bad", "10"))
	if _, err := readLinuxProcessIdentity(1); err == nil {
		t.Fatal("bad parent succeeded")
	}

	writeProcStat(t, root, 1, procStatLine(1, "0", "10"))
	writeProcStat(t, root, 2, procStatLine(2, "1", "20"))
	writeProcStat(t, root, 3, procStatLine(3, "2", "30"))
	if err := os.Mkdir(filepath.Join(root, "not-a-pid"), 0o700); err != nil {
		t.Fatal(err)
	}
	descendants, err := linuxDescendants(1)
	if err != nil || len(descendants) != 2 || descendants[0].pid != 2 || descendants[1].pid != 3 {
		t.Fatalf("descendants = %#v, %v", descendants, err)
	}

	if err := os.Mkdir(filepath.Join(root, "4"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := linuxDescendants(1); err != nil {
		t.Fatalf("vanished process should be skipped: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "5"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "5", "stat"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := linuxDescendants(1); err == nil {
		t.Fatal("unreadable process stat was ignored")
	}

	turnSupervisorProcRoot = filepath.Join(root, "missing")
	if _, err := linuxDescendants(1); err == nil {
		t.Fatal("missing proc root succeeded")
	}
}

func writeProcStat(t *testing.T, root string, pid int, value string) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func procStatLine(pid int, parent string, start string) string {
	fields := []string{"S", parent}
	for len(fields) < 19 {
		fields = append(fields, "0")
	}
	fields = append(fields, start)

	return strconv.Itoa(pid) + " (command with spaces) " + strings.Join(fields, " ")
}

func TestSignalLinuxIdentityBranches(t *testing.T) {
	restoreTurnSupervisorSeams(t)
	want := linuxProcessIdentity{pid: 7, startTime: "10"}

	turnSupervisorIdentity = func(int) (linuxProcessIdentity, error) { return linuxProcessIdentity{}, os.ErrNotExist }
	if err := signalLinuxIdentity(want, syscall.SIGKILL); err != nil {
		t.Fatalf("missing identity: %v", err)
	}
	turnSupervisorIdentity = func(int) (linuxProcessIdentity, error) { return linuxProcessIdentity{startTime: "11"}, nil }
	if err := signalLinuxIdentity(want, syscall.SIGKILL); err != nil {
		t.Fatalf("reused identity: %v", err)
	}
	wantErr := errors.New("identity")
	turnSupervisorIdentity = func(int) (linuxProcessIdentity, error) { return linuxProcessIdentity{}, wantErr }
	if err := signalLinuxIdentity(want, syscall.SIGKILL); !errors.Is(err, wantErr) {
		t.Fatalf("identity error = %v", err)
	}

	turnSupervisorIdentity = func(int) (linuxProcessIdentity, error) { return want, nil }
	syscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	if err := signalLinuxIdentity(want, syscall.SIGKILL); err != nil {
		t.Fatalf("ESRCH signal: %v", err)
	}
	syscallKill = func(int, syscall.Signal) error { return syscall.EPERM }
	if err := signalLinuxIdentity(want, syscall.SIGKILL); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("signal error = %v", err)
	}
	syscallKill = func(int, syscall.Signal) error { return nil }
	if err := signalLinuxIdentity(want, syscall.SIGKILL); err != nil {
		t.Fatalf("signal identity: %v", err)
	}
}

func TestTurnInterruptPropagatesTreeContainmentFailure(t *testing.T) {
	path, state := fakeAmpPath(t, "sigint-ignore")
	turn, err := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()}).Continue(
		context.Background(), "T-1", map[string]any{"type": "user"},
	)
	if err != nil {
		t.Fatalf("Continue: %v", err)
	}
	waitForFile(t, filepath.Join(state, "stdin.jsonl"))

	original := processTreeTerminateAndWait
	processTreeTerminateAndWait = func(*processTree, time.Duration) error { return ErrProcessContainmentIncomplete }
	err = turn.Interrupt(context.Background(), 10*time.Millisecond)
	processTreeTerminateAndWait = original
	if !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("Interrupt error = %v, want proof sentinel", err)
	}
	if err := turn.Close(); errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("Close retained stale proof sentinel: %v", err)
	}
}

const (
	oneShotDeathPhaseEnv = adapterOneShotDeathPhaseEnv
	oneShotDeathPathEnv  = adapterOneShotDeathPathEnv
	oneShotDeathStateEnv = adapterOneShotDeathStateEnv
)

func TestOneShotSupervisorContainsTreeAfterAdapterDeath(t *testing.T) {
	if os.Getenv(oneShotDeathPhaseEnv) == "adapter" {
		path := os.Getenv(oneShotDeathPathEnv)
		state := os.Getenv(oneShotDeathStateEnv)
		result := make(chan error, 1)

		go func() {
			_, err := newTestClient(t, nil, Options{
				CLIPath: path,
				Cwd:     filepath.Dir(path),
				Env:     map[string]string{"AMP_CHILD_PID_FILE": filepath.Join(state, "child.pid")},
			}).outputRaw(context.Background(), "parent-death")
			result <- err
		}()

		ready := filepath.Join(state, "child.pid")
		deadline := time.Now().Add(5 * time.Second)
		for {
			if info, err := os.Stat(ready); err == nil && info.Size() > 0 {
				os.Exit(0)
			}

			select {
			case err := <-result:
				_, _ = fmt.Fprintln(os.Stderr, "one-shot command returned before adapter death:", err)
				os.Exit(2)
			default:
			}

			if time.Now().After(deadline) {
				_, _ = fmt.Fprintln(os.Stderr, "one-shot detached descendant did not become ready")
				os.Exit(3)
			}

			time.Sleep(10 * time.Millisecond)
		}
	}

	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "amp")
	script := "#!/bin/sh\nsetsid sh -c 'trap \"\" INT TERM HUP; echo $$ > \"$AMP_CHILD_PID_FILE\"; while :; do sleep 1; done' &\nwhile [ ! -s \"$AMP_CHILD_PID_FILE\" ]; do sleep 0.01; done\ntrap '' INT TERM HUP\nwhile :; do sleep 1; done\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	adapter := exec.Command(os.Args[0], "-test.run=^TestOneShotSupervisorContainsTreeAfterAdapterDeath$")
	adapter.Env = append(os.Environ(),
		oneShotDeathPhaseEnv+"=adapter",
		oneShotDeathPathEnv+"="+path,
		oneShotDeathStateEnv+"="+state,
	)
	if output, err := adapter.CombinedOutput(); err != nil {
		t.Fatalf("adapter death helper: %v\n%s", err, output)
	}

	rawPID, err := os.ReadFile(filepath.Join(state, "child.pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for processPIDAlive(pid) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if processPIDAlive(pid) {
		t.Fatalf("setsid descendant pid %d survived adapter death", pid)
	}
}

func TestTurnCloseBoundsPersistentSupervisorContainmentFailure(t *testing.T) {
	originalProof := processTreeTerminateAndWait
	originalKill := syscallKill
	t.Cleanup(func() {
		processTreeTerminateAndWait = originalProof
		syscallKill = originalKill
	})

	processTreeTerminateAndWait = func(*processTree, time.Duration) error {
		return ErrProcessContainmentIncomplete
	}
	syscallKill = func(int, syscall.Signal) error { return nil }

	releaseWait := make(chan struct{})
	incomplete := 0
	waiter, beginWait := startPausedCommandWait(func() error {
		<-releaseWait

		return nil
	})
	beginWait()
	turn := &Turn{
		cmd:             &exec.Cmd{Process: &os.Process{Pid: 12345}},
		tree:            &processTree{pgid: 12345, supervised: true, waiter: waiter},
		processObserver: ProcessSnapshotObserver{Incomplete: func() { incomplete++ }},
	}

	started := time.Now()
	err := turn.Close()
	elapsed := time.Since(started)
	close(releaseWait)

	if !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("Close error = %v, want proof sentinel", err)
	}
	if elapsed > time.Second {
		t.Fatalf("Close blocked on persistent supervisor proof for %s", elapsed)
	}
	if incomplete != 1 {
		t.Fatalf("incomplete observations = %d, want 1", incomplete)
	}
}

func TestPreparedCommandAndClientErrorBranches(t *testing.T) {
	path, _ := fakeAmpPath(t, "")
	originalPrepare := prepareProcessTree
	originalProof := processTreeTerminateAndWait
	originalWait := startProcessTreeWait
	originalWaitTimeout := commandWaitTimeout
	t.Cleanup(func() {
		prepareProcessTree = originalPrepare
		processTreeTerminateAndWait = originalProof
		startProcessTreeWait = originalWait
		commandWaitTimeout = originalWaitTimeout
	})

	prepareProcessTree = func(_ *exec.Cmd, _ processLaunchOptions) (*processTreeCommand, error) {
		return nil, errors.New("prepare")
	}
	client := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()})
	if _, err := client.Continue(t.Context(), "T-1", map[string]any{"type": "user"}); err == nil {
		t.Fatal("Continue prepare failure was ignored")
	}
	if _, err := client.outputRaw(t.Context(), "version"); err == nil || !strings.Contains(err.Error(), "prepare") {
		t.Fatalf("one-shot prepare failure = %v", err)
	}

	for _, test := range []struct {
		name  string
		shape func(*exec.Cmd)
	}{
		{name: "stdin", shape: func(cmd *exec.Cmd) { cmd.Stdin = strings.NewReader("taken") }},
		{name: "stdout", shape: func(cmd *exec.Cmd) { cmd.Stdout = io.Discard }},
		{name: "stderr", shape: func(cmd *exec.Cmd) { cmd.Stderr = io.Discard }},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepareProcessTree = func(cmd *exec.Cmd, _ processLaunchOptions) (*processTreeCommand, error) {
				test.shape(cmd)

				return &processTreeCommand{cmd: cmd}, nil
			}
			if _, err := client.Continue(t.Context(), "T-1", map[string]any{"type": "user"}); err == nil {
				t.Fatal("prepared pipe conflict succeeded")
			}
			prepareProcessTree = func(cmd *exec.Cmd, _ processLaunchOptions) (*processTreeCommand, error) {
				test.shape(cmd)

				return &processTreeCommand{cmd: cmd}, nil
			}
			if _, err := client.outputRaw(t.Context(), "threads", "list"); err == nil {
				t.Fatal("prepared one-shot pipe conflict succeeded")
			}
		})
	}

	prepareProcessTree = originalPrepare
	processTreeTerminateAndWait = func(*processTree, time.Duration) error {
		return ErrProcessContainmentIncomplete
	}
	if _, err := client.outputRaw(t.Context(), "threads", "list"); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("empty-stderr proof failure = %v", err)
	}

	if got := normalizeWaitDelay(exec.ErrWaitDelay, nil); got != nil {
		t.Fatalf("normalized WaitDelay = %v", got)
	}
	want := errors.New("wait")
	if got := normalizeWaitDelay(want, nil); !errors.Is(got, want) {
		t.Fatalf("ordinary wait error = %v", got)
	}
	if got := (&Turn{}).exitError(want); !strings.Contains(got.Error(), "amp process exited") {
		t.Fatalf("empty-detail exit error = %v", got)
	}

	(*processTreeCommand)(nil).close()
	launch := &processTreeCommand{cmd: exec.Command(filepath.Join(t.TempDir(), "missing"))}
	if _, err := startProcessTree(launch); err == nil {
		t.Fatal("supervisor start failure succeeded")
	}

	processTreeTerminateAndWait = func(*processTree, time.Duration) error { return nil }
	commandWaitTimeout = time.Millisecond
	startProcessTreeWait = func(wait func() error) (*commandWait, func()) {
		_ = startCommandWait(wait)

		return &commandWait{done: make(chan struct{})}, func() {}
	}
	ready, err := os.CreateTemp(t.TempDir(), "not-a-pipe")
	if err != nil {
		t.Fatal(err)
	}
	launch = &processTreeCommand{cmd: exec.Command("sh", "-c", "exit 0"), ready: ready}
	if _, err := startProcessTree(launch); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("failed-launch wait timeout = %v", err)
	}
}

func TestOneShotWaitTimeoutReportsIncompleteBoundary(t *testing.T) {
	path, _ := fakeAmpPath(t, "")
	originalWait := startProcessTreeWait
	originalWaitTimeout := commandWaitTimeout
	t.Cleanup(func() {
		startProcessTreeWait = originalWait
		commandWaitTimeout = originalWaitTimeout
	})

	startProcessTreeWait = func(wait func() error) (*commandWait, func()) {
		_ = startCommandWait(wait)

		return &commandWait{done: make(chan struct{})}, func() {}
	}
	commandWaitTimeout = time.Millisecond

	incomplete := 0
	client := newTestClient(t, nil, Options{
		CLIPath: path,
		Cwd:     t.TempDir(),
		NewProcessSnapshotObserver: func(context.Context, ProcessInventory) ProcessSnapshotObserver {
			return ProcessSnapshotObserver{Incomplete: func() { incomplete++ }}
		},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.outputRaw(ctx, "version"); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("one-shot wait timeout = %v", err)
	}
	if incomplete != 1 {
		t.Fatalf("one-shot incomplete observations = %d, want 1", incomplete)
	}
}

func TestCommandWaitNilAndCloseWaitTimeout(t *testing.T) {
	var nilWait *commandWait
	if err, completed := nilWait.await(t.Context()); err != nil || !completed {
		t.Fatalf("nil command wait = %v, %t", err, completed)
	}

	originalTimeout := commandWaitTimeout
	commandWaitTimeout = time.Millisecond
	t.Cleanup(func() { commandWaitTimeout = originalTimeout })

	release := make(chan struct{})
	incomplete := 0
	turn := &Turn{
		waitFunc: func() error {
			<-release

			return nil
		},
		processObserver: ProcessSnapshotObserver{Incomplete: func() { incomplete++ }},
	}
	err := turn.Close()
	close(release)
	if !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("Close wait timeout = %v", err)
	}
	if incomplete != 1 {
		t.Fatalf("Close timeout incomplete observations = %d, want 1", incomplete)
	}
}
