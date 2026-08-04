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
	openFile := turnSupervisorOpenFile
	closeOnExec := turnSupervisorCloseOnExec
	prctl := turnSupervisorPrctl
	setrlimit := turnSupervisorSetrlimit
	acquireLock := turnSupervisorAcquireLock
	sealConfig := turnSupervisorSealConfig
	effectiveUID := turnSupervisorEffectiveUID
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
		turnSupervisorOpenFile = openFile
		turnSupervisorCloseOnExec = closeOnExec
		turnSupervisorPrctl = prctl
		turnSupervisorSetrlimit = setrlimit
		turnSupervisorAcquireLock = acquireLock
		turnSupervisorSealConfig = sealConfig
		turnSupervisorEffectiveUID = effectiveUID
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
	turnSupervisorCloseOnExec = func(int) { closeOnExec++ }
	config, control, ready, err := inheritedTurnSupervisorInput()
	if err != nil {
		t.Fatalf("inherited input: %v", err)
	}
	_ = config.Close()
	_ = control.Close()
	_ = ready.Close()
	if closeOnExec != 3 {
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
		turnSupervisorAcquireLock = func(uint32, bool, string, <-chan struct{}, <-chan os.Signal) (*agentIdentityLock, error) {
			return &agentIdentityLock{}, nil
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

func TestTrustedSupervisorRejectsNativeSignalsProofForgeryAndDaemonEscape(t *testing.T) {
	const phaseEnv = "ACP_GO_AMP_TEST_TRUSTED_SUPERVISOR_PHASE"
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
	proof := filepath.Join(root, "proof")

	if os.Getenv(phaseEnv) == "child" {
		proofFile, err := os.OpenFile(proof, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer proofFile.Close()
		unix.CloseOnExec(int(proofFile.Fd()))
		controlRead, controlWrite := io.Pipe()
		defer controlRead.Close()
		defer controlWrite.Close()
		script := `supervisor=$PPID
if kill -STOP "$supervisor" 2>/dev/null; then echo stop=allowed; else echo stop=blocked; fi > "$1"
if printf 'forged\n' > "/proc/$supervisor/fd/$3" 2>/dev/null; then echo forge=allowed; else echo forge=blocked; fi >> "$1"
groups=$(sed -n 's/^Groups:[[:space:]]*//p' "/proc/$$/status")
if [ -z "$groups" ]; then echo groups=empty; else echo groups="$groups"; fi >> "$1"
setsid sh -c 'trap "" INT TERM; while :; do sleep 30; done' & echo $! > "$2"
if kill -KILL "$supervisor" 2>/dev/null; then echo kill=allowed; else echo kill=blocked; fi >> "$1"`
		config := turnSupervisorConfig{
			Path:      "/bin/sh",
			Args:      []string{"sh", "-c", script, "probe", status, daemon, strconv.Itoa(int(proofFile.Fd()))},
			Env:       []string{"PATH=/usr/bin:/bin"},
			Isolation: ProcessIsolation{UID: 65534, GID: 65534, BaseEnvironment: map[string]string{}},
		}
		if err := runTurnSupervisor(encodeSupervisorConfig(t, config), controlRead, proofFile); err != nil {
			t.Fatalf("run trusted supervisor: %v", err)
		}
		return
	}

	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid is unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTrustedSupervisorRejectsNativeSignalsProofForgeryAndDaemonEscape$")
	child.Env = append(os.Environ(), phaseEnv+"=child")
	child.Env = append(child.Env, "ACP_GO_AMP_TEST_ROOT="+root)
	child.Dir = root
	go func() {
		time.Sleep(500 * time.Millisecond)
		if child.Process != nil {
			_ = child.Process.Signal(syscall.SIGCONT)
		}
	}()
	output, err := child.CombinedOutput()
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
		t.Fatalf("trusted supervisor helper: %v\n%s", err, output)
	}
	result, err := os.ReadFile(status)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != "stop=blocked\nforge=blocked\ngroups=empty\nkill=blocked\n" {
		t.Fatalf("native attack results = %q", result)
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
	if launch.cmd == nil || len(launch.inherited) != 3 || launch.control == nil || launch.ready == nil {
		t.Fatalf("prepared launch = %#v", launch)
	}
	launch.close()
	launch.close()
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
	turnSupervisorAcquireLock = func(uint32, bool, string, <-chan struct{}, <-chan os.Signal) (*agentIdentityLock, error) {
		return &agentIdentityLock{}, nil
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
