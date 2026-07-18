//go:build darwin

package amp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

type darwinTestReadCloser struct {
	io.Reader
	closeErr error
	closed   bool
}

type darwinTestWriteCloser struct {
	bytes.Buffer
	closed bool
}

func (w *darwinTestWriteCloser) Close() error {
	w.closed = true

	return nil
}

func (r *darwinTestReadCloser) Close() error {
	r.closed = true

	return r.closeErr
}

func TestConfigureCommandDarwin(t *testing.T) {
	cmd := exec.Command("true")
	configureCommand(cmd)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		t.Fatalf("SysProcAttr = %#v, want Setpgid", cmd.SysProcAttr)
	}
}

func TestDarwinLaunchFailsClosedWithoutExplicitOptIn(t *testing.T) {
	launch, err := prepareProcessTreeCommand(exec.Command("true"), processLaunchOptions{})
	if launch != nil || !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("launch = %#v, error = %v", launch, err)
	}
}

func TestDarwinLaunchBootstrapProtocol(t *testing.T) {
	originalExec := darwinLaunchExec
	t.Cleanup(func() { darwinLaunchExec = originalExec })
	configBytes, err := json.Marshal(darwinLaunchConfig{Path: "/native/amp", Args: []string{"amp", "version"}, Env: []string{"A=B"}})
	if err != nil {
		t.Fatal(err)
	}

	config := &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}
	gate := &darwinTestReadCloser{Reader: bytes.NewReader([]byte{1})}
	var got darwinLaunchConfig
	darwinLaunchExec = func(path string, args []string, env []string) error {
		got = darwinLaunchConfig{Path: path, Args: args, Env: env}

		return nil
	}
	if err := runDarwinLaunchBootstrap(config, gate); err != nil {
		t.Fatal(err)
	}
	if !config.closed || !gate.closed || got.Path != "/native/amp" || len(got.Args) != 2 || len(got.Env) != 1 {
		t.Fatalf("closed=(%v,%v), exec=%#v", config.closed, gate.closed, got)
	}

	for _, test := range []struct {
		name   string
		config *darwinTestReadCloser
		gate   *darwinTestReadCloser
		exec   func(string, []string, []string) error
	}{
		{name: "decode", config: &darwinTestReadCloser{Reader: strings.NewReader("{")}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "incomplete", config: &darwinTestReadCloser{Reader: strings.NewReader(`{}`)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "gate eof", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("")}},
		{name: "gate byte", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("x")}},
		{name: "close", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes), closeErr: errors.New("close config")}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}},
		{name: "exec", config: &darwinTestReadCloser{Reader: bytes.NewReader(configBytes)}, gate: &darwinTestReadCloser{Reader: strings.NewReader("\x01")}, exec: func(string, []string, []string) error { return errors.New("exec") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			darwinLaunchExec = test.exec
			if darwinLaunchExec == nil {
				darwinLaunchExec = func(string, []string, []string) error { return nil }
			}
			if err := runDarwinLaunchBootstrap(test.config, test.gate); err == nil {
				t.Fatal("bootstrap error = nil")
			}
		})
	}
}

func TestDarwinLaunchBootstrapDispatch(t *testing.T) {
	originalInput := darwinLaunchInput
	originalExit := darwinLaunchExit
	originalExec := darwinLaunchExec
	t.Cleanup(func() {
		darwinLaunchInput = originalInput
		darwinLaunchExit = originalExit
		darwinLaunchExec = originalExec
	})
	t.Setenv(adapterSupervisorModeEnv, darwinLaunchBootstrapMode)
	darwinLaunchExec = func(string, []string, []string) error { return nil }
	var exits []int
	darwinLaunchExit = func(code int) { exits = append(exits, code) }
	failureStatus := &darwinTestWriteCloser{}
	darwinLaunchInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return nil, nil, failureStatus, errors.New("input")
	}
	darwinLaunchBootstrap()
	config, err := json.Marshal(darwinLaunchConfig{Path: "/native/amp", Args: []string{"amp"}})
	if err != nil {
		t.Fatal(err)
	}
	status := &darwinTestWriteCloser{}
	darwinLaunchInput = func() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
		return io.NopCloser(bytes.NewReader(config)), io.NopCloser(strings.NewReader("\x01")), status, nil
	}
	darwinLaunchBootstrap()
	if len(exits) != 2 || exits[0] != 1 || exits[1] != 0 {
		t.Fatalf("exit codes = %v", exits)
	}
	if !status.closed || status.Len() != 0 {
		t.Fatalf("successful status = %q, closed=%v", status.String(), status.closed)
	}
	if !failureStatus.closed || failureStatus.String() != "input" {
		t.Fatalf("failure status = %q, closed=%v", failureStatus.String(), failureStatus.closed)
	}
}

func TestInheritedDarwinLaunchInputAndBoundedStatus(t *testing.T) {
	originalOpen := darwinLaunchOpenFile
	originalCloseExec := darwinLaunchCloseExec
	t.Cleanup(func() {
		darwinLaunchOpenFile = originalOpen
		darwinLaunchCloseExec = originalCloseExec
	})

	files := make([]*os.File, 3)
	for index := range files {
		file, err := os.CreateTemp(t.TempDir(), "descriptor")
		if err != nil {
			t.Fatal(err)
		}
		files[index] = file
	}
	index := 0
	darwinLaunchOpenFile = func(uintptr, string) *os.File {
		file := files[index]
		index++

		return file
	}
	closeExecFD := -1
	darwinLaunchCloseExec = func(fd int) { closeExecFD = fd }
	config, gate, status, err := inheritedDarwinLaunchInput()
	if err != nil || config == nil || gate == nil || status == nil || closeExecFD != int(files[2].Fd()) {
		t.Fatalf("inherited input = (%v,%v,%v), close-on-exec=%d, err=%v", config, gate, status, closeExecFD, err)
	}
	_ = config.Close()
	_ = gate.Close()
	_ = status.Close()

	index = 0
	darwinLaunchOpenFile = func(uintptr, string) *os.File {
		index++
		if index == 3 {
			return nil
		}

		return files[index-1]
	}
	if _, _, _, err := inheritedDarwinLaunchInput(); err == nil {
		t.Fatal("missing status descriptor was accepted")
	}

	writer := &darwinTestWriteCloser{}
	reportDarwinLaunchStatus(writer, errors.New(strings.Repeat("x", darwinLaunchStatusLimit+100)))
	if !writer.closed || writer.Len() != darwinLaunchStatusLimit {
		t.Fatalf("bounded status length=%d closed=%v", writer.Len(), writer.closed)
	}
	reportDarwinLaunchStatus(nil, errors.New("ignored"))
}

func TestAwaitDarwinNativeExecStatus(t *testing.T) {
	originalTimeout := darwinLaunchTimeout
	t.Cleanup(func() { darwinLaunchTimeout = originalTimeout })

	t.Run("success eof", func(t *testing.T) {
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		_ = write.Close()
		launch := &processTreeCommand{ready: read}
		if err := awaitProcessTreeReady(launch); err != nil || launch.ready != nil {
			t.Fatalf("await success error=%v ready=%v", err, launch.ready)
		}
	})

	t.Run("failure payload", func(t *testing.T) {
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.WriteString(write, "exec native Amp command: missing")
		_ = write.Close()
		err = awaitProcessTreeReady(&processTreeCommand{ready: read})
		if !errors.Is(err, ErrProcessContainmentIncomplete) || !strings.Contains(err.Error(), "missing") {
			t.Fatalf("await payload error = %v", err)
		}
	})

	t.Run("oversized payload", func(t *testing.T) {
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.WriteString(write, strings.Repeat("x", darwinLaunchStatusLimit+1))
		_ = write.Close()
		err = awaitProcessTreeReady(&processTreeCommand{ready: read})
		if !errors.Is(err, ErrProcessContainmentIncomplete) || !strings.Contains(err.Error(), "exceeded") {
			t.Fatalf("oversized status error = %v", err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		read, write, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer write.Close()
		darwinLaunchTimeout = 10 * time.Millisecond
		err = awaitProcessTreeReady(&processTreeCommand{ready: read})
		if !errors.Is(err, ErrProcessContainmentIncomplete) {
			t.Fatalf("timeout error = %v", err)
		}
	})

	t.Run("deadline unsupported", func(t *testing.T) {
		file, err := os.CreateTemp(t.TempDir(), "regular")
		if err != nil {
			t.Fatal(err)
		}
		err = awaitProcessTreeReady(&processTreeCommand{ready: file})
		if !errors.Is(err, ErrProcessContainmentIncomplete) || !strings.Contains(err.Error(), "deadline") {
			t.Fatalf("regular-file ready error = %v", err)
		}
	})

	if err := awaitProcessTreeReady(nil); err != nil {
		t.Fatal(err)
	}
}

func TestDarwinMissingNativeExecutableFailsBeforeLaunchAdmission(t *testing.T) {
	generation := &DarwinGeneration{RuntimeID: strings.Repeat("a", 32), ScratchRoot: t.TempDir()}
	launch, err := prepareProcessTreeCommand(exec.Command(filepath.Join(t.TempDir(), "missing-native")), processLaunchOptions{
		DarwinBestEffort: true,
		Generation:       generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = startProcessTree(launch)
	if !errors.Is(err, ErrProcessContainmentIncomplete) || !strings.Contains(err.Error(), "exec native Amp command") {
		t.Fatalf("missing native launch error = %v", err)
	}
}

func TestDarwinBootstrapEnvironmentIsPrivate(t *testing.T) {
	privateKey := adapterPrivateEnvPrefix + "SECRET"
	t.Setenv(privateKey, "must-not-pass")
	t.Setenv(DarwinRuntimeIDEnv, strings.Repeat("b", 32))
	t.Setenv(DarwinScratchRootEnv, "/private/root")
	t.Setenv("GORACE", "halt_on_error=1")

	env := environmentMap(darwinLaunchBootstrapEnvironment())
	if env[adapterSupervisorModeEnv] != darwinLaunchBootstrapMode || env["GORACE"] != "halt_on_error=1" {
		t.Fatalf("bootstrap environment = %#v", env)
	}
	for _, key := range []string{privateKey, DarwinRuntimeIDEnv, DarwinScratchRootEnv} {
		if _, ok := env[key]; ok {
			t.Fatalf("private environment leaked %s: %#v", key, env)
		}
	}
}

func TestPrepareDarwinLaunchResourceFailures(t *testing.T) {
	originalCreateTemp := darwinLaunchCreateTemp
	originalRemove := darwinLaunchRemove
	originalPipe := darwinLaunchPipe
	originalEncode := darwinLaunchEncode
	originalExecutable := darwinLaunchExecutable
	t.Cleanup(func() {
		darwinLaunchCreateTemp = originalCreateTemp
		darwinLaunchRemove = originalRemove
		darwinLaunchPipe = originalPipe
		darwinLaunchEncode = originalEncode
		darwinLaunchExecutable = originalExecutable
	})

	options := processLaunchOptions{DarwinBestEffort: true, Generation: &DarwinGeneration{ScratchRoot: t.TempDir()}}
	if _, err := prepareProcessTreeCommand(exec.Command("true"), processLaunchOptions{DarwinBestEffort: true}); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("missing generation error = %v", err)
	}
	if _, err := prepareProcessTreeCommand(&exec.Cmd{}, options); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("incomplete command error = %v", err)
	}

	want := errors.New("resource")
	darwinLaunchCreateTemp = func(string, string) (*os.File, error) { return nil, want }
	if _, err := prepareProcessTreeCommand(exec.Command("true"), options); !errors.Is(err, want) {
		t.Fatalf("create config error = %v", err)
	}
	darwinLaunchCreateTemp = func(dir, pattern string) (*os.File, error) {
		file, err := os.CreateTemp(dir, pattern)
		if err == nil {
			_ = file.Close()
		}

		return file, err
	}
	if _, err := prepareProcessTreeCommand(exec.Command("true"), options); err == nil || !strings.Contains(err.Error(), "secure") {
		t.Fatalf("chmod config error = %v", err)
	}
	darwinLaunchCreateTemp = originalCreateTemp

	darwinLaunchEncode = func(io.Writer, any) error { return want }
	if _, err := prepareProcessTreeCommand(exec.Command("true"), options); !errors.Is(err, want) {
		t.Fatalf("encode config error = %v", err)
	}
	darwinLaunchEncode = func(output io.Writer, value any) error {
		file, ok := output.(*os.File)
		if !ok {
			return errors.New("launch config output is not a file")
		}

		if err := json.NewEncoder(file).Encode(value); err != nil {
			return err
		}

		return file.Close()
	}
	if _, err := prepareProcessTreeCommand(exec.Command("true"), options); err == nil || !strings.Contains(err.Error(), "rewind") {
		t.Fatalf("seek config error = %v", err)
	}
	darwinLaunchEncode = originalEncode

	darwinLaunchRemove = func(string) error { return want }
	if _, err := prepareProcessTreeCommand(exec.Command("true"), options); !errors.Is(err, want) {
		t.Fatalf("unlink config error = %v", err)
	}
	darwinLaunchRemove = originalRemove

	darwinLaunchPipe = func() (*os.File, *os.File, error) { return nil, nil, want }
	if _, err := prepareProcessTreeCommand(exec.Command("true"), options); !errors.Is(err, want) || !strings.Contains(err.Error(), "gate") {
		t.Fatalf("gate pipe error = %v", err)
	}
	pipeCalls := 0
	darwinLaunchPipe = func() (*os.File, *os.File, error) {
		pipeCalls++
		if pipeCalls == 2 {
			return nil, nil, want
		}

		return os.Pipe()
	}
	if _, err := prepareProcessTreeCommand(exec.Command("true"), options); !errors.Is(err, want) || !strings.Contains(err.Error(), "status") {
		t.Fatalf("status pipe error = %v", err)
	}
	darwinLaunchPipe = originalPipe

	darwinLaunchExecutable = func() (string, error) { return "", want }
	if _, err := prepareProcessTreeCommand(exec.Command("true"), options); !errors.Is(err, want) || !strings.Contains(err.Error(), "bootstrap") {
		t.Fatalf("executable error = %v", err)
	}
}

func TestDarwinRemainingContainmentErrorBranches(t *testing.T) {
	originalNow := darwinContainmentNow
	originalSleep := darwinContainmentSleep
	originalKill := syscallKill
	originalFastWait := darwinFastExitWait
	originalGetpgid := syscallGetpgid
	t.Cleanup(func() {
		darwinContainmentNow = originalNow
		darwinContainmentSleep = originalSleep
		syscallKill = originalKill
		darwinFastExitWait = originalFastWait
		syscallGetpgid = originalGetpgid
	})

	tree := &processTree{pgid: 42}
	if err := tree.runDarwinCleanupAfterTerm(time.Now().Add(time.Second), time.Now(), syscall.EIO); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("TERM error = %v", err)
	}

	now := time.Unix(100, 0)
	darwinContainmentNow = func() time.Time { return now }
	darwinContainmentSleep = func(time.Duration) {}
	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == syscall.SIGKILL {
			return syscall.EIO
		}

		return nil
	}
	tree = &processTree{pgid: 43}
	if err := tree.runDarwinCleanupAfterTerm(now.Add(time.Second), now, nil); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("KILL error = %v", err)
	}

	darwinFastExitWait = time.Millisecond
	syscallKill = func(int, syscall.Signal) error { return syscall.ESRCH }
	waiter := &commandWait{done: make(chan struct{})}
	tree = &processTree{pgid: 44, waiter: waiter}
	if err := handleDarwinFastExit(&processTreeCommand{}, tree, func() {}); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("fast-exit reap timeout = %v", err)
	}

	syscallKill = func(int, syscall.Signal) error { return syscall.EIO }
	if err := signalProcessGroupID(45, syscall.SIGTERM); !errors.Is(err, syscall.EIO) {
		t.Fatalf("signal process group error = %v", err)
	}
	if err := signalProcessGroupID(0, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	syscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }
	blocked := make(chan struct{})
	turn := &Turn{
		cmd: &exec.Cmd{Process: &os.Process{Pid: 999999}},
		waitFunc: func() error {
			<-blocked

			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := turn.Interrupt(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Interrupt context branch = %v", err)
	}
	close(blocked)
}

func TestDarwinReadinessFailureReportsUnreapedWaiter(t *testing.T) {
	originalReady := processTreeReadyWait
	originalTerminate := processTreeTerminateAndWait
	originalTimeout := commandWaitTimeout
	t.Cleanup(func() {
		processTreeReadyWait = originalReady
		processTreeTerminateAndWait = originalTerminate
		commandWaitTimeout = originalTimeout
	})

	want := errors.New("readiness")
	processTreeReadyWait = func(*processTreeCommand) error { return want }
	processTreeTerminateAndWait = func(*processTree, time.Duration) error { return nil }
	commandWaitTimeout = time.Millisecond
	cmd := exec.Command("/bin/sleep", "10")
	configureCommand(cmd)
	launch := &processTreeCommand{cmd: cmd}
	_, err := startProcessTree(launch)
	if !errors.Is(err, want) || !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("readiness wait error = %v", err)
	}
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}

func TestProcessTreeCommandDarwinGateAndClose(t *testing.T) {
	if err := (*processTreeCommand)(nil).releaseStartGate(); err != nil {
		t.Fatal(err)
	}
	(*processTreeCommand)(nil).abortStartGate()
	if err := (*processTreeCommand)(nil).close(); err != nil {
		t.Fatal(err)
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	command := &processTreeCommand{startGate: write, control: read, ready: read}
	if releaseErr := command.releaseStartGate(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	command.abortStartGate()
	if closeErr := command.close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	_, closedWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := closedWrite.Close(); err != nil {
		t.Fatal(err)
	}
	if err := (&processTreeCommand{startGate: closedWrite}).releaseStartGate(); err == nil {
		t.Fatal("closed gate release error = nil")
	}
}

func TestDarwinNativeAdmissionRejectionFinalizesUnspawnedGeneration(t *testing.T) {
	want := errors.New("native admission rejected")
	finished := 0
	generation := &DarwinGeneration{
		RuntimeID:   "00000000000000000000000000000000",
		ScratchRoot: t.TempDir(),
		RecordFinished: func(complete bool) error {
			if !complete {
				t.Fatal("unspawned generation was marked incomplete")
			}
			finished++

			return nil
		},
	}
	launch, err := prepareProcessTreeCommand(exec.Command("true"), processLaunchOptions{DarwinBestEffort: true, Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	launch.acquireNative = func() (func(), error) { return nil, want }
	if _, err := startProcessTree(launch); !errors.Is(err, want) {
		t.Fatalf("start error = %v", err)
	}
	if finished != 1 {
		t.Fatalf("generation finalizations = %d", finished)
	}
}

func TestDarwinNativeAdmissionRejectionSurfacesFinalizationFailure(t *testing.T) {
	want := errors.New("native admission rejected")
	cleanupErr := errors.New("record finalization failed")
	generation := &DarwinGeneration{
		RuntimeID:      "00000000000000000000000000000000",
		ScratchRoot:    t.TempDir(),
		RecordFinished: func(bool) error { return cleanupErr },
	}
	launch, err := prepareProcessTreeCommand(exec.Command("true"), processLaunchOptions{DarwinBestEffort: true, Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	launch.acquireNative = func() (func(), error) { return nil, want }
	_, err = startProcessTree(launch)
	if !errors.Is(err, want) || !errors.Is(err, ErrProcessContainmentIncomplete) || !strings.Contains(err.Error(), cleanupErr.Error()) {
		t.Fatalf("start error = %v", err)
	}
}

func TestDarwinStartProcessTreeFailureBoundaries(t *testing.T) {
	originalGetpgid := syscallGetpgid
	originalReady := processTreeReadyWait
	t.Cleanup(func() {
		syscallGetpgid = originalGetpgid
		processTreeReadyWait = originalReady
	})

	newLaunch := func(t *testing.T, generation *DarwinGeneration) *processTreeCommand {
		t.Helper()
		launch, err := prepareProcessTreeCommand(exec.Command("true"), processLaunchOptions{DarwinBestEffort: true, Generation: generation})
		if err != nil {
			t.Fatal(err)
		}

		return launch
	}

	t.Run("start", func(t *testing.T) {
		launch := newLaunch(t, &DarwinGeneration{ScratchRoot: t.TempDir()})
		launch.cmd.Path = filepath.Join(t.TempDir(), "missing")
		if _, err := startProcessTree(launch); err == nil {
			t.Fatal("start error = nil")
		}
	})

	t.Run("pgid validation", func(t *testing.T) {
		launch := newLaunch(t, &DarwinGeneration{ScratchRoot: t.TempDir()})
		syscallGetpgid = func(int) (int, error) { return 0, syscall.EPERM }
		if _, err := startProcessTree(launch); !errors.Is(err, ErrProcessContainmentIncomplete) {
			t.Fatalf("validation error = %v", err)
		}
		syscallGetpgid = originalGetpgid
	})

	t.Run("record", func(t *testing.T) {
		want := errors.New("record")
		launch := newLaunch(t, &DarwinGeneration{ScratchRoot: t.TempDir(), RecordStarted: func(int, int) error { return want }})
		if _, err := startProcessTree(launch); !errors.Is(err, want) {
			t.Fatalf("record error = %v", err)
		}
	})

	t.Run("gate", func(t *testing.T) {
		launch := newLaunch(t, &DarwinGeneration{ScratchRoot: t.TempDir()})
		if err := launch.startGate.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := startProcessTree(launch); !errors.Is(err, ErrProcessContainmentIncomplete) {
			t.Fatalf("gate error = %v", err)
		}
	})

	t.Run("readiness", func(t *testing.T) {
		want := errors.New("readiness")
		processTreeReadyWait = func(*processTreeCommand) error { return want }
		launch := newLaunch(t, &DarwinGeneration{ScratchRoot: t.TempDir()})
		if _, err := startProcessTree(launch); !errors.Is(err, want) {
			t.Fatalf("readiness error = %v", err)
		}
		processTreeReadyWait = originalReady
	})
}

func TestDarwinGetpgidESRCHFallback(t *testing.T) {
	originalGetpgid := syscallGetpgid
	originalKill := syscallKill
	t.Cleanup(func() {
		syscallGetpgid = originalGetpgid
		syscallKill = originalKill
	})

	tests := []struct {
		name           string
		probe          error
		term           error
		wantIncomplete bool
		wantFinished   bool
		wantSignals    []syscall.Signal
	}{
		{name: "group absent", probe: syscall.ESRCH, wantFinished: true, wantSignals: []syscall.Signal{0}},
		{name: "group present", probe: syscall.EPERM, term: syscall.ESRCH, wantFinished: true, wantSignals: []syscall.Signal{0, syscall.SIGTERM}},
		{name: "probe failure", probe: syscall.EIO, wantIncomplete: true, wantSignals: []syscall.Signal{0}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			syscallGetpgid = func(int) (int, error) { return 0, syscall.ESRCH }

			var signals []syscall.Signal
			syscallKill = func(_ int, signal syscall.Signal) error {
				signals = append(signals, signal)
				if signal == 0 {
					return test.probe
				}

				return test.term
			}

			started := 0
			finished := false
			generation := &DarwinGeneration{
				RecordStarted: func(int, int) error {
					started++

					return nil
				},
				RecordFinished: func(complete bool) error {
					finished = complete

					return nil
				},
			}
			launch := &processTreeCommand{cmd: exec.Command("true"), bestEffort: true, generation: generation}
			_, err := startProcessTree(launch)
			if err == nil || !strings.Contains(err.Error(), "before Darwin process-group identity validation") {
				t.Fatalf("fallback error = %v", err)
			}
			if errors.Is(err, ErrProcessContainmentIncomplete) != test.wantIncomplete {
				t.Fatalf("incomplete=%t error=%v", errors.Is(err, ErrProcessContainmentIncomplete), err)
			}
			if started != 0 || finished != test.wantFinished {
				t.Fatalf("record callbacks: started=%d finished=%t", started, finished)
			}
			if !reflect.DeepEqual(signals, test.wantSignals) {
				t.Fatalf("signals = %v, want %v", signals, test.wantSignals)
			}
		})
	}
}

func TestDarwinProcessTreeHelperBranches(t *testing.T) {
	if (*processTree)(nil).commandWait() != nil {
		t.Fatal("nil tree command waiter is non-nil")
	}
	want := errors.New("finish")
	if err := (*processTree)(nil).finish(want); !errors.Is(err, want) {
		t.Fatalf("nil finish error = %v", err)
	}
	if err := (*processTree)(nil).terminateAndWait(time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := (*processTree)(nil).runDarwinCleanup(); err != nil {
		t.Fatal(err)
	}
	if err := (&processTree{}).runDarwinCleanup(); err != nil {
		t.Fatal(err)
	}
	if err := signalDarwinProcessGroupID(0, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := abortUnvalidatedProcessTree(nil); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("nil abort error = %v", err)
	}

	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })
	var signals []syscall.Signal
	syscallKill = func(_ int, signal syscall.Signal) error {
		signals = append(signals, signal)

		return syscall.ESRCH
	}
	if err := (&processTree{pgid: 77}).interrupt(); err != nil {
		t.Fatal(err)
	}
	if err := (&processTree{pgid: 77}).kill(); err != nil {
		t.Fatal(err)
	}
	if len(signals) != 2 || signals[0] != syscall.SIGINT || signals[1] != syscall.SIGKILL {
		t.Fatalf("signals = %v", signals)
	}

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = read.Close()
	tree := &processTree{supervised: true, control: write}
	if err := tree.kill(); err != nil {
		t.Fatal(err)
	}
	if err := tree.kill(); err != nil {
		t.Fatal(err)
	}

	waiter := &commandWait{done: make(chan struct{})}
	close(waiter.done)
	if err := (&processTree{pgid: 88, waiter: waiter, generation: &DarwinGeneration{}}).interrupt(); err != nil {
		t.Fatal(err)
	}
}

func TestAbortUnvalidatedDarwinProcessTreeTimers(t *testing.T) {
	originalKillAfter := darwinAbortKillAfter
	originalWait := darwinAbortWait
	t.Cleanup(func() {
		darwinAbortKillAfter = originalKillAfter
		darwinAbortWait = originalWait
	})
	darwinAbortKillAfter = time.Millisecond
	darwinAbortWait = 5 * time.Millisecond

	waiter := &commandWait{done: make(chan struct{})}
	close(waiter.done)
	if err := abortUnvalidatedProcessTree(&processTree{waiter: waiter}); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("reaped abort error = %v", err)
	}

	command := exec.Command("sleep", "30")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waiter = startCommandWait(command.Wait)
	if err := abortUnvalidatedProcessTree(&processTree{process: command.Process, waiter: waiter}); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("killed abort error = %v", err)
	}

	if err := abortUnvalidatedProcessTree(&processTree{waiter: &commandWait{done: make(chan struct{})}}); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("deadline abort error = %v", err)
	}
}

func TestDarwinCancellationAfterGenerationPreparationDoesNotSpawn(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	finished := 0
	released := 0
	acquired := 0
	client := NewClient(nil, Options{
		CLIPath:          "/does/not/run",
		DarwinBestEffort: true,
		NewDarwinGeneration: func(context.Context) (*DarwinGeneration, error) {
			cancel()

			return &DarwinGeneration{
				RuntimeID:   "00000000000000000000000000000000",
				ScratchRoot: t.TempDir(),
				RecordFinished: func(complete bool) error {
					if !complete {
						t.Fatal("unspawned canceled generation was marked incomplete")
					}
					finished++

					return nil
				},
				Release: func(bool) error {
					released++

					return nil
				},
			}, nil
		},
		AcquireNativeRoot: func(context.Context) (func(), error) {
			acquired++

			return func() {}, nil
		},
	})
	_, err := client.outputRaw(ctx, "version")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("output error = %v", err)
	}
	if finished != 1 || released != 1 || acquired != 0 {
		t.Fatalf("finished=%d released=%d acquired=%d", finished, released, acquired)
	}
}

func TestDarwinCleanupLadderAndMemoization(t *testing.T) {
	originalKill := syscallKill
	originalNow := darwinContainmentNow
	originalSleep := darwinContainmentSleep
	t.Cleanup(func() {
		syscallKill = originalKill
		darwinContainmentNow = originalNow
		darwinContainmentSleep = originalSleep
	})

	now := time.Unix(1, 0)
	darwinContainmentNow = func() time.Time { return now }
	darwinContainmentSleep = func(duration time.Duration) { now = now.Add(duration) }
	var signals []syscall.Signal
	live := true
	syscallKill = func(pid int, signal syscall.Signal) error {
		if pid != -42 {
			t.Fatalf("pid = %d, want -42", pid)
		}
		signals = append(signals, signal)
		if signal == syscall.SIGKILL {
			live = false
		}
		if signal == 0 && !live {
			return syscall.ESRCH
		}
		if signal == 0 {
			return syscall.EPERM
		}

		return nil
	}
	waiter := &commandWait{done: make(chan struct{})}
	close(waiter.done)
	tree := &processTree{pgid: 42, waiter: waiter, generation: &DarwinGeneration{}}
	if err := tree.terminateAndWait(time.Second); err != nil {
		t.Fatal(err)
	}
	firstCount := len(signals)
	if err := tree.terminateAndWait(time.Second); err != nil {
		t.Fatal(err)
	}
	if len(signals) != firstCount {
		t.Fatalf("memoized cleanup emitted additional signals: %v", signals)
	}
	if signals[0] != syscall.SIGTERM || !containsSignal(signals, syscall.SIGKILL) {
		t.Fatalf("signals = %v, want TERM then KILL", signals)
	}
}

func TestDarwinCleanupStopsUsingPGIDAfterAbsence(t *testing.T) {
	originalKill := syscallKill
	originalNow := darwinContainmentNow
	originalSleep := darwinContainmentSleep
	t.Cleanup(func() {
		syscallKill = originalKill
		darwinContainmentNow = originalNow
		darwinContainmentSleep = originalSleep
	})

	now := time.Unix(1, 0)
	darwinContainmentNow = func() time.Time { return now }
	darwinContainmentSleep = func(duration time.Duration) { now = now.Add(duration) }
	var signals []syscall.Signal
	syscallKill = func(_ int, signal syscall.Signal) error {
		signals = append(signals, signal)
		if signal == 0 {
			return syscall.ESRCH
		}

		return nil
	}
	tree := &processTree{pgid: 7, waiter: &commandWait{done: make(chan struct{})}, generation: &DarwinGeneration{}}
	err := tree.terminateAndWait(time.Second)
	if !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("cleanup error = %v", err)
	}
	if len(signals) != 2 || signals[0] != syscall.SIGTERM || signals[1] != 0 {
		t.Fatalf("signals after first ESRCH = %v", signals)
	}
}

func TestDarwinCleanupStopsUsingPGIDWhenInitialTermReportsAbsence(t *testing.T) {
	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })

	calls := 0
	syscallKill = func(_ int, _ syscall.Signal) error {
		calls++

		return syscall.ESRCH
	}
	waiter := &commandWait{done: make(chan struct{})}
	close(waiter.done)
	err := (&processTree{pgid: 11, waiter: waiter, generation: &DarwinGeneration{}}).terminateAndWait(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("PGID operations = %d, want only initial TERM", calls)
	}
}

func TestDarwinCleanupSignalsBeforeReleasingDirectWaiter(t *testing.T) {
	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })

	released := make(chan struct{})
	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal != syscall.SIGTERM {
			t.Fatalf("first signal = %v, want SIGTERM", signal)
		}
		select {
		case <-released:
			t.Fatal("direct waiter was released before the captured group was signalled")
		default:
		}

		return syscall.ESRCH
	}
	waiter := &commandWait{done: make(chan struct{})}
	close(waiter.done)
	tree := &processTree{
		pgid:   12,
		waiter: waiter,
		releaseWaiter: func() {
			close(released)
		},
		generation: &DarwinGeneration{},
	}
	if err := tree.terminateAndWait(time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-released:
	default:
		t.Fatal("direct waiter was not released after the captured group signal")
	}
}

func TestDarwinCleanupStopsUsingPGIDWhenKillReportsAbsence(t *testing.T) {
	originalKill := syscallKill
	originalNow := darwinContainmentNow
	originalSleep := darwinContainmentSleep
	t.Cleanup(func() {
		syscallKill = originalKill
		darwinContainmentNow = originalNow
		darwinContainmentSleep = originalSleep
	})
	now := time.Unix(1, 0)
	darwinContainmentNow = func() time.Time { return now }
	darwinContainmentSleep = func(duration time.Duration) { now = now.Add(duration) }
	var calls []syscall.Signal
	syscallKill = func(_ int, signal syscall.Signal) error {
		calls = append(calls, signal)
		if signal == syscall.SIGKILL {
			return syscall.ESRCH
		}
		if signal == 0 {
			return syscall.EPERM
		}

		return nil
	}
	waiter := &commandWait{done: make(chan struct{})}
	close(waiter.done)
	if err := (&processTree{pgid: 13, waiter: waiter, generation: &DarwinGeneration{}}).terminateAndWait(time.Second); err != nil {
		t.Fatal(err)
	}
	if calls[len(calls)-1] != syscall.SIGKILL {
		t.Fatalf("PGID operations after KILL ESRCH = %v", calls)
	}
}

func TestDarwinCleanupTreatsSignalEPERMAsObservable(t *testing.T) {
	for _, test := range []struct {
		name     string
		epermAt  syscall.Signal
		wantLive bool
	}{
		{name: "term", epermAt: syscall.SIGTERM},
		{name: "kill", epermAt: syscall.SIGKILL, wantLive: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			originalKill := syscallKill
			originalNow := darwinContainmentNow
			originalSleep := darwinContainmentSleep
			t.Cleanup(func() {
				syscallKill = originalKill
				darwinContainmentNow = originalNow
				darwinContainmentSleep = originalSleep
			})
			now := time.Unix(1, 0)
			darwinContainmentNow = func() time.Time { return now }
			darwinContainmentSleep = func(duration time.Duration) { now = now.Add(duration) }
			var calls []syscall.Signal
			syscallKill = func(_ int, signal syscall.Signal) error {
				calls = append(calls, signal)
				if signal == test.epermAt {
					return syscall.EPERM
				}
				if signal == 0 && !test.wantLive {
					return syscall.ESRCH
				}
				if signal == 0 {
					return syscall.EPERM
				}

				return nil
			}
			waiter := &commandWait{done: make(chan struct{})}
			close(waiter.done)
			err := (&processTree{pgid: 12, waiter: waiter, generation: &DarwinGeneration{}}).terminateAndWait(time.Second)
			if test.wantLive && !errors.Is(err, ErrProcessContainmentIncomplete) {
				t.Fatalf("live group error = %v", err)
			}
			if !test.wantLive && err != nil {
				t.Fatal(err)
			}
			if !containsSignal(calls, test.epermAt) {
				t.Fatalf("calls = %v", calls)
			}
		})
	}
}

func TestDarwinCleanupProbeError(t *testing.T) {
	originalKill := syscallKill
	t.Cleanup(func() { syscallKill = originalKill })
	syscallKill = func(_ int, signal syscall.Signal) error {
		if signal == 0 {
			return syscall.EINVAL
		}

		return nil
	}
	waiter := &commandWait{done: make(chan struct{})}
	close(waiter.done)
	err := (&processTree{pgid: 9, waiter: waiter, generation: &DarwinGeneration{}}).terminateAndWait(time.Second)
	if !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("cleanup error = %v", err)
	}
}

func TestDarwinCleanupSyscallErrorsKillAndReapDirectChild(t *testing.T) {
	for _, failure := range []string{"term", "probe", "kill"} {
		t.Run(failure, func(t *testing.T) {
			originalKill := syscallKill
			originalNow := darwinContainmentNow
			originalSleep := darwinContainmentSleep
			t.Cleanup(func() {
				syscallKill = originalKill
				darwinContainmentNow = originalNow
				darwinContainmentSleep = originalSleep
			})

			now := time.Unix(1, 0)
			darwinContainmentNow = func() time.Time { return now }
			darwinContainmentSleep = func(duration time.Duration) { now = now.Add(duration) }
			syscallKill = func(_ int, signal syscall.Signal) error {
				switch failure {
				case "term":
					if signal == syscall.SIGTERM {
						return syscall.EIO
					}
				case "probe":
					if signal == 0 {
						return syscall.EIO
					}
				case "kill":
					if signal == syscall.SIGKILL {
						return syscall.EIO
					}
					if signal == 0 {
						return syscall.EPERM
					}
				}

				return nil
			}

			command := exec.Command("/bin/sleep", "30")
			configureCommand(command)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waiter := startCommandWait(command.Wait)
			t.Cleanup(func() {
				_ = command.Process.Kill()
				select {
				case <-waiter.done:
				case <-time.After(time.Second):
					t.Errorf("direct child remained unreaped after test cleanup")
				}
			})

			tree := &processTree{
				pgid:       command.Process.Pid,
				process:    command.Process,
				waiter:     waiter,
				generation: &DarwinGeneration{},
			}
			if err := tree.terminateAndWait(defaultCloseWait); !errors.Is(err, ErrProcessContainmentIncomplete) {
				t.Fatalf("cleanup error = %v", err)
			}
			select {
			case <-waiter.done:
			default:
				t.Fatal("syscall-error cleanup returned before direct-child reap")
			}
			if command.ProcessState == nil {
				t.Fatal("direct child was not reaped")
			}
		})
	}
}

func TestAbortDarwinCleanupRemainingBranches(t *testing.T) {
	exited := exec.Command("/usr/bin/true")
	if err := exited.Run(); err != nil {
		t.Fatal(err)
	}
	if err := (&processTree{process: exited.Process, generation: &DarwinGeneration{}}).abortDarwinCleanup(
		time.Now().Add(time.Second),
		ErrProcessContainmentIncomplete,
	); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("finished-process cleanup = %v", err)
	}

	released := false
	expired := &processTree{
		waiter:        &commandWait{done: make(chan struct{})},
		releaseWaiter: func() { released = true },
		generation:    &DarwinGeneration{},
	}
	if err := expired.abortDarwinCleanup(time.Now().Add(-time.Second), ErrProcessContainmentIncomplete); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("expired cleanup = %v", err)
	}
	if !released {
		t.Fatal("abort cleanup did not release the direct waiter")
	}

	timedOut := &processTree{waiter: &commandWait{done: make(chan struct{})}, generation: &DarwinGeneration{}}
	if err := timedOut.abortDarwinCleanup(time.Now().Add(time.Millisecond), ErrProcessContainmentIncomplete); !errors.Is(err, ErrProcessContainmentIncomplete) {
		t.Fatalf("waiter-timeout cleanup = %v", err)
	}
}

func TestDarwinFastExitAndSameGroupDescendant(t *testing.T) {
	for _, script := range []string{"exit 0", "sleep 30 & wait"} {
		cmd := exec.Command("sh", "-c", script)
		generation := &DarwinGeneration{RuntimeID: "00000000000000000000000000000000", ScratchRoot: t.TempDir()}
		launch, err := prepareProcessTreeCommand(cmd, processLaunchOptions{DarwinBestEffort: true, Generation: generation})
		if err != nil {
			t.Fatal(err)
		}
		tree, err := startProcessTree(launch)
		if err != nil {
			t.Fatalf("start %q: %v", script, err)
		}
		if err := tree.terminateAndWait(defaultCloseWait); err != nil {
			t.Fatalf("cleanup %q: %v", script, err)
		}
	}
}

func TestDarwinSetsidEscapeSurvivesSelectedBoundary(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pid")
	readyFile := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0], "-test.run=TestDarwinContainmentHelper")
	cmd.Env = append(os.Environ(),
		"ACP_GO_AMP_DARWIN_CONTAINMENT_HELPER=parent",
		"ACP_GO_AMP_DARWIN_CONTAINMENT_PID_FILE="+pidFile,
		"ACP_GO_AMP_DARWIN_CONTAINMENT_READY_FILE="+readyFile,
	)
	generation := &DarwinGeneration{RuntimeID: "00000000000000000000000000000000", ScratchRoot: t.TempDir()}
	launch, err := prepareProcessTreeCommand(cmd, processLaunchOptions{DarwinBestEffort: true, Generation: generation})
	if err != nil {
		t.Fatal(err)
	}
	tree, err := startProcessTree(launch)
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, statErr := os.Stat(readyFile); statErr == nil {
			break
		}
	}
	pidBytes, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	escapedPID, err := strconv.Atoi(string(pidBytes))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(escapedPID, syscall.SIGKILL)
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if errors.Is(syscall.Kill(escapedPID, 0), syscall.ESRCH) {
				return
			}
		}
		t.Errorf("setsid escapee pid %d remained after test cleanup deadline", escapedPID)
	})
	if err := tree.terminateAndWait(defaultCloseWait); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(escapedPID, 0); err != nil {
		t.Fatalf("setsid escapee did not survive original-group cleanup: %v", err)
	}
}

func TestDarwinClientOutputBoundsInheritedPipesFromSetsidEscape(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pid")
	readyFile := filepath.Join(dir, "ready")
	script := filepath.Join(dir, "amp")
	contents := "#!/bin/sh\n" +
		"ACP_GO_AMP_DARWIN_CONTAINMENT_HELPER=pipe-parent " +
		"ACP_GO_AMP_DARWIN_CONTAINMENT_PID_FILE=" + shellQuote(pidFile) + " " +
		"ACP_GO_AMP_DARWIN_CONTAINMENT_READY_FILE=" + shellQuote(readyFile) + " " +
		"exec " + shellQuote(os.Args[0]) + " -test.run=TestDarwinContainmentHelper -- \"$@\"\n"
	if err := os.WriteFile(script, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
	client := newTestClient(t, nil, Options{CLIPath: script, Cwd: dir})
	ctx, cancel := context.WithTimeout(t.Context(), 2*defaultCloseWait)
	defer cancel()
	started := time.Now()
	if _, err := client.outputRaw(ctx, "pipe-escape"); err != nil {
		t.Fatalf("selected-boundary output failed: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= defaultCloseWait {
		t.Fatalf("selected-boundary output took %v", elapsed)
	}
	escapedPID := readPIDFile(t, pidFile)
	cleanupDarwinEscapee(t, escapedPID)
	if err := syscall.Kill(escapedPID, 0); err != nil {
		t.Fatalf("setsid pipe holder did not survive selected-boundary completion: %v", err)
	}
}

func readPIDFile(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(raw))
	if err != nil {
		t.Fatal(err)
	}

	return pid
}

func cleanupDarwinEscapee(t *testing.T, pid int) {
	t.Helper()
	t.Cleanup(func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
				return
			}
		}
		t.Errorf("setsid escapee pid %d remained after test cleanup deadline", pid)
	})
}

func TestDarwinContainmentHelper(t *testing.T) {
	switch os.Getenv("ACP_GO_AMP_DARWIN_CONTAINMENT_HELPER") {
	case "parent", "pipe-parent":
		child := exec.Command(os.Args[0], "-test.run=TestDarwinContainmentHelper")
		child.Env = append(os.Environ(),
			"ACP_GO_AMP_DARWIN_CONTAINMENT_HELPER=child",
			"ACP_GO_AMP_DARWIN_CONTAINMENT_READY_FILE="+os.Getenv("ACP_GO_AMP_DARWIN_CONTAINMENT_READY_FILE"),
		)
		if os.Getenv("ACP_GO_AMP_DARWIN_CONTAINMENT_HELPER") == "pipe-parent" {
			child.Stdout = os.Stdout
			child.Stderr = os.Stderr
		}
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(os.Getenv("ACP_GO_AMP_DARWIN_CONTAINMENT_PID_FILE"), []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
			os.Exit(3)
		}
		if os.Getenv("ACP_GO_AMP_DARWIN_CONTAINMENT_HELPER") == "pipe-parent" {
			for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				if _, err := os.Stat(os.Getenv("ACP_GO_AMP_DARWIN_CONTAINMENT_READY_FILE")); err == nil {
					os.Exit(0)
				}
			}
			os.Exit(6)
		}
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "child":
		if _, err := syscall.Setsid(); err != nil {
			os.Exit(4)
		}
		if err := os.WriteFile(os.Getenv("ACP_GO_AMP_DARWIN_CONTAINMENT_READY_FILE"), []byte("ready"), 0o600); err != nil {
			os.Exit(5)
		}
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
}

func containsSignal(signals []syscall.Signal, wanted syscall.Signal) bool {
	for _, signal := range signals {
		if signal == wanted {
			return true
		}
	}

	return false
}
