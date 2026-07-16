//go:build linux

package amp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	turnSupervisorModeEnv = adapterSupervisorModeEnv
	turnSupervisorMode    = "1"
	turnSupervisorFDName  = "acp-go-amp-native-supervisor"
	turnSupervisorReady   = "ready\n"
)

type turnSupervisorConfig struct {
	Path string   `json:"path"`
	Args []string `json:"args"`
	Dir  string   `json:"dir"`
	Env  []string `json:"env"`
}

type linuxProcessIdentity struct {
	pid       int
	parentPID int
	state     byte
	startTime string
}

var (
	turnSupervisorExecutable   = os.Executable
	turnSupervisorMemfd        = unix.MemfdCreate
	turnSupervisorPipe         = os.Pipe
	turnSupervisorExit         = os.Exit
	turnSupervisorSignalNotify = signal.Notify
	turnSupervisorSignalStop   = signal.Stop
	turnSupervisorEnable       = enableTurnSupervisor
	turnSupervisorCommand      = exec.Command
	turnSupervisorContain      = awaitLinuxSupervisorContainment
	turnSupervisorProcessID    = os.Getpid
	turnSupervisorSignalGroup  = signalProcessGroupID
	turnSupervisorWriteConfig  = writeTurnSupervisorConfig
	turnSupervisorDescendants  = linuxDescendants
	turnSupervisorIdentity     = readLinuxProcessIdentity
	turnSupervisorSignalPID    = signalLinuxIdentity
	turnSupervisorWait4        = unix.Wait4
	turnSupervisorSleep        = time.Sleep
	turnSupervisorProcRoot     = "/proc"
	turnSupervisorRun          = runTurnSupervisor
	turnSupervisorOpenFile     = os.NewFile
	turnSupervisorCloseOnExec  = unix.CloseOnExec
	turnSupervisorInput        = inheritedTurnSupervisorInput
)

func enableTurnSupervisor() error {
	return unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
}

func inheritedTurnSupervisorInput() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
	config := turnSupervisorOpenFile(3, "amp-turn-supervisor-config")
	control := turnSupervisorOpenFile(4, "amp-turn-supervisor-control")

	ready := turnSupervisorOpenFile(5, "amp-turn-supervisor-ready")
	if config == nil || control == nil || ready == nil {
		return nil, nil, nil, errors.New("native supervisor inherited descriptors are unavailable")
	}

	turnSupervisorCloseOnExec(int(config.Fd()))
	turnSupervisorCloseOnExec(int(control.Fd()))
	turnSupervisorCloseOnExec(int(ready.Fd()))

	return config, control, ready, nil
}

func init() {
	turnSupervisorBootstrap()
}

func turnSupervisorBootstrap() {
	if os.Getenv(turnSupervisorModeEnv) != turnSupervisorMode {
		return
	}

	config, control, ready, err := turnSupervisorInput()
	if err == nil {
		err = turnSupervisorRun(config, control, ready)
	}

	if config != nil {
		_ = config.Close()
	}

	if control != nil {
		_ = control.Close()
	}

	if ready != nil {
		_ = ready.Close()
	}

	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "acp-go-amp native supervisor:", err)

		turnSupervisorExit(1)

		return
	}

	turnSupervisorExit(0)
}

func prepareProcessTreeCommand(native *exec.Cmd) (*processTreeCommand, error) {
	config := turnSupervisorConfig{
		Path: native.Path,
		Args: append([]string(nil), native.Args...),
		Dir:  native.Dir,
		Env:  append([]string(nil), native.Env...),
	}
	if config.Path == "" || len(config.Args) == 0 {
		return nil, errors.New("prepare Amp native supervisor: native command is incomplete")
	}

	configFD, err := turnSupervisorMemfd(turnSupervisorFDName, unix.MFD_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("prepare Amp native supervisor config: %w", err)
	}

	configFile := os.NewFile(uintptr(configFD), turnSupervisorFDName)
	if writeErr := turnSupervisorWriteConfig(configFile, config); writeErr != nil {
		_ = configFile.Close()

		return nil, writeErr
	}

	controlRead, controlWrite, err := turnSupervisorPipe()
	if err != nil {
		_ = configFile.Close()

		return nil, fmt.Errorf("prepare Amp native supervisor control: %w", err)
	}

	readyRead, readyWrite, err := turnSupervisorPipe()
	if err != nil {
		_ = configFile.Close()
		_ = controlRead.Close()
		_ = controlWrite.Close()

		return nil, fmt.Errorf("prepare Amp native supervisor readiness: %w", err)
	}

	executable, err := turnSupervisorExecutable()
	if err != nil {
		_ = configFile.Close()
		_ = controlRead.Close()
		_ = controlWrite.Close()
		_ = readyRead.Close()
		_ = readyWrite.Close()

		return nil, fmt.Errorf("resolve embedded Amp native supervisor: %w", err)
	}

	helper := turnSupervisorCommand(executable) // #nosec G204 -- the current executable hosts the private supervisor mode.
	helper.Env = turnSupervisorEnvironment()
	helper.ExtraFiles = []*os.File{configFile, controlRead, readyWrite}
	helper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	return &processTreeCommand{
		cmd:       helper,
		inherited: []*os.File{configFile, controlRead, readyWrite},
		control:   controlWrite,
		ready:     readyRead,
	}, nil
}

func awaitProcessTreeReady(launch *processTreeCommand) error {
	if launch.ready == nil {
		return nil
	}

	defer func() {
		_ = launch.ready.Close()
		launch.ready = nil
	}()

	if err := launch.ready.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("arm Amp native supervisor readiness: %w", err)
	}

	line, err := bufio.NewReader(launch.ready).ReadString('\n')
	if err != nil {
		return fmt.Errorf("await Amp native supervisor readiness: %w", err)
	}

	if line != turnSupervisorReady {
		return fmt.Errorf("invalid Amp native supervisor readiness %q", strings.TrimSpace(line))
	}

	return nil
}

func writeTurnSupervisorConfig(file io.WriteSeeker, config turnSupervisorConfig) error {
	if err := json.NewEncoder(file).Encode(config); err != nil {
		return fmt.Errorf("encode Amp native supervisor config: %w", err)
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind Amp native supervisor config: %w", err)
	}

	return nil
}

func turnSupervisorEnvironment() []string {
	env := make([]string, 0, len(os.Environ())+2)

	var raceOptions string

	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, turnSupervisorModeEnv+"=") {
			continue
		}

		if strings.HasPrefix(entry, "GORACE=") {
			raceOptions = strings.TrimPrefix(entry, "GORACE=")

			continue
		}

		env = append(env, entry)
	}

	if raceOptions != "" {
		raceOptions += " "
	}

	raceOptions += "atexit_sleep_ms=0"

	return append(
		env,
		turnSupervisorModeEnv+"="+turnSupervisorMode,
		"GORACE="+raceOptions,
	)
}

func runTurnSupervisor(configInput io.Reader, controlInput io.Reader, readyOutput io.Writer) error {
	var config turnSupervisorConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		return fmt.Errorf("decode Amp native supervisor config: %w", err)
	}

	if config.Path == "" || len(config.Args) == 0 {
		return errors.New("amp native supervisor config is incomplete")
	}

	if err := turnSupervisorEnable(); err != nil {
		return fmt.Errorf("enable Amp native subreaper: %w", err)
	}

	signals := make(chan os.Signal, 2)

	turnSupervisorSignalNotify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer turnSupervisorSignalStop(signals)

	native := turnSupervisorCommand(config.Path, config.Args[1:]...) // #nosec G204 -- private config was built from the operator-selected Amp command.
	native.Args = append([]string(nil), config.Args...)
	native.Dir = config.Dir
	native.Env = append([]string(nil), config.Env...)
	native.Stdin = os.Stdin
	native.Stdout = os.Stdout
	native.Stderr = os.Stderr
	configureCommand(native)

	if err := native.Start(); err != nil {
		return fmt.Errorf("start supervised Amp native root: %w", err)
	}

	if _, err := io.WriteString(readyOutput, turnSupervisorReady); err != nil {
		_ = turnSupervisorSignalGroup(native.Process.Pid, syscall.SIGKILL)
		waitErr := native.Wait()
		containErr := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid)

		return errors.Join(fmt.Errorf("publish Amp native supervisor readiness: %w", err), containErr, waitErr)
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- native.Wait() }()

	controlDone := make(chan struct{})

	go func() {
		_, _ = io.Copy(io.Discard, controlInput)

		close(controlDone)
	}()

	for {
		select {
		case waitErr := <-waitDone:
			if err := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid); err != nil {
				return err
			}

			return waitErr
		case <-controlDone:
			_ = turnSupervisorSignalGroup(native.Process.Pid, syscall.SIGKILL)
			waitErr := <-waitDone

			if err := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid); err != nil {
				return err
			}

			return waitErr
		case received := <-signals:
			nativeSignal, ok := received.(syscall.Signal)
			if !ok {
				continue
			}

			_ = turnSupervisorSignalGroup(native.Process.Pid, nativeSignal)
		}
	}
}

// awaitLinuxSupervisorContainment never lets the dedicated subreaper exit on
// an unproven tree. The adapter retains the managed-root permit when its bounded
// parent-side wait expires; meanwhile the helper keeps retrying until it can
// truthfully publish completion by exiting.
func awaitLinuxSupervisorContainment(supervisorPID int, nativePID int) error {
	for {
		err := containLinuxSupervisorDescendants(supervisorPID, nativePID)
		if err == nil {
			return nil
		}

		turnSupervisorSleep(time.Second)
	}
}

func containLinuxSupervisorDescendants(supervisorPID int, nativePID int) error {
	_ = turnSupervisorSignalGroup(nativePID, syscall.SIGKILL)

	for {
		waited, waitErr := turnSupervisorWait4(-1, nil, unix.WNOHANG, nil)
		switch {
		case waited > 0:
			continue
		case errors.Is(waitErr, unix.EINTR):
			continue
		case errors.Is(waitErr, unix.ECHILD):
			return nil
		case waitErr != nil:
			return fmt.Errorf("%w: reap supervised Amp descendants: %v", ErrProcessTreeNotQuiescent, waitErr)
		case waited < 0:
			return fmt.Errorf("%w: invalid supervised Amp wait result %d", ErrProcessTreeNotQuiescent, waited)
		}

		descendants, err := turnSupervisorDescendants(supervisorPID)
		if err != nil {
			return fmt.Errorf("%w: enumerate supervised Amp descendants: %v", ErrProcessTreeNotQuiescent, err)
		}

		for _, descendant := range descendants {
			if descendant.state != 'Z' {
				if err := turnSupervisorSignalPID(descendant, syscall.SIGKILL); err != nil {
					return fmt.Errorf("%w: kill supervised Amp descendant %d: %v", ErrProcessTreeNotQuiescent, descendant.pid, err)
				}
			}
		}

		turnSupervisorSleep(5 * time.Millisecond)
	}
}

func linuxDescendants(rootPID int) ([]linuxProcessIdentity, error) {
	entries, err := os.ReadDir(turnSupervisorProcRoot)
	if err != nil {
		return nil, err
	}

	children := make(map[int][]linuxProcessIdentity)

	for _, entry := range entries {
		pid, parseErr := strconv.Atoi(entry.Name())
		if parseErr != nil {
			continue
		}

		identity, readErr := turnSupervisorIdentity(pid)
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}

		if readErr != nil {
			return nil, readErr
		}

		children[identity.parentPID] = append(children[identity.parentPID], identity)
	}

	result := make([]linuxProcessIdentity, 0)

	queue := append([]linuxProcessIdentity(nil), children[rootPID]...)
	for len(queue) > 0 {
		identity := queue[0]
		queue = queue[1:]

		result = append(result, identity)
		queue = append(queue, children[identity.pid]...)
	}

	return result, nil
}

func readLinuxProcessIdentity(pid int) (linuxProcessIdentity, error) {
	raw, err := os.ReadFile(filepath.Join(turnSupervisorProcRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return linuxProcessIdentity{}, err
	}

	line := string(raw)

	closing := strings.LastIndexByte(line, ')')
	if closing < 0 || closing+2 >= len(line) {
		return linuxProcessIdentity{}, fmt.Errorf("parse /proc/%d/stat: malformed comm field", pid)
	}

	fields := strings.Fields(line[closing+2:])
	if len(fields) < 20 || len(fields[0]) != 1 {
		return linuxProcessIdentity{}, fmt.Errorf("parse /proc/%d/stat: incomplete fields", pid)
	}

	parentPID, err := strconv.Atoi(fields[1])
	if err != nil {
		return linuxProcessIdentity{}, fmt.Errorf("parse /proc/%d/stat parent: %w", pid, err)
	}

	return linuxProcessIdentity{
		pid:       pid,
		parentPID: parentPID,
		state:     fields[0][0],
		startTime: fields[19],
	}, nil
}

func signalLinuxIdentity(identity linuxProcessIdentity, processSignal syscall.Signal) error {
	current, err := turnSupervisorIdentity(identity.pid)
	if errors.Is(err, os.ErrNotExist) || (err == nil && current.startTime != identity.startTime) {
		return nil
	}

	if err != nil {
		return err
	}

	if err := syscallKill(identity.pid, processSignal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	return nil
}
