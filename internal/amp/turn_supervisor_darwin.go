//go:build darwin

package amp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

const darwinLaunchBootstrapMode = "darwin-launch"

const darwinLaunchStatusLimit = 4096

type darwinLaunchConfig struct {
	Path string   `json:"path"`
	Args []string `json:"args"`
	Env  []string `json:"env"`
}

var (
	darwinLaunchExecutable = os.Executable
	darwinLaunchCommand    = exec.Command
	darwinLaunchExit       = os.Exit
	darwinLaunchExec       = syscall.Exec
	darwinLaunchOpenFile   = os.NewFile
	darwinLaunchCloseExec  = syscall.CloseOnExec
	darwinLaunchInput      = inheritedDarwinLaunchInput
	darwinLaunchTimeout    = defaultCloseWait
	darwinLaunchCreateTemp = os.CreateTemp
	darwinLaunchRemove     = os.Remove
	darwinLaunchPipe       = os.Pipe
	darwinLaunchEncode     = func(output io.Writer, value any) error { return json.NewEncoder(output).Encode(value) }
)

func init() {
	darwinLaunchBootstrap()
}

func darwinLaunchBootstrap() {
	if os.Getenv(adapterSupervisorModeEnv) != darwinLaunchBootstrapMode {
		return
	}

	configFile, gate, status, err := darwinLaunchInput()
	if err == nil {
		err = runDarwinLaunchBootstrap(configFile, gate)
	}

	reportDarwinLaunchStatus(status, err)

	if configFile != nil {
		_ = configFile.Close()
	}

	if gate != nil {
		_ = gate.Close()
	}

	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "acp-go-amp Darwin launch bootstrap:", err)

		darwinLaunchExit(1)

		return
	}

	darwinLaunchExit(0)
}

func inheritedDarwinLaunchInput() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
	configFile := darwinLaunchOpenFile(3, "amp-darwin-launch-config")
	gate := darwinLaunchOpenFile(4, "amp-darwin-launch-gate")

	status := darwinLaunchOpenFile(5, "amp-darwin-launch-status")
	if configFile == nil || gate == nil || status == nil {
		return configFile, gate, status, errors.New("darwin native launch descriptors are unavailable")
	}

	darwinLaunchCloseExec(int(status.Fd()))

	return configFile, gate, status, nil
}

func reportDarwinLaunchStatus(status io.WriteCloser, launchErr error) {
	if status == nil {
		return
	}
	defer status.Close()

	if launchErr == nil {
		return
	}

	payload := []byte(launchErr.Error())
	if len(payload) > darwinLaunchStatusLimit {
		payload = payload[:darwinLaunchStatusLimit]
	}

	_, _ = status.Write(payload)
}

func runDarwinLaunchBootstrap(configInput io.ReadCloser, gate io.ReadCloser) error {
	var config darwinLaunchConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		return fmt.Errorf("decode native launch config: %w", err)
	}

	if config.Path == "" || len(config.Args) == 0 {
		return errors.New("native launch config is incomplete")
	}

	var release [1]byte
	if _, err := io.ReadFull(gate, release[:]); err != nil || release[0] != 1 {
		return errors.Join(errors.New("native launch was not released after containment validation"), err)
	}

	if err := errors.Join(configInput.Close(), gate.Close()); err != nil {
		return fmt.Errorf("close Darwin native launch descriptors: %w", err)
	}

	if err := darwinLaunchExec(config.Path, config.Args, config.Env); err != nil {
		return fmt.Errorf("exec native Amp command: %w", err)
	}

	return nil
}

func prepareProcessTreeCommand(native *exec.Cmd, options processLaunchOptions) (*processTreeCommand, error) {
	if !options.DarwinBestEffort {
		return nil, fmt.Errorf("%w: Darwin best-effort containment requires explicit opt-in", ErrProcessContainmentIncomplete)
	}

	if options.Generation == nil {
		return nil, fmt.Errorf("%w: Darwin containment generation is unavailable", ErrProcessContainmentIncomplete)
	}

	config := darwinLaunchConfig{
		Path: native.Path,
		Args: append([]string(nil), native.Args...),
		Env:  withoutPrivateAdapterEnv(native.Env),
	}
	if config.Path == "" || len(config.Args) == 0 {
		return nil, errors.New("prepare Darwin native launch: command is incomplete")
	}

	configFile, err := darwinLaunchCreateTemp(options.Generation.ScratchRoot, ".launch-*")
	if err != nil {
		return nil, fmt.Errorf("create Darwin native launch config: %w", err)
	}

	cleanupConfig := func() {
		_ = configFile.Close()
		_ = darwinLaunchRemove(configFile.Name())
	}
	if chmodErr := configFile.Chmod(0o600); chmodErr != nil {
		cleanupConfig()

		return nil, fmt.Errorf("secure Darwin native launch config: %w", chmodErr)
	}

	if encodeErr := darwinLaunchEncode(configFile, config); encodeErr != nil {
		cleanupConfig()

		return nil, fmt.Errorf("encode Darwin native launch config: %w", encodeErr)
	}

	if _, seekErr := configFile.Seek(0, io.SeekStart); seekErr != nil {
		cleanupConfig()

		return nil, fmt.Errorf("rewind Darwin native launch config: %w", seekErr)
	}

	if removeErr := darwinLaunchRemove(configFile.Name()); removeErr != nil {
		cleanupConfig()

		return nil, fmt.Errorf("unlink Darwin native launch config: %w", removeErr)
	}

	gateRead, gateWrite, err := darwinLaunchPipe()
	if err != nil {
		_ = configFile.Close()

		return nil, fmt.Errorf("create Darwin native launch gate: %w", err)
	}

	statusRead, statusWrite, err := darwinLaunchPipe()
	if err != nil {
		_ = configFile.Close()
		_ = gateRead.Close()
		_ = gateWrite.Close()

		return nil, fmt.Errorf("create Darwin native launch status: %w", err)
	}

	executable, err := darwinLaunchExecutable()
	if err != nil {
		_ = configFile.Close()
		_ = gateRead.Close()
		_ = gateWrite.Close()
		_ = statusRead.Close()
		_ = statusWrite.Close()

		return nil, fmt.Errorf("resolve Darwin native launch bootstrap: %w", err)
	}

	helper := darwinLaunchCommand(executable) // #nosec G204 -- the current executable hosts the private launch bootstrap.
	helper.Dir = native.Dir
	helper.Env = darwinLaunchBootstrapEnvironment()
	helper.Stdin = native.Stdin
	helper.Stdout = native.Stdout
	helper.Stderr = native.Stderr
	helper.WaitDelay = native.WaitDelay
	helper.ExtraFiles = []*os.File{configFile, gateRead, statusWrite}
	configureCommand(helper)

	return &processTreeCommand{
		cmd: helper, inherited: []*os.File{configFile, gateRead, statusWrite}, startGate: gateWrite, ready: statusRead,
		bestEffort: true, generation: options.Generation,
	}, nil
}

func darwinLaunchBootstrapEnvironment() []string {
	env := []string{adapterSupervisorModeEnv + "=" + darwinLaunchBootstrapMode}

	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "GORACE=") {
			env = append(env, entry)
		}
	}

	return env
}

func awaitProcessTreeReady(launch *processTreeCommand) error {
	if launch == nil || launch.ready == nil {
		return nil
	}

	ready := launch.ready
	launch.ready = nil

	defer ready.Close()

	if err := ready.SetReadDeadline(time.Now().Add(darwinLaunchTimeout)); err != nil {
		return fmt.Errorf("%w: set Darwin native exec-status deadline: %v", ErrProcessContainmentIncomplete, err)
	}

	payload, err := io.ReadAll(io.LimitReader(ready, darwinLaunchStatusLimit+1))
	if len(payload) > darwinLaunchStatusLimit {
		return fmt.Errorf("%w: Darwin native launch status exceeded %d bytes", ErrProcessContainmentIncomplete, darwinLaunchStatusLimit)
	}

	if err != nil {
		return fmt.Errorf("%w: await Darwin native exec status: %v", ErrProcessContainmentIncomplete, err)
	}

	if len(payload) != 0 {
		return fmt.Errorf("%w: Darwin native exec failed: %s", ErrProcessContainmentIncomplete, payload)
	}

	return nil
}
