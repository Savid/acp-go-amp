//go:build linux

package amp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	turnSupervisorModeEnv          = adapterSupervisorModeEnv
	turnSupervisorMode             = "guardian"
	turnSupervisorLivenessMode     = "liveness"
	turnSupervisorFDName           = "acp-go-amp-native-supervisor"
	turnSupervisorReady            = "ready\n"
	turnSupervisorOriginBorrowed   = "borrowed"
	turnSupervisorOriginStandalone = "standalone"
	turnSupervisorOriginShared     = "shared"

	// turnSupervisorFailure prefixes the terminal readiness frame a supervisor
	// writes in place of the readiness it never reached. A refusal reason is
	// otherwise only ever printed to a stderr the parent discards, and every
	// caller sees a bare "await Amp native supervisor readiness: EOF" instead of
	// the reason. The frame changes nothing about the verdict — a refusal is
	// still a refusal — it only lets the refusal name itself. A child that dies
	// without writing the frame still closes the pipe wordless, and EOF remains
	// the honest answer for that.
	turnSupervisorFailure = "error:"
)

type turnSupervisorConfig struct {
	Path            string                `json:"path"`
	Args            []string              `json:"args"`
	Dir             string                `json:"dir"`
	Env             []string              `json:"env"`
	Isolation       ProcessIsolation      `json:"isolation"`
	IdentityLock    bool                  `json:"identityLock"`
	AuthorityDomain bool                  `json:"authorityDomain"`
	AuthorityOrigin string                `json:"authorityOrigin"`
	StandaloneOwner *agentStandaloneOwner `json:"standaloneOwner,omitempty"`
}

type linuxProcessIdentity struct {
	pid       int
	parentPID int
	state     byte
	startTime string
}

var (
	turnSupervisorExecutable        = os.Executable
	turnSupervisorMemfd             = unix.MemfdCreate
	turnSupervisorPipe              = os.Pipe
	turnSupervisorExit              = os.Exit
	turnSupervisorSignalNotify      = signal.Notify
	turnSupervisorSignalStop        = signal.Stop
	turnSupervisorEnable            = enableTurnSupervisor
	turnSupervisorCommand           = exec.Command
	turnSupervisorContain           = awaitLinuxSupervisorContainment
	turnSupervisorProcessID         = os.Getpid
	turnSupervisorSignalGroup       = signalProcessGroupID
	turnSupervisorWriteConfig       = writeTurnSupervisorConfig
	turnSupervisorDescendants       = linuxDescendants
	turnSupervisorIdentity          = readLinuxProcessIdentity
	turnSupervisorSignalPID         = signalLinuxIdentity
	turnSupervisorWait4             = unix.Wait4
	turnSupervisorSleep             = time.Sleep
	turnSupervisorProcRoot          = "/proc"
	turnSupervisorRun               = runTurnSupervisorGuardian
	turnSupervisorRunLiveness       = runTurnSupervisorLiveness
	turnSupervisorOpenFile          = os.NewFile
	turnSupervisorFcntl             = unix.FcntlInt
	turnSupervisorInput             = inheritedTurnSupervisorInput
	turnSupervisorPrctl             = unix.Prctl
	turnSupervisorSetrlimit         = unix.Setrlimit
	turnSupervisorAcquireStandalone = acquireAgentStandaloneIdentity
	turnSupervisorSealConfig        = unix.FcntlInt
	turnSupervisorEffectiveUID      = os.Geteuid
	turnSupervisorPoll              = unix.Poll
	turnSupervisorReadDeadline      = (*os.File).SetReadDeadline
)

func enableTurnSupervisor() error {
	if err := turnSupervisorSetrlimit(unix.RLIMIT_CORE, &unix.Rlimit{}); err != nil {
		return fmt.Errorf("disable Amp native core dumps: %w", err)
	}

	if err := turnSupervisorPrctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return err
	}

	if err := turnSupervisorPrctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return err
	}

	return turnSupervisorPrctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)
}

func inheritedTurnSupervisorInput() (io.ReadCloser, io.ReadCloser, io.WriteCloser, error) {
	config := turnSupervisorOpenFile(3, "amp-turn-supervisor-config")
	control := turnSupervisorOpenFile(4, "amp-turn-supervisor-control")

	ready := turnSupervisorOpenFile(5, "amp-turn-supervisor-ready")
	if config == nil || control == nil || ready == nil {
		return nil, nil, nil, errors.New("native supervisor inherited descriptors are unavailable")
	}

	for _, file := range []*os.File{config, control, ready} {
		if err := setTurnSupervisorCloseOnExec(file); err != nil {
			_ = config.Close()
			_ = control.Close()
			_ = ready.Close()

			return nil, nil, nil, err
		}
	}

	return config, control, ready, nil
}

func setTurnSupervisorCloseOnExec(file *os.File) error {
	flags, err := turnSupervisorFcntl(file.Fd(), unix.F_GETFD, 0)
	if err != nil {
		return fmt.Errorf("read inherited Amp supervisor descriptor flags: %w", err)
	}

	if _, err = turnSupervisorFcntl(file.Fd(), unix.F_SETFD, flags|unix.FD_CLOEXEC); err != nil {
		return fmt.Errorf("protect inherited Amp supervisor descriptor from exec: %w", err)
	}

	return nil
}

func init() {
	turnSupervisorBootstrap()
}

func turnSupervisorBootstrap() {
	mode := os.Getenv(turnSupervisorModeEnv)
	if mode != turnSupervisorMode && mode != turnSupervisorLivenessMode {
		return
	}

	var (
		err             error
		config, control io.ReadCloser
		ready           io.WriteCloser
	)

	config, control, ready, err = turnSupervisorInput()
	if err == nil {
		if mode == turnSupervisorLivenessMode {
			err = turnSupervisorRunLiveness(config, control, ready)
		} else {
			err = turnSupervisorRun(config, control, ready)
		}
	}

	if config != nil {
		_ = config.Close()
	}

	if control != nil {
		_ = control.Close()
	}

	if err != nil && ready != nil {
		_, _ = fmt.Fprintln(ready, turnSupervisorFailure+err.Error())
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

func prepareProcessTreeCommand(native *exec.Cmd, options processLaunchOptions) (*processTreeCommand, error) {
	if err := validateProcessIsolation(options.Isolation); err != nil {
		return nil, fmt.Errorf("prepare Amp native supervisor isolation: %w", err)
	}

	if err := validateTurnSupervisorIdentity(options.Isolation); err != nil {
		return nil, fmt.Errorf("prepare Amp native supervisor identity: %w", err)
	}

	if (options.Isolation.IdentityLock == nil) != (options.Isolation.AuthorityDomain == nil) {
		return nil, errors.New("prepare Amp native supervisor: UID lock and authority domain must be supplied together")
	}

	config := turnSupervisorConfig{
		Path:            native.Path,
		Args:            append([]string(nil), native.Args...),
		Dir:             native.Dir,
		Env:             append([]string(nil), native.Env...),
		Isolation:       *options.Isolation,
		IdentityLock:    options.Isolation.IdentityLock != nil,
		AuthorityDomain: options.Isolation.AuthorityDomain != nil,
	}

	// The origin travels in the sealed config so the guardian and the liveness
	// child inherit the one decision the parent made. Each of them re-derives it
	// from its own identity and refuses a config that disagrees, so the stamp
	// can direct the launch without being trusted on its own.
	switch {
	case config.IdentityLock:
		config.AuthorityOrigin = turnSupervisorOriginBorrowed
	case sharedProcessIdentity(options.Isolation):
		config.AuthorityOrigin = turnSupervisorOriginShared
	}

	if config.Path == "" || len(config.Args) == 0 {
		return nil, errors.New("prepare Amp native supervisor: native command is incomplete")
	}

	configFD, err := turnSupervisorMemfd(turnSupervisorFDName, unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("prepare Amp native supervisor config: %w", err)
	}

	configFile := os.NewFile(uintptr(configFD), turnSupervisorFDName)
	if writeErr := turnSupervisorWriteConfig(configFile, config); writeErr != nil {
		_ = configFile.Close()

		return nil, writeErr
	}

	if _, sealErr := turnSupervisorSealConfig(configFile.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); sealErr != nil {
		_ = configFile.Close()

		return nil, fmt.Errorf("seal Amp native supervisor config: %w", sealErr)
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

	completionRead, completionWrite, err := turnSupervisorPipe()
	if err != nil {
		_ = configFile.Close()
		_ = controlRead.Close()
		_ = controlWrite.Close()
		_ = readyRead.Close()
		_ = readyWrite.Close()

		return nil, fmt.Errorf("prepare Amp native supervisor completion: %w", err)
	}

	executable, err := turnSupervisorExecutable()
	if err != nil {
		_ = configFile.Close()
		_ = controlRead.Close()
		_ = controlWrite.Close()
		_ = readyRead.Close()
		_ = readyWrite.Close()
		_ = completionRead.Close()
		_ = completionWrite.Close()

		return nil, fmt.Errorf("resolve embedded Amp native supervisor: %w", err)
	}

	helper := turnSupervisorCommand(executable) // #nosec G204 -- the current executable hosts the private supervisor mode.
	helper.Dir = "/"
	helper.Env = turnSupervisorEnvironment()
	helper.Stdin = native.Stdin
	helper.Stdout = native.Stdout
	helper.Stderr = native.Stderr
	helper.ExtraFiles = []*os.File{configFile, controlRead, readyWrite, completionWrite}

	if options.Isolation.IdentityLock != nil {
		identityLock, duplicateErr := options.Isolation.IdentityLock.Duplicate()
		if duplicateErr != nil {
			_ = configFile.Close()
			_ = controlRead.Close()
			_ = controlWrite.Close()
			_ = readyRead.Close()
			_ = readyWrite.Close()
			_ = completionRead.Close()
			_ = completionWrite.Close()

			return nil, fmt.Errorf("duplicate Amp agent identity lock: %w", duplicateErr)
		}

		helper.ExtraFiles = append(helper.ExtraFiles, identityLock)

		authorityDomain, duplicateErr := options.Isolation.AuthorityDomain.Duplicate()
		if duplicateErr != nil {
			_ = identityLock.Close()
			_ = configFile.Close()
			_ = controlRead.Close()
			_ = controlWrite.Close()
			_ = readyRead.Close()
			_ = readyWrite.Close()
			_ = completionRead.Close()
			_ = completionWrite.Close()

			return nil, fmt.Errorf("duplicate Amp agent authority domain: %w", duplicateErr)
		}

		helper.ExtraFiles = append(helper.ExtraFiles, authorityDomain)
	}

	helper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	launch := &processTreeCommand{
		cmd:             helper,
		inherited:       append([]*os.File(nil), helper.ExtraFiles...),
		control:         controlWrite,
		ready:           readyRead,
		completion:      completionRead,
		nativeIsolation: true,
	}
	launch.wait = func() error {
		return awaitTurnSupervisorCompletion(helper.Wait, completionRead)
	}

	return launch, nil
}

func awaitProcessTreeReady(launch *processTreeCommand) error {
	if launch.ready == nil {
		return nil
	}

	defer func() {
		_ = launch.ready.Close()
		launch.ready = nil
	}()

	// This deadline is armed before the guardian child claims its standalone
	// identity and is only satisfied once the claim has completed, so it spans
	// the claim and must clear the claim budget. Proving an identity vacant
	// walks every task in the initial PID namespace until the set is stable,
	// twice, so its cost scales with the host. Naming agentStandaloneClaimMax
	// rather than repeating its value keeps the two from being tuned apart: a
	// shorter readiness bound would cancel a claim that was still making
	// progress and report a containment failure that never happened.
	if err := turnSupervisorReadDeadline(launch.ready, time.Now().Add(agentStandaloneClaimMax)); err != nil {
		return fmt.Errorf("arm Amp native supervisor readiness: %w", err)
	}

	line, err := bufio.NewReader(launch.ready).ReadString('\n')
	if err != nil {
		return fmt.Errorf("await Amp native supervisor readiness: %w", err)
	}

	if failure, ok := strings.CutPrefix(strings.TrimSpace(line), turnSupervisorFailure); ok {
		return fmt.Errorf("amp native supervisor failed before readiness: %s", failure)
	}

	if line != turnSupervisorReady {
		return fmt.Errorf("invalid Amp native supervisor readiness %q", strings.TrimSpace(line))
	}

	return nil
}

const (
	turnSupervisorCompleteLine = "complete\n"
	turnSupervisorDoneLine     = "done\n"
)

func awaitTurnSupervisorCompletion(wait func() error, completion *os.File) error {
	waitErr := wait()
	if completion == nil {
		return errors.Join(waitErr, ErrProcessContainmentIncomplete)
	}
	defer completion.Close()

	line, err := bufio.NewReader(completion).ReadString('\n')
	if err != nil {
		return errors.Join(
			waitErr,
			fmt.Errorf("%w: await Amp liveness completion proof: %v", ErrProcessContainmentIncomplete, err),
		)
	}

	if line != turnSupervisorCompleteLine {
		return errors.Join(
			waitErr,
			fmt.Errorf("%w: invalid Amp liveness completion proof %q", ErrProcessContainmentIncomplete, strings.TrimSpace(line)),
		)
	}

	return waitErr
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
	return turnSupervisorEnvironmentFor(turnSupervisorMode)
}

func turnSupervisorEnvironmentFor(mode string) []string {
	return []string{
		turnSupervisorModeEnv + "=" + mode,
	}
}

func startTurnSupervisorNative(
	native *exec.Cmd,
	isolation *ProcessIsolation,
	preStart func() error,
) (<-chan error, error, error) {
	var privilegeErr error

	waitDone, startErr := startCommandOnCreatorThread(func() error {
		if err := turnSupervisorEnable(); err != nil {
			privilegeErr = err

			return err
		}

		if err := applyProcessIsolation(native, isolation); err != nil {
			privilegeErr = fmt.Errorf("apply Amp native process isolation: %w", err)

			return privilegeErr
		}

		if preStart != nil {
			if err := preStart(); err != nil {
				privilegeErr = err

				return err
			}
		}

		return native.Start()
	}, native.Wait)

	if privilegeErr != nil {
		return nil, privilegeErr, nil
	}

	return waitDone, nil, startErr
}

func runTurnSupervisorGuardian(configInput io.Reader, controlInput io.Reader, readyOutput io.Writer) (runErr error) {
	completion := turnSupervisorOpenFile(6, "amp-turn-supervisor-completion")
	if completion == nil {
		return errors.New("amp guardian completion descriptor is unavailable")
	}
	defer completion.Close()

	if err := setTurnSupervisorCloseOnExec(completion); err != nil {
		return err
	}

	controlFile, ok := controlInput.(*os.File)
	if !ok {
		_, _ = completion.WriteString(turnSupervisorCompleteLine)

		return errors.New("amp guardian control input is not an inheritable file")
	}

	var config turnSupervisorConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		_, _ = completion.WriteString(turnSupervisorCompleteLine)

		return fmt.Errorf("decode Amp guardian config: %w", err)
	}

	if err := validateTurnSupervisorConfig(config); err != nil {
		_, _ = completion.WriteString(turnSupervisorCompleteLine)

		return err
	}

	signals := make(chan os.Signal, 2)

	turnSupervisorSignalNotify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer turnSupervisorSignalStop(signals)

	controlDone := make(chan struct{})

	go func() {
		_, _ = io.Copy(io.Discard, controlFile)

		close(controlDone)
	}()

	authority, err := acquireTurnSupervisorAuthority(config, 7, 8, controlDone, signals)
	if err != nil {
		_, _ = completion.WriteString(turnSupervisorCompleteLine)

		return err
	}
	defer func() { runErr = errors.Join(runErr, authority.Close()) }()

	if err = turnSupervisorEnable(); err != nil {
		_, _ = completion.WriteString(turnSupervisorCompleteLine)

		return fmt.Errorf("enable Amp guardian privileges: %w", err)
	}

	if err = validateTurnSupervisorAuthorityDisposition(config, authority); err != nil {
		containErr := turnSupervisorContain(turnSupervisorProcessID(), 0)
		if containErr == nil {
			_, _ = completion.WriteString(turnSupervisorCompleteLine)
		}

		return errors.Join(fmt.Errorf("validate Amp guardian identity disposition: %w", err), containErr)
	}

	liveness, data, peer, err := startTurnSupervisorLiveness(
		config, controlFile, completion, authority,
	)
	if err != nil {
		containErr := turnSupervisorContain(turnSupervisorProcessID(), 0)
		if containErr == nil {
			_, _ = completion.WriteString(turnSupervisorCompleteLine)
		}

		return errors.Join(err, containErr)
	}
	defer data.Close()
	defer peer.Close()

	waiter := startCommandWait(liveness.Wait)
	reader := bufio.NewReader(data)
	// This read sits behind the authority claim, not in front of it: the
	// guardian already holds its identity by the time it gets here, so nothing
	// this deadline covers can still be proving an identity vacant. It is a
	// liveness bound on a handshake with a child that is already running, and
	// deliberately not the standalone claim maximum.
	if err = turnSupervisorReadDeadline(data, time.Now().Add(5*time.Second)); err != nil {
		_ = peer.Close()
		waitErr, _ := waiter.await(context.Background())

		containErr := turnSupervisorContain(turnSupervisorProcessID(), 0)
		if containErr == nil {
			_, _ = completion.WriteString(turnSupervisorCompleteLine)
		}

		return errors.Join(err, waitErr, containErr)
	}

	line, readyErr := reader.ReadString('\n')
	if readyErr != nil {
		_ = peer.Close()
		waitErr, _ := waiter.await(context.Background())

		containErr := turnSupervisorContain(turnSupervisorProcessID(), 0)
		if containErr == nil {
			_, _ = completion.WriteString(turnSupervisorCompleteLine)
		}

		return errors.Join(fmt.Errorf("await Amp liveness readiness: %w", readyErr), waitErr, containErr)
	}

	if err = turnSupervisorReadDeadline(data, time.Time{}); err != nil {
		_ = peer.Close()

		return err
	}

	nativePID, err := parseTurnSupervisorLivenessReady(line)
	if err != nil {
		_ = peer.Close()
		waitErr, _ := waiter.await(context.Background())

		containErr := turnSupervisorContain(turnSupervisorProcessID(), 0)
		if containErr == nil {
			_, _ = completion.WriteString(turnSupervisorCompleteLine)
		}

		return errors.Join(err, waitErr, containErr)
	}

	if _, err = io.WriteString(readyOutput, turnSupervisorReady); err != nil {
		_ = peer.Close()
		waitErr, _ := waiter.await(context.Background())

		containErr := turnSupervisorContain(turnSupervisorProcessID(), nativePID)
		if containErr == nil {
			_, _ = completion.WriteString(turnSupervisorCompleteLine)
		}

		return errors.Join(err, waitErr, containErr)
	}

	var waitErr error

	for {
		select {
		case <-waiter.done:
			waitErr = waiter.err

			goto livenessExited
		case <-controlDone:
			_ = peer.Close()
			waitErr, _ = waiter.await(context.Background())

			goto livenessExited
		case received := <-signals:
			nativeSignal, signalOK := received.(syscall.Signal)
			if signalOK {
				_ = signalProcessGroupID(liveness.Process.Pid, nativeSignal)
			}
		}
	}

livenessExited:
	doneLine, doneErr := reader.ReadString('\n')

	if doneErr == nil && doneLine == turnSupervisorDoneLine {
		return waitErr
	}

	containErr := turnSupervisorContain(turnSupervisorProcessID(), nativePID)
	if containErr == nil {
		_, _ = completion.WriteString(turnSupervisorCompleteLine)
	}

	return errors.Join(waitErr, fmt.Errorf("amp liveness exited without completion report: %v", doneErr), containErr)
}

type turnSupervisorAuthority struct {
	identity   *agentIdentityLock
	domain     *agentIdentityLock
	standalone *agentStandaloneIdentity
}

func validateTurnSupervisorAuthorityDisposition(
	config turnSupervisorConfig,
	authority *turnSupervisorAuthority,
) error {
	testOnly := config.Isolation.TestOnlyNoCredential || config.Isolation.TestOnlyIdentityLockRoot != ""
	if authority != nil && authority.standalone != nil {
		return validateStandaloneAgentIdentityDisposition(
			authority.standalone.owner, testOnly, config.Isolation.TestOnlyIdentityLockRoot,
		)
	}

	return validateTurnSupervisorConfigDisposition(config, testOnly)
}

func validateTurnSupervisorConfigDisposition(config turnSupervisorConfig, testOnly bool) error {
	switch config.AuthorityOrigin {
	case "":
		return nil
	case turnSupervisorOriginShared:
		// There is no durable disposition to re-read: a shared identity claims
		// no owner binding, writes no marker, and leaves no temporaries behind.
		return nil
	case turnSupervisorOriginBorrowed:
		return validateBorrowedAgentIdentityDisposition(
			config.Isolation.UID, config.Isolation.GID, testOnly, config.Isolation.TestOnlyIdentityLockRoot,
		)
	case turnSupervisorOriginStandalone:
		if config.StandaloneOwner == nil {
			return errors.New("amp standalone authority owner tuple is unavailable")
		}

		return validateStandaloneAgentIdentityDisposition(
			*config.StandaloneOwner, testOnly, config.Isolation.TestOnlyIdentityLockRoot,
		)
	default:
		return fmt.Errorf("amp authority origin %q is invalid", config.AuthorityOrigin)
	}
}

func (authority *turnSupervisorAuthority) Close() error {
	if authority == nil {
		return nil
	}

	if authority.standalone != nil {
		return authority.standalone.Close()
	}

	return errors.Join(authority.identity.Close(), authority.domain.Close())
}

func acquireTurnSupervisorAuthority(
	config turnSupervisorConfig,
	identityFD uintptr,
	domainFD uintptr,
	canceled <-chan struct{},
	signals <-chan os.Signal,
) (*turnSupervisorAuthority, error) {
	// A shared identity carries no authority. The durable registry records who
	// may enter an identity nobody is in, and the supervisor is already in this
	// one, so there is nothing to claim, adopt, publish or release.
	if config.AuthorityOrigin == turnSupervisorOriginShared {
		return &turnSupervisorAuthority{identity: &agentIdentityLock{}, domain: &agentIdentityLock{}}, nil
	}

	if !config.IdentityLock && config.Isolation.TestOnlyNoCredential &&
		config.Isolation.StandaloneOwnerID == "" && config.Isolation.StandaloneStateRoot == "" {
		return &turnSupervisorAuthority{
			identity: &agentIdentityLock{}, domain: &agentIdentityLock{},
		}, nil
	}

	if config.IdentityLock {
		identity, err := adoptAgentIdentityLock(
			turnSupervisorOpenFile(identityFD, "amp-agent-identity-lock"),
			config.Isolation.UID,
			config.Isolation.TestOnlyNoCredential || config.Isolation.TestOnlyIdentityLockRoot != "",
			config.Isolation.TestOnlyIdentityLockRoot,
		)
		if err != nil {
			return nil, fmt.Errorf("adopt Amp agent identity lock: %w", err)
		}

		domain, err := adoptAgentAuthorityDomain(
			turnSupervisorOpenFile(domainFD, "amp-agent-authority-domain"),
			config.Isolation.TestOnlyNoCredential || config.Isolation.TestOnlyIdentityLockRoot != "",
			config.Isolation.TestOnlyIdentityLockRoot,
		)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("adopt Amp agent authority domain: %w", err), identity.Close())
		}

		return &turnSupervisorAuthority{identity: identity, domain: domain}, nil
	}

	standalone, err := turnSupervisorAcquireStandalone(
		config.Isolation.UID,
		config.Isolation.GID,
		config.Isolation.StandaloneOwnerID,
		config.Isolation.StandaloneStateRoot,
		config.Isolation.TestOnlyNoCredential,
		config.Isolation.TestOnlyIdentityLockRoot,
		canceled,
		signals,
	)
	if err != nil {
		return nil, fmt.Errorf("acquire Amp standalone agent identity authority: %w", err)
	}

	return &turnSupervisorAuthority{
		identity: standalone.identity, domain: standalone.authority, standalone: standalone,
	}, nil
}

func startTurnSupervisorLiveness(
	config turnSupervisorConfig,
	control *os.File,
	completion *os.File,
	authority *turnSupervisorAuthority,
) (*exec.Cmd, *os.File, *os.File, error) {
	var identity, domain *agentIdentityLock
	if authority != nil {
		identity = authority.identity
		domain = authority.domain
	}

	borrowedAuthority := identity != nil && identity.file != nil && domain != nil && domain.file != nil

	// The liveness child re-derives the shared decision from its own identity
	// and refuses a config that disagrees, so the origin has to survive the
	// re-stamp the borrowed and standalone legs perform on their way down.
	shared := config.AuthorityOrigin == turnSupervisorOriginShared
	config.IdentityLock = borrowedAuthority
	config.AuthorityDomain = borrowedAuthority
	config.AuthorityOrigin = ""
	config.StandaloneOwner = nil

	switch {
	case borrowedAuthority && authority.standalone != nil:
		owner := authority.standalone.owner
		config.AuthorityOrigin = turnSupervisorOriginStandalone
		config.StandaloneOwner = &owner
	case borrowedAuthority:
		config.AuthorityOrigin = turnSupervisorOriginBorrowed
	case shared:
		config.AuthorityOrigin = turnSupervisorOriginShared
	}

	config.Isolation.IdentityLock = nil
	config.Isolation.AuthorityDomain = nil
	config.Isolation.StandaloneOwnerID = ""
	config.Isolation.StandaloneStateRoot = ""

	configFD, err := turnSupervisorMemfd(turnSupervisorFDName+"-liveness", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, nil, nil, err
	}

	configFile := os.NewFile(uintptr(configFD), turnSupervisorFDName+"-liveness")
	if err = turnSupervisorWriteConfig(configFile, config); err != nil {
		_ = configFile.Close()

		return nil, nil, nil, err
	}

	if _, err = turnSupervisorSealConfig(
		configFile.Fd(), unix.F_ADD_SEALS,
		unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL,
	); err != nil {
		_ = configFile.Close()

		return nil, nil, nil, err
	}

	dataRead, dataWrite, err := turnSupervisorPipe()
	if err != nil {
		_ = configFile.Close()

		return nil, nil, nil, err
	}

	peerRead, peerWrite, err := turnSupervisorPipe()
	if err != nil {
		_ = configFile.Close()
		_ = dataRead.Close()
		_ = dataWrite.Close()

		return nil, nil, nil, err
	}

	var identityDuplicate *os.File
	if borrowedAuthority {
		identityDuplicate, err = identity.Duplicate()
	} else {
		identityDuplicate, err = os.Open("/dev/null")
	}

	if err != nil {
		_ = configFile.Close()
		_ = dataRead.Close()
		_ = dataWrite.Close()
		_ = peerRead.Close()
		_ = peerWrite.Close()

		return nil, nil, nil, err
	}

	var domainDuplicate *os.File
	if borrowedAuthority {
		domainDuplicate, err = domain.Duplicate()
	} else {
		domainDuplicate, err = os.Open("/dev/null")
	}

	if err != nil {
		_ = identityDuplicate.Close()
		_ = configFile.Close()
		_ = dataRead.Close()
		_ = dataWrite.Close()
		_ = peerRead.Close()
		_ = peerWrite.Close()

		return nil, nil, nil, err
	}

	executable, err := turnSupervisorExecutable()
	if err != nil {
		_ = identityDuplicate.Close()
		_ = domainDuplicate.Close()
		_ = configFile.Close()
		_ = dataRead.Close()
		_ = dataWrite.Close()
		_ = peerRead.Close()
		_ = peerWrite.Close()

		return nil, nil, nil, err
	}

	liveness := turnSupervisorCommand(executable)
	liveness.Dir = "/"
	liveness.Env = turnSupervisorEnvironmentFor(turnSupervisorLivenessMode)
	liveness.Stdin = os.Stdin
	liveness.Stdout = os.Stdout
	liveness.Stderr = os.Stderr
	liveness.ExtraFiles = []*os.File{
		configFile, control, dataWrite, identityDuplicate, domainDuplicate, completion, peerRead,
	}

	liveness.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = liveness.Start(); err != nil {
		_ = identityDuplicate.Close()
		_ = domainDuplicate.Close()
		_ = configFile.Close()
		_ = dataRead.Close()
		_ = dataWrite.Close()
		_ = peerRead.Close()
		_ = peerWrite.Close()

		return nil, nil, nil, err
	}

	for _, file := range []*os.File{configFile, dataWrite, identityDuplicate, domainDuplicate, peerRead} {
		_ = file.Close()
	}

	return liveness, dataRead, peerWrite, nil
}

func parseTurnSupervisorLivenessReady(line string) (int, error) {
	text, ok := strings.CutSuffix(line, "\n")
	if !ok {
		return 0, errors.New("amp liveness readiness is not newline terminated")
	}

	if failure, failed := strings.CutPrefix(text, turnSupervisorFailure); failed {
		return 0, fmt.Errorf("amp liveness failed before readiness: %s", failure)
	}

	pidText, ok := strings.CutPrefix(text, "ready:")
	if !ok {
		return 0, fmt.Errorf("invalid Amp liveness readiness %q", text)
	}

	pid, err := strconv.Atoi(pidText)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("invalid Amp liveness native pid %q", pidText)
	}

	return pid, nil
}

func validateTurnSupervisorAuthorityOrigin(config turnSupervisorConfig) error {
	switch config.AuthorityOrigin {
	case "":
		if config.IdentityLock || config.StandaloneOwner != nil {
			return errors.New("amp native supervisor inherited authority origin is required")
		}
	case turnSupervisorOriginShared:
		if config.IdentityLock || config.StandaloneOwner != nil ||
			config.Isolation.StandaloneOwnerID != "" || config.Isolation.StandaloneStateRoot != "" {
			return errors.New("amp native supervisor shared authority origin is inconsistent")
		}
	case turnSupervisorOriginBorrowed:
		if !config.IdentityLock || config.StandaloneOwner != nil {
			return errors.New("amp native supervisor borrowed authority origin is inconsistent")
		}
	case turnSupervisorOriginStandalone:
		owner := config.StandaloneOwner
		if !config.IdentityLock || owner == nil || owner.Version != 1 || owner.UID != config.Isolation.UID ||
			owner.GID != config.Isolation.GID || owner.Kind != agentStandaloneOwnerKind ||
			!knownAgentStandaloneProvider(owner.Provider) || owner.OwnerID == "" ||
			!filepath.IsAbs(owner.StateRoot.Path) || filepath.Clean(owner.StateRoot.Path) != owner.StateRoot.Path ||
			owner.StateRoot.Dev == 0 || owner.StateRoot.Ino == 0 {
			return errors.New("amp native supervisor standalone authority origin is inconsistent")
		}
	default:
		return fmt.Errorf("amp native supervisor authority origin %q is invalid", config.AuthorityOrigin)
	}

	return nil
}

func validateTurnSupervisorConfig(config turnSupervisorConfig) error {
	if config.Path == "" || len(config.Args) == 0 {
		return errors.New("amp native supervisor config is incomplete")
	}

	if config.IdentityLock != config.AuthorityDomain {
		return errors.New("amp native supervisor identity lock and authority domain must be provided together")
	}

	// Every process in the tree derives the origin from its own identity, and a
	// child that disagrees with the config it was handed refuses rather than
	// following it: the stamp decides which steps run, so a stamp that does not
	// describe the process running them can only be wrong.
	if sharedProcessIdentity(&config.Isolation) != (config.AuthorityOrigin == turnSupervisorOriginShared) {
		return errors.New("amp native supervisor authority origin does not match the identity it runs as")
	}

	if err := validateTurnSupervisorAuthorityOrigin(config); err != nil {
		return err
	}

	validation := config.Isolation
	if config.IdentityLock {
		placeholder := &agentIdentityLock{}
		validation.IdentityLock = placeholder
		validation.AuthorityDomain = placeholder
	}

	if err := validateProcessIsolation(&validation); err != nil {
		return fmt.Errorf("validate Amp native supervisor isolation: %w", err)
	}

	if err := validateTurnSupervisorIdentity(&config.Isolation); err != nil {
		return fmt.Errorf("validate Amp native supervisor identity: %w", err)
	}

	return nil
}

func runTurnSupervisorLiveness(configInput io.Reader, controlInput io.Reader, readyOutput io.Writer) error {
	completion := turnSupervisorOpenFile(8, "amp-turn-supervisor-completion")

	peer := turnSupervisorOpenFile(9, "amp-turn-supervisor-guardian-peer")
	if completion == nil || peer == nil {
		if completion != nil {
			_ = completion.Close()
		}

		if peer != nil {
			_ = peer.Close()
		}

		return errors.New("amp liveness inherited descriptors are unavailable")
	}
	defer completion.Close()
	defer peer.Close()

	if err := setTurnSupervisorCloseOnExec(completion); err != nil {
		return err
	}

	if err := setTurnSupervisorCloseOnExec(peer); err != nil {
		return err
	}

	return runTurnSupervisorNative(
		configInput, []io.Reader{controlInput}, peer, readyOutput, completion, 6, 7, true,
	)
}

// acquireTurnSupervisorNativeAuthority establishes the agent identity authority this
// launch runs under: the pair the caller borrowed and passed down, the empty
// pair the credential-free tests use, or a standalone identity claimed here.
// The standalone result is returned alongside so the caller can release the
// whole claim rather than the two locks it lends out.
func acquireTurnSupervisorNativeAuthority(
	config turnSupervisorConfig,
	identityFD uintptr,
	authorityFD uintptr,
	controlDone <-chan struct{},
	signals <-chan os.Signal,
) (*agentIdentityLock, *agentIdentityLock, *agentStandaloneIdentity, error) {
	switch {
	case config.AuthorityOrigin == turnSupervisorOriginShared:
		// Nothing to adopt and nothing to claim: the launch never leaves the
		// identity this process was started under.
		return &agentIdentityLock{}, &agentIdentityLock{}, nil, nil
	case config.IdentityLock:
		identityLock, adoptErr := adoptAgentIdentityLock(
			turnSupervisorOpenFile(identityFD, "amp-agent-identity-lock"),
			config.Isolation.UID,
			config.Isolation.TestOnlyNoCredential || config.Isolation.TestOnlyIdentityLockRoot != "",
			config.Isolation.TestOnlyIdentityLockRoot,
		)
		if adoptErr != nil {
			return nil, nil, nil, fmt.Errorf("adopt Amp agent identity lock: %w", adoptErr)
		}

		authorityDomain, domainErr := adoptAgentAuthorityDomain(
			turnSupervisorOpenFile(authorityFD, "amp-agent-authority-domain"),
			config.Isolation.TestOnlyNoCredential || config.Isolation.TestOnlyIdentityLockRoot != "",
			config.Isolation.TestOnlyIdentityLockRoot,
		)
		if domainErr != nil {
			return nil, nil, nil, errors.Join(fmt.Errorf("adopt Amp agent authority domain: %w", domainErr), identityLock.Close())
		}

		return identityLock, authorityDomain, nil, nil
	case config.Isolation.TestOnlyNoCredential &&
		config.Isolation.StandaloneOwnerID == "" && config.Isolation.StandaloneStateRoot == "":
		return &agentIdentityLock{}, &agentIdentityLock{}, nil, nil
	default:
		standalone, acquireErr := turnSupervisorAcquireStandalone(
			config.Isolation.UID,
			config.Isolation.GID,
			config.Isolation.StandaloneOwnerID,
			config.Isolation.StandaloneStateRoot,
			config.Isolation.TestOnlyNoCredential,
			config.Isolation.TestOnlyIdentityLockRoot,
			controlDone,
			signals,
		)
		if acquireErr != nil {
			return nil, nil, nil, fmt.Errorf("acquire Amp standalone agent identity authority: %w", acquireErr)
		}

		return standalone.identity, standalone.authority, standalone, nil
	}
}

func runTurnSupervisorNative(
	configInput io.Reader,
	controlInputs []io.Reader,
	guardianPeer *os.File,
	readyOutput io.Writer,
	completionOutput io.Writer,
	identityFD uintptr,
	authorityFD uintptr,
	publishCompletion bool,
) (runErr error) {
	var config turnSupervisorConfig
	if err := json.NewDecoder(configInput).Decode(&config); err != nil {
		return fmt.Errorf("decode Amp native supervisor config: %w", err)
	}

	if err := validateTurnSupervisorConfig(config); err != nil {
		return err
	}

	signals := make(chan os.Signal, 2)

	turnSupervisorSignalNotify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer turnSupervisorSignalStop(signals)

	controlDone := make(chan struct{})

	var controlOnce sync.Once

	for _, controlInput := range controlInputs {
		go func(input io.Reader) {
			_, _ = io.Copy(io.Discard, input)

			controlOnce.Do(func() { close(controlDone) })
		}(controlInput)
	}

	guardianDone := make(chan struct{})

	if guardianPeer != nil {
		go func() {
			_, _ = io.Copy(io.Discard, guardianPeer)

			close(guardianDone)
			controlOnce.Do(func() { close(controlDone) })
		}()
	}

	identityLock, authorityDomain, standalone, authorityErr := acquireTurnSupervisorNativeAuthority(
		config, identityFD, authorityFD, controlDone, signals,
	)
	if authorityErr != nil {
		return authorityErr
	}

	defer func() {
		if standalone != nil {
			runErr = errors.Join(runErr, standalone.Close())

			return
		}

		runErr = errors.Join(runErr, identityLock.Close(), authorityDomain.Close())
	}()

	if identityLock == nil || authorityDomain == nil {
		return errors.New("amp agent identity authority is incomplete")
	}

	contained := false

	if publishCompletion {
		defer func() {
			if !contained {
				return
			}

			if _, err := io.WriteString(completionOutput, "complete\n"); err != nil {
				runErr = errors.Join(runErr, fmt.Errorf("publish Amp liveness completion: %w", err))
			}

			_, _ = io.WriteString(readyOutput, "done\n")
		}()
	}

	native := turnSupervisorCommand(config.Path, config.Args[1:]...) // #nosec G204 -- private config was built from the operator-selected Amp command.
	native.Args = append([]string(nil), config.Args...)
	native.Dir = config.Dir
	native.Env = append([]string(nil), config.Env...)
	native.Stdin = os.Stdin
	native.Stdout = os.Stdout
	native.Stderr = os.Stderr
	configureCommand(native)

	nativeIsolation := config.Isolation
	nativeIsolation.IdentityLock = identityLock
	nativeIsolation.AuthorityDomain = authorityDomain
	nativeIsolation.StandaloneOwnerID = ""
	nativeIsolation.StandaloneStateRoot = ""

	if err := validateTurnSupervisorGuardianPeer(guardianPeer, guardianDone); err != nil {
		containErr := turnSupervisorContain(turnSupervisorProcessID(), 0)
		contained = containErr == nil

		return errors.Join(err, containErr)
	}

	var lateValidationErr error

	waitDone, enableErr, startErr := startTurnSupervisorNative(native, &nativeIsolation, func() error {
		if config.AuthorityOrigin != "" {
			testOnly := config.Isolation.TestOnlyNoCredential || config.Isolation.TestOnlyIdentityLockRoot != ""

			lateValidationErr = validateTurnSupervisorConfigDisposition(config, testOnly)
			if lateValidationErr != nil {
				return lateValidationErr
			}
		}

		lateValidationErr = validateTurnSupervisorGuardianPeer(guardianPeer, guardianDone)

		return lateValidationErr
	})

	if lateValidationErr != nil {
		containErr := turnSupervisorContain(turnSupervisorProcessID(), 0)
		contained = containErr == nil

		return errors.Join(lateValidationErr, containErr)
	}

	if enableErr != nil {
		return fmt.Errorf("enable Amp native supervisor privileges: %w", enableErr)
	}

	if startErr != nil {
		return fmt.Errorf("start supervised Amp native root: %w", startErr)
	}

	ready := turnSupervisorReady
	if publishCompletion {
		ready = fmt.Sprintf("ready:%d\n", native.Process.Pid)
	}

	if _, err := io.WriteString(readyOutput, ready); err != nil {
		_ = turnSupervisorSignalGroup(native.Process.Pid, syscall.SIGKILL)
		waitErr := <-waitDone
		containErr := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid)
		contained = containErr == nil

		return errors.Join(fmt.Errorf("publish Amp native supervisor readiness: %w", err), containErr, waitErr)
	}

	for {
		select {
		case waitErr := <-waitDone:
			if err := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid); err != nil {
				return err
			}

			contained = true

			return waitErr
		case <-controlDone:
			_ = turnSupervisorSignalGroup(native.Process.Pid, syscall.SIGKILL)
			waitErr := <-waitDone

			if err := turnSupervisorContain(turnSupervisorProcessID(), native.Process.Pid); err != nil {
				return err
			}

			contained = true

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

func validateTurnSupervisorGuardianPeer(peer *os.File, done <-chan struct{}) error {
	if peer == nil {
		return nil
	}

	select {
	case <-done:
		return errors.New("amp guardian exited before native launch")
	default:
	}

	poll := []unix.PollFd{{
		Fd:     pollFD(peer),
		Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR,
	}}

	ready, err := turnSupervisorPoll(poll, 0)
	if err != nil {
		return fmt.Errorf("poll Amp guardian before native launch: %w", err)
	}

	if ready != 0 || poll[0].Revents != 0 {
		return errors.New("amp guardian exited before native launch")
	}

	return nil
}

func validateTurnSupervisorIdentity(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation is required")
	}

	// The supervisor drops privilege to reach the native identity, so it has to
	// hold a higher one first. When the native identity is the one it already
	// runs as there is no descent to make, and demanding root would refuse the
	// only launch such a deployment can perform.
	if sharedProcessIdentity(isolation) {
		return nil
	}

	effectiveUID := turnSupervisorEffectiveUID()
	if effectiveUID != 0 {
		return fmt.Errorf("trusted root identity is required, effective uid is %d", effectiveUID)
	}

	if isolation.UID == uint32(effectiveUID) {
		return errors.New("native target identity must differ from the trusted supervisor")
	}

	return nil
}

// awaitLinuxSupervisorContainment never lets the dedicated subreaper exit on
// an incomplete tree. The adapter retains the managed-root permit when its bounded
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
	if nativePID > 0 {
		_ = turnSupervisorSignalGroup(nativePID, syscall.SIGKILL)
	}

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
			return fmt.Errorf("%w: reap supervised Amp descendants: %v", ErrProcessContainmentIncomplete, waitErr)
		case waited < 0:
			return fmt.Errorf("%w: invalid supervised Amp wait result %d", ErrProcessContainmentIncomplete, waited)
		}

		descendants, err := turnSupervisorDescendants(supervisorPID)
		if err != nil {
			return fmt.Errorf("%w: enumerate supervised Amp descendants: %v", ErrProcessContainmentIncomplete, err)
		}

		for _, descendant := range descendants {
			if descendant.state != 'Z' {
				if err := turnSupervisorSignalPID(descendant, syscall.SIGKILL); err != nil {
					return fmt.Errorf("%w: kill supervised Amp descendant %d: %v", ErrProcessContainmentIncomplete, descendant.pid, err)
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

// Seam for the fail-closed guard in pollFD. Linux hands out small descriptors,
// so the guard is unreachable through a real *os.File; tests swap this to reach it.
var pollFDSource = (*os.File).Fd

// pollFD narrows a descriptor to the int32 unix.PollFd carries. Linux hands out
// small non-negative descriptors, so the guard never fires; when the value
// cannot be represented it yields -1, which poll reports as EBADF rather than
// aliasing onto a live descriptor.
func pollFD(file *os.File) int32 {
	fd := pollFDSource(file)
	if fd > math.MaxInt32 {
		return -1
	}

	return int32(fd)
}
