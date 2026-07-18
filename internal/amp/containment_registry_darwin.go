//go:build darwin

package amp

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	darwinRegistryDir        = "acp-go-amp-containment"
	darwinRecordFormat       = 1
	darwinRecordLimit        = 4096
	darwinRecordLifetime     = 30 * 24 * time.Hour
	darwinVendor             = "amp"
	darwinBestEffortMode     = "best_effort"
	darwinStateRunning       = "running"
	darwinStateGroupAbsent   = "group_absent"
	darwinStateIncomplete    = "cleanup_incomplete"
	darwinLifecyclePrompt    = "prompt"
	darwinContainmentWarning = "PID-by-PID cleanup has a PID-reuse time-of-check/time-of-use race and can signal an unrelated reused PID; correlation is not ownership or proof of absence; inherited markers can be scrubbed"
)

var (
	darwinContainmentCandidates = correlatedDarwinCandidates
	darwinContainmentRevalidate = revalidateDarwinCandidate
	darwinContainmentSignalPID  = syscall.Kill
	darwinCleanupNow            = time.Now
	darwinCleanupSleep          = time.Sleep
	darwinRuntimeIDRead         = rand.Read
	darwinProcessIdentityLookup = currentDarwinProcessIdentity
	darwinEnvironmentLookup     = darwinProcessEnvironment
	darwinRemoveGenerationRoot  = os.RemoveAll
	darwinRecordAbs             = filepath.Abs
	darwinRegistryMkdirAll      = os.MkdirAll
	darwinRegistryLstat         = os.Lstat
	darwinRegistryChmod         = os.Chmod
	darwinRegistryReadDir       = os.ReadDir
	darwinRegistryRemove        = os.Remove
	darwinRegistryOpen          = unix.Open
	darwinRegistryFlock         = unix.Flock
	darwinRecordMarshal         = json.Marshal
	darwinRecordStat            = os.Stat
	darwinRecordCreateTemp      = os.CreateTemp
	darwinRecordFileChmod       = func(file *os.File, mode os.FileMode) error { return file.Chmod(mode) }
	darwinRecordFileWrite       = func(file *os.File, data []byte) error {
		_, err := file.Write(data)

		return err
	}
	darwinRecordFileSync    = func(file *os.File) error { return file.Sync() }
	darwinRecordFileClose   = func(file *os.File) error { return file.Close() }
	darwinRecordFileStat    = func(file *os.File) (os.FileInfo, error) { return file.Stat() }
	darwinRecordRename      = os.Rename
	darwinKinfoProc         = unix.SysctlKinfoProc
	darwinKinfoProcSlice    = unix.SysctlKinfoProcSlice
	darwinProcargsRaw       = unix.SysctlRaw
	darwinEvalSymlinks      = filepath.EvalSymlinks
	darwinIdentitySameUID   = func(identity darwinProcessIdentity) bool { return identity.UID == uint32(os.Getuid()) }
	darwinRegistryProcessMu sync.Mutex
)

type darwinProcessIdentity struct {
	PID       int
	StartSec  int64
	StartUsec int32
	UID       uint32
}

type darwinContainmentRecord struct {
	SchemaVersion        int    `json:"schema_version"` //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	Vendor               string `json:"vendor"`
	Containment          string `json:"containment"`
	LifecycleKind        string `json:"lifecycle_kind"`                    //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	RuntimeID            string `json:"runtime_id"`                        //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	GenerationRoot       string `json:"generation_root"`                   //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	WrapperPID           int    `json:"wrapper_pid"`                       //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	WrapperStartSec      int64  `json:"wrapper_start_sec"`                 //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	WrapperStartUsec     int32  `json:"wrapper_start_usec"`                //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	DirectChildPID       *int   `json:"direct_child_pid,omitempty"`        //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	DirectChildStartSec  *int64 `json:"direct_child_start_sec,omitempty"`  //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	DirectChildStartUsec *int32 `json:"direct_child_start_usec,omitempty"` //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	OriginalPGID         *int   `json:"original_pgid,omitempty"`           //nolint:tagliatelle // Registry schema is a fixed snake_case contract.
	State                string `json:"state"`
}

type darwinDiagnosticRecord struct {
	RuntimeID      string `json:"runtime_id"` //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	State          string `json:"state"`
	GenerationRoot string `json:"generation_root"` //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	CorrelatedPIDs []int  `json:"correlated_pids"` //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	AmbiguousPIDs  []int  `json:"ambiguous_pids"`  //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
}

type darwinDiagnosticReport struct {
	Vendor        string                   `json:"vendor"`
	Containment   string                   `json:"containment"`
	ScratchParent string                   `json:"scratch_parent"` //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	Warning       string                   `json:"warning"`
	Records       []darwinDiagnosticRecord `json:"records"`
}

type darwinCleanupReport struct {
	Vendor                  string `json:"vendor"`
	Containment             string `json:"containment"`
	ScratchParent           string `json:"scratch_parent"` //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	Warning                 string `json:"warning"`
	RuntimeID               string `json:"runtime_id"`                //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	GenerationRoot          string `json:"generation_root"`           //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	TermSignaledPIDs        []int  `json:"term_signaled_pids"`        //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	KillSignaledPIDs        []int  `json:"kill_signaled_pids"`        //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	RemainingCorrelatedPIDs []int  `json:"remaining_correlated_pids"` //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	AmbiguousPIDs           []int  `json:"ambiguous_pids"`            //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
	RootRemoved             bool   `json:"root_removed"`              //nolint:tagliatelle // Operator JSON is a fixed snake_case contract.
}

type darwinCleanupState struct {
	record          darwinContainmentRecord
	deadline        time.Time
	deadlineExpired bool
	ambiguous       map[int]struct{}
	termIdentities  map[DarwinContainmentCandidate]struct{}
	termSignaled    []int
	killSignaled    []int
}

type darwinGenerationRootValidation struct {
	path     string
	identity os.FileInfo
	exists   bool
}

func NewDarwinGenerationRecord(scratchParent, generationRoot, lifecycle string) (*DarwinGeneration, error) {
	parent, err := darwinRecordAbs(scratchParent)
	if err != nil {
		return nil, fmt.Errorf("resolve containment scratch parent: %w", err)
	}

	root, err := darwinRecordAbs(generationRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve containment generation root: %w", err)
	}

	if !pathWithin(parent, root) {
		return nil, errors.New("containment generation root is outside the scratch parent")
	}

	runtimeID, err := randomRuntimeID()
	if err != nil {
		return nil, err
	}

	wrapper, err := darwinProcessIdentityLookup(os.Getpid())
	if err != nil {
		return nil, fmt.Errorf("read wrapper process identity: %w", err)
	}

	record := darwinContainmentRecord{
		SchemaVersion:    darwinRecordFormat,
		Vendor:           darwinVendor,
		Containment:      darwinBestEffortMode,
		LifecycleKind:    lifecycle,
		RuntimeID:        runtimeID,
		GenerationRoot:   root,
		WrapperPID:       wrapper.PID,
		WrapperStartSec:  wrapper.StartSec,
		WrapperStartUsec: wrapper.StartUsec,
		State:            darwinStateRunning,
	}
	if err := createDarwinRecord(parent, record); err != nil {
		return nil, err
	}

	return &DarwinGeneration{
		RuntimeID:   runtimeID,
		ScratchRoot: root,
		RecordStarted: func(pid, pgid int) error {
			identity, identityErr := darwinProcessIdentityLookup(pid)
			if identityErr != nil {
				return fmt.Errorf("read direct-child identity: %w", identityErr)
			}

			record.DirectChildPID = &identity.PID
			record.DirectChildStartSec = &identity.StartSec
			record.DirectChildStartUsec = &identity.StartUsec
			record.OriginalPGID = &pgid

			return replaceDarwinRecord(parent, record)
		},
		RecordFinished: func(complete bool) error {
			if complete {
				record.State = darwinStateGroupAbsent
			} else {
				record.State = darwinStateIncomplete
			}

			return replaceDarwinRecord(parent, record)
		},
	}, nil
}

func randomRuntimeID() (string, error) {
	var value [16]byte
	if _, err := darwinRuntimeIDRead(value[:]); err != nil {
		return "", fmt.Errorf("generate containment runtime id: %w", err)
	}

	return hex.EncodeToString(value[:]), nil
}

func createDarwinRecord(parent string, record darwinContainmentRecord) error {
	registry, err := ensureDarwinRegistry(parent)
	if err != nil {
		return err
	}

	lock, err := lockDarwinRegistry(registry)
	if err != nil {
		return err
	}
	defer unlockDarwinRegistry(lock)

	entries, err := darwinRegistryReadDir(registry)
	if err != nil {
		return fmt.Errorf("read containment registry: %w", err)
	}

	now := time.Now()
	count := 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		info, infoErr := entry.Info()
		if infoErr != nil {
			return fmt.Errorf("inspect containment record: %w", infoErr)
		}

		if now.Sub(info.ModTime()) > darwinRecordLifetime {
			runtimeID := strings.TrimSuffix(entry.Name(), ".json")

			_, expiredRecords, readErr := readDarwinRecords(parent, runtimeID)
			if readErr != nil {
				return fmt.Errorf("inspect expired containment record: %w", readErr)
			}

			if len(expiredRecords) != 1 {
				return errors.New("inspect expired containment record: selected record is unavailable or ambiguous")
			}

			if expiredRecords[0].State != darwinStateGroupAbsent {
				count++

				continue
			}

			if removeErr := darwinRegistryRemove(filepath.Join(registry, entry.Name())); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return fmt.Errorf("expire containment record: %w", removeErr)
			}

			continue
		}

		count++
	}

	if count >= darwinRecordLimit {
		return errors.New("darwin containment registry is full")
	}

	return writeDarwinRecordAtomic(registry, record, true)
}

func replaceDarwinRecord(parent string, record darwinContainmentRecord) error {
	registry, err := ensureDarwinRegistry(parent)
	if err != nil {
		return err
	}

	lock, err := lockDarwinRegistry(registry)
	if err != nil {
		return err
	}
	defer unlockDarwinRegistry(lock)

	return writeDarwinRecordAtomic(registry, record, false)
}

func ensureDarwinRegistry(parent string) (string, error) {
	registry := filepath.Join(parent, darwinRegistryDir)
	if err := darwinRegistryMkdirAll(registry, 0o700); err != nil {
		return "", fmt.Errorf("create containment registry: %w", err)
	}

	info, err := darwinRegistryLstat(registry)
	if err != nil {
		return "", fmt.Errorf("inspect containment registry: %w", err)
	}

	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("containment registry is not a real directory")
	}

	if err := darwinRegistryChmod(registry, 0o700); err != nil {
		return "", fmt.Errorf("secure containment registry: %w", err)
	}

	return registry, nil
}

func lockDarwinRegistry(registry string) (_ *os.File, returnErr error) {
	darwinRegistryProcessMu.Lock()
	defer func() {
		if returnErr != nil {
			darwinRegistryProcessMu.Unlock()
		}
	}()

	lockPath := filepath.Join(registry, ".lock")

	info, err := darwinRegistryLstat(lockPath)
	if err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600) {
		return nil, errors.New("containment registry lock is not a mode-0600 regular file")
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect containment registry lock: %w", err)
	}

	fd, err := darwinRegistryOpen(lockPath, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open containment registry lock: %w", err)
	}

	file := os.NewFile(uintptr(fd), lockPath)

	openedInfo, err := darwinRecordFileStat(file)
	if err != nil || !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm() != 0o600 {
		_ = file.Close()

		return nil, errors.Join(errors.New("containment registry lock is not a mode-0600 regular file"), err)
	}

	if err := darwinRegistryFlock(int(file.Fd()), unix.LOCK_EX); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("lock containment registry: %w", err)
	}

	return file, nil
}

func unlockDarwinRegistry(file *os.File) {
	if file == nil {
		return
	}

	_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
	_ = file.Close()
	darwinRegistryProcessMu.Unlock()
}

func writeDarwinRecordAtomic(registry string, record darwinContainmentRecord, exclusive bool) error {
	data, err := darwinRecordMarshal(record)
	if err != nil {
		return fmt.Errorf("encode containment record: %w", err)
	}

	data = append(data, '\n')

	target := filepath.Join(registry, record.RuntimeID+".json")
	if exclusive {
		if _, statErr := darwinRecordStat(target); statErr == nil {
			return errors.New("containment runtime id collision")
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("inspect containment record target: %w", statErr)
		}
	}

	file, err := darwinRecordCreateTemp(registry, "."+record.RuntimeID+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create containment record: %w", err)
	}

	temporary := file.Name()
	if err := darwinRecordFileChmod(file, 0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(temporary)

		return fmt.Errorf("secure containment record: %w", err)
	}

	writeErr := func() error {
		if err := darwinRecordFileWrite(file, data); err != nil {
			return err
		}

		if err := darwinRecordFileSync(file); err != nil {
			return err
		}

		return darwinRecordFileClose(file)
	}()
	if writeErr != nil {
		_ = file.Close()
		_ = os.Remove(temporary)

		return fmt.Errorf("write containment record: %w", writeErr)
	}

	if err := darwinRecordRename(temporary, target); err != nil {
		_ = os.Remove(temporary)

		return fmt.Errorf("publish containment record: %w", err)
	}

	return nil
}

func currentDarwinProcessIdentity(pid int) (darwinProcessIdentity, error) {
	info, err := darwinKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return darwinProcessIdentity{}, err
	}

	if int(info.Proc.P_pid) != pid {
		return darwinProcessIdentity{}, os.ErrNotExist
	}

	return darwinProcessIdentity{
		PID: pid, StartSec: info.Proc.P_starttime.Sec, StartUsec: info.Proc.P_starttime.Usec,
		UID: info.Eproc.Ucred.Uid,
	}, nil
}

type DarwinContainmentCandidate struct {
	PID       int
	StartSec  int64
	StartUsec int32
}

type darwinCandidateStatus uint8

const (
	darwinCandidateGone darwinCandidateStatus = iota
	darwinCandidateCorrelated
	darwinCandidateAmbiguous
)

func DiagnoseDarwinContainment(scratchParent string, output io.Writer) error {
	parent, records, err := readDarwinRecords(scratchParent, "")
	if err != nil {
		return err
	}

	report := darwinDiagnosticReport{
		Vendor:        darwinVendor,
		Containment:   darwinBestEffortMode,
		ScratchParent: parent,
		Warning:       darwinContainmentWarning,
		Records:       make([]darwinDiagnosticRecord, 0, len(records)),
	}

	for index := range records {
		record := &records[index]

		candidates, scanErr := darwinContainmentCandidates(*record)
		if scanErr != nil {
			return scanErr
		}

		ambiguous := ambiguousDarwinRecordedIdentity(*record)

		ambiguous = append([]int{}, ambiguous...)
		sort.Ints(ambiguous)
		report.Records = append(report.Records, darwinDiagnosticRecord{
			RuntimeID: record.RuntimeID, State: record.State, GenerationRoot: record.GenerationRoot,
			CorrelatedPIDs: candidatePIDs(candidates), AmbiguousPIDs: ambiguous,
		})
	}

	return json.NewEncoder(output).Encode(report)
}

func CleanupDarwinContainment(scratchParent, runtimeID string, force bool, output io.Writer) error { //nolint:gocyclo // The fixed TERM/KILL/deadline ladder keeps every terminal check explicit.
	if !force {
		return errors.New("containment cleanup requires -force")
	}

	if len(runtimeID) != 32 || strings.ToLower(runtimeID) != runtimeID {
		return errors.New("containment cleanup requires a 128-bit lowercase hexadecimal -runtime-id")
	}

	if _, err := hex.DecodeString(runtimeID); err != nil {
		return errors.New("containment cleanup requires a 128-bit lowercase hexadecimal -runtime-id")
	}

	deadline := darwinCleanupNow().Add(defaultCloseWait)

	parent, records, err := readDarwinRecords(scratchParent, runtimeID)
	if err != nil {
		return err
	}

	if !darwinCleanupNow().Before(deadline) {
		return errors.New("containment cleanup deadline expired during record validation")
	}

	if len(records) != 1 {
		return errors.New("selected containment runtime id is unavailable or ambiguous")
	}

	record := records[0]

	validatedRoot, err := inspectDarwinGenerationRoot(parent, record.GenerationRoot)
	if err != nil {
		return err
	}

	if !darwinCleanupNow().Before(deadline) {
		return errors.New("containment cleanup deadline expired during root validation")
	}

	if !validatedRoot.exists && record.State != darwinStateGroupAbsent {
		return errors.New("selected containment root is missing for an incomplete runtime")
	}

	cleanup := darwinCleanupState{
		record:         record,
		deadline:       deadline,
		ambiguous:      make(map[int]struct{}),
		termIdentities: make(map[DarwinContainmentCandidate]struct{}),
		termSignaled:   make([]int, 0),
		killSignaled:   make([]int, 0),
	}
	finish := func(remaining []DarwinContainmentCandidate) error {
		if !cleanup.deadlineReached() {
			recordedAmbiguous := ambiguousDarwinRecordedIdentity(record)
			for _, pid := range recordedAmbiguous {
				cleanup.ambiguous[pid] = struct{}{}
			}

			cleanup.deadlineReached()
		}

		ambiguousPIDs := sortedPIDSet(cleanup.ambiguous)
		rootRemoved := false

		if !cleanup.deadlineReached() && validatedRoot.exists && len(remaining) == 0 && len(ambiguousPIDs) == 0 {
			currentRoot, validationErr := inspectDarwinGenerationRoot(parent, record.GenerationRoot)
			if !cleanup.deadlineReached() {
				if validationErr != nil || !currentRoot.exists || currentRoot.path != validatedRoot.path ||
					!os.SameFile(validatedRoot.identity, currentRoot.identity) {
					return errors.New("selected containment root identity, type, or path changed before removal")
				}

				removeErr := darwinRemoveGenerationRoot(currentRoot.path)

				cleanup.deadlineReached()

				if removeErr != nil {
					return fmt.Errorf("remove selected containment root: %w", removeErr)
				}

				rootRemoved = true
			}
		}

		report := darwinCleanupReport{
			Vendor:                  darwinVendor,
			Containment:             darwinBestEffortMode,
			ScratchParent:           parent,
			Warning:                 darwinContainmentWarning,
			RuntimeID:               runtimeID,
			GenerationRoot:          record.GenerationRoot,
			TermSignaledPIDs:        cleanup.termSignaled,
			KillSignaledPIDs:        cleanup.killSignaled,
			RemainingCorrelatedPIDs: candidatePIDs(remaining),
			AmbiguousPIDs:           ambiguousPIDs,
			RootRemoved:             rootRemoved,
		}

		sort.Ints(report.TermSignaledPIDs)
		sort.Ints(report.KillSignaledPIDs)

		if encodeErr := json.NewEncoder(output).Encode(report); encodeErr != nil {
			return encodeErr
		}

		if cleanup.deadlineReached() {
			return errors.New("containment cleanup deadline expired before completion")
		}

		if len(remaining) != 0 || len(ambiguousPIDs) != 0 {
			return errors.New("correlated or ambiguous processes remained after the cleanup deadline")
		}

		return nil
	}

	if cleanup.deadlineReached() {
		return finish(nil)
	}

	candidates, err := darwinContainmentCandidates(record)
	if err != nil {
		return err
	}

	if cleanup.deadlineReached() {
		return finish(candidates)
	}

	if _, signalErr := cleanup.signalTermCandidates(candidates); signalErr != nil {
		return signalErr
	}

	if cleanup.deadlineExpired {
		return finish(candidates)
	}

	darwinCleanupSleep(cleanupSleepDuration(defaultCloseKillAfter, cleanup.deadline))

	if cleanup.deadlineReached() {
		return finish(candidates)
	}

	preKillCandidates, err := darwinContainmentCandidates(record)
	if err != nil {
		return err
	}

	if cleanup.deadlineReached() {
		return finish(preKillCandidates)
	}

	newTerm, err := cleanup.signalTermCandidates(preKillCandidates)
	if err != nil {
		return err
	}

	if cleanup.deadlineExpired {
		return finish(preKillCandidates)
	}

	if newTerm {
		darwinCleanupSleep(cleanupSleepDuration(defaultCloseKillAfter, cleanup.deadline))

		if cleanup.deadlineReached() {
			return finish(preKillCandidates)
		}
	}

	killCandidates, err := darwinContainmentCandidates(record)
	if err != nil {
		return err
	}

	if cleanup.deadlineReached() {
		return finish(killCandidates)
	}

	if signalErr := cleanup.signalKillCandidates(killCandidates); signalErr != nil {
		return signalErr
	}

	if cleanup.deadlineExpired {
		return finish(killCandidates)
	}

	remaining, err := cleanup.pollRemaining()
	if err != nil {
		return err
	}

	return finish(remaining)
}

func (cleanup *darwinCleanupState) deadlineReached() bool {
	if cleanup.deadlineExpired {
		return true
	}

	cleanup.deadlineExpired = !darwinCleanupNow().Before(cleanup.deadline)

	return cleanup.deadlineExpired
}

func (cleanup *darwinCleanupState) signalTermCandidates(candidates []DarwinContainmentCandidate) (bool, error) {
	signaled := false

	for _, candidate := range candidates {
		if _, alreadySignaled := cleanup.termIdentities[candidate]; alreadySignaled {
			continue
		}

		if cleanup.deadlineReached() {
			cleanup.ambiguous[candidate.PID] = struct{}{}

			break
		}

		if candidate.PID == os.Getpid() {
			continue
		}

		current, status := darwinContainmentRevalidate(cleanup.record, candidate)
		switch status {
		case darwinCandidateGone:
			continue
		case darwinCandidateAmbiguous:
			cleanup.ambiguous[candidate.PID] = struct{}{}

			continue
		}

		if cleanup.deadlineReached() {
			cleanup.ambiguous[current.PID] = struct{}{}

			break
		}

		signalErr := darwinContainmentSignalPID(current.PID, syscall.SIGTERM)
		if errors.Is(signalErr, syscall.ESRCH) {
			continue
		}

		if signalErr != nil {
			return false, fmt.Errorf("terminate correlated pid %d: %w", current.PID, signalErr)
		}

		cleanup.termSignaled = append(cleanup.termSignaled, current.PID)
		cleanup.termIdentities[current] = struct{}{}
		signaled = true
	}

	return signaled, nil
}

func (cleanup *darwinCleanupState) signalKillCandidates(candidates []DarwinContainmentCandidate) error {
	for _, candidate := range candidates {
		if cleanup.deadlineReached() {
			cleanup.ambiguous[candidate.PID] = struct{}{}

			break
		}

		if candidate.PID == os.Getpid() {
			continue
		}

		current, status := darwinContainmentRevalidate(cleanup.record, candidate)
		switch status {
		case darwinCandidateGone:
			continue
		case darwinCandidateAmbiguous:
			cleanup.ambiguous[candidate.PID] = struct{}{}

			continue
		}

		if _, receivedTerm := cleanup.termIdentities[current]; !receivedTerm {
			cleanup.ambiguous[current.PID] = struct{}{}

			continue
		}

		if cleanup.deadlineReached() {
			cleanup.ambiguous[current.PID] = struct{}{}

			break
		}

		signalErr := darwinContainmentSignalPID(current.PID, syscall.SIGKILL)
		if errors.Is(signalErr, syscall.ESRCH) {
			continue
		}

		if signalErr != nil {
			return fmt.Errorf("kill correlated pid %d: %w", current.PID, signalErr)
		}

		cleanup.killSignaled = append(cleanup.killSignaled, current.PID)
	}

	return nil
}

func (cleanup *darwinCleanupState) pollRemaining() ([]DarwinContainmentCandidate, error) {
	for {
		if cleanup.deadlineReached() {
			return nil, nil
		}

		remaining, err := darwinContainmentCandidates(cleanup.record)
		if err != nil {
			return nil, err
		}

		if cleanup.deadlineReached() || len(remaining) == 0 {
			return remaining, nil
		}

		darwinCleanupSleep(25 * time.Millisecond)
	}
}

func cleanupSleepDuration(wanted time.Duration, deadline time.Time) time.Duration {
	remaining := deadline.Sub(darwinCleanupNow())
	if remaining <= 0 {
		return 0
	}

	if remaining < wanted {
		return remaining
	}

	return wanted
}

func readDarwinRecords(scratchParent, runtimeID string) (string, []darwinContainmentRecord, error) {
	if scratchParent == "" {
		return "", nil, errors.New("containment command requires -scratch-dir")
	}

	parent, err := darwinRecordAbs(scratchParent)
	if err != nil {
		return "", nil, fmt.Errorf("resolve scratch parent: %w", err)
	}

	registry := filepath.Join(parent, darwinRegistryDir)

	registryInfo, err := darwinRegistryLstat(registry)
	if errors.Is(err, os.ErrNotExist) {
		return parent, nil, nil
	}

	if err != nil {
		return "", nil, fmt.Errorf("inspect containment registry: %w", err)
	}

	if !registryInfo.IsDir() || registryInfo.Mode()&os.ModeSymlink != 0 || registryInfo.Mode().Perm() != 0o700 {
		return "", nil, errors.New("containment registry is not a mode-0700 real directory")
	}

	entries, err := darwinRegistryReadDir(registry)
	if err != nil {
		return "", nil, fmt.Errorf("read containment registry: %w", err)
	}

	records := make([]darwinContainmentRecord, 0)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		if runtimeID != "" && entry.Name() != runtimeID+".json" {
			continue
		}

		recordPath := filepath.Join(registry, entry.Name())

		info, infoErr := darwinRegistryLstat(recordPath)
		if infoErr != nil {
			return "", nil, fmt.Errorf("inspect containment record: %w", infoErr)
		}

		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 {
			return "", nil, fmt.Errorf("containment record %s is not a mode-0600 regular file", entry.Name())
		}

		fd, openErr := darwinRegistryOpen(recordPath, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
		if openErr != nil {
			return "", nil, openErr
		}

		file := os.NewFile(uintptr(fd), recordPath)

		openedInfo, statErr := darwinRecordFileStat(file)
		if statErr != nil || !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm() != 0o600 {
			_ = file.Close()

			return "", nil, errors.Join(fmt.Errorf("containment record %s is not a mode-0600 regular file", entry.Name()), statErr)
		}

		decoder := json.NewDecoder(file)
		decoder.DisallowUnknownFields()

		var record darwinContainmentRecord

		decodeErr := decoder.Decode(&record)
		if decodeErr == nil {
			var trailing any
			if trailingErr := decoder.Decode(&trailing); !errors.Is(trailingErr, io.EOF) {
				decodeErr = errors.New("containment record has trailing data")
			}
		}

		closeErr := darwinRecordFileClose(file)
		if decodeErr != nil || closeErr != nil {
			return "", nil, errors.Join(decodeErr, closeErr)
		}

		if !validCurrentDarwinRecord(parent, strings.TrimSuffix(entry.Name(), ".json"), record) {
			return "", nil, fmt.Errorf("invalid current-format containment record %s", entry.Name())
		}

		records = append(records, record)
	}

	sort.Slice(records, func(i, j int) bool { return records[i].RuntimeID < records[j].RuntimeID })

	return parent, records, nil
}

func validCurrentDarwinRecord(parent, filenameID string, record darwinContainmentRecord) bool {
	if record.SchemaVersion != darwinRecordFormat || record.Vendor != darwinVendor || record.Containment != darwinBestEffortMode ||
		record.RuntimeID != filenameID || !validDarwinRuntimeID(record.RuntimeID) || !validDarwinLifecycle(record.LifecycleKind) ||
		!filepath.IsAbs(record.GenerationRoot) || !pathWithin(parent, record.GenerationRoot) ||
		!strings.HasPrefix(filepath.Base(record.GenerationRoot), "acp-go-amp-command-") ||
		record.WrapperPID <= 0 || record.WrapperStartSec < 0 || record.WrapperStartUsec < 0 || record.WrapperStartUsec >= 1_000_000 {
		return false
	}

	switch record.State {
	case darwinStateRunning, darwinStateGroupAbsent, darwinStateIncomplete:
	default:
		return false
	}

	present := 0

	for _, value := range []bool{
		record.DirectChildPID != nil, record.DirectChildStartSec != nil,
		record.DirectChildStartUsec != nil, record.OriginalPGID != nil,
	} {
		if value {
			present++
		}
	}

	if present == 0 {
		return true
	}

	return present == 4 && *record.DirectChildPID > 0 && *record.DirectChildStartSec >= 0 &&
		*record.DirectChildStartUsec >= 0 && *record.DirectChildStartUsec < 1_000_000 &&
		*record.OriginalPGID == *record.DirectChildPID
}

func validDarwinRuntimeID(runtimeID string) bool {
	if len(runtimeID) != 32 || runtimeID != strings.ToLower(runtimeID) {
		return false
	}

	_, err := hex.DecodeString(runtimeID)

	return err == nil
}

func validDarwinLifecycle(lifecycle string) bool {
	switch lifecycle {
	case "runtime", "session", darwinLifecyclePrompt, "discovery":
		return true
	default:
		return false
	}
}

func validatedDarwinGenerationRoot(parent, root string) (string, bool, error) {
	validated, err := inspectDarwinGenerationRoot(parent, root)

	return validated.path, validated.exists, err
}

func inspectDarwinGenerationRoot(parent, root string) (darwinGenerationRootValidation, error) {
	if !strings.HasPrefix(filepath.Base(root), "acp-go-amp-command-") {
		return darwinGenerationRootValidation{}, errors.New("selected containment root has an invalid generation prefix")
	}

	resolvedParent, err := darwinEvalSymlinks(parent)
	if err != nil {
		return darwinGenerationRootValidation{}, fmt.Errorf("resolve selected scratch parent: %w", err)
	}

	info, err := darwinRegistryLstat(root)
	if errors.Is(err, os.ErrNotExist) {
		absoluteRoot, absoluteErr := darwinRecordAbs(root)
		if absoluteErr != nil || !pathWithin(parent, absoluteRoot) {
			return darwinGenerationRootValidation{}, errors.New("selected containment root is outside the scratch parent")
		}

		return darwinGenerationRootValidation{path: absoluteRoot}, nil
	}

	if err != nil {
		return darwinGenerationRootValidation{}, fmt.Errorf("inspect selected containment root: %w", err)
	}

	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return darwinGenerationRootValidation{}, errors.New("selected containment root is not a real directory")
	}

	resolvedRoot, err := darwinEvalSymlinks(root)
	if err != nil {
		return darwinGenerationRootValidation{}, fmt.Errorf("resolve selected containment root: %w", err)
	}

	if !pathWithin(resolvedParent, resolvedRoot) {
		return darwinGenerationRootValidation{}, errors.New("selected containment root is outside the scratch parent")
	}

	return darwinGenerationRootValidation{path: resolvedRoot, identity: info, exists: true}, nil
}

func correlatedDarwinCandidates(record darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
	processes, err := darwinKinfoProcSlice("kern.proc.uid", os.Getuid())
	if err != nil {
		return nil, fmt.Errorf("enumerate same-uid processes: %w", err)
	}

	candidates := make([]DarwinContainmentCandidate, 0)

	for index := range processes {
		process := &processes[index]
		pid := int(process.Proc.P_pid)

		env, envErr := darwinEnvironmentLookup(pid)
		if envErr != nil {
			continue
		}

		if env[DarwinRuntimeIDEnv] != record.RuntimeID || env[DarwinScratchRootEnv] != record.GenerationRoot {
			continue
		}

		candidates = append(candidates, DarwinContainmentCandidate{PID: pid, StartSec: process.Proc.P_starttime.Sec, StartUsec: process.Proc.P_starttime.Usec})
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].PID < candidates[j].PID })

	return candidates, nil
}

func ambiguousDarwinRecordedIdentity(record darwinContainmentRecord) []int {
	if record.State == darwinStateGroupAbsent {
		return nil
	}

	if record.DirectChildPID == nil || record.DirectChildStartSec == nil || record.DirectChildStartUsec == nil {
		return nil
	}

	identity, err := darwinProcessIdentityLookup(*record.DirectChildPID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return nil
	}

	if err != nil {
		return []int{*record.DirectChildPID}
	}

	if !darwinIdentitySameUID(identity) || identity.StartSec != *record.DirectChildStartSec || identity.StartUsec != *record.DirectChildStartUsec {
		return []int{*record.DirectChildPID}
	}

	env, err := darwinEnvironmentLookup(identity.PID)
	if err != nil || env[DarwinRuntimeIDEnv] != record.RuntimeID || env[DarwinScratchRootEnv] != record.GenerationRoot {
		return []int{identity.PID}
	}

	return nil
}

func darwinProcessEnvironment(pid int) (map[string]string, error) {
	raw, err := darwinProcargsRaw("kern.procargs2", pid)
	if err != nil {
		return nil, err
	}

	return parseDarwinProcessEnvironment(raw)
}

func parseDarwinProcessEnvironment(raw []byte) (map[string]string, error) {
	if len(raw) < 4 {
		return nil, errors.New("process arguments are truncated")
	}

	argc := int(binary.LittleEndian.Uint32(raw[:4]))
	if argc <= 0 || argc > len(raw) {
		return nil, errors.New("process argument count is invalid")
	}

	offset := 4

	_, offset, ok := nextDarwinProcString(raw, offset)
	if !ok {
		return nil, errors.New("process executable path is truncated")
	}

	for offset < len(raw) && raw[offset] == 0 {
		offset++
	}

	for range argc {
		argument, next, found := nextDarwinProcString(raw, offset)
		if !found {
			return nil, errors.New("process argv is truncated")
		}

		if argument == "" {
			return nil, errors.New("process argv is ambiguous")
		}

		offset = next
	}

	env := make(map[string]string)
	terminated := false

	for offset < len(raw) {
		part, next, found := nextDarwinProcString(raw, offset)
		if !found {
			return nil, errors.New("process environment is truncated")
		}

		offset = next

		if part == "" {
			if len(env) == 0 {
				return nil, errors.New("process environment is ambiguous")
			}

			terminated = true

			break
		}

		key, value, ok := strings.Cut(part, "=")
		if ok {
			env[key] = value
		}
	}

	if !terminated {
		return nil, errors.New("process environment is unterminated")
	}

	return env, nil
}

func nextDarwinProcString(raw []byte, offset int) (string, int, bool) {
	if offset < 0 || offset >= len(raw) {
		return "", offset, false
	}

	for index := offset; index < len(raw); index++ {
		if raw[index] == 0 {
			return string(raw[offset:index]), index + 1, true
		}
	}

	return "", offset, false
}

func revalidateDarwinCandidate(record darwinContainmentRecord, candidate DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
	identity, err := darwinProcessIdentityLookup(candidate.PID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return DarwinContainmentCandidate{}, darwinCandidateGone
	}

	if err != nil || !darwinIdentitySameUID(identity) || identity.StartSec != candidate.StartSec || identity.StartUsec != candidate.StartUsec {
		return DarwinContainmentCandidate{}, darwinCandidateAmbiguous
	}

	env, err := darwinEnvironmentLookup(candidate.PID)
	if err != nil || env[DarwinRuntimeIDEnv] != record.RuntimeID || env[DarwinScratchRootEnv] != record.GenerationRoot {
		return DarwinContainmentCandidate{}, darwinCandidateAmbiguous
	}

	return candidate, darwinCandidateCorrelated
}

func candidatePIDs(candidates []DarwinContainmentCandidate) []int {
	pids := make([]int, 0, len(candidates))

	for _, candidate := range candidates {
		pids = append(pids, candidate.PID)
	}

	sort.Ints(pids)

	return pids
}

func sortedPIDSet(values map[int]struct{}) []int {
	pids := make([]int, 0, len(values))

	for pid := range values {
		pids = append(pids, pid)
	}

	sort.Ints(pids)

	return pids
}
