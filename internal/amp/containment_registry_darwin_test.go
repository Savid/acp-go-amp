//go:build darwin

package amp

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestDarwinGenerationRecordCurrentFormatAndDistinctIdentity(t *testing.T) {
	parent := t.TempDir()
	root1 := filepath.Join(parent, "acp-go-amp-command-one")
	root2 := filepath.Join(parent, "acp-go-amp-command-two")
	for _, root := range []string{root1, root2} {
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	first, err := NewDarwinGenerationRecord(parent, root1, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewDarwinGenerationRecord(parent, root2, "discovery")
	if err != nil {
		t.Fatal(err)
	}
	if first.RuntimeID == second.RuntimeID || len(first.RuntimeID) != 32 || len(second.RuntimeID) != 32 {
		t.Fatalf("runtime ids = %q, %q", first.RuntimeID, second.RuntimeID)
	}
	if startErr := first.started(os.Getpid(), syscall.Getpgrp()); startErr != nil {
		t.Fatal(startErr)
	}
	if finishErr := first.finish(true); finishErr != nil {
		t.Fatal(finishErr)
	}

	recordPath := filepath.Join(parent, darwinRegistryDir, first.RuntimeID+".json")
	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	wantFields := []string{
		"schema_version", "vendor", "containment", "lifecycle_kind", "runtime_id", "generation_root",
		"wrapper_pid", "wrapper_start_sec", "wrapper_start_usec", "direct_child_pid",
		"direct_child_start_sec", "direct_child_start_usec", "original_pgid", "state",
	}
	for _, field := range wantFields {
		if _, ok := fields[field]; !ok {
			t.Fatalf("record missing %q: %s", field, data)
		}
	}
	if fields["state"] != "group_absent" || fields["containment"] != "best_effort" {
		t.Fatalf("record = %s", data)
	}
	if info, err := os.Stat(recordPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("record mode = %v, %v", info, err)
	}
	if info, err := os.Stat(filepath.Join(parent, darwinRegistryDir)); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("registry mode = %v, %v", info, err)
	}
}

func TestDarwinRegistrySerializesConcurrentReplacement(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-concurrent")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}

	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}

	_, records, err := readDarwinRecords(parent, generation.RuntimeID)
	if err != nil {
		t.Fatal(err)
	}

	record := records[0]
	registry := filepath.Join(parent, darwinRegistryDir)
	staleTemporary := filepath.Join(registry, "."+record.RuntimeID+"."+strconv.Itoa(os.Getpid())+".tmp")
	if writeErr := os.WriteFile(staleTemporary, []byte("stale"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	const replacements = 64
	errCh := make(chan error, replacements)
	var wait sync.WaitGroup

	for index := range replacements {
		wait.Add(1)

		go func() {
			defer wait.Done()

			copyRecord := record
			if index%2 == 0 {
				copyRecord.State = darwinStateGroupAbsent
			} else {
				copyRecord.State = darwinStateIncomplete
			}

			errCh <- replaceDarwinRecord(parent, copyRecord)
		}()
	}

	wait.Wait()
	close(errCh)

	for replaceErr := range errCh {
		if replaceErr != nil {
			t.Fatal(replaceErr)
		}
	}

	_, records, err = readDarwinRecords(parent, generation.RuntimeID)
	if err != nil || len(records) != 1 {
		t.Fatalf("read replaced record: count=%d err=%v", len(records), err)
	}
}

func TestParseDarwinProcessEnvironmentRejectsMarkerArgvAndAmbiguity(t *testing.T) {
	raw := darwinProcargsFixture([]string{"tool", DarwinRuntimeIDEnv + "=argv-spoof"}, []string{
		DarwinRuntimeIDEnv + "=real-runtime",
		DarwinScratchRootEnv + "=/real/root",
	})
	env, err := parseDarwinProcessEnvironment(raw)
	if err != nil {
		t.Fatal(err)
	}
	if env[DarwinRuntimeIDEnv] != "real-runtime" || env[DarwinScratchRootEnv] != "/real/root" {
		t.Fatalf("environment = %#v", env)
	}
	paddedTail := append(append([]byte{}, raw...), 0, 'n', 'o', 'n', 'z', 'e', 'r', 'o')
	env, err = parseDarwinProcessEnvironment(paddedTail)
	if err != nil || env[DarwinRuntimeIDEnv] != "real-runtime" || env[DarwinScratchRootEnv] != "/real/root" {
		t.Fatalf("environment with kernel padding = %#v, %v", env, err)
	}

	if _, err := parseDarwinProcessEnvironment(darwinProcargsFixture(nil, nil)); err == nil {
		t.Fatal("zero argc was accepted")
	}
	if _, err := parseDarwinProcessEnvironment(darwinProcargsFixture([]string{"tool", ""}, nil)); err == nil {
		t.Fatal("empty argv entry was accepted")
	}
	if _, err := parseDarwinProcessEnvironment([]byte{1, 2}); err == nil {
		t.Fatal("truncated procargs was accepted")
	}
	padded := darwinProcargsFixture([]string{"tool"}, []string{DarwinRuntimeIDEnv + "=smuggled"})
	envStart := 4 + len("/bin/tool") + 2 + len("tool") + 1
	padded = append(padded[:envStart], append([]byte{0}, padded[envStart:]...)...)
	if _, err := parseDarwinProcessEnvironment(padded); err == nil {
		t.Fatal("post-argv NUL padding was accepted")
	}
}

func TestDarwinCandidateRevalidationRejectsUIDChange(t *testing.T) {
	identity, err := currentDarwinProcessIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	originalSameUID := darwinIdentitySameUID
	darwinIdentitySameUID = func(darwinProcessIdentity) bool { return false }
	t.Cleanup(func() { darwinIdentitySameUID = originalSameUID })
	candidate := DarwinContainmentCandidate{PID: identity.PID, StartSec: identity.StartSec, StartUsec: identity.StartUsec}
	_, status := revalidateDarwinCandidate(darwinContainmentRecord{}, candidate)
	if status != darwinCandidateAmbiguous {
		t.Fatalf("UID-changed candidate status = %d", status)
	}
}

func TestDarwinDiagnoseIsReadOnly(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-diagnose")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "discovery")
	if err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(parent, darwinRegistryDir, generation.RuntimeID+".json")
	before, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if diagnoseErr := DiagnoseDarwinContainment(parent, &output); diagnoseErr != nil {
		t.Fatal(diagnoseErr)
	}
	after, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || !strings.Contains(output.String(), darwinContainmentWarning) {
		t.Fatalf("diagnose mutated record or omitted warning: %q", output.String())
	}
}

func TestDarwinCleanupRequiresSelectionAndSignalsFreshIndividualCandidates(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-cleanup")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, false, &bytes.Buffer{}); err == nil {
		t.Fatal("cleanup accepted missing force")
	}
	if err := CleanupDarwinContainment(parent, "bad", true, &bytes.Buffer{}); err == nil {
		t.Fatal("cleanup accepted invalid runtime id")
	}
	if err := CleanupDarwinContainment(parent, strings.Repeat("f", 32), true, &bytes.Buffer{}); err == nil {
		t.Fatal("cleanup accepted missing selected record")
	}

	originalCandidates := darwinContainmentCandidates
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	originalNow := darwinCleanupNow
	originalSleep := darwinCleanupSleep
	t.Cleanup(func() {
		darwinContainmentCandidates = originalCandidates
		darwinContainmentRevalidate = originalRevalidate
		darwinContainmentSignalPID = originalSignal
		darwinCleanupNow = originalNow
		darwinCleanupSleep = originalSleep
	})

	first := DarwinContainmentCandidate{PID: 101, StartSec: 1, StartUsec: 1}
	spawnedDuringGrace := DarwinContainmentCandidate{PID: 202, StartSec: 2, StartUsec: 2}
	scans := [][]DarwinContainmentCandidate{{first}, {first, spawnedDuringGrace}, {first, spawnedDuringGrace}, {}}
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		result := scans[0]
		scans = scans[1:]

		return result, nil
	}
	darwinContainmentRevalidate = func(_ darwinContainmentRecord, candidate DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
		return candidate, darwinCandidateCorrelated
	}
	type signalCall struct {
		pid    int
		signal syscall.Signal
	}
	var calls []signalCall
	darwinContainmentSignalPID = func(pid int, signal syscall.Signal) error {
		calls = append(calls, signalCall{pid: pid, signal: signal})

		return nil
	}
	now := time.Unix(1, 0)
	darwinCleanupNow = func() time.Time { return now }
	darwinCleanupSleep = func(duration time.Duration) { now = now.Add(duration) }
	var output bytes.Buffer
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, &output); err != nil {
		t.Fatal(err)
	}
	want := []signalCall{{101, syscall.SIGTERM}, {202, syscall.SIGTERM}, {101, syscall.SIGKILL}, {202, syscall.SIGKILL}}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("signal calls = %#v, want %#v", calls, want)
	}
	for _, call := range calls {
		if call.pid <= 0 {
			t.Fatalf("cleanup attempted broad/group signal: %#v", calls)
		}
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("selected root still exists: %v", err)
	}
}

func TestDarwinCleanupDoesNotReportESRCHAsSignaled(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-esrch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	originalCandidates := darwinContainmentCandidates
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	originalSleep := darwinCleanupSleep
	t.Cleanup(func() {
		darwinContainmentCandidates = originalCandidates
		darwinContainmentRevalidate = originalRevalidate
		darwinContainmentSignalPID = originalSignal
		darwinCleanupSleep = originalSleep
	})
	candidate := DarwinContainmentCandidate{PID: 303, StartSec: 3, StartUsec: 3}
	scans := [][]DarwinContainmentCandidate{{candidate}, {}, {}, {}}
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		result := scans[0]
		scans = scans[1:]

		return result, nil
	}
	darwinContainmentRevalidate = func(_ darwinContainmentRecord, candidate DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
		return candidate, darwinCandidateCorrelated
	}
	darwinContainmentSignalPID = func(int, syscall.Signal) error { return syscall.ESRCH }
	darwinCleanupSleep = func(time.Duration) {}
	var output bytes.Buffer
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"term_signaled_pids":[]`) || !strings.Contains(output.String(), `"kill_signaled_pids":[]`) {
		t.Fatalf("cleanup output = %s", output.String())
	}
}

func TestDarwinCleanupRevalidationCannotOutrunSignalDeadline(t *testing.T) {
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	originalNow := darwinCleanupNow
	t.Cleanup(func() {
		darwinContainmentRevalidate = originalRevalidate
		darwinContainmentSignalPID = originalSignal
		darwinCleanupNow = originalNow
	})

	for _, rung := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		t.Run(rung.String(), func(t *testing.T) {
			now := time.Unix(1, 0)
			candidate := DarwinContainmentCandidate{PID: 404, StartSec: 4, StartUsec: 4}
			cleanup := darwinCleanupState{
				deadline:       now.Add(defaultCloseWait),
				ambiguous:      make(map[int]struct{}),
				termIdentities: make(map[DarwinContainmentCandidate]struct{}),
			}
			if rung == syscall.SIGKILL {
				cleanup.termIdentities[candidate] = struct{}{}
			}

			darwinCleanupNow = func() time.Time { return now }
			darwinContainmentRevalidate = func(_ darwinContainmentRecord, current DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
				now = cleanup.deadline

				return current, darwinCandidateCorrelated
			}
			signalCalls := 0
			darwinContainmentSignalPID = func(int, syscall.Signal) error {
				signalCalls++

				return nil
			}

			if rung == syscall.SIGTERM {
				signaled, err := cleanup.signalTermCandidates([]DarwinContainmentCandidate{candidate})
				if err != nil || signaled {
					t.Fatalf("TERM result = %t, %v", signaled, err)
				}
			} else if err := cleanup.signalKillCandidates([]DarwinContainmentCandidate{candidate}); err != nil {
				t.Fatal(err)
			}

			if signalCalls != 0 {
				t.Fatalf("cleanup emitted %d %s signals after revalidation exhausted the deadline", signalCalls, rung)
			}
			if _, retained := cleanup.ambiguous[candidate.PID]; !retained {
				t.Fatalf("deadline-expired candidate %d was not retained as ambiguous", candidate.PID)
			}
		})
	}
}

func TestDarwinCleanupScanDeadlineTerminalizesWithoutLateOperations(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-scan-deadline")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}

	originalCandidates := darwinContainmentCandidates
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	originalNow := darwinCleanupNow
	originalSleep := darwinCleanupSleep
	originalRemove := darwinRemoveGenerationRoot
	t.Cleanup(func() {
		darwinContainmentCandidates = originalCandidates
		darwinContainmentRevalidate = originalRevalidate
		darwinContainmentSignalPID = originalSignal
		darwinCleanupNow = originalNow
		darwinCleanupSleep = originalSleep
		darwinRemoveGenerationRoot = originalRemove
	})

	now := time.Unix(1, 0)
	darwinCleanupNow = func() time.Time { return now }
	scans := 0
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		scans++
		if scans != 1 {
			t.Fatalf("cleanup performed late candidate scan %d", scans)
		}
		now = now.Add(defaultCloseWait)

		return nil, nil
	}
	revalidations := 0
	darwinContainmentRevalidate = func(_ darwinContainmentRecord, candidate DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
		revalidations++

		return candidate, darwinCandidateCorrelated
	}
	signals := 0
	darwinContainmentSignalPID = func(int, syscall.Signal) error {
		signals++

		return nil
	}
	removals := 0
	darwinRemoveGenerationRoot = func(string) error {
		removals++

		return nil
	}
	darwinCleanupSleep = func(time.Duration) { t.Fatal("cleanup slept after the scan exhausted its deadline") }

	var output bytes.Buffer
	err = CleanupDarwinContainment(parent, generation.RuntimeID, true, &output)
	if err == nil || !strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("cleanup error = %v", err)
	}
	if scans != 1 || revalidations != 0 || signals != 0 || removals != 0 {
		t.Fatalf("late operations: scans=%d revalidations=%d signals=%d removals=%d", scans, revalidations, signals, removals)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("deadline-expired cleanup removed the selected root: %v", err)
	}
	var report darwinCleanupReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.RootRemoved || len(report.TermSignaledPIDs) != 0 || len(report.KillSignaledPIDs) != 0 {
		t.Fatalf("deadline-expired report = %#v", report)
	}
}

func TestDarwinCleanupRetainsReplacementGenerationRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-replaced")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}

	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	if finishErr := generation.RecordFinished(true); finishErr != nil {
		t.Fatal(finishErr)
	}

	originalCandidates := darwinContainmentCandidates
	originalSleep := darwinCleanupSleep
	originalRemove := darwinRemoveGenerationRoot
	t.Cleanup(func() {
		darwinContainmentCandidates = originalCandidates
		darwinCleanupSleep = originalSleep
		darwinRemoveGenerationRoot = originalRemove
	})

	replacementFile := filepath.Join(root, "replacement")
	replaced := false
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		if !replaced {
			if renameErr := os.Rename(root, root+"-original"); renameErr != nil {
				t.Fatal(renameErr)
			}
			if mkdirErr := os.Mkdir(root, 0o700); mkdirErr != nil {
				t.Fatal(mkdirErr)
			}
			if writeErr := os.WriteFile(replacementFile, []byte("replacement"), 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
			replaced = true
		}

		return nil, nil
	}
	darwinCleanupSleep = func(time.Duration) {}
	removeCalls := 0
	darwinRemoveGenerationRoot = func(string) error {
		removeCalls++

		return nil
	}

	err = CleanupDarwinContainment(parent, generation.RuntimeID, true, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "changed before removal") {
		t.Fatalf("replacement cleanup error = %v", err)
	}
	if !replaced || removeCalls != 0 {
		t.Fatalf("replacement race: replaced=%t remove calls=%d", replaced, removeCalls)
	}
	if contents, readErr := os.ReadFile(replacementFile); readErr != nil || string(contents) != "replacement" {
		t.Fatalf("replacement entry = %q, %v", contents, readErr)
	}
}

func TestDarwinCleanupNeverRemovesRootCreatedAfterMissingValidation(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-created-late")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}

	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	if finishErr := generation.RecordFinished(true); finishErr != nil {
		t.Fatal(finishErr)
	}
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}

	originalCandidates := darwinContainmentCandidates
	originalSleep := darwinCleanupSleep
	originalRemove := darwinRemoveGenerationRoot
	t.Cleanup(func() {
		darwinContainmentCandidates = originalCandidates
		darwinCleanupSleep = originalSleep
		darwinRemoveGenerationRoot = originalRemove
	})

	created := false
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		if !created {
			if mkdirErr := os.Mkdir(root, 0o700); mkdirErr != nil {
				t.Fatal(mkdirErr)
			}
			created = true
		}

		return nil, nil
	}
	darwinCleanupSleep = func(time.Duration) {}
	removeCalls := 0
	darwinRemoveGenerationRoot = func(string) error {
		removeCalls++

		return nil
	}

	var output bytes.Buffer
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, &output); err != nil {
		t.Fatal(err)
	}
	if !created || removeCalls != 0 {
		t.Fatalf("late creation: created=%t remove calls=%d", created, removeCalls)
	}
	if info, statErr := os.Lstat(root); statErr != nil || !info.IsDir() {
		t.Fatalf("late-created root = %v, %v", info, statErr)
	}

	var report darwinCleanupReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.RootRemoved {
		t.Fatalf("late-created root reported removed: %#v", report)
	}
}

func TestDarwinCleanupDeadlineIsTerminalAtEveryEmptyCandidateStage(t *testing.T) {
	originalCandidates := darwinContainmentCandidates
	originalNow := darwinCleanupNow
	originalSleep := darwinCleanupSleep
	t.Cleanup(func() {
		darwinContainmentCandidates = originalCandidates
		darwinCleanupNow = originalNow
		darwinCleanupSleep = originalSleep
	})

	for _, expireAt := range []int{2, 3, 4, 7, 8, 9, 10} {
		t.Run(strconv.Itoa(expireAt), func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "acp-go-amp-command-deadline-stage")
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal(err)
			}
			generation, err := NewDarwinGenerationRecord(parent, root, "runtime")
			if err != nil {
				t.Fatal(err)
			}

			base := time.Unix(1, 0)
			nowCalls := 0
			darwinCleanupNow = func() time.Time {
				nowCalls++
				if nowCalls >= expireAt {
					return base.Add(defaultCloseWait)
				}

				return base
			}
			darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
				return nil, nil
			}
			darwinCleanupSleep = func(time.Duration) {}

			var output bytes.Buffer
			err = CleanupDarwinContainment(parent, generation.RuntimeID, true, &output)
			if err == nil || !strings.Contains(err.Error(), "deadline expired") {
				t.Fatalf("cleanup error at time call %d = %v", expireAt, err)
			}
			if _, statErr := os.Stat(root); statErr != nil {
				t.Fatalf("deadline stage %d removed root: %v", expireAt, statErr)
			}
		})
	}
}

func TestDarwinCleanupRootRemovalConsumesBudget(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-removal-deadline")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "discovery")
	if err != nil {
		t.Fatal(err)
	}

	originalCandidates := darwinContainmentCandidates
	originalNow := darwinCleanupNow
	originalSleep := darwinCleanupSleep
	originalRemove := darwinRemoveGenerationRoot
	t.Cleanup(func() {
		darwinContainmentCandidates = originalCandidates
		darwinCleanupNow = originalNow
		darwinCleanupSleep = originalSleep
		darwinRemoveGenerationRoot = originalRemove
	})

	now := time.Unix(450, 0)
	deadline := now.Add(defaultCloseWait)
	removed := false
	darwinCleanupNow = func() time.Time { return now }
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		if removed {
			t.Fatal("cleanup performed a process scan after root removal exhausted its deadline")
		}

		return nil, nil
	}
	darwinCleanupSleep = func(time.Duration) {}
	darwinRemoveGenerationRoot = func(path string) error {
		if removeErr := originalRemove(path); removeErr != nil {
			return removeErr
		}

		removed = true
		now = deadline

		return nil
	}

	var output bytes.Buffer
	err = CleanupDarwinContainment(parent, generation.RuntimeID, true, &output)
	if err == nil || !strings.Contains(err.Error(), "deadline expired before completion") {
		t.Fatalf("cleanup error = %v", err)
	}
	var report darwinCleanupReport
	if decodeErr := json.Unmarshal(output.Bytes(), &report); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if !removed || !report.RootRemoved {
		t.Fatalf("removed=%t report=%#v", removed, report)
	}
	if _, statErr := os.Stat(root); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("removed generation root still exists: %v", statErr)
	}
}

func TestDarwinCleanupDeadlineIsTerminalAfterEachSignalStage(t *testing.T) {
	originalCandidates := darwinContainmentCandidates
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	originalNow := darwinCleanupNow
	originalSleep := darwinCleanupSleep
	t.Cleanup(func() {
		darwinContainmentCandidates = originalCandidates
		darwinContainmentRevalidate = originalRevalidate
		darwinContainmentSignalPID = originalSignal
		darwinCleanupNow = originalNow
		darwinCleanupSleep = originalSleep
	})

	for _, stage := range []string{"initial-term", "pre-kill-term", "pre-kill-term-grace", "kill"} {
		t.Run(stage, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "acp-go-amp-command-signal-stage")
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal(err)
			}
			generation, err := NewDarwinGenerationRecord(parent, root, "runtime")
			if err != nil {
				t.Fatal(err)
			}

			base := time.Unix(1, 0)
			now := base
			candidate := DarwinContainmentCandidate{PID: 404, StartSec: 4, StartUsec: 4}
			scans := 0
			revalidations := 0
			sleeps := 0
			darwinCleanupNow = func() time.Time { return now }
			darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
				scans++
				if (stage == "pre-kill-term" || stage == "pre-kill-term-grace") && scans == 1 {
					return nil, nil
				}

				return []DarwinContainmentCandidate{candidate}, nil
			}
			darwinContainmentRevalidate = func(_ darwinContainmentRecord, value DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
				revalidations++
				if stage == "initial-term" && revalidations == 1 ||
					stage == "pre-kill-term" && revalidations == 1 ||
					stage == "kill" && revalidations == 2 {
					now = base.Add(defaultCloseWait)
				}

				return value, darwinCandidateCorrelated
			}
			darwinContainmentSignalPID = func(int, syscall.Signal) error { return nil }
			darwinCleanupSleep = func(time.Duration) {
				sleeps++
				if stage == "pre-kill-term-grace" && sleeps == 2 {
					now = base.Add(defaultCloseWait)
				}
			}

			var output bytes.Buffer
			err = CleanupDarwinContainment(parent, generation.RuntimeID, true, &output)
			if err == nil || !strings.Contains(err.Error(), "deadline expired") {
				t.Fatalf("cleanup error at %s = %v", stage, err)
			}
			if _, statErr := os.Stat(root); statErr != nil {
				t.Fatalf("signal stage %s removed root: %v", stage, statErr)
			}
		})
	}
}

func TestDarwinCleanupDeadlineIncludesRecordValidation(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-deadline")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}

	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}

	originalReadDir := darwinRegistryReadDir
	originalCandidates := darwinContainmentCandidates
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	originalNow := darwinCleanupNow
	originalSleep := darwinCleanupSleep
	t.Cleanup(func() {
		darwinRegistryReadDir = originalReadDir
		darwinContainmentCandidates = originalCandidates
		darwinContainmentRevalidate = originalRevalidate
		darwinContainmentSignalPID = originalSignal
		darwinCleanupNow = originalNow
		darwinCleanupSleep = originalSleep
	})

	now := time.Unix(1, 0)
	darwinCleanupNow = func() time.Time { return now }
	darwinRegistryReadDir = func(path string) ([]os.DirEntry, error) {
		entries, readErr := originalReadDir(path)
		now = now.Add(defaultCloseWait)

		return entries, readErr
	}

	candidate := DarwinContainmentCandidate{PID: 404, StartSec: 4, StartUsec: 4}
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		return []DarwinContainmentCandidate{candidate}, nil
	}
	darwinContainmentRevalidate = func(_ darwinContainmentRecord, candidate DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
		return candidate, darwinCandidateCorrelated
	}
	signalCalls := 0
	darwinContainmentSignalPID = func(int, syscall.Signal) error {
		signalCalls++

		return nil
	}
	darwinCleanupSleep = func(time.Duration) {}

	err = CleanupDarwinContainment(parent, generation.RuntimeID, true, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "deadline expired") {
		t.Fatalf("deadline cleanup error = %v", err)
	}
	if signalCalls != 0 {
		t.Fatalf("cleanup signaled %d processes after record validation exhausted the deadline", signalCalls)
	}
}

func TestDarwinCleanupRejectsRootOutsideParent(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	_, records, err := readDarwinRecords(parent, generation.RuntimeID)
	if err != nil {
		t.Fatal(err)
	}
	record := records[0]
	outsideParent := t.TempDir()
	record.GenerationRoot = filepath.Join(outsideParent, "acp-go-amp-command-outside")
	if err := os.Mkdir(record.GenerationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := replaceDarwinRecord(parent, record); err != nil {
		t.Fatal(err)
	}
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "invalid current-format") {
		t.Fatalf("outside-root cleanup error = %v", err)
	}
}

func TestDarwinCleanupRejectsMissingIncompleteRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-missing")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	if removeErr := os.Remove(root); removeErr != nil {
		t.Fatal(removeErr)
	}
	err = CleanupDarwinContainment(parent, generation.RuntimeID, true, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "missing for an incomplete runtime") {
		t.Fatalf("missing-root cleanup error = %v", err)
	}
}

func TestDarwinRecordValidationRejectsMalformedCurrentFormat(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*darwinContainmentRecord)
	}{
		{name: "state", mutate: func(record *darwinContainmentRecord) { record.State = "complete" }},
		{name: "lifecycle", mutate: func(record *darwinContainmentRecord) { record.LifecycleKind = "unknown" }},
		{name: "relative root", mutate: func(record *darwinContainmentRecord) { record.GenerationRoot = "acp-go-amp-command-relative" }},
		{name: "partial child", mutate: func(record *darwinContainmentRecord) { pid := 1; record.DirectChildPID = &pid }},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent := t.TempDir()
			root := filepath.Join(parent, "acp-go-amp-command-record")
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal(err)
			}
			generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
			if err != nil {
				t.Fatal(err)
			}
			_, records, err := readDarwinRecords(parent, generation.RuntimeID)
			if err != nil {
				t.Fatal(err)
			}
			record := records[0]
			test.mutate(&record)
			if err := replaceDarwinRecord(parent, record); err != nil {
				t.Fatal(err)
			}
			if _, _, err := readDarwinRecords(parent, generation.RuntimeID); err == nil {
				t.Fatal("malformed current-format record was accepted")
			}
		})
	}
}

func TestDarwinRecordReaderRejectsUnsafeFilesystemObjects(t *testing.T) {
	t.Run("record mode", func(t *testing.T) {
		parent := t.TempDir()
		root := filepath.Join(parent, "acp-go-amp-command-mode")
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(parent, darwinRegistryDir, generation.RuntimeID+".json")
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readDarwinRecords(parent, generation.RuntimeID); err == nil {
			t.Fatal("mode-0644 record was accepted")
		}
	})
	t.Run("record symlink", func(t *testing.T) {
		parent := t.TempDir()
		registry := filepath.Join(parent, darwinRegistryDir)
		if err := os.Mkdir(registry, 0o700); err != nil {
			t.Fatal(err)
		}
		runtimeID := strings.Repeat("a", 32)
		target := filepath.Join(parent, "target.json")
		if err := os.WriteFile(target, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(registry, runtimeID+".json")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readDarwinRecords(parent, runtimeID); err == nil {
			t.Fatal("symlink record was accepted")
		}
	})
	t.Run("registry mode", func(t *testing.T) {
		parent := t.TempDir()
		if err := os.Mkdir(filepath.Join(parent, darwinRegistryDir), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readDarwinRecords(parent, ""); err == nil {
			t.Fatal("mode-0755 registry was accepted")
		}
	})
	t.Run("registry symlink", func(t *testing.T) {
		parent := t.TempDir()
		target := t.TempDir()
		if err := os.Symlink(target, filepath.Join(parent, darwinRegistryDir)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := readDarwinRecords(parent, ""); err == nil {
			t.Fatal("symlink registry was accepted")
		}
	})
	t.Run("lock symlink", func(t *testing.T) {
		parent := t.TempDir()
		registry, err := ensureDarwinRegistry(parent)
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(parent, "lock-target")
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(registry, ".lock")); err != nil {
			t.Fatal(err)
		}
		root := filepath.Join(parent, "acp-go-amp-command-lock")
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := NewDarwinGenerationRecord(parent, root, "prompt"); err == nil {
			t.Fatal("symlink registry lock was accepted")
		}
	})
}

func TestDarwinCleanupReportsAmbiguousIdentityAndRetainsRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-ambiguous")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := currentDarwinProcessIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	_, records, err := readDarwinRecords(parent, generation.RuntimeID)
	if err != nil {
		t.Fatal(err)
	}
	record := records[0]
	record.DirectChildPID = &identity.PID
	record.DirectChildStartSec = &identity.StartSec
	record.DirectChildStartUsec = &identity.StartUsec
	pgid := identity.PID
	record.OriginalPGID = &pgid
	if replaceErr := replaceDarwinRecord(parent, record); replaceErr != nil {
		t.Fatal(replaceErr)
	}
	var output bytes.Buffer
	err = CleanupDarwinContainment(parent, generation.RuntimeID, true, &output)
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("cleanup error = %v", err)
	}
	if !strings.Contains(output.String(), `"ambiguous_pids":[`+strconv.Itoa(os.Getpid())) {
		t.Fatalf("cleanup output = %s", output.String())
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("ambiguous root was removed: %v", err)
	}
}

func darwinProcargsFixture(argv, env []string) []byte {
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, uint32(len(argv)))
	raw = append(raw, "/bin/tool"...)
	raw = append(raw, 0, 0)
	for _, value := range argv {
		raw = append(raw, value...)
		raw = append(raw, 0)
	}
	for _, value := range env {
		raw = append(raw, value...)
		raw = append(raw, 0)
	}
	if len(env) > 0 {
		raw = append(raw, 0)
	}

	return raw
}
