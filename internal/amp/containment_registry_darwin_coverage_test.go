//go:build darwin

package amp

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestDarwinRegistryCreationAndGenerationFailures(t *testing.T) {
	originalRead := darwinRuntimeIDRead
	originalIdentity := darwinProcessIdentityLookup
	t.Cleanup(func() {
		darwinRuntimeIDRead = originalRead
		darwinProcessIdentityLookup = originalIdentity
	})

	parent := t.TempDir()
	outside := filepath.Join(t.TempDir(), "acp-go-amp-command-outside")
	if _, err := NewDarwinGenerationRecord(parent, outside, "prompt"); err == nil {
		t.Fatal("outside generation root was accepted")
	}

	darwinRuntimeIDRead = func([]byte) (int, error) { return 0, errors.New("random") }
	if _, err := NewDarwinGenerationRecord(parent, filepath.Join(parent, "acp-go-amp-command-random"), "prompt"); err == nil || !strings.Contains(err.Error(), "runtime id") {
		t.Fatalf("random failure = %v", err)
	}
	darwinRuntimeIDRead = originalRead

	darwinProcessIdentityLookup = func(int) (darwinProcessIdentity, error) { return darwinProcessIdentity{}, errors.New("identity") }
	if _, err := NewDarwinGenerationRecord(parent, filepath.Join(parent, "acp-go-amp-command-identity"), "prompt"); err == nil || !strings.Contains(err.Error(), "wrapper") {
		t.Fatalf("identity failure = %v", err)
	}
	darwinProcessIdentityLookup = originalIdentity

	parentFile := filepath.Join(t.TempDir(), "parent-file")
	if err := os.WriteFile(parentFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDarwinGenerationRecord(parentFile, filepath.Join(parentFile, "acp-go-amp-command-create"), "prompt"); err == nil {
		t.Fatal("file scratch parent was accepted")
	}

	root := filepath.Join(parent, "acp-go-amp-command-start")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}
	darwinProcessIdentityLookup = func(int) (darwinProcessIdentity, error) { return darwinProcessIdentity{}, errors.New("child") }
	if startErr := generation.started(42, 42); startErr == nil || !strings.Contains(startErr.Error(), "direct-child") {
		t.Fatalf("child identity failure = %v", startErr)
	}
	darwinProcessIdentityLookup = originalIdentity
	if finishErr := generation.finish(false); finishErr != nil {
		t.Fatal(finishErr)
	}
	_, records, err := readDarwinRecords(parent, generation.RuntimeID)
	if err != nil || len(records) != 1 || records[0].State != darwinStateIncomplete {
		t.Fatalf("incomplete record = %#v, err=%v", records, err)
	}
}

func TestDarwinRegistryMaintenanceAndAtomicWriterBranches(t *testing.T) {
	parent := t.TempDir()
	registry, registryErr := ensureDarwinRegistry(parent)
	if registryErr != nil {
		t.Fatal(registryErr)
	}
	if _, ensureErr := ensureDarwinRegistry(parent); ensureErr != nil {
		t.Fatal(ensureErr)
	}

	fileRegistryParent := t.TempDir()
	if writeErr := os.WriteFile(filepath.Join(fileRegistryParent, darwinRegistryDir), nil, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, ensureErr := ensureDarwinRegistry(fileRegistryParent); ensureErr == nil {
		t.Fatal("file registry was accepted")
	}

	unlockDarwinRegistry(nil)
	record := validDarwinTestRecord(parent, strings.Repeat("1", 32))
	if writeErr := writeDarwinRecordAtomic(registry, record, true); writeErr != nil {
		t.Fatal(writeErr)
	}
	if writeErr := writeDarwinRecordAtomic(registry, record, true); writeErr == nil || !strings.Contains(writeErr.Error(), "collision") {
		t.Fatalf("collision error = %v", writeErr)
	}
	if writeErr := writeDarwinRecordAtomic(filepath.Join(parent, "missing"), record, false); writeErr == nil {
		t.Fatal("missing registry accepted")
	}

	expiredRecord := validDarwinTestRecord(parent, strings.Repeat("a", 32))
	expiredRecord.State = darwinStateGroupAbsent
	if err := writeDarwinRecordAtomic(registry, expiredRecord, true); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(registry, expiredRecord.RuntimeID+".json")
	oldTime := time.Now().Add(-darwinRecordLifetime - time.Hour)
	if err := os.Chtimes(old, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(registry, "ignored.txt"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(registry, "ignored.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	record.RuntimeID = strings.Repeat("2", 32)
	if err := createDarwinRecord(parent, record); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired record remains: %v", err)
	}

	for index, state := range []string{darwinStateRunning, darwinStateIncomplete} {
		retained := validDarwinTestRecord(parent, strings.Repeat(string(rune('b'+index)), 32))
		retained.State = state
		if err := writeDarwinRecordAtomic(registry, retained, true); err != nil {
			t.Fatal(err)
		}
		retainedPath := filepath.Join(registry, retained.RuntimeID+".json")
		if err := os.Chtimes(retainedPath, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}
		record.RuntimeID = strings.Repeat(string(rune('d'+index)), 32)
		if err := createDarwinRecord(parent, record); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(retainedPath); err != nil {
			t.Fatalf("expired %s record was removed: %v", state, err)
		}
	}

	danglingParent := t.TempDir()
	danglingRegistry, danglingErr := ensureDarwinRegistry(danglingParent)
	if danglingErr != nil {
		t.Fatal(danglingErr)
	}
	if err := os.Symlink(filepath.Join(danglingParent, "missing"), filepath.Join(danglingRegistry, "dangling.json")); err != nil {
		t.Fatal(err)
	}
	record = validDarwinTestRecord(danglingParent, strings.Repeat("3", 32))
	if err := createDarwinRecord(danglingParent, record); err != nil {
		t.Fatalf("symlink registry entry maintenance = %v", err)
	}

	badLockParent := t.TempDir()
	badLockRegistry, badLockErr := ensureDarwinRegistry(badLockParent)
	if badLockErr != nil {
		t.Fatal(badLockErr)
	}
	if err := os.WriteFile(filepath.Join(badLockRegistry, ".lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := lockDarwinRegistry(badLockRegistry); err == nil {
		t.Fatal("mode-0644 lock was accepted")
	}
}

func TestCreateDarwinRecordExpiredReadFailures(t *testing.T) {
	t.Run("malformed", func(t *testing.T) {
		parent := t.TempDir()
		registry, err := ensureDarwinRegistry(parent)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(registry, strings.Repeat("a", 32)+".json")
		if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-darwinRecordLifetime - time.Hour)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
		record := validDarwinTestRecord(parent, strings.Repeat("b", 32))
		if err := createDarwinRecord(parent, record); err == nil || !strings.Contains(err.Error(), "inspect expired") {
			t.Fatalf("malformed expired record error = %v", err)
		}
	})

	t.Run("vanished during reread", func(t *testing.T) {
		parent := t.TempDir()
		registry, err := ensureDarwinRegistry(parent)
		if err != nil {
			t.Fatal(err)
		}
		expired := validDarwinTestRecord(parent, strings.Repeat("c", 32))
		expired.State = darwinStateGroupAbsent
		if writeErr := writeDarwinRecordAtomic(registry, expired, true); writeErr != nil {
			t.Fatal(writeErr)
		}
		path := filepath.Join(registry, expired.RuntimeID+".json")
		old := time.Now().Add(-darwinRecordLifetime - time.Hour)
		if chtimesErr := os.Chtimes(path, old, old); chtimesErr != nil {
			t.Fatal(chtimesErr)
		}
		entries, err := os.ReadDir(registry)
		if err != nil {
			t.Fatal(err)
		}
		var selected os.DirEntry
		for _, entry := range entries {
			if entry.Name() == expired.RuntimeID+".json" {
				selected = entry

				break
			}
		}
		if selected == nil {
			t.Fatal("expired record entry missing")
		}

		originalReadDir := darwinRegistryReadDir
		t.Cleanup(func() { darwinRegistryReadDir = originalReadDir })
		calls := 0
		darwinRegistryReadDir = func(string) ([]os.DirEntry, error) {
			calls++
			if calls == 1 {
				return []os.DirEntry{selected}, nil
			}

			return nil, nil
		}
		record := validDarwinTestRecord(parent, strings.Repeat("d", 32))
		if err := createDarwinRecord(parent, record); err == nil || !strings.Contains(err.Error(), "unavailable or ambiguous") {
			t.Fatalf("vanished expired record error = %v", err)
		}
	})
}

func TestDarwinDiagnoseAndCleanupFailureBoundaries(t *testing.T) {
	parent, root, generation := newDarwinCoverageGeneration(t, "cleanup-errors")
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

	want := errors.New("scan")
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) { return nil, want }
	if err := DiagnoseDarwinContainment(parent, io.Discard); !errors.Is(err, want) {
		t.Fatalf("diagnose scan error = %v", err)
	}
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, io.Discard); !errors.Is(err, want) {
		t.Fatalf("cleanup first scan error = %v", err)
	}

	for failAt := 2; failAt <= 4; failAt++ {
		calls := 0
		darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
			calls++
			if calls == failAt {
				return nil, want
			}

			return nil, nil
		}
		if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, io.Discard); !errors.Is(err, want) {
			t.Fatalf("cleanup scan %d error = %v", failAt, err)
		}
	}

	candidate := DarwinContainmentCandidate{PID: 701, StartSec: 7, StartUsec: 1}
	darwinContainmentRevalidate = func(_ darwinContainmentRecord, value DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
		return value, darwinCandidateCorrelated
	}
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		return []DarwinContainmentCandidate{candidate}, nil
	}
	darwinContainmentSignalPID = func(int, syscall.Signal) error { return syscall.EPERM }
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, io.Discard); err == nil || !strings.Contains(err.Error(), "terminate") {
		t.Fatalf("TERM failure = %v", err)
	}

	scans := 0
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		scans++
		if scans >= 4 {
			return nil, nil
		}

		return []DarwinContainmentCandidate{candidate}, nil
	}
	darwinContainmentSignalPID = func(_ int, signal syscall.Signal) error {
		if signal == syscall.SIGKILL {
			return syscall.EPERM
		}

		return nil
	}
	darwinCleanupSleep = func(time.Duration) {}
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, io.Discard); err == nil || !strings.Contains(err.Error(), "kill") {
		t.Fatalf("KILL failure = %v", err)
	}

	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) { return nil, nil }
	darwinContainmentSignalPID = originalSignal
	darwinRemoveGenerationRoot = func(string) error { return errors.New("remove") }
	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, io.Discard); err == nil || !strings.Contains(err.Error(), "remove") {
		t.Fatalf("root removal failure = %v", err)
	}
	darwinRemoveGenerationRoot = originalRemove
	if _, err := os.Stat(root); err != nil {
		t.Fatal(err)
	}

	if err := CleanupDarwinContainment(parent, generation.RuntimeID, true, failingWriter{err: errors.New("write")}); err == nil || !strings.Contains(err.Error(), "write") {
		t.Fatalf("report writer failure = %v", err)
	}
}

func TestDarwinCleanupStateCandidateBranches(t *testing.T) {
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	originalNow := darwinCleanupNow
	originalCandidates := darwinContainmentCandidates
	originalSleep := darwinCleanupSleep
	t.Cleanup(func() {
		darwinContainmentRevalidate = originalRevalidate
		darwinContainmentSignalPID = originalSignal
		darwinCleanupNow = originalNow
		darwinContainmentCandidates = originalCandidates
		darwinCleanupSleep = originalSleep
	})

	now := time.Unix(10, 0)
	darwinCleanupNow = func() time.Time { return now }
	state := darwinCleanupState{
		deadline: now.Add(time.Second), ambiguous: map[int]struct{}{},
		termIdentities: map[DarwinContainmentCandidate]struct{}{},
	}
	self := DarwinContainmentCandidate{PID: os.Getpid()}
	gone := DarwinContainmentCandidate{PID: 10}
	ambiguous := DarwinContainmentCandidate{PID: 11}
	correlated := DarwinContainmentCandidate{PID: 12, StartSec: 1}
	darwinContainmentRevalidate = func(_ darwinContainmentRecord, candidate DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
		switch candidate.PID {
		case gone.PID:
			return DarwinContainmentCandidate{}, darwinCandidateGone
		case ambiguous.PID:
			return DarwinContainmentCandidate{}, darwinCandidateAmbiguous
		default:
			return candidate, darwinCandidateCorrelated
		}
	}
	darwinContainmentSignalPID = func(int, syscall.Signal) error { return nil }
	signaled, err := state.signalTermCandidates([]DarwinContainmentCandidate{self, gone, ambiguous, correlated, correlated})
	if err != nil || !signaled || !reflect.DeepEqual(state.termSignaled, []int{correlated.PID}) {
		t.Fatalf("TERM branches signaled=%v pids=%v err=%v", signaled, state.termSignaled, err)
	}

	state.termIdentities = map[DarwinContainmentCandidate]struct{}{correlated: {}}
	notTermed := DarwinContainmentCandidate{PID: 13}
	if err := state.signalKillCandidates([]DarwinContainmentCandidate{self, gone, ambiguous, notTermed, correlated}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state.killSignaled, []int{correlated.PID}) {
		t.Fatalf("KILL branches = %v", state.killSignaled)
	}

	now = state.deadline
	deadlineCandidate := DarwinContainmentCandidate{PID: 14}
	if signaled, err := state.signalTermCandidates([]DarwinContainmentCandidate{deadlineCandidate}); err != nil || signaled {
		t.Fatalf("deadline TERM = %v, %v", signaled, err)
	}
	state.termIdentities[deadlineCandidate] = struct{}{}
	if err := state.signalKillCandidates([]DarwinContainmentCandidate{deadlineCandidate}); err != nil {
		t.Fatal(err)
	}

	now = time.Unix(20, 0)
	state.deadline = now.Add(time.Second)
	state.deadlineExpired = false
	scans := 0
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		scans++
		if scans == 1 {
			return []DarwinContainmentCandidate{correlated}, nil
		}

		return nil, nil
	}
	darwinCleanupSleep = func(time.Duration) {}
	if remaining, err := state.pollRemaining(); err != nil || len(remaining) != 0 {
		t.Fatalf("poll remaining = %v, %v", remaining, err)
	}
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) { return nil, errors.New("poll") }
	if _, err := state.pollRemaining(); err == nil {
		t.Fatal("poll scan error ignored")
	}

	if got := cleanupSleepDuration(time.Second, now); got != 0 {
		t.Fatalf("expired sleep = %v", got)
	}
	if got := cleanupSleepDuration(time.Second, now.Add(10*time.Millisecond)); got != 10*time.Millisecond {
		t.Fatalf("bounded sleep = %v", got)
	}
	if got := cleanupSleepDuration(time.Millisecond, now.Add(time.Second)); got != time.Millisecond {
		t.Fatalf("wanted sleep = %v", got)
	}
}

func TestDarwinRecordReaderParserAndValidationEdges(t *testing.T) {
	if parent, records, err := readDarwinRecords(t.TempDir(), ""); err != nil || parent == "" || records != nil {
		t.Fatalf("missing registry = %q %#v %v", parent, records, err)
	}
	if _, _, err := readDarwinRecords("", ""); err == nil {
		t.Fatal("empty scratch parent accepted")
	}

	parent := t.TempDir()
	registry, registryErr := ensureDarwinRegistry(parent)
	if registryErr != nil {
		t.Fatal(registryErr)
	}
	for name, data := range map[string]string{
		strings.Repeat("4", 32) + ".json": "{",
		strings.Repeat("5", 32) + ".json": "{}\n{}\n",
		strings.Repeat("6", 32) + ".json": `{"unknown":true}`,
	} {
		t.Run(name[:1], func(t *testing.T) {
			path := filepath.Join(registry, name)
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := readDarwinRecords(parent, strings.TrimSuffix(name, ".json")); err == nil {
				t.Fatal("malformed record accepted")
			}
		})
	}

	base := validDarwinTestRecord(parent, strings.Repeat("a", 32))
	invalids := []darwinContainmentRecord{
		{},
		func() darwinContainmentRecord {
			value := base
			value.State = "bad"

			return value
		}(),
		func() darwinContainmentRecord {
			value := base
			value.WrapperStartUsec = 1_000_000

			return value
		}(),
		func() darwinContainmentRecord {
			value := base
			pid := 0
			sec := int64(0)
			usec := int32(0)
			pgid := 1
			value.DirectChildPID = &pid
			value.DirectChildStartSec = &sec
			value.DirectChildStartUsec = &usec
			value.OriginalPGID = &pgid

			return value
		}(),
	}
	for _, record := range invalids {
		if validCurrentDarwinRecord(parent, record.RuntimeID, record) {
			t.Fatalf("invalid record accepted: %#v", record)
		}
	}
	if !validCurrentDarwinRecord(parent, base.RuntimeID, base) {
		t.Fatal("valid pre-spawn record rejected")
	}
	pid, pgid, sec, usec := 42, 42, int64(1), int32(2)
	base.DirectChildPID, base.OriginalPGID = &pid, &pgid
	base.DirectChildStartSec, base.DirectChildStartUsec = &sec, &usec
	if !validCurrentDarwinRecord(parent, base.RuntimeID, base) {
		t.Fatal("valid child record rejected")
	}

	for _, value := range []string{"short", strings.Repeat("A", 32), strings.Repeat("z", 32)} {
		if validDarwinRuntimeID(value) {
			t.Fatalf("invalid runtime id accepted: %q", value)
		}
	}
	if !validDarwinRuntimeID(strings.Repeat("b", 32)) {
		t.Fatal("valid runtime id rejected")
	}
	for _, lifecycle := range []string{"runtime", "session", "prompt", "discovery"} {
		if !validDarwinLifecycle(lifecycle) {
			t.Fatalf("valid lifecycle rejected: %s", lifecycle)
		}
	}
	if validDarwinLifecycle("other") {
		t.Fatal("invalid lifecycle accepted")
	}
}

func TestDarwinGenerationRootAndProcessCorrelationEdges(t *testing.T) {
	parent := t.TempDir()
	if _, _, err := validatedDarwinGenerationRoot(parent, filepath.Join(parent, "wrong-prefix")); err == nil {
		t.Fatal("wrong root prefix accepted")
	}
	if _, _, err := validatedDarwinGenerationRoot(filepath.Join(parent, "missing-parent"), filepath.Join(parent, "acp-go-amp-command-missing")); err == nil {
		t.Fatal("missing parent accepted")
	}
	outside := filepath.Join(t.TempDir(), "acp-go-amp-command-missing")
	if _, _, err := validatedDarwinGenerationRoot(parent, outside); err == nil {
		t.Fatal("missing outside root accepted")
	}
	file := filepath.Join(parent, "acp-go-amp-command-file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validatedDarwinGenerationRoot(parent, file); err == nil {
		t.Fatal("file root accepted")
	}
	realRoot := filepath.Join(parent, "acp-go-amp-command-real")
	if err := os.Mkdir(realRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "acp-go-amp-command-link")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := validatedDarwinGenerationRoot(parent, link); err == nil {
		t.Fatal("symlink root accepted")
	}
	resolvedRealRoot, err := filepath.EvalSymlinks(realRoot)
	if err != nil {
		t.Fatal(err)
	}
	if root, exists, err := validatedDarwinGenerationRoot(parent, realRoot); err != nil || !exists || root != resolvedRealRoot {
		t.Fatalf("valid root = %q %v %v", root, exists, err)
	}

	originalIdentity := darwinProcessIdentityLookup
	originalEnvironment := darwinEnvironmentLookup
	originalSameUID := darwinIdentitySameUID
	t.Cleanup(func() {
		darwinProcessIdentityLookup = originalIdentity
		darwinEnvironmentLookup = originalEnvironment
		darwinIdentitySameUID = originalSameUID
	})
	record := validDarwinTestRecord(parent, strings.Repeat("c", 32))
	pid, sec, usec := 99, int64(10), int32(20)
	record.DirectChildPID, record.DirectChildStartSec, record.DirectChildStartUsec = &pid, &sec, &usec

	record.State = darwinStateGroupAbsent
	if values := ambiguousDarwinRecordedIdentity(record); values != nil {
		t.Fatalf("group-absent ambiguity = %v", values)
	}
	record.State = darwinStateRunning
	darwinProcessIdentityLookup = func(int) (darwinProcessIdentity, error) { return darwinProcessIdentity{}, os.ErrNotExist }
	if values := ambiguousDarwinRecordedIdentity(record); values != nil {
		t.Fatalf("gone identity ambiguous = %v", values)
	}
	darwinProcessIdentityLookup = func(int) (darwinProcessIdentity, error) { return darwinProcessIdentity{}, errors.New("unreadable") }
	if values := ambiguousDarwinRecordedIdentity(record); !reflect.DeepEqual(values, []int{pid}) {
		t.Fatalf("unreadable ambiguity = %v", values)
	}
	identity := darwinProcessIdentity{PID: pid, StartSec: sec, StartUsec: usec, UID: uint32(os.Getuid())}
	darwinProcessIdentityLookup = func(int) (darwinProcessIdentity, error) { return identity, nil }
	darwinIdentitySameUID = func(darwinProcessIdentity) bool { return false }
	if values := ambiguousDarwinRecordedIdentity(record); !reflect.DeepEqual(values, []int{pid}) {
		t.Fatalf("uid ambiguity = %v", values)
	}
	darwinIdentitySameUID = originalSameUID
	darwinEnvironmentLookup = func(int) (map[string]string, error) { return nil, errors.New("env") }
	if values := ambiguousDarwinRecordedIdentity(record); !reflect.DeepEqual(values, []int{pid}) {
		t.Fatalf("env ambiguity = %v", values)
	}
	darwinEnvironmentLookup = func(int) (map[string]string, error) {
		return map[string]string{DarwinRuntimeIDEnv: record.RuntimeID, DarwinScratchRootEnv: record.GenerationRoot}, nil
	}
	if values := ambiguousDarwinRecordedIdentity(record); values != nil {
		t.Fatalf("correlated identity ambiguous = %v", values)
	}

	candidate := DarwinContainmentCandidate{PID: pid, StartSec: sec, StartUsec: usec}
	darwinProcessIdentityLookup = func(int) (darwinProcessIdentity, error) { return darwinProcessIdentity{}, syscall.ESRCH }
	if _, status := revalidateDarwinCandidate(record, candidate); status != darwinCandidateGone {
		t.Fatalf("gone status = %d", status)
	}
	darwinProcessIdentityLookup = func(int) (darwinProcessIdentity, error) { return identity, nil }
	darwinEnvironmentLookup = func(int) (map[string]string, error) { return map[string]string{}, nil }
	if _, status := revalidateDarwinCandidate(record, candidate); status != darwinCandidateAmbiguous {
		t.Fatalf("marker mismatch status = %d", status)
	}
	darwinEnvironmentLookup = func(int) (map[string]string, error) {
		return map[string]string{DarwinRuntimeIDEnv: record.RuntimeID, DarwinScratchRootEnv: record.GenerationRoot}, nil
	}
	if got, status := revalidateDarwinCandidate(record, candidate); status != darwinCandidateCorrelated || got != candidate {
		t.Fatalf("correlated status = %#v %d", got, status)
	}

	if got := candidatePIDs([]DarwinContainmentCandidate{{PID: 3}, {PID: 1}}); !reflect.DeepEqual(got, []int{1, 3}) {
		t.Fatalf("candidate pids = %v", got)
	}
	if got := sortedPIDSet(map[int]struct{}{9: {}, 2: {}}); !reflect.DeepEqual(got, []int{2, 9}) {
		t.Fatalf("sorted pid set = %v", got)
	}
}

func TestParseDarwinProcessEnvironmentAllMalformedShapes(t *testing.T) {
	truncatedExecutable := make([]byte, 4)
	truncatedExecutable[0] = 1
	for name, raw := range map[string][]byte{
		"argc too large":        {9, 0, 0, 0},
		"truncated executable":  truncatedExecutable,
		"truncated argv":        append([]byte{2, 0, 0, 0}, []byte("/bin/tool\x00\x00only-one\x00")...),
		"truncated environment": append(darwinProcargsFixture([]string{"tool"}, nil), []byte("KEY=value")...),
		"unterminated environment": append(
			darwinProcargsFixture([]string{"tool"}, nil), []byte("KEY=value\x00")...,
		),
		"ambiguous environment": append(darwinProcargsFixture([]string{"tool"}, nil), []byte("\x00junk")...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseDarwinProcessEnvironment(raw); err == nil {
				t.Fatalf("malformed procargs accepted: %v", raw)
			}
		})
	}
	if value, offset, ok := nextDarwinProcString([]byte("value"), 0); ok || value != "" || offset != 0 {
		t.Fatalf("unterminated string = %q %d %v", value, offset, ok)
	}
	if _, _, ok := nextDarwinProcString(nil, -1); ok {
		t.Fatal("negative offset accepted")
	}
}

func TestDarwinRegistrySyscallErrorSeams(t *testing.T) { //nolint:gocyclo // One table-like seam audit keeps global restoration atomic.
	originalAbs := darwinRecordAbs
	originalMkdir := darwinRegistryMkdirAll
	originalLstat := darwinRegistryLstat
	originalChmod := darwinRegistryChmod
	originalReadDir := darwinRegistryReadDir
	originalRemove := darwinRegistryRemove
	originalOpen := darwinRegistryOpen
	originalFlock := darwinRegistryFlock
	originalMarshal := darwinRecordMarshal
	originalStat := darwinRecordStat
	originalCreateTemp := darwinRecordCreateTemp
	originalFileChmod := darwinRecordFileChmod
	originalFileWrite := darwinRecordFileWrite
	originalFileSync := darwinRecordFileSync
	originalFileClose := darwinRecordFileClose
	originalFileStat := darwinRecordFileStat
	originalRename := darwinRecordRename
	originalKinfo := darwinKinfoProc
	originalKinfoSlice := darwinKinfoProcSlice
	originalRaw := darwinProcargsRaw
	originalEval := darwinEvalSymlinks
	restore := func() {
		darwinRecordAbs = originalAbs
		darwinRegistryMkdirAll = originalMkdir
		darwinRegistryLstat = originalLstat
		darwinRegistryChmod = originalChmod
		darwinRegistryReadDir = originalReadDir
		darwinRegistryRemove = originalRemove
		darwinRegistryOpen = originalOpen
		darwinRegistryFlock = originalFlock
		darwinRecordMarshal = originalMarshal
		darwinRecordStat = originalStat
		darwinRecordCreateTemp = originalCreateTemp
		darwinRecordFileChmod = originalFileChmod
		darwinRecordFileWrite = originalFileWrite
		darwinRecordFileSync = originalFileSync
		darwinRecordFileClose = originalFileClose
		darwinRecordFileStat = originalFileStat
		darwinRecordRename = originalRename
		darwinKinfoProc = originalKinfo
		darwinKinfoProcSlice = originalKinfoSlice
		darwinProcargsRaw = originalRaw
		darwinEvalSymlinks = originalEval
	}
	t.Cleanup(restore)

	want := errors.New("seam")
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-seam")
	absCalls := 0
	darwinRecordAbs = func(path string) (string, error) {
		absCalls++
		if absCalls == 1 {
			return "", want
		}

		return originalAbs(path)
	}
	if _, err := NewDarwinGenerationRecord(parent, root, "prompt"); !errors.Is(err, want) {
		t.Fatalf("parent abs error = %v", err)
	}
	absCalls = 0
	darwinRecordAbs = func(path string) (string, error) {
		absCalls++
		if absCalls == 2 {
			return "", want
		}

		return originalAbs(path)
	}
	if _, err := NewDarwinGenerationRecord(parent, root, "prompt"); !errors.Is(err, want) {
		t.Fatalf("root abs error = %v", err)
	}
	darwinRecordAbs = originalAbs

	darwinRegistryMkdirAll = func(string, os.FileMode) error { return want }
	if _, err := ensureDarwinRegistry(parent); !errors.Is(err, want) {
		t.Fatalf("registry mkdir error = %v", err)
	}
	darwinRegistryMkdirAll = originalMkdir
	darwinRegistryLstat = func(string) (os.FileInfo, error) { return nil, want }
	if _, err := ensureDarwinRegistry(parent); !errors.Is(err, want) {
		t.Fatalf("registry lstat error = %v", err)
	}
	darwinRegistryLstat = originalLstat
	darwinRegistryChmod = func(string, os.FileMode) error { return want }
	if _, err := ensureDarwinRegistry(parent); !errors.Is(err, want) {
		t.Fatalf("registry chmod error = %v", err)
	}
	darwinRegistryChmod = originalChmod

	registry, registryErr := ensureDarwinRegistry(parent)
	if registryErr != nil {
		t.Fatal(registryErr)
	}
	darwinRegistryLstat = func(string) (os.FileInfo, error) { return nil, want }
	if _, lockErr := lockDarwinRegistry(registry); !errors.Is(lockErr, want) {
		t.Fatalf("lock lstat error = %v", lockErr)
	}
	darwinRegistryLstat = originalLstat
	darwinRegistryOpen = func(string, int, uint32) (int, error) { return -1, want }
	if _, lockErr := lockDarwinRegistry(registry); !errors.Is(lockErr, want) {
		t.Fatalf("lock open error = %v", lockErr)
	}
	darwinRegistryOpen = originalOpen
	darwinRecordFileStat = func(*os.File) (os.FileInfo, error) { return nil, want }
	if _, lockErr := lockDarwinRegistry(registry); !errors.Is(lockErr, want) {
		t.Fatalf("lock stat error = %v", lockErr)
	}
	darwinRecordFileStat = originalFileStat
	darwinRegistryFlock = func(int, int) error { return want }
	if _, lockErr := lockDarwinRegistry(registry); !errors.Is(lockErr, want) {
		t.Fatalf("flock error = %v", lockErr)
	}
	darwinRegistryFlock = originalFlock

	record := validDarwinTestRecord(parent, strings.Repeat("7", 32))
	darwinRecordMarshal = func(any) ([]byte, error) { return nil, want }
	if writeErr := writeDarwinRecordAtomic(registry, record, false); !errors.Is(writeErr, want) {
		t.Fatalf("marshal error = %v", writeErr)
	}
	darwinRecordMarshal = originalMarshal
	darwinRecordStat = func(string) (os.FileInfo, error) { return nil, want }
	if writeErr := writeDarwinRecordAtomic(registry, record, true); !errors.Is(writeErr, want) {
		t.Fatalf("target stat error = %v", writeErr)
	}
	darwinRecordStat = originalStat
	darwinRecordCreateTemp = func(string, string) (*os.File, error) { return nil, want }
	if writeErr := writeDarwinRecordAtomic(registry, record, false); !errors.Is(writeErr, want) {
		t.Fatalf("create temp error = %v", writeErr)
	}
	darwinRecordCreateTemp = originalCreateTemp

	for name, install := range map[string]func(){
		"chmod": func() { darwinRecordFileChmod = func(*os.File, os.FileMode) error { return want } },
		"write": func() { darwinRecordFileWrite = func(*os.File, []byte) error { return want } },
		"sync":  func() { darwinRecordFileSync = func(*os.File) error { return want } },
		"close": func() { darwinRecordFileClose = func(*os.File) error { return want } },
	} {
		t.Run(name, func(t *testing.T) {
			restore()
			install()
			writeErr := writeDarwinRecordAtomic(registry, record, false)
			if !errors.Is(writeErr, want) {
				t.Fatalf("%s error = %v", name, writeErr)
			}
		})
	}
	restore()
	darwinRecordRename = func(string, string) error { return want }
	if writeErr := writeDarwinRecordAtomic(registry, record, false); !errors.Is(writeErr, want) {
		t.Fatalf("rename error = %v", writeErr)
	}
	restore()

	darwinRegistryReadDir = func(string) ([]os.DirEntry, error) { return nil, want }
	if createErr := createDarwinRecord(parent, record); !errors.Is(createErr, want) {
		t.Fatalf("create read-dir error = %v", createErr)
	}
	darwinRegistryReadDir = originalReadDir
	infoPath := filepath.Join(t.TempDir(), "entry.json")
	if writeErr := os.WriteFile(infoPath, nil, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	info, statErr := os.Stat(infoPath)
	if statErr != nil {
		t.Fatal(statErr)
	}
	darwinRegistryReadDir = func(string) ([]os.DirEntry, error) {
		return []os.DirEntry{generationDirEntry{info: info, err: want}}, nil
	}
	if createErr := createDarwinRecord(parent, record); !errors.Is(createErr, want) {
		t.Fatalf("entry info error = %v", createErr)
	}

	darwinRegistryReadDir = originalReadDir
	expiredRecord := validDarwinTestRecord(parent, strings.Repeat("6", 32))
	expiredRecord.State = darwinStateGroupAbsent
	expiredPath := filepath.Join(registry, expiredRecord.RuntimeID+".json")
	if writeErr := writeDarwinRecordAtomic(registry, expiredRecord, true); writeErr != nil {
		t.Fatal(writeErr)
	}
	oldTime := time.Now().Add(-darwinRecordLifetime - time.Hour)
	if chtimesErr := os.Chtimes(expiredPath, oldTime, oldTime); chtimesErr != nil {
		t.Fatal(chtimesErr)
	}
	darwinRegistryRemove = func(string) error { return want }
	record.RuntimeID = strings.Repeat("7", 32)
	if err := createDarwinRecord(parent, record); !errors.Is(err, want) {
		t.Fatalf("expire removal error = %v", err)
	}
	darwinRegistryRemove = originalRemove
	entries := make([]os.DirEntry, darwinRecordLimit)
	for index := range entries {
		entries[index] = generationDirEntry{info: info}
	}
	darwinRegistryReadDir = func(string) ([]os.DirEntry, error) { return entries, nil }
	if err := createDarwinRecord(parent, record); err == nil || !strings.Contains(err.Error(), "full") {
		t.Fatalf("full registry error = %v", err)
	}
	restore()

	darwinRegistryMkdirAll = func(string, os.FileMode) error { return want }
	if err := replaceDarwinRecord(parent, record); !errors.Is(err, want) {
		t.Fatalf("replace ensure error = %v", err)
	}
	restore()
	if err := os.WriteFile(filepath.Join(registry, ".lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(registry, ".lock"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := replaceDarwinRecord(parent, record); err == nil {
		t.Fatal("replace lock error ignored")
	}
	if err := os.Remove(filepath.Join(registry, ".lock")); err != nil {
		t.Fatal(err)
	}

	darwinKinfoProc = func(string, ...int) (*unix.KinfoProc, error) { return nil, want }
	if _, err := currentDarwinProcessIdentity(1); !errors.Is(err, want) {
		t.Fatalf("kinfo error = %v", err)
	}
	darwinKinfoProc = func(string, ...int) (*unix.KinfoProc, error) { return &unix.KinfoProc{}, nil }
	if _, err := currentDarwinProcessIdentity(1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("kinfo pid mismatch = %v", err)
	}
	restore()
	darwinKinfoProcSlice = func(string, ...int) ([]unix.KinfoProc, error) { return nil, want }
	if _, err := correlatedDarwinCandidates(record); !errors.Is(err, want) {
		t.Fatalf("process enumeration error = %v", err)
	}
	darwinProcargsRaw = func(string, ...int) ([]byte, error) { return nil, want }
	if _, err := darwinProcessEnvironment(1); !errors.Is(err, want) {
		t.Fatalf("procargs error = %v", err)
	}
	restore()

	darwinRecordAbs = func(string) (string, error) { return "", want }
	if _, _, err := readDarwinRecords(parent, ""); !errors.Is(err, want) {
		t.Fatalf("reader abs error = %v", err)
	}
	restore()
	darwinRegistryLstat = func(string) (os.FileInfo, error) { return nil, want }
	if _, _, err := readDarwinRecords(parent, ""); !errors.Is(err, want) {
		t.Fatalf("reader registry lstat error = %v", err)
	}
	restore()
	darwinRegistryReadDir = func(string) ([]os.DirEntry, error) { return nil, want }
	if _, _, err := readDarwinRecords(parent, ""); !errors.Is(err, want) {
		t.Fatalf("reader read-dir error = %v", err)
	}
	restore()

	validRecord := validDarwinTestRecord(parent, strings.Repeat("8", 32))
	if err := writeDarwinRecordAtomic(registry, validRecord, false); err != nil {
		t.Fatal(err)
	}
	darwinRegistryLstat = func(path string) (os.FileInfo, error) {
		if strings.HasSuffix(path, validRecord.RuntimeID+".json") {
			return nil, want
		}

		return originalLstat(path)
	}
	if _, _, err := readDarwinRecords(parent, validRecord.RuntimeID); !errors.Is(err, want) {
		t.Fatalf("record lstat error = %v", err)
	}
	restore()
	darwinRegistryOpen = func(string, int, uint32) (int, error) { return -1, want }
	if _, _, err := readDarwinRecords(parent, validRecord.RuntimeID); !errors.Is(err, want) {
		t.Fatalf("record open error = %v", err)
	}
	restore()
	darwinRecordFileStat = func(*os.File) (os.FileInfo, error) { return nil, want }
	if _, _, err := readDarwinRecords(parent, validRecord.RuntimeID); !errors.Is(err, want) {
		t.Fatalf("record fstat error = %v", err)
	}
	restore()
	darwinRecordFileClose = func(file *os.File) error {
		_ = file.Close()

		return want
	}
	if _, _, err := readDarwinRecords(parent, validRecord.RuntimeID); !errors.Is(err, want) {
		t.Fatalf("record close error = %v", err)
	}
	restore()

	if err := CleanupDarwinContainment(parent, strings.Repeat("g", 32), true, io.Discard); err == nil {
		t.Fatal("non-hex runtime id accepted")
	}
	darwinRecordAbs = func(string) (string, error) { return "", want }
	if err := CleanupDarwinContainment(parent, validRecord.RuntimeID, true, io.Discard); !errors.Is(err, want) {
		t.Fatalf("cleanup reader error = %v", err)
	}
	restore()

	darwinRecordAbs = func(string) (string, error) { return "", want }
	if _, _, err := validatedDarwinGenerationRoot(parent, filepath.Join(parent, "acp-go-amp-command-missing")); err == nil {
		t.Fatalf("missing-root abs error = %v", err)
	}
	restore()
	darwinRegistryLstat = func(string) (os.FileInfo, error) { return nil, want }
	if _, _, err := validatedDarwinGenerationRoot(parent, filepath.Join(parent, "acp-go-amp-command-root")); !errors.Is(err, want) {
		t.Fatalf("root lstat error = %v", err)
	}
	restore()
	evalRoot := filepath.Join(parent, "acp-go-amp-command-eval")
	if err := os.Mkdir(evalRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	darwinEvalSymlinks = func(path string) (string, error) {
		if path == parent {
			return originalEval(path)
		}

		return "", want
	}
	if _, _, err := validatedDarwinGenerationRoot(parent, evalRoot); !errors.Is(err, want) {
		t.Fatalf("root eval error = %v", err)
	}
	restore()

	state := darwinCleanupState{
		deadline: time.Now().Add(time.Second), record: validRecord, ambiguous: map[int]struct{}{},
		termIdentities: map[DarwinContainmentCandidate]struct{}{},
	}
	candidate := DarwinContainmentCandidate{PID: 44}
	state.termIdentities[candidate] = struct{}{}
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	darwinContainmentRevalidate = func(darwinContainmentRecord, DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
		return candidate, darwinCandidateCorrelated
	}
	darwinContainmentSignalPID = func(int, syscall.Signal) error { return syscall.ESRCH }
	if err := state.signalKillCandidates([]DarwinContainmentCandidate{candidate}); err != nil {
		t.Fatalf("KILL ESRCH = %v", err)
	}
	darwinContainmentRevalidate = originalRevalidate
	darwinContainmentSignalPID = originalSignal
}

func TestDarwinRegistryFinalLogicalBranches(t *testing.T) {
	originalLstat := darwinRegistryLstat
	originalAbs := darwinRecordAbs
	originalCandidates := darwinContainmentCandidates
	originalRevalidate := darwinContainmentRevalidate
	originalSignal := darwinContainmentSignalPID
	originalSleep := darwinCleanupSleep
	originalKinfoSlice := darwinKinfoProcSlice
	originalEnvironment := darwinEnvironmentLookup
	t.Cleanup(func() {
		darwinRegistryLstat = originalLstat
		darwinRecordAbs = originalAbs
		darwinContainmentCandidates = originalCandidates
		darwinContainmentRevalidate = originalRevalidate
		darwinContainmentSignalPID = originalSignal
		darwinCleanupSleep = originalSleep
		darwinKinfoProcSlice = originalKinfoSlice
		darwinEnvironmentLookup = originalEnvironment
	})

	parent := t.TempDir()
	regularPath := filepath.Join(parent, "regular")
	if err := os.WriteFile(regularPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	regularInfo, statErr := os.Stat(regularPath)
	if statErr != nil {
		t.Fatal(statErr)
	}
	darwinRegistryLstat = func(string) (os.FileInfo, error) { return regularInfo, nil }
	if _, ensureErr := ensureDarwinRegistry(parent); ensureErr == nil {
		t.Fatal("non-directory registry seam was accepted")
	}
	darwinRegistryLstat = originalLstat

	want := errors.New("read")
	darwinRecordAbs = func(string) (string, error) { return "", want }
	if diagnoseErr := DiagnoseDarwinContainment(parent, io.Discard); !errors.Is(diagnoseErr, want) {
		t.Fatalf("diagnose reader error = %v", diagnoseErr)
	}
	darwinRecordAbs = originalAbs

	fileParent := t.TempDir()
	fileRoot := filepath.Join(fileParent, "acp-go-amp-command-file")
	if writeErr := os.WriteFile(fileRoot, nil, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	generation, generationErr := NewDarwinGenerationRecord(fileParent, fileRoot, "prompt")
	if generationErr != nil {
		t.Fatal(generationErr)
	}
	if cleanupErr := CleanupDarwinContainment(fileParent, generation.RuntimeID, true, io.Discard); cleanupErr == nil || !strings.Contains(cleanupErr.Error(), "real directory") {
		t.Fatalf("cleanup invalid root error = %v", cleanupErr)
	}

	cleanupParent, _, cleanupGeneration := newDarwinCoverageGeneration(t, "second-term")
	candidate := DarwinContainmentCandidate{PID: 333, StartSec: 3}
	scans := 0
	darwinContainmentCandidates = func(darwinContainmentRecord) ([]DarwinContainmentCandidate, error) {
		scans++
		if scans == 2 {
			return []DarwinContainmentCandidate{candidate}, nil
		}

		return nil, nil
	}
	darwinContainmentRevalidate = func(_ darwinContainmentRecord, value DarwinContainmentCandidate) (DarwinContainmentCandidate, darwinCandidateStatus) {
		return value, darwinCandidateCorrelated
	}
	darwinContainmentSignalPID = func(int, syscall.Signal) error { return want }
	darwinCleanupSleep = func(time.Duration) {}
	if cleanupErr := CleanupDarwinContainment(cleanupParent, cleanupGeneration.RuntimeID, true, io.Discard); !errors.Is(cleanupErr, want) {
		t.Fatalf("second TERM error = %v", cleanupErr)
	}
	darwinContainmentCandidates = originalCandidates
	darwinContainmentRevalidate = originalRevalidate
	darwinContainmentSignalPID = originalSignal
	darwinCleanupSleep = originalSleep

	sortParent := t.TempDir()
	for _, runtimeID := range []string{strings.Repeat("b", 32), strings.Repeat("a", 32)} {
		registry, ensureErr := ensureDarwinRegistry(sortParent)
		if ensureErr != nil {
			t.Fatal(ensureErr)
		}
		if writeErr := writeDarwinRecordAtomic(registry, validDarwinTestRecord(sortParent, runtimeID), false); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	_, records, recordsErr := readDarwinRecords(sortParent, "")
	if recordsErr != nil || len(records) != 2 || records[0].RuntimeID >= records[1].RuntimeID {
		t.Fatalf("sorted records = %#v, err=%v", records, recordsErr)
	}

	symlinkParent := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(symlinkParent, "link")
	if symlinkErr := os.Symlink(outside, link); symlinkErr != nil {
		t.Fatal(symlinkErr)
	}
	linkedRoot := filepath.Join(link, "acp-go-amp-command-outside")
	if mkdirErr := os.Mkdir(linkedRoot, 0o700); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	if _, _, validationErr := validatedDarwinGenerationRoot(symlinkParent, linkedRoot); validationErr == nil || !strings.Contains(validationErr.Error(), "outside") {
		t.Fatalf("resolved outside root error = %v", validationErr)
	}

	record := validDarwinTestRecord(parent, strings.Repeat("d", 32))
	processes := make([]unix.KinfoProc, 4)
	processes[0].Proc.P_pid = 30
	processes[1].Proc.P_pid = 20
	processes[2].Proc.P_pid = 10
	processes[3].Proc.P_pid = 40
	darwinKinfoProcSlice = func(string, ...int) ([]unix.KinfoProc, error) { return processes, nil }
	darwinEnvironmentLookup = func(pid int) (map[string]string, error) {
		switch pid {
		case 30:
			return nil, want
		case 40:
			return map[string]string{}, nil
		default:
			return map[string]string{DarwinRuntimeIDEnv: record.RuntimeID, DarwinScratchRootEnv: record.GenerationRoot}, nil
		}
	}
	candidates, err := correlatedDarwinCandidates(record)
	if err != nil || len(candidates) != 2 || candidates[0].PID != 10 || candidates[1].PID != 20 {
		t.Fatalf("correlated candidates = %#v, err=%v", candidates, err)
	}

	raw := append(darwinProcargsFixture([]string{"tool"}, []string{"KEY=value"}), 0, 0)
	if env, err := parseDarwinProcessEnvironment(raw); err != nil || env["KEY"] != "value" {
		t.Fatalf("terminated environment = %#v, err=%v", env, err)
	}
}

func validDarwinTestRecord(parent, runtimeID string) darwinContainmentRecord {
	return darwinContainmentRecord{
		SchemaVersion: darwinRecordFormat, Vendor: darwinVendor, Containment: darwinBestEffortMode,
		LifecycleKind: "prompt", RuntimeID: runtimeID,
		GenerationRoot: filepath.Join(parent, "acp-go-amp-command-"+runtimeID[:4]),
		WrapperPID:     1, WrapperStartSec: 0, WrapperStartUsec: 0, State: darwinStateRunning,
	}
}

func newDarwinCoverageGeneration(t *testing.T, suffix string) (string, string, *DarwinGeneration) {
	t.Helper()
	parent := t.TempDir()
	root := filepath.Join(parent, "acp-go-amp-command-"+suffix)
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	generation, err := NewDarwinGenerationRecord(parent, root, "prompt")
	if err != nil {
		t.Fatal(err)
	}

	return parent, root, generation
}
