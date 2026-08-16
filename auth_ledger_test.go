package ampacp

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// failingLedgerFile fails one step of the atomic write so every branch of the
// write-temp, fsync, rename, fsync-dir sequence is exercised.
type failingLedgerFile struct {
	ledgerFile
	writeErr error
	chmodErr error
	syncErr  error
}

func (f failingLedgerFile) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}

	return f.ledgerFile.Write(p)
}

func (f failingLedgerFile) Chmod(mode os.FileMode) error {
	if f.chmodErr != nil {
		return f.chmodErr
	}

	return f.ledgerFile.Chmod(mode)
}

func (f failingLedgerFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}

	return f.ledgerFile.Sync()
}

func newTestLedger(t *testing.T) *authLedger {
	t.Helper()

	ledger, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("newAuthLedger: %v", err)
	}

	return ledger
}

func TestAuthLedgerRootValidation(t *testing.T) {
	if authLedgerRootConfigured(Options{}) {
		t.Fatal("an unset root reported as configured")
	}

	if !authLedgerRootConfigured(Options{ProviderAuthRoot: "/root"}) {
		t.Fatal("a set root reported as unconfigured")
	}

	if _, err := newAuthLedger(Options{ProviderAuthRoot: "relative"}); err == nil {
		t.Fatal("a relative root was accepted")
	}

	// A root the operator already created carries whatever mode it was made
	// with, so the configured root itself is restricted and not just the leaf.
	root := filepath.Join(t.TempDir(), "provider-auth")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}

	ledger, err := newAuthLedger(Options{ProviderAuthRoot: root})
	if err != nil {
		t.Fatalf("newAuthLedger: %v", err)
	}

	if ledger.dir != filepath.Join(root, authLedgerVendorDir, authLedgerLeafDir) {
		t.Fatalf("ledger dir = %q", ledger.dir)
	}

	for _, dir := range []string{root, ledger.dir} {
		info, statErr := os.Stat(dir)
		if statErr != nil || info.Mode().Perm() != authLedgerDirMode {
			t.Fatalf("%s mode = %v/%v", dir, info, statErr)
		}
	}
}

func TestAuthLedgerRootFailuresLeaveTheSurfaceUnadvertised(t *testing.T) {
	want := errors.New("no root")

	// The configured root and the leaf under it are prepared separately, so each
	// step is failed on its own path rather than everywhere at once.
	cases := map[string]func(root string){
		"rootMkdir": func(root string) {
			ledgerMkdirAll = func(path string, mode os.FileMode) error {
				if path == root {
					return want
				}

				return os.MkdirAll(path, mode)
			}
		},
		"rootChmod": func(root string) {
			ledgerChmod = func(path string, mode os.FileMode) error {
				if path == root {
					return want
				}

				return os.Chmod(path, mode)
			}
		},
		"leafMkdir": func(root string) {
			ledgerMkdirAll = func(path string, mode os.FileMode) error {
				if path != root {
					return want
				}

				return os.MkdirAll(path, mode)
			}
		},
		"leafChmod": func(root string) {
			ledgerChmod = func(path string, mode os.FileMode) error {
				if path != root {
					return want
				}

				return os.Chmod(path, mode)
			}
		},
		"stat":   func(string) { ledgerStat = func(string) (os.FileInfo, error) { return nil, want } },
		"create": func(string) { ledgerCreateTemp = func(string, string) (ledgerFile, error) { return nil, want } },
	}

	originals := func() func() {
		mkdir, chmod, stat, create := ledgerMkdirAll, ledgerChmod, ledgerStat, ledgerCreateTemp

		return func() { ledgerMkdirAll, ledgerChmod, ledgerStat, ledgerCreateTemp = mkdir, chmod, stat, create }
	}

	for name, install := range cases {
		restore := originals()
		root := t.TempDir()

		install(root)

		_, err := newAuthLedger(Options{ProviderAuthRoot: root})

		restore()

		if err == nil {
			t.Fatalf("%s: an unusable root was accepted", name)
		}
	}

	// A root that resolves to a file rather than a directory is rejected too.
	restore := originals()
	ledgerStat = func(string) (os.FileInfo, error) { return fileModeInfo{}, nil }

	_, err := newAuthLedger(Options{ProviderAuthRoot: t.TempDir()})

	restore()

	if err == nil {
		t.Fatal("a non-directory root was accepted")
	}
}

// fileModeInfo is an os.FileInfo that reports a regular file.
type fileModeInfo struct{ os.FileInfo }

func (fileModeInfo) IsDir() bool { return false }

func TestAuthLedgerWritesAtomicallyAndDurably(t *testing.T) {
	ledger := newTestLedger(t)

	record := authLedgerRecord{
		ProviderID: authProviderID, Method: authMethodLogin, ConnectionID: "connection-1",
		Revision: 1, BindingGeneration: 1, FlowID: "F1",
		AuthorizeRequestID: "R1", State: authLedgerIntent, CreatedAt: 1, UpdatedAt: 1,
	}
	if err := ledger.write(record); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, ok, err := ledger.read(authProviderID, "connection-1")
	if err != nil || !ok || got != record {
		t.Fatalf("read = %#v/%v/%v", got, ok, err)
	}

	info, err := os.Stat(ledger.path(authProviderID, "connection-1"))
	if err != nil || info.Mode().Perm() != authLedgerFileMode {
		t.Fatalf("entry mode = %v/%v", info, err)
	}

	// Ledger content is closed: no credential material, no URL, no prompt answer.
	contents, err := os.ReadFile(ledger.path(authProviderID, "connection-1"))
	if err != nil {
		t.Fatal(err)
	}

	fields := map[string]any{}
	if decodeErr := json.Unmarshal(contents, &fields); decodeErr != nil {
		t.Fatal(decodeErr)
	}

	want := []string{
		"providerId", "method", "connectionId", "revision", "bindingGeneration",
		"flowId", "authorizeRequestId", "state", "createdAt", "updatedAt",
	}
	if len(fields) != len(want) {
		t.Fatalf("ledger entry = %#v, want exactly %v", fields, want)
	}

	for _, key := range want {
		if _, ok := fields[key]; !ok {
			t.Fatalf("ledger entry missing %q: %#v", key, fields)
		}
	}

	records, err := ledger.list()
	if err != nil || len(records) != 1 || records[0] != record {
		t.Fatalf("list = %#v/%v", records, err)
	}

	// The listing is ordered by provider slot so a residence answer is stable.
	second := record
	second.ProviderID = "aardvark"

	if writeErr := ledger.write(second); writeErr != nil {
		t.Fatalf("write: %v", writeErr)
	}

	records, err = ledger.list()
	if err != nil || len(records) != 2 || records[0].ProviderID != "aardvark" {
		t.Fatalf("ordered list = %#v/%v", records, err)
	}

	if err := os.Remove(ledger.path(second.ProviderID, second.ConnectionID)); err != nil {
		t.Fatal(err)
	}

	// An absent entry is absence rather than a failure.
	if _, ok, err := ledger.read("other", "connection-1"); ok || err != nil {
		t.Fatalf("absent read = %v/%v", ok, err)
	}
}

func TestAuthLedgerSurfacesWriteFailures(t *testing.T) {
	ledger := newTestLedger(t)
	record := authLedgerRecord{ProviderID: authProviderID, State: authLedgerIntent}
	want := errors.New("disk full")

	originalTemp, originalMarshal, originalRename, originalOpen := ledgerCreateTemp, ledgerMarshal, ledgerRename, ledgerOpen

	t.Cleanup(func() {
		ledgerCreateTemp, ledgerMarshal, ledgerRename, ledgerOpen = originalTemp, originalMarshal, originalRename, originalOpen
	})

	ledgerMarshal = func(any) ([]byte, error) { return nil, want }

	if err := ledger.write(record); !errors.Is(err, want) {
		t.Fatalf("marshal failure = %v", err)
	}

	ledgerMarshal = originalMarshal
	ledgerCreateTemp = func(string, string) (ledgerFile, error) { return nil, want }

	if err := ledger.write(record); !errors.Is(err, want) {
		t.Fatalf("temp failure = %v", err)
	}

	for name, wrap := range map[string]func(ledgerFile) ledgerFile{
		"write": func(f ledgerFile) ledgerFile { return failingLedgerFile{ledgerFile: f, writeErr: want} },
		"chmod": func(f ledgerFile) ledgerFile { return failingLedgerFile{ledgerFile: f, chmodErr: want} },
		"sync":  func(f ledgerFile) ledgerFile { return failingLedgerFile{ledgerFile: f, syncErr: want} },
	} {
		ledgerCreateTemp = func(dir string, pattern string) (ledgerFile, error) {
			file, err := originalTemp(dir, pattern)
			if err != nil {
				return nil, err
			}

			return wrap(file), nil
		}

		if err := ledger.write(record); !errors.Is(err, want) {
			t.Fatalf("%s failure = %v", name, err)
		}
	}

	ledgerCreateTemp = originalTemp
	ledgerRename = func(string, string) error { return want }

	if err := ledger.write(record); !errors.Is(err, want) {
		t.Fatalf("rename failure = %v", err)
	}

	ledgerRename = originalRename
	ledgerOpen = func(string) (*os.File, error) { return nil, want }

	if err := ledger.write(record); !errors.Is(err, want) {
		t.Fatalf("dir sync failure = %v", err)
	}
}

func TestAuthLedgerSurfacesReadFailures(t *testing.T) {
	ledger := newTestLedger(t)
	want := errors.New("read denied")

	originalRead, originalDir := ledgerReadFile, ledgerReadDir

	t.Cleanup(func() { ledgerReadFile, ledgerReadDir = originalRead, originalDir })

	ledgerReadFile = func(string) ([]byte, error) { return nil, want }

	if _, _, err := ledger.read(authProviderID, "connection-1"); !errors.Is(err, want) {
		t.Fatalf("read failure = %v", err)
	}

	ledgerReadFile = func(string) ([]byte, error) { return []byte("{"), nil }

	if _, _, err := ledger.read(authProviderID, "connection-1"); err == nil {
		t.Fatal("a malformed entry decoded")
	}

	ledgerReadFile = originalRead
	ledgerReadDir = func(string) ([]os.DirEntry, error) { return nil, want }

	if _, err := ledger.list(); !errors.Is(err, want) {
		t.Fatalf("list failure = %v", err)
	}

	ledgerReadDir = originalDir

	// A directory and a non-JSON name are both skipped rather than decoded.
	if err := os.MkdirAll(filepath.Join(ledger.dir, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(ledger.dir, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	if records, err := ledger.list(); err != nil || len(records) != 0 {
		t.Fatalf("list = %#v/%v", records, err)
	}

	if err := os.WriteFile(filepath.Join(ledger.dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := ledger.list(); err == nil {
		t.Fatal("a malformed entry listed")
	}

	ledgerReadFile = func(string) ([]byte, error) { return nil, want }

	if _, err := ledger.list(); !errors.Is(err, want) {
		t.Fatalf("list read failure = %v", err)
	}
}

func TestInventoryDerivesProofFromTheLedgerAndAProbe(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	var empty authInventoryResult
	if err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &empty); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if len(empty.Entries) != 0 {
		t.Fatalf("inventory before any flow = %#v", empty.Entries)
	}

	authorized := fixture.mustAuthorize("connection-1")

	// A write-ahead intent with no confirmation is never more than not_confirmed,
	// however plainly the slot is occupied.
	var intent authInventoryResult
	if err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &intent); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if len(intent.Entries) != 1 || intent.Entries[0].ProofSource != authProofNotConfirmed {
		t.Fatalf("inventory on intent = %#v", intent.Entries)
	}

	if err := fixture.callback(authorized.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	var confirmed authInventoryResult
	if err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &confirmed); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	entry := confirmed.Entries[0]
	if entry.ProviderID != authProviderID || entry.ConnectionID != "connection-1" ||
		entry.Revision != 1 || entry.BindingGeneration != 1 || entry.ProofSource != authProofConfirmedPresent {
		t.Fatalf("inventory after completion = %#v", entry)
	}

	// A released connection is not reported at all.
	if err := fixture.call(AuthDisconnectMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID,
		authFieldConnectionID: "connection-1", authFieldBindingGeneration: 1,
	}, nil); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	var released authInventoryResult
	if err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &released); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if len(released.Entries) != 0 {
		t.Fatalf("inventory after disconnect = %#v", released.Entries)
	}
}

// TestInventoryKeepsEveryConnectionsBinding pins the ledger's addressing. Amp
// brokers exactly one provider, so a record addressed by provider alone is the
// whole ledger: a second connection's authorize would take the first's binding
// with it, leaving a connection nothing can release and nothing can harvest.
func TestInventoryKeepsEveryConnectionsBinding(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	first := fixture.mustAuthorize("connection-1")
	if err := fixture.callback(first.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	second := fixture.mustAuthorize("connection-2")
	if err := fixture.callback(second.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	var both authInventoryResult
	if err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &both); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if len(both.Entries) != 2 {
		t.Fatalf("inventory holds %d bindings, want 2: %#v", len(both.Entries), both.Entries)
	}

	for index, want := range []string{"connection-1", "connection-2"} {
		entry := both.Entries[index]
		if entry.ConnectionID != want || entry.Revision != 1 || entry.BindingGeneration != 1 {
			t.Fatalf("binding %d = %#v", index, entry)
		}
	}

	// The earlier binding is still releasable on the generation it was issued
	// under, which it could not be if the later authorize had rewritten it.
	if err := fixture.call(AuthDisconnectMethod, map[string]any{
		authFieldSessionID: string(fixture.session.id), authFieldProviderID: authProviderID,
		authFieldConnectionID: "connection-1", authFieldBindingGeneration: 1,
	}, nil); err != nil {
		t.Fatalf("disconnect: %v", err)
	}

	var remaining authInventoryResult
	if err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &remaining); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if len(remaining.Entries) != 1 || remaining.Entries[0].ConnectionID != "connection-2" {
		t.Fatalf("inventory after one release = %#v", remaining.Entries)
	}
}

func TestInventoryReportsConfirmedAbsence(t *testing.T) {
	fixture := newAuthFixture(t, "login")
	authorized := fixture.mustAuthorize("connection-1")

	if err := fixture.callback(authorized.FlowID, "pasted"); err != nil {
		t.Fatalf("callback: %v", err)
	}

	flow, err := fixture.broker.addressFlow(fixture.session.id, authProviderID, authorized.FlowID)
	if err != nil {
		t.Fatalf("addressFlow: %v", err)
	}

	fixture.broker.mu.Lock()
	residence := flow.residence
	fixture.broker.mu.Unlock()

	if err := os.Remove(filepath.Join(residence, "amp", "secrets.json")); err != nil {
		t.Fatal(err)
	}

	var absent authInventoryResult
	if err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, &absent); err != nil {
		t.Fatalf("inventory: %v", err)
	}

	if absent.Entries[0].ProofSource != authProofConfirmedAbsent {
		t.Fatalf("inventory = %#v", absent.Entries)
	}
}

func TestInventoryRejectsAddressingAndProbeFailures(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	if err := fixture.call(AuthInventoryMethod, map[string]any{}, nil); err == nil {
		t.Fatal("inventory answered with no session")
	}

	if err := fixture.call(AuthInventoryMethod, map[string]any{"unexpected": 1}, nil); err == nil {
		t.Fatal("inventory accepted an unknown field")
	}

	if err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: "T-unknown"}, nil); err == nil {
		t.Fatal("inventory answered for an unknown session")
	}

	fixture.mustAuthorize("connection-1")

	want := errors.New("read denied")
	originalDir, originalSecret := ledgerReadDir, authSecretPresent

	t.Cleanup(func() { ledgerReadDir, authSecretPresent = originalDir, originalSecret })

	ledgerReadDir = func(string) ([]os.DirEntry, error) { return nil, want }

	err := fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, nil)
	ledgerReadDir = originalDir

	if err == nil {
		t.Fatal("inventory answered with an unreadable ledger")
	}

	requireAuthCause(t, err, authCauseHarvestFailed)

	authSecretPresent = func(string) (bool, error) { return false, want }

	err = fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, nil)
	authSecretPresent = originalSecret

	if err == nil {
		t.Fatal("inventory answered with an unreadable slot")
	}

	requireAuthCause(t, err, authCauseHarvestFailed)

	// An unasserted native store vetoes the probe rather than answering from a
	// store the adapter cannot prove is authoritative.
	if writeErr := os.WriteFile(fixture.session.settingsFile,
		[]byte(`{"amp.experimental.cli.nativeSecretsStorage.enabled":true}`), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}

	err = fixture.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(fixture.session.id)}, nil)
	if err == nil {
		t.Fatal("inventory answered against an unasserted native store")
	}

	requireAuthCause(t, err, authCauseNativeVeto)
}

func TestAuthResidenceFallsBackToTheSessionHome(t *testing.T) {
	fixture := newAuthFixture(t, "login")

	if residence := fixture.broker.authResidence(fixture.session, authProviderID, "connection-1"); residence != fixture.session.authDataHome() {
		t.Fatalf("residence with no flow = %q", residence)
	}

	fixture.mustAuthorize("connection-1")

	if residence := fixture.broker.authResidence(fixture.session, "openai", "connection-1"); residence != fixture.session.authDataHome() {
		t.Fatalf("residence for an unrelated provider = %q", residence)
	}

	// A connection with no flow of its own here names a slot nothing in this
	// session established, so it falls back rather than borrowing another
	// connection's residence.
	if residence := fixture.broker.authResidence(fixture.session, authProviderID, "connection-2"); residence != fixture.session.authDataHome() {
		t.Fatalf("residence for an unrelated connection = %q", residence)
	}

	if residence := fixture.broker.authResidence(fixture.session, authProviderID, "connection-1"); residence == "" {
		t.Fatal("residence for a live flow is empty")
	}
}

func TestAuthProofSourceIsATotalFunction(t *testing.T) {
	cases := []struct {
		state   string
		present bool
		want    string
	}{
		{state: authLedgerConfirmed, present: true, want: authProofConfirmedPresent},
		{state: authLedgerConfirmed, want: authProofConfirmedAbsent},
		{state: authLedgerIntent, present: true, want: authProofNotConfirmed},
		{state: authLedgerIntent, want: authProofNotConfirmed},
		{state: authLedgerRemoved, present: true, want: authProofNotConfirmed},
	}

	for _, testCase := range cases {
		if got := authProofSource(testCase.state, testCase.present); got != testCase.want {
			t.Fatalf("authProofSource(%q,%v) = %q, want %q", testCase.state, testCase.present, got, testCase.want)
		}
	}
}

func TestManualAPIKeyRestartProofAndDisconnectFence(t *testing.T) {
	first := newAuthFixture(t, "login")
	manual := first.mustAuthorizeMethod("connection-restart", authMethodAPIKey)
	if err := first.callbackMethod(manual.FlowID, authMethodAPIKey, manualAmpKeyCanary); err != nil {
		t.Fatalf("manual material: %v", err)
	}
	harvest := harvestAuthCredential(t, first, manual.FlowID)

	if err := first.agent.Close(); err != nil {
		t.Fatalf("close first agent: %v", err)
	}

	restartedAgent := newTestAgent(
		WithExecutablePath(first.agent.options.ExecutablePath),
		WithScratchDir(testScratchDir(t)),
		WithProviderAuthRoot(first.root),
	)
	t.Cleanup(func() { _ = restartedAgent.Close() })
	restartedSession, err := newAgentSession(t.Context(), restartedAgent, "T-restarted", t.TempDir(), parsedSessionMeta{}, "", nil)
	if err != nil {
		t.Fatalf("restart session: %v", err)
	}
	restartedAgent.mu.Lock()
	restartedAgent.sessions[restartedSession.id] = restartedSession
	restartedAgent.mu.Unlock()
	restarted := &authFixture{t: t, agent: restartedAgent, broker: restartedAgent.providerAuth, session: restartedSession, root: first.root}

	var inventory authInventoryResult
	if err := restarted.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(restartedSession.id)}, &inventory); err != nil {
		t.Fatalf("restart inventory: %v", err)
	}
	if len(inventory.Entries) != 1 || inventory.Entries[0].ProviderID != authProviderID ||
		inventory.Entries[0].ConnectionID != "connection-restart" ||
		inventory.Entries[0].BindingGeneration != harvest.BindingGeneration ||
		inventory.Entries[0].ProofSource != authProofNotConfirmed {
		t.Fatalf("restart inventory = %#v", inventory.Entries)
	}

	if err := restarted.call(AuthDisconnectMethod, map[string]any{
		authFieldSessionID: string(restartedSession.id), authFieldProviderID: authProviderID,
		authFieldConnectionID: "connection-restart", authFieldBindingGeneration: harvest.BindingGeneration,
	}, nil); err != nil {
		t.Fatalf("restart disconnect: %v", err)
	}

	var after authInventoryResult
	if err := restarted.call(AuthInventoryMethod, map[string]any{authFieldSessionID: string(restartedSession.id)}, &after); err != nil {
		t.Fatalf("inventory after disconnect: %v", err)
	}
	if len(after.Entries) != 0 {
		t.Fatalf("disconnected restart inventory = %#v", after.Entries)
	}
}
