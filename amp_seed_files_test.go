package ampacp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWithSeedFilesClones(t *testing.T) {
	input := map[string]string{"settings.json": `{"a":1}`}
	options := applyOptions([]Option{WithSeedFiles(input)})
	input["settings.json"] = "changed"
	if options.SeedFiles["settings.json"] != `{"a":1}` {
		t.Fatalf("seed files not cloned: %#v", options.SeedFiles)
	}
	if nilSeeds := applyOptions([]Option{WithSeedFiles(nil)}); nilSeeds.SeedFiles != nil {
		t.Fatalf("nil seed files not preserved: %#v", nilSeeds.SeedFiles)
	}
}

func TestResolveSeedPathConfinement(t *testing.T) {
	root := t.TempDir()
	for _, key := range []string{"", "   ", "/abs/path", "..", "../escape", "sub/../..", "."} {
		if _, _, err := resolveSeedPath(root, key); err == nil {
			t.Fatalf("key %q accepted", key)
		}
	}
	target, _, err := resolveSeedPath(root, "sub/dir/settings.json")
	if err != nil {
		t.Fatalf("valid key rejected: %v", err)
	}
	if target != filepath.Join(root, "sub", "dir", "settings.json") {
		t.Fatalf("unexpected target %q", target)
	}
}

func TestWriteSeedFiles(t *testing.T) {
	root := t.TempDir()
	if err := writeSeedFiles(root, nil); err != nil {
		t.Fatalf("nil seeds: %v", err)
	}
	seeds := map[string]string{
		"settings.json":       `{"seed":true}`,
		"nested/dir/file.txt": "hello",
	}
	if err := writeSeedFiles(root, seeds); err != nil {
		t.Fatalf("writeSeedFiles: %v", err)
	}
	for rel, want := range seeds {
		full := filepath.Join(root, rel)
		got, err := os.ReadFile(full)
		if err != nil {
			t.Fatalf("read %q: %v", rel, err)
		}
		if string(got) != want {
			t.Fatalf("seed %q = %q, want %q", rel, got, want)
		}
		info, err := os.Stat(full)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("seed %q mode = %v, want 0600", rel, info.Mode().Perm())
		}
	}
	if err := writeSeedFiles(root, map[string]string{"../escape.json": "x"}); err == nil {
		t.Fatal("escaping seed accepted")
	}
}

func TestWriteSeedFilesErrors(t *testing.T) {
	previousMkdirAll := mkdirAll
	mkdirAll = func(string, os.FileMode) error { return errors.New("mkdir seed failed") }
	t.Cleanup(func() { mkdirAll = previousMkdirAll })
	if err := writeSeedFiles(t.TempDir(), map[string]string{"a/b.txt": "x"}); err == nil {
		t.Fatal("seed parent mkdir error ignored")
	}
	mkdirAll = previousMkdirAll

	previousWriteFile := writeFile
	writeFile = func(string, []byte, os.FileMode) error { return errors.New("write seed failed") }
	t.Cleanup(func() { writeFile = previousWriteFile })
	if err := writeSeedFiles(t.TempDir(), map[string]string{"a/b.txt": "x"}); err == nil {
		t.Fatal("seed write error ignored")
	}
	writeFile = previousWriteFile
}

func TestWriteSeedFilesManifestFirstWrite(t *testing.T) {
	root := t.TempDir()
	if err := writeSeedFiles(root, map[string]string{
		"config.yaml": "a: 1",
		"a/b.json":    `{"b":2}`,
	}); err != nil {
		t.Fatalf("writeSeedFiles: %v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, seedManifestName))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if got := string(manifest); got != `["a/b.json","config.yaml"]` {
		t.Fatalf("manifest = %q, want sorted [a/b.json config.yaml]", got)
	}
	// No backups on a first write.
	if _, err := os.Stat(filepath.Join(root, "config.yaml"+seedBackupSuffix)); !os.IsNotExist(err) {
		t.Fatalf("unexpected backup on first write: %v", err)
	}
}

func TestWriteSeedFilesIdempotent(t *testing.T) {
	root := t.TempDir()
	seeds := map[string]string{"config.yaml": "a: 1"}
	if err := writeSeedFiles(root, seeds); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if err := writeSeedFiles(root, seeds); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "config.yaml"+seedBackupSuffix)); !os.IsNotExist(err) {
		t.Fatalf("re-seeding identical content created a backup: %v", err)
	}
}

func TestWriteSeedFilesBacksUpChangedManagedFile(t *testing.T) {
	root := t.TempDir()
	if err := writeSeedFiles(root, map[string]string{"config.yaml": "a: 1"}); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if err := writeSeedFiles(root, map[string]string{"config.yaml": "a: 2"}); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	backup, err := os.ReadFile(filepath.Join(root, "config.yaml"+seedBackupSuffix))
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if string(backup) != "a: 1" {
		t.Fatalf("backup = %q, want old bytes %q", backup, "a: 1")
	}
	current, err := os.ReadFile(filepath.Join(root, "config.yaml"))
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if string(current) != "a: 2" {
		t.Fatalf("current = %q, want %q", current, "a: 2")
	}
}

func TestWriteSeedFilesManifestSurvivesPasses(t *testing.T) {
	root := t.TempDir()
	if err := writeSeedFiles(root, map[string]string{"config.yaml": "a: 1"}); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	manifest, err := loadSeedManifest(root)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if !manifest["config.yaml"] {
		t.Fatalf("manifest missing managed path after first pass: %#v", manifest)
	}
	// A second pass over the same reused root must see the file as managed and
	// overwrite it rather than fail closed.
	if err := writeSeedFiles(root, map[string]string{"config.yaml": "a: 3"}); err != nil {
		t.Fatalf("second pass over managed file: %v", err)
	}
}

func TestWriteSeedFilesFailsClosedOnUnmanaged(t *testing.T) {
	root := t.TempDir()
	// An operator-authored file the wrapper never recorded.
	operator := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(operator, []byte("operator: true"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := writeSeedFiles(root, map[string]string{
		"config.yaml": "a: 1",
		"other.json":  "{}",
	})
	if err == nil {
		t.Fatal("seed over unmanaged operator file accepted")
	}
	requireUnsupportedField(t, err, `seedFiles["config.yaml"]`)
	// Nothing was written or changed: operator file intact, sibling seed absent,
	// and no manifest was created.
	got, readErr := os.ReadFile(operator)
	if readErr != nil {
		t.Fatalf("read operator file: %v", readErr)
	}
	if string(got) != "operator: true" {
		t.Fatalf("operator file changed: %q", got)
	}
	if _, statErr := os.Stat(filepath.Join(root, "other.json")); !os.IsNotExist(statErr) {
		t.Fatalf("sibling seed written despite fail-closed: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(root, seedManifestName)); !os.IsNotExist(statErr) {
		t.Fatalf("manifest written despite fail-closed: %v", statErr)
	}
}

func TestWriteSeedFilesManifestReadError(t *testing.T) {
	root := t.TempDir()
	previous := readFile
	readFile = func(name string) ([]byte, error) {
		if filepath.Base(name) == seedManifestName {
			return nil, errors.New("manifest read boom")
		}

		return previous(name)
	}
	t.Cleanup(func() { readFile = previous })
	if err := writeSeedFiles(root, map[string]string{"a.txt": "x"}); err == nil {
		t.Fatal("manifest read error ignored")
	}
}

func TestWriteSeedFilesManifestParseError(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, seedManifestName), []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeSeedFiles(root, map[string]string{"a.txt": "x"}); err == nil {
		t.Fatal("malformed manifest accepted")
	}
}

func TestWriteSeedFilesTargetReadError(t *testing.T) {
	root := t.TempDir()
	previous := readFile
	readFile = func(name string) ([]byte, error) {
		if filepath.Base(name) == "a.txt" {
			return nil, errors.New("target read boom")
		}

		return previous(name)
	}
	t.Cleanup(func() { readFile = previous })
	if err := writeSeedFiles(root, map[string]string{"a.txt": "x"}); err == nil {
		t.Fatal("target read error ignored")
	}
}

func TestWriteSeedFilesBackupWriteError(t *testing.T) {
	root := t.TempDir()
	if err := writeSeedFiles(root, map[string]string{"config.yaml": "a: 1"}); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	previous := writeFile
	writeFile = func(name string, data []byte, perm os.FileMode) error {
		if strings.HasSuffix(name, seedBackupSuffix) {
			return errors.New("backup write boom")
		}

		return previous(name, data, perm)
	}
	t.Cleanup(func() { writeFile = previous })
	if err := writeSeedFiles(root, map[string]string{"config.yaml": "a: 2"}); err == nil {
		t.Fatal("backup write error ignored")
	}
}

func TestWriteSeedFilesManifestWriteError(t *testing.T) {
	root := t.TempDir()
	previous := writeFile
	writeFile = func(name string, data []byte, perm os.FileMode) error {
		if filepath.Base(name) == seedManifestName {
			return errors.New("manifest write boom")
		}

		return previous(name, data, perm)
	}
	t.Cleanup(func() { writeFile = previous })
	if err := writeSeedFiles(root, map[string]string{"a.txt": "x"}); err == nil {
		t.Fatal("manifest write error ignored")
	}
}

func TestNewAgentSessionSeedFiles(t *testing.T) {
	agent := NewAgent(WithScratchDir(t.TempDir()), WithSeedFiles(map[string]string{
		"custom/settings.json": `{"seed":true}`,
	}))
	session, err := newAgentSession(agent, "T-seed", t.TempDir(), parsedSessionMeta{}, "", nil)
	if err != nil {
		t.Fatalf("newAgentSession: %v", err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	// Anchor is the isolated HOME child of the session dir.
	got, err := os.ReadFile(filepath.Join(session.settingsDir, "home", "custom", "settings.json"))
	if err != nil {
		t.Fatalf("read seed: %v", err)
	}
	if string(got) != `{"seed":true}` {
		t.Fatalf("seed contents = %q", got)
	}

	// The wrapper's managed settings file under XDG_CONFIG_HOME is untouched.
	managed, err := os.ReadFile(session.settingsFile)
	if err != nil {
		t.Fatalf("read managed settings: %v", err)
	}
	if strings.TrimSpace(string(managed)) != "{}" {
		t.Fatalf("managed settings overwritten: %q", managed)
	}
}

func TestNewAgentSessionSeedFilesInvalid(t *testing.T) {
	agent := NewAgent(WithScratchDir(t.TempDir()), WithSeedFiles(map[string]string{
		"../escape.json": "x",
	}))
	_, err := newAgentSession(agent, "T-bad", t.TempDir(), parsedSessionMeta{}, "", nil)
	if err == nil {
		t.Fatal("escaping seed accepted at session start")
	}
	requireUnsupportedField(t, err, `seedFiles["../escape.json"]`)
}
