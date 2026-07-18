package amp

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type generationDirEntry struct {
	info fs.FileInfo
	err  error
}

func (entry generationDirEntry) Name() string               { return entry.info.Name() }
func (entry generationDirEntry) IsDir() bool                { return entry.info.IsDir() }
func (entry generationDirEntry) Type() fs.FileMode          { return entry.info.Mode().Type() }
func (entry generationDirEntry) Info() (fs.FileInfo, error) { return entry.info, entry.err }

func TestDarwinGenerationPrepareCommandCopiesWritableInputs(t *testing.T) {
	writable := t.TempDir()
	home := filepath.Join(writable, "home")
	config := filepath.Join(writable, "config")
	for _, path := range []string{home, config} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(home, "credentials.json"), []byte("home"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, "config.json"), []byte("config"), 0o600); err != nil {
		t.Fatal(err)
	}
	settings := filepath.Join(t.TempDir(), "settings.json")
	mcp := filepath.Join(t.TempDir(), "mcp.json")
	if err := os.WriteFile(settings, []byte("settings"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mcp, []byte("mcp"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "generation")
	generation := &DarwinGeneration{RuntimeID: strings.Repeat("a", 32), ScratchRoot: root}
	cmd := exec.Command("amp", "--settings-file", settings, "--mcp-config", mcp)
	cmd.Env = []string{"HOME=" + home, "XDG_CONFIG_HOME=" + config, "MALFORMED", "XDG_CACHE_HOME=/old"}
	if err := generation.prepareCommand(cmd, writable); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		filepath.Join(root, "home", "credentials.json"),
		filepath.Join(root, "xdg-config", "config.json"),
		filepath.Join(root, "xdg-config", "amp", "settings.json"),
		filepath.Join(root, "mcp.json"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("copied input %s: %v", path, err)
		}
	}
	values := environmentMap(cmd.Env)
	for _, key := range []string{"HOME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", DarwinRuntimeIDEnv, DarwinScratchRootEnv} {
		if values[key] == "" {
			t.Fatalf("rewritten environment missing %s: %#v", key, values)
		}
	}
	if !strings.HasSuffix(cmd.Args[2], "settings.json") || !strings.HasSuffix(cmd.Args[4], "mcp.json") {
		t.Fatalf("rewritten args = %v", cmd.Args)
	}
}

func TestDarwinGenerationPrepareCommandValidation(t *testing.T) {
	if err := (*DarwinGeneration)(nil).prepareCommand(exec.Command("amp"), ""); err == nil {
		t.Fatal("nil generation accepted")
	}
	if err := (&DarwinGeneration{ScratchRoot: t.TempDir()}).prepareCommand(nil, ""); err == nil {
		t.Fatal("nil command accepted")
	}

	fileRoot := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(fileRoot, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (&DarwinGeneration{ScratchRoot: fileRoot}).prepareCommand(exec.Command("amp"), ""); err == nil {
		t.Fatal("file scratch root accepted")
	}

	writable := t.TempDir()
	outside := t.TempDir()
	cmd := exec.Command("amp")
	cmd.Env = []string{"HOME=" + outside, "XDG_CONFIG_HOME=" + outside}
	if err := (&DarwinGeneration{ScratchRoot: t.TempDir()}).prepareCommand(cmd, writable); err == nil {
		t.Fatal("outside writable input accepted")
	}

	input := filepath.Join(t.TempDir(), "input")
	if err := os.Mkdir(input, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(input, "link")); err != nil {
		t.Fatal(err)
	}
	if err := copyRegularTree(input, t.TempDir()); err == nil {
		t.Fatal("symlink input accepted")
	}
	if err := copyRegularTree(filepath.Join(t.TempDir(), "missing"), t.TempDir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing tree error = %v", err)
	}

	readFailure := exec.Command("amp", "--settings-file", t.TempDir())
	if err := (&DarwinGeneration{ScratchRoot: t.TempDir()}).prepareCommand(readFailure, ""); err == nil {
		t.Fatal("directory settings input accepted")
	}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "mcp.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	mcp := filepath.Join(t.TempDir(), "mcp.json")
	if err := os.WriteFile(mcp, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeFailure := exec.Command("amp", "--mcp-config", mcp)
	if err := (&DarwinGeneration{ScratchRoot: root}).prepareCommand(writeFailure, ""); err == nil {
		t.Fatal("directory MCP destination accepted")
	}

	missing := exec.Command("amp", "--settings-file", filepath.Join(t.TempDir(), "missing"))
	missingRoot := t.TempDir()
	if err := (&DarwinGeneration{ScratchRoot: missingRoot}).prepareCommand(missing, ""); err != nil {
		t.Fatal(err)
	}
	if missing.Args[2] != filepath.Join(missingRoot, "xdg-config", "amp", "settings.json") {
		t.Fatalf("missing settings rewrite = %v", missing.Args)
	}
}

func TestDarwinGenerationLifecycleHelpers(t *testing.T) {
	if err := (*DarwinGeneration)(nil).started(1, 1); err != nil {
		t.Fatal(err)
	}
	if err := (&DarwinGeneration{}).started(1, 1); err != nil {
		t.Fatal(err)
	}
	want := errors.New("started")
	if err := (&DarwinGeneration{RecordStarted: func(int, int) error { return want }}).started(1, 1); !errors.Is(err, want) {
		t.Fatalf("started error = %v", err)
	}
	if err := (*DarwinGeneration)(nil).finish(true); err != nil {
		t.Fatal(err)
	}
	released := 0
	generation := &DarwinGeneration{Release: func(complete bool) error {
		if !complete {
			t.Fatal("complete = false")
		}
		released++

		return nil
	}}
	if err := generation.finish(true); err != nil {
		t.Fatal(err)
	}
	if err := generation.finish(false); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("release count = %d", released)
	}
	recorded := &DarwinGeneration{
		RecordFinished: func(bool) error { return want },
		Release: func(bool) error {
			t.Fatal("release called after record failure")

			return nil
		},
	}
	if err := recorded.finish(true); !errors.Is(err, ErrProcessContainmentIncomplete) || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("record finish error = %v", err)
	}
}

func TestDarwinGenerationFilesystemSeams(t *testing.T) {
	originalAbs := darwinGenerationAbs
	originalWalk := darwinGenerationWalkDir
	originalRel := darwinGenerationRel
	originalRead := darwinGenerationReadFile
	originalMkdir := darwinGenerationMkdirAll
	originalWrite := darwinGenerationWrite
	t.Cleanup(func() {
		darwinGenerationAbs = originalAbs
		darwinGenerationWalkDir = originalWalk
		darwinGenerationRel = originalRel
		darwinGenerationReadFile = originalRead
		darwinGenerationMkdirAll = originalMkdir
		darwinGenerationWrite = originalWrite
	})

	want := errors.New("filesystem")
	darwinGenerationAbs = func(string) (string, error) { return "", want }
	cmd := exec.Command("amp")
	if err := (&DarwinGeneration{ScratchRoot: t.TempDir()}).prepareCommand(cmd, "relative"); !errors.Is(err, want) {
		t.Fatalf("writable-root resolution error = %v", err)
	}
	darwinGenerationAbs = originalAbs

	darwinGenerationWalkDir = func(_ string, walk fs.WalkDirFunc) error { return walk("source", nil, want) }
	if err := copyRegularTree("source", "destination"); !errors.Is(err, want) {
		t.Fatalf("walk error = %v", err)
	}

	filePath := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(filePath, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatal(err)
	}
	entry := generationDirEntry{info: info}
	darwinGenerationWalkDir = func(_ string, walk fs.WalkDirFunc) error {
		return walk("source", generationDirEntry{info: info, err: want}, nil)
	}
	if err := copyRegularTree("source", "destination"); !errors.Is(err, want) {
		t.Fatalf("entry info error = %v", err)
	}

	darwinGenerationWalkDir = func(_ string, walk fs.WalkDirFunc) error { return walk("source/file", entry, nil) }
	darwinGenerationRel = func(string, string) (string, error) { return "", want }
	if err := copyRegularTree("source", "destination"); !errors.Is(err, want) {
		t.Fatalf("relative path error = %v", err)
	}
	darwinGenerationRel = func(string, string) (string, error) { return "file", nil }
	darwinGenerationReadFile = func(string) ([]byte, error) { return nil, want }
	if err := copyRegularTree("source", "destination"); !errors.Is(err, want) {
		t.Fatalf("read error = %v", err)
	}
	darwinGenerationReadFile = func(string) ([]byte, error) { return []byte("data"), nil }
	darwinGenerationMkdirAll = func(string, os.FileMode) error { return want }
	if err := copyRegularTree("source", "destination"); !errors.Is(err, want) {
		t.Fatalf("mkdir error = %v", err)
	}
	darwinGenerationMkdirAll = func(string, os.FileMode) error { return nil }
	darwinGenerationWrite = func(string, []byte, os.FileMode) error { return want }
	if err := copyRegularTree("source", "destination"); !errors.Is(err, want) {
		t.Fatalf("write error = %v", err)
	}

	darwinGenerationWalkDir = originalWalk
	darwinGenerationRel = originalRel
	darwinGenerationReadFile = originalRead
	darwinGenerationMkdirAll = originalMkdir
	darwinGenerationWrite = originalWrite

	writable := t.TempDir()
	home := filepath.Join(writable, "home")
	config := filepath.Join(writable, "config")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(config, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing", filepath.Join(home, "link")); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("amp")
	cmd.Env = []string{"HOME=" + home, "XDG_CONFIG_HOME=" + config}
	if err := (&DarwinGeneration{ScratchRoot: t.TempDir()}).prepareCommand(cmd, writable); err == nil {
		t.Fatal("writable tree copy error was ignored")
	}
}
