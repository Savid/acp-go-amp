package amp

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCaptureOrdinaryEnvironmentFiltersAndCanonicalizesEntries(t *testing.T) {
	original := ordinaryEnvironmentEntries
	t.Cleanup(func() { ordinaryEnvironmentEntries = original })
	ordinaryEnvironmentEntries = func() []string {
		return []string{
			"BROKEN",
			"PATH=/first",
			"PATH=/last",
			adapterPrivateEnvPrefix + "TOKEN=secret",
			scrubbedTracebackEnv + "=crash",
			scrubbedRedactionEnv + "=1",
			"EMPTY=",
		}
	}

	require.Equal(t, map[string]string{"PATH": "/last", "EMPTY": ""}, CaptureOrdinaryEnvironment())
}

func TestOrdinaryWindowsExecutableExtensions(t *testing.T) {
	require.Equal(t, []string{".com", ".exe", ".bat", ".cmd"}, ordinaryWindowsExecutableExtensions(""))
	require.Equal(t, []string{".exe", ".cmd", ".bat"}, ordinaryWindowsExecutableExtensions(" EXE ; .CmD ;; bat "))

	rules := windowsOrdinaryExecutableRules([]string{"Path=/bin", "pathext=.ONE;.TWO"})
	require.Equal(t, `:\/`, rules.pathSeparators)
	require.Equal(t, []string{".one", ".two"}, rules.extensions)
	require.True(t, rules.foldEnvironmentKey)
	require.False(t, rules.requireExecuteBit)
}

func TestLookPathInEnvironment(t *testing.T) {
	dir := t.TempDir()
	executable := filepath.Join(dir, "amp")
	require.NoError(t, os.WriteFile(executable, []byte("x"), 0o700))
	notExecutable := filepath.Join(dir, "plain")
	require.NoError(t, os.WriteFile(notExecutable, []byte("x"), 0o600))

	path, err := lookPathInEnvironment("amp", []string{"PATH=/missing:" + dir})
	require.NoError(t, err)
	require.Equal(t, executable, path)

	path, err = lookPathInEnvironment(executable, nil)
	require.NoError(t, err)
	require.Equal(t, executable, path)

	for _, test := range []struct {
		name string
		file string
		env  []string
	}{
		{"empty", "", nil},
		{"relative path", filepath.Join("relative", "amp"), nil},
		{"missing PATH", "amp", nil},
		{"not found", "missing", []string{"PATH=" + dir}},
		{"not executable", notExecutable, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, resolveErr := lookPathInEnvironment(test.file, test.env)
			require.Error(t, resolveErr)
		})
	}
}

func TestLookPathInOrdinaryEnvironmentWithPlatformRules(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	require.NoError(t, os.Mkdir(bin, 0o700))
	executable := filepath.Join(bin, "tool")
	require.NoError(t, os.WriteFile(executable, []byte("x"), 0o700))
	withoutExtension := filepath.Join(bin, "windows-tool")
	require.NoError(t, os.WriteFile(withoutExtension+".exe", []byte("x"), 0o600))
	withExtension := filepath.Join(bin, "named.cmd")
	require.NoError(t, os.WriteFile(withExtension, []byte("x"), 0o600))

	path, err := lookPathInOrdinaryEnvironment("bin/tool", nil, dir)
	require.NoError(t, err)
	require.Equal(t, executable, path)

	path, err = lookPathInOrdinaryEnvironmentWithRules("windows-tool", []string{"path=" + bin}, dir,
		windowsOrdinaryExecutableRules([]string{"PATHEXT=.EXE;.CMD"}))
	require.NoError(t, err)
	require.Equal(t, withoutExtension+".exe", path)

	path, err = ordinaryExecutableFile(withExtension, ordinaryExecutableSearchRules{extensions: []string{".exe", ".cmd"}})
	require.NoError(t, err)
	require.Equal(t, withExtension, path)

	path, err = lookPathInOrdinaryEnvironmentWithRules("tool", []string{"PATH=.:missing"}, bin, unixOrdinaryExecutableRules())
	require.NoError(t, err)
	require.Equal(t, executable, path)
	path, err = lookPathInOrdinaryEnvironmentWithRules("tool", []string{"PATH=:missing"}, bin, unixOrdinaryExecutableRules())
	require.NoError(t, err)
	require.Equal(t, executable, path)

	original := ordinaryEnvironmentGetwd
	t.Cleanup(func() { ordinaryEnvironmentGetwd = original })
	ordinaryEnvironmentGetwd = func() (string, error) { return "", errors.New("getwd failed") }
	_, err = lookPathInOrdinaryEnvironment("tool", []string{"PATH=" + bin}, "")
	require.ErrorContains(t, err, "get working directory")

	ordinaryEnvironmentGetwd = func() (string, error) { return dir, nil }
	path, err = lookPathInOrdinaryEnvironment("bin/tool", nil, "")
	require.NoError(t, err)
	require.Equal(t, executable, path)

	for _, file := range []string{"", "missing"} {
		_, resolveErr := lookPathInOrdinaryEnvironmentWithRules(file, []string{"PATH=" + bin}, dir, unixOrdinaryExecutableRules())
		require.Error(t, resolveErr)
	}
	_, err = ordinaryExecutableFile(filepath.Join(bin, "missing"), ordinaryExecutableSearchRules{extensions: []string{".exe"}})
	require.ErrorContains(t, err, "no executable extension")
}

func TestMatchOrdinaryExecutableFileAndContainment(t *testing.T) {
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain")
	require.NoError(t, os.WriteFile(plain, []byte("x"), 0o600))

	path, err := matchOrdinaryExecutableFile(plain, false)
	require.NoError(t, err)
	require.Equal(t, plain, path)

	_, err = matchOrdinaryExecutableFile(plain, true)
	require.Error(t, err)
	_, err = matchOrdinaryExecutableFile(dir, false)
	require.Error(t, err)
	_, err = matchOrdinaryExecutableFile(filepath.Join(dir, "missing"), false)
	require.Error(t, err)

	require.True(t, ProcessContainmentComplete(nil))
	require.True(t, ProcessContainmentComplete(errors.New("other")))
	require.False(t, ProcessContainmentComplete(ErrContainmentIncomplete))
	require.False(t, ProcessContainmentComplete(errors.Join(errors.New("outer"), ErrContainmentIncomplete)))
}
