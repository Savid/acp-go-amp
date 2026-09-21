package amp

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/savid/acp-go-core/process"
	"github.com/stretchr/testify/require"
)

func TestBridgeOwnsOnlyItsTemporaryPlugin(t *testing.T) {
	t.Parallel()
	root, scratch := t.TempDir(), t.TempDir()
	plugins := filepath.Join(root, "amp", "plugins")
	require.NoError(t, os.MkdirAll(plugins, 0o700))
	user := filepath.Join(plugins, "user.ts")
	require.NoError(t, os.WriteFile(user, []byte("user plugin"), 0o600))
	request := process.Request{Dir: t.TempDir(), Env: []string{"XDG_CONFIG_HOME=" + root, "AMP_SETTINGS_FILE=" + filepath.Join(t.TempDir(), "settings.json")}}
	first, err := installBridge(&request, filepath.Join(scratch, "one"), "thread-one")
	require.NoError(t, err)
	defer first.close()
	secondRequest := process.Request{Dir: request.Dir, Env: []string{"XDG_CONFIG_HOME=" + root}}
	second, err := installBridge(&secondRequest, filepath.Join(scratch, "two"), "thread-two")
	require.NoError(t, err)
	defer second.close()
	require.Equal(t, plugins, filepath.Dir(first.plugin))
	require.NotEqual(t, first.plugin, second.plugin)
	token, ok := process.Lookup(request.Env, InternalEnvPrefix+"PLUGIN")
	require.True(t, ok)
	source, err := os.ReadFile(first.plugin)
	require.NoError(t, err)
	require.Contains(t, string(source), "const launch = '"+token+"'")
	first.close()
	require.NoFileExists(t, first.plugin)
	require.NoDirExists(t, first.directory)
	require.FileExists(t, second.plugin)
	original, err := os.ReadFile(user)
	require.NoError(t, err)
	require.Equal(t, "user plugin", string(original))
}

func TestBridgeWaitsForACompleteReceiptLine(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "events")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	writer, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	b := bridge{events: file}
	_, err = writer.WriteString(`{"type":"end","threadId":"thread","id":"M-1","status":"done","messages":[`)
	require.NoError(t, err)
	events, err := b.read()
	require.NoError(t, err)
	require.Empty(t, events)
	_, err = writer.WriteString("{\"id\":\"M-1\",\"role\":\"user\",\"content\":[]}] }\n")
	require.NoError(t, err)
	events, err = b.read()
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, StatusDone, events[0].Status)
	require.Len(t, events[0].Messages, 1)
}

func TestSweepRemovesOnlyDeadAdapterPlugins(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// A plugin left by a process that has already exited.
	cmd := exec.Command("true")
	require.NoError(t, cmd.Run())
	deadPID := cmd.Process.Pid
	orphan := filepath.Join(dir, pluginPrefix+strconv.Itoa(deadPID)+"-abc.ts")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o600))

	// A plugin owned by this live process, and a foreign file.
	live := filepath.Join(dir, pluginPrefix+strconv.Itoa(os.Getpid())+"-def.ts")
	require.NoError(t, os.WriteFile(live, []byte("live"), 0o600))
	foreign := filepath.Join(dir, "user.ts")
	require.NoError(t, os.WriteFile(foreign, []byte("user"), 0o600))

	sweepOrphanPlugins(dir)

	require.NoFileExists(t, orphan, "a dead adapter's plugin is swept")
	require.FileExists(t, live, "a live adapter's own plugin is kept")
	require.FileExists(t, foreign, "a foreign plugin is untouched")
}
