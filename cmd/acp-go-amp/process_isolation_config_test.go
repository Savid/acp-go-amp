package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	ampacp "github.com/savid/acp-go-amp"
)

const testProcessIsolationConfigPath = "/test/process-isolation.json"

const cliOrdinaryHarnessSource = `package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	args := os.Args[1:]
	ledger := filepath.Join(filepath.Dir(os.Args[0]), "launches.log")
	file, err := os.OpenFile(ledger, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(3)
	}
	_, _ = fmt.Fprintln(file, strings.Join(args, " "))
	_ = file.Close()

	for _, arg := range args {
		if arg == "T-00000000-0000-0000-0000-000000000000" {
			_, _ = fmt.Fprintln(os.Stderr, "Thread not found")
			os.Exit(1)
		}
	}

	if len(args) > 0 && args[0] == "version" {
		_, _ = fmt.Fprintln(os.Stdout, "0.0.1784765892-gfake")
		return
	}
	for i, arg := range args {
		if arg == "threads" && i+1 < len(args) && args[i+1] == "list" {
			_, _ = fmt.Fprintln(os.Stdout, "[]")
			return
		}
	}

	_, _ = io.ReadAll(os.Stdin)
	_, _ = fmt.Fprintln(os.Stdout, "{\"type\":\"system\",\"subtype\":\"init\",\"cwd\":\"/tmp/project\",\"session_id\":\"T-cli-ordinary\"}")
	_, _ = fmt.Fprintln(os.Stdout, "{\"type\":\"assistant\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"ordinary\"}]},\"session_id\":\"T-cli-ordinary\"}")
	_, _ = fmt.Fprintln(os.Stdout, "{\"type\":\"result\",\"subtype\":\"success\",\"duration_ms\":1,\"is_error\":false,\"num_turns\":1,\"result\":\"done\",\"session_id\":\"T-cli-ordinary\"}")
}
`

func cliOrdinaryHarness(t *testing.T) (string, string) {
	t.Helper()

	dir := t.TempDir()
	source := filepath.Join(dir, "amp.go")
	if err := os.WriteFile(source, []byte(cliOrdinaryHarnessSource), 0o600); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "amp")
	if runtime.GOOS == "windows" {
		path += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", path, source).CombinedOutput(); err != nil {
		t.Fatalf("build CLI ordinary harness: %v\n%s", err, out)
	}

	ledger := filepath.Join(dir, "launches.log")

	return path, ledger
}

func cliOrdinaryLaunches(t *testing.T, ledger string) string {
	t.Helper()

	data, err := os.ReadFile(ledger)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}

func stubProcessIsolationConfig(t *testing.T) {
	t.Helper()

	original := processIsolationConfigLoader
	processIsolationConfigLoader = func(path string) (processIsolationConfig, error) {
		if path != testProcessIsolationConfigPath {
			t.Fatalf("process isolation config path = %q", path)
		}

		return processIsolationConfig{
			UID:                 20001,
			GID:                 20001,
			BaseEnvironment:     map[string]string{"PATH": "/usr/bin", "HOME": "/var/empty/acp", "USER": "acp", "LOGNAME": "acp"},
			StandaloneOwnerID:   "test-owner",
			StandaloneStateRoot: "/var/empty/acp",
		}, nil
	}
	t.Cleanup(func() { processIsolationConfigLoader = original })
}

func isolatedArgs(args ...string) []string {
	return append([]string{"-" + processIsolationConfigFlag, testProcessIsolationConfigPath}, args...)
}

func TestDecodeProcessIsolationConfigStrict(t *testing.T) {
	config, err := decodeProcessIsolationConfig([]byte(`{"uid":20001,"gid":20002,"baseEnvironment":{"PATH":"/usr/bin"},"inheritEnvironment":["AMP_API_KEY"],"standaloneOwnerId":"deployment-a","standaloneStateRoot":"/var/lib/acp"}`))
	if err != nil {
		t.Fatal(err)
	}
	if config.UID != 20001 || config.GID != 20002 || config.BaseEnvironment["PATH"] != "/usr/bin" || len(config.InheritEnvironment) != 1 {
		t.Fatalf("decoded config = %#v", config)
	}

	for _, document := range []string{
		`{"uid":1,"gid":2,"baseEnvironment":{},"unknown":true}`,
		`{"uid":1,"gid":2,"baseEnvironment":{}} {}`,
		`{} @`,
		`{1:2}`,
		`[@]`,
		`{"uid":"invalid","gid":2,"baseEnvironment":{}}`,
		`{"uid":1,"uid":2,"gid":2,"baseEnvironment":{}}`,
		`{"uid":1,"gid":2,"baseEnvironment":{"PATH":"/bin","PATH":"/usr/bin"}}`,
		``,
	} {
		if _, err := decodeProcessIsolationConfig([]byte(document)); err == nil {
			t.Fatalf("decode unexpectedly accepted %q", document)
		}
	}
}

func TestRunWithoutProcessIsolationConfigUsesOrdinaryMode(t *testing.T) {
	originalServe, originalLoader := serve, processIsolationConfigLoader
	t.Cleanup(func() {
		serve = originalServe
		processIsolationConfigLoader = originalLoader
	})
	t.Setenv("AMP_API_KEY", "test-key")

	harness, ledger := cliOrdinaryHarness(t)

	processIsolationConfigLoader = func(string) (processIsolationConfig, error) {
		t.Fatal("omitted -process-isolation-config must not load a policy")

		return processIsolationConfig{}, nil
	}

	var got ampacp.Options

	serve = func(ctx context.Context, _ io.Reader, _ io.Writer, opts ...ampacp.Option) (returnErr error) {
		for _, opt := range opts {
			opt(&got)
		}
		if got.ProcessIsolation != nil {
			return errors.New("ordinary CLI setup appended process isolation")
		}

		agent := ampacp.NewAgent(opts...)
		defer func() { returnErr = errors.Join(returnErr, agent.Close()) }()

		session, err := agent.NewSession(ctx, ampacp.NewSessionRequest(t.TempDir()))
		if err != nil {
			return err
		}
		_, err = agent.Prompt(ctx, ampacp.TextPromptRequest(session.SessionId, "cli-ordinary", "hello"))

		return err
	}

	var stderr strings.Builder
	if code := run(
		t.Context(), []string{"-path", harness, "-scratch-dir", t.TempDir()},
		strings.NewReader(""), &strings.Builder{}, &stderr,
	); code != 0 {
		t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
	}

	if got.ProcessIsolation != nil {
		t.Fatalf("ProcessIsolation = %#v, want nil", got.ProcessIsolation)
	}
	launches := cliOrdinaryLaunches(t, ledger)
	if launches == "" || !strings.Contains(launches, "-x") {
		t.Fatalf("ordinary CLI did not reach a native execute launch: %q", launches)
	}
}

func TestRunWithExplicitProcessIsolationConfigIsFailClosed(t *testing.T) {
	originalServe, originalLoader := serve, processIsolationConfigLoader
	t.Cleanup(func() {
		serve = originalServe
		processIsolationConfigLoader = originalLoader
	})
	harness, ledger := cliOrdinaryHarness(t)

	for _, failure := range []struct {
		name string
		err  error
	}{
		{name: "read", err: errors.New("read policy")},
		{name: "decode", err: errors.New("decode policy")},
		{name: "validation", err: errors.New("invalid policy")},
		{name: "root", err: errors.New("root supervisor required")},
		{name: "authority", err: errors.New("account authority unproven")},
		{name: "unsupported platform", err: errors.New("supported only on linux")},
	} {
		t.Run(failure.name, func(t *testing.T) {
			loaderCalls, serveCalls := 0, 0
			processIsolationConfigLoader = func(path string) (processIsolationConfig, error) {
				loaderCalls++
				if path != testProcessIsolationConfigPath {
					t.Fatalf("loader path = %q", path)
				}

				return processIsolationConfig{}, failure.err
			}
			serve = func(context.Context, io.Reader, io.Writer, ...ampacp.Option) error {
				serveCalls++

				return nil
			}

			var stderr strings.Builder
			code := run(
				t.Context(), []string{"-path", harness, "-" + processIsolationConfigFlag, testProcessIsolationConfigPath},
				strings.NewReader(""), &strings.Builder{}, &stderr,
			)
			if code != 1 || !strings.Contains(stderr.String(), failure.err.Error()) {
				t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
			}
			if loaderCalls != 1 || serveCalls != 0 {
				t.Fatalf("loader calls = %d, serve calls = %d", loaderCalls, serveCalls)
			}
			if launches := cliOrdinaryLaunches(t, ledger); launches != "" {
				t.Fatalf("%s refusal spawned native work: %q", failure.name, launches)
			}
		})
	}

	t.Run("supplied policy never retries as ordinary", func(t *testing.T) {
		processIsolationConfigLoader = func(string) (processIsolationConfig, error) {
			return processIsolationConfig{
				UID: 20001, GID: 20001,
				BaseEnvironment: map[string]string{"PATH": filepath.Dir(harness)},
			}, nil
		}

		serveCalls := 0
		serve = func(ctx context.Context, _ io.Reader, _ io.Writer, opts ...ampacp.Option) error {
			serveCalls++
			var got ampacp.Options
			for _, opt := range opts {
				opt(&got)
			}
			if got.ProcessIsolation == nil {
				return errors.New("explicit policy was dropped")
			}

			agent := ampacp.NewAgent(opts...)
			defer agent.Close()
			_, err := agent.NewSession(ctx, ampacp.NewSessionRequest(t.TempDir()))

			return err
		}

		var stderr strings.Builder
		code := run(
			t.Context(), []string{"-path", harness, "-" + processIsolationConfigFlag, testProcessIsolationConfigPath},
			strings.NewReader(""), &strings.Builder{}, &stderr,
		)
		if code != 1 || serveCalls != 1 {
			t.Fatalf("run code = %d, serve calls = %d, stderr = %q", code, serveCalls, stderr.String())
		}
		if launches := cliOrdinaryLaunches(t, ledger); launches != "" {
			t.Fatalf("explicit refusal retried ordinary native work: %q", launches)
		}
	})
}
