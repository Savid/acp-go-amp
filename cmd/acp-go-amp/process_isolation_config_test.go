package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	ampacp "github.com/savid/acp-go-amp"
)

const testProcessIsolationConfigPath = "/test/process-isolation.json"

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

	processIsolationConfigLoader = func(string) (processIsolationConfig, error) {
		t.Fatal("omitted -process-isolation-config must not load a policy")

		return processIsolationConfig{}, nil
	}

	var got ampacp.Options

	serve = func(_ context.Context, _ io.Reader, _ io.Writer, opts ...ampacp.Option) error {
		for _, opt := range opts {
			opt(&got)
		}

		return nil
	}

	var stderr strings.Builder
	if code := run(t.Context(), nil, strings.NewReader(""), &strings.Builder{}, &stderr); code != 0 {
		t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
	}

	if got.ProcessIsolation != nil {
		t.Fatalf("ProcessIsolation = %#v, want nil", got.ProcessIsolation)
	}
}

func TestRunWithExplicitProcessIsolationConfigIsFailClosed(t *testing.T) {
	originalServe, originalLoader := serve, processIsolationConfigLoader
	t.Cleanup(func() {
		serve = originalServe
		processIsolationConfigLoader = originalLoader
	})

	processIsolationConfigLoader = func(string) (processIsolationConfig, error) {
		return processIsolationConfig{}, errors.New("invalid policy")
	}
	serve = func(context.Context, io.Reader, io.Writer, ...ampacp.Option) error {
		t.Fatal("a rejected explicit policy must never reach Serve")

		return nil
	}

	var stderr strings.Builder
	code := run(
		t.Context(), []string{"-" + processIsolationConfigFlag, "/invalid-policy"},
		strings.NewReader(""), &strings.Builder{}, &stderr,
	)
	if code != 1 || !strings.Contains(stderr.String(), "process isolation: invalid policy") {
		t.Fatalf("run code = %d, stderr = %q", code, stderr.String())
	}
}
