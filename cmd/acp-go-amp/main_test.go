package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	ampacp "github.com/savid/acp-go-amp"
)

func TestMainUsesExitCode(t *testing.T) {
	origExit := exit
	origArgs := os.Args
	t.Cleanup(func() {
		exit = origExit
		os.Args = origArgs
	})
	var code int
	exit = func(got int) { code = got }
	os.Args = []string{"acp-go-amp", "-version"}
	main()
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
}

func TestRunCLI(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := runCLI([]string{"-version"}, strings.NewReader(""), &stdout, &stderr); code != 0 {
		t.Fatalf("version exit = %d", code)
	}
	if strings.TrimSpace(stdout.String()) != version {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if code := runCLI([]string{"-bad"}, strings.NewReader(""), &stdout, &stderr); code != 1 {
		t.Fatalf("bad flag exit = %d", code)
	}
	if !strings.Contains(stderr.String(), "serve failed") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunServeEOFAndDebug(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"-debug", "-path", "/tmp/amp", "-home", t.TempDir(), "-model", "ignored"}, strings.NewReader(""), &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunReturnsServeError(t *testing.T) {
	orig := serveACP
	t.Cleanup(func() { serveACP = orig })
	serveACP = func(context.Context, io.Reader, io.Writer, ...ampacp.Option) error {
		return errors.New("serve boom")
	}
	var stdout, stderr bytes.Buffer
	if err := run(nil, strings.NewReader(""), &stdout, &stderr); err == nil || err.Error() != "serve boom" {
		t.Fatalf("run error = %v", err)
	}
}
