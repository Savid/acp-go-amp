//go:build !windows

package amp

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// helperLogin answers the `login` subcommand for the fake amp binary. Each mode
// is one shape of the hosted paste-back leg the auth surface has to survive.
func helperLogin(mode string, state string) {
	recordHelperJSON(state, "login-env.jsonl", os.Environ())

	switch mode {
	case "login-no-url":
		os.Stdout.WriteString("If your browser does not open automatically, visit:\n\nnot a url at all\n")
	case "login-bad-url":
		os.Stdout.WriteString("http://ampcode.com/auth/cli-login\nhttps://evil.example/auth\n")
	case "login-fragment-url":
		os.Stdout.WriteString("https://ampcode.com/auth/cli-login?authToken=fake#fragment\n")
	case "login-settled":
		os.Stdout.WriteString(helperLoginURL + "\n")
	case "login-settled-fail":
		os.Stdout.WriteString(helperLoginURL + "\n")
		os.Stderr.WriteString("Login failed: that code looks incomplete\n")
		os.Exit(1)
	case "login-hang":
		os.Stdout.WriteString(helperLoginURL + "\n")

		for {
			time.Sleep(time.Hour)
		}
	case "login-refuse":
		os.Stdout.WriteString(helperLoginURL + "\n")
		_, _ = io.ReadAll(os.Stdin)
		os.Stderr.WriteString("Login failed: that code looks incomplete\n")
		os.Exit(1)
	default:
		os.Stdout.WriteString("If your browser does not open automatically, visit:\n\n" + helperLoginURL + "\n\n")
		os.Stdout.WriteString("When prompted, paste your code here: ")

		pasted, _ := io.ReadAll(os.Stdin)
		recordHelperJSON(state, "login-stdin.jsonl", strings.TrimSpace(string(pasted)))
		helperWriteSecret(strings.TrimSpace(string(pasted)))
	}
}

// helperWriteSecret plants the account entry where amp would, so the harvest
// leg reads a real file rather than a stub.
func helperWriteSecret(pasted string) {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		return
	}

	dir := filepath.Join(dataHome, authSecretsDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		os.Exit(2)
	}

	body := `{"` + authSecretEntryKey + `":"secret-for-` + pasted + `"}`
	if err := os.WriteFile(filepath.Join(dir, authSecretsFileName), []byte(body), 0o600); err != nil {
		os.Exit(2)
	}
}

func TestAuthReadSecretReadsOnlyTheNamedEntry(t *testing.T) {
	dataHome := t.TempDir()
	dir := filepath.Join(dataHome, authSecretsDirName)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}

	secret, present, err := AuthReadSecret(dataHome)
	if err != nil || present || secret != "" {
		t.Fatalf("absent file = %q/%v/%v", secret, present, err)
	}

	write := func(contents string) {
		t.Helper()

		if writeErr := os.WriteFile(AuthSecretsPath(dataHome), []byte(contents), 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}

	// Every key outside the named one belongs to amp and is dropped rather than
	// forwarded or rejected.
	write(`{"` + authSecretEntryKey + `":"live-key","apiKey@https://other/":"foreign"}`)

	secret, present, err = AuthReadSecret(dataHome)
	if err != nil || !present || secret != "live-key" {
		t.Fatalf("named entry = %q/%v/%v", secret, present, err)
	}

	if present, err := AuthSecretPresent(dataHome); err != nil || !present {
		t.Fatalf("AuthSecretPresent = %v/%v", present, err)
	}

	write(`{"apiKey@https://other/":"foreign"}`)

	if _, present, err := AuthReadSecret(dataHome); err != nil || present {
		t.Fatalf("unnamed-only store = %v/%v", present, err)
	}

	write("{")

	if _, _, err := AuthReadSecret(dataHome); err == nil {
		t.Fatal("malformed secrets decoded")
	}

	for _, contents := range []string{`{"` + authSecretEntryKey + `":42}`, `{"` + authSecretEntryKey + `":""}`} {
		write(contents)

		if _, _, err := AuthReadSecret(dataHome); !errors.Is(err, errAuthSecret) {
			t.Fatalf("%s decoded as a secret: %v", contents, err)
		}
	}
}

func startTestLogin(t *testing.T, mode string, dataHome string) *AuthLogin {
	t.Helper()

	path, _ := fakeAmpPath(t, mode)
	client := newTestClient(t, nil, Options{
		CLIPath: path,
		Cwd:     t.TempDir(),
		Env:     map[string]string{"XDG_DATA_HOME": dataHome},
	})

	login, err := client.StartAuthLogin(t.Context())
	if err != nil {
		t.Fatalf("StartAuthLogin: %v", err)
	}

	t.Cleanup(func() { _ = login.Close() })

	return login
}

func TestStartAuthLoginRelaysThePrintedURL(t *testing.T) {
	dataHome := t.TempDir()
	login := startTestLogin(t, "login", dataHome)

	url, err := login.URL(t.Context())
	if err != nil || url != helperLoginURL {
		t.Fatalf("URL = %q, %v", url, err)
	}

	if settled, settleErr := login.Settled(); settled || settleErr != nil {
		t.Fatalf("a login awaiting its paste reported settled: %v/%v", settled, settleErr)
	}

	if submitErr := login.Submit(t.Context(), "pasted-envelope"); submitErr != nil {
		t.Fatalf("Submit: %v", submitErr)
	}

	// The residence is the child's own data home, which a containment boundary
	// that redirects the environment moves out from under the session.
	secret, present, err := AuthReadSecret(login.DataHome())
	if err != nil || !present || secret != "secret-for-pasted-envelope" {
		t.Fatalf("harvest after submit = %q/%v/%v", secret, present, err)
	}

	if runtime.GOOS != "darwin" && login.DataHome() != dataHome {
		t.Fatalf("DataHome = %q, want the session data home %q", login.DataHome(), dataHome)
	}

	// Wait has already released ordinary os/exec pipe descriptors; that normal
	// terminal release is still a successful, idempotent close.
	if first, second := login.Close(), login.Close(); first != nil || second != nil {
		t.Fatalf("Close = %v then %v", first, second)
	}
}

func TestStartAuthLoginScrubsTheAmbientKeyFromTheChild(t *testing.T) {
	t.Setenv(AuthAPIKeyEnv, "ambient-key")

	dataHome := t.TempDir()
	path, state := fakeAmpPath(t, "login-settled")
	client := newTestClient(t, nil, Options{
		CLIPath: path,
		Cwd:     t.TempDir(),
		Env:     map[string]string{"XDG_DATA_HOME": dataHome, AuthAPIKeyEnv: "override-key"},
	})

	login, err := client.StartAuthLogin(t.Context())
	if err != nil {
		t.Fatalf("StartAuthLogin: %v", err)
	}

	defer func() { _ = login.Close() }()

	if _, err := login.URL(t.Context()); err != nil {
		t.Fatalf("URL: %v", err)
	}

	for _, entries := range readHelperJSON[[]string](t, filepath.Join(state, "login-env.jsonl")) {
		for _, entry := range entries {
			if strings.HasPrefix(entry, AuthAPIKeyEnv+"=") {
				t.Fatalf("the login child saw an ambient key: %q", entry)
			}
		}
	}
}

func TestAuthLoginReportsNoURLWhenTheChildPrintsNone(t *testing.T) {
	for _, mode := range []string{"login-no-url", "login-bad-url"} {
		login := startTestLogin(t, mode, t.TempDir())
		if _, err := login.URL(t.Context()); !errors.Is(err, errAuthNoURL) {
			t.Fatalf("%s: URL = %v, want no-url", mode, err)
		}
	}
}

func TestAuthLoginURLHonoursItsDeadline(t *testing.T) {
	login := startTestLogin(t, "login-hang", t.TempDir())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := login.URL(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("URL = %v, want cancellation", err)
	}
}

func TestAuthLoginSettledReportsAnIndependentCompletion(t *testing.T) {
	for _, testCase := range []struct {
		mode    string
		wantErr bool
	}{{mode: "login-settled"}, {mode: "login-settled-fail", wantErr: true}} {
		login := startTestLogin(t, testCase.mode, t.TempDir())

		if _, err := login.URL(t.Context()); err != nil {
			t.Fatalf("%s: URL: %v", testCase.mode, err)
		}

		var (
			settled bool
			err     error
		)

		for range 200 {
			if settled, err = login.Settled(); settled {
				break
			}

			time.Sleep(25 * time.Millisecond)
		}

		if !settled {
			t.Fatalf("%s: the exited child never reported settled", testCase.mode)
		}

		if (err != nil) != testCase.wantErr {
			t.Fatalf("%s: Settled err = %v, wantErr %v", testCase.mode, err, testCase.wantErr)
		}
	}
}

func TestAuthLoginSubmitReportsARefusal(t *testing.T) {
	login := startTestLogin(t, "login-refuse", t.TempDir())

	if _, err := login.URL(t.Context()); err != nil {
		t.Fatalf("URL: %v", err)
	}

	if err := login.Submit(t.Context(), "bad-code"); err == nil {
		t.Fatal("a refused paste reported success")
	}
}

func TestAuthLoginSubmitHonoursItsDeadline(t *testing.T) {
	login := startTestLogin(t, "login-hang", t.TempDir())

	if _, err := login.URL(t.Context()); err != nil {
		t.Fatalf("URL: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	if err := login.Submit(ctx, "pasted"); err == nil {
		t.Fatal("a submit against a hung child reported success")
	}
}

func TestAuthLoginSubmitReportsAClosedStdin(t *testing.T) {
	login := startTestLogin(t, "login-hang", t.TempDir())

	if err := login.closeStdin(); err != nil {
		t.Fatalf("closeStdin: %v", err)
	}

	if err := login.Submit(t.Context(), "pasted"); err == nil {
		t.Fatal("a write to closed stdin reported success")
	}

	// The memoized close error is reported to every later caller.
	if err := login.closeStdin(); err != nil {
		t.Fatalf("second closeStdin: %v", err)
	}
}

func TestStartAuthLoginReportsAWorkingDirectoryFailure(t *testing.T) {
	want := errors.New("no working directory")
	original := getwd
	getwd = func() (string, error) { return "", want }

	t.Cleanup(func() { getwd = original })

	path, _ := fakeAmpPath(t, "login")
	if _, err := newTestClient(t, nil, Options{CLIPath: path}).StartAuthLogin(t.Context()); !errors.Is(err, want) {
		t.Fatalf("StartAuthLogin = %v, want %v", err, want)
	}
}

func TestAuthSurfaceResidualBoundaryErrors(t *testing.T) {
	invalid := NewClient(nil, Options{Env: map[string]string{"bad=key": "x"}})
	if invalid.AuthDeploymentSupported() {
		t.Fatal("deployment support accepted an invalid login environment")
	}
	if _, err := invalid.StartAuthLogin(t.Context()); err == nil {
		t.Fatal("login accepted an invalid environment")
	}
	if _, err := authLoginEnv(invalid, map[string]string{"bad=key": "x"}, t.TempDir()); err == nil {
		t.Fatal("authLoginEnv accepted an invalid environment")
	}

	path, _ := fakeAmpPath(t, "login")
	withoutShim := NewClient(nil, Options{
		CLIPath:                   path,
		Cwd:                       t.TempDir(),
		Env:                       map[string]string{dataHomeEnv: t.TempDir()},
		OrdinaryEnvironment:       map[string]string{"PATH": os.Getenv("PATH")},
		TestOnlyAuthLoginPlatform: linuxPlatform,
	})
	if err := withoutShim.CheckAuthLoginSafety(t.Context()); err != nil {
		t.Fatalf("CheckAuthLoginSafety: %v", err)
	}
	if _, err := withoutShim.StartAuthLogin(t.Context()); !errors.Is(err, ErrBrowserLaunchUnsupported) {
		t.Fatalf("StartAuthLogin without shim = %v", err)
	}
}
