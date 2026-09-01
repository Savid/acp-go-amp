package amp

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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

const helperLoginURL = "https://ampcode.com/auth/cli-login?authToken=deadbeef"

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

func TestAuthSettingsDocumentAssertsTheFlagFalse(t *testing.T) {
	if !strings.Contains(string(AuthSettingsDocument()), authNativeSecretsSetting) {
		t.Fatalf("settings document does not name the namespaced key: %q", AuthSettingsDocument())
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

func TestAuthReadSecretSurfacesAnUnreadableStore(t *testing.T) {
	want := errors.New("read denied")
	original := authReadFile
	authReadFile = func(string) ([]byte, error) { return nil, want }

	t.Cleanup(func() { authReadFile = original })

	if _, _, err := AuthReadSecret(t.TempDir()); !errors.Is(err, want) {
		t.Fatalf("AuthReadSecret = %v, want %v", err, want)
	}
}

func TestAuthLoginEnvDropsTheAmbientKey(t *testing.T) {
	t.Setenv(AuthAPIKeyEnv, "ambient-key")

	client := NewClient(nil, Options{OrdinaryEnvironment: map[string]string{"PATH": os.Getenv("PATH")}})
	env, err := authLoginEnv(client, map[string]string{AuthAPIKeyEnv: "override-key", "AMP_URL": "https://amp.example"}, "/work")
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range env {
		if strings.HasPrefix(entry, AuthAPIKeyEnv+"=") {
			t.Fatalf("login env kept the api key: %q", entry)
		}
	}

	if !slices.Contains(env, authHeadlessOAuthEnv+"=1") {
		t.Fatalf("login env does not force the headless leg: %v", env)
	}

	if !slices.Contains(env, AuthDeploymentEnv+"=https://amp.example") {
		t.Fatalf("login env dropped a session override: %v", env)
	}
}

// TestAuthDeploymentSupported pins which deployments the brokering legs will
// run against. The deployment selector reaches the login child either way —
// ordinary Amp commands honour it — so it is the auth surface, whose URL host
// and store key are measured facts about one deployment, that refuses.
func TestAuthDeploymentSupported(t *testing.T) {
	t.Setenv(AuthDeploymentEnv, "")

	cases := map[string]bool{
		"":                              true,
		"https://" + AuthURLHost:        true,
		"https://" + AuthURLHost + "/":  true,
		"https://amp.example":           false,
		"https://ampcode.com.evil.test": false,
		"://":                           false,
	}

	for value, want := range cases {
		client := newTestClient(t, nil, Options{Env: map[string]string{AuthDeploymentEnv: value}})
		if got := client.AuthDeploymentSupported(); got != want {
			t.Fatalf("AuthDeploymentSupported(%q) = %v, want %v", value, got, want)
		}
	}

	// An unset selector is the default deployment, which is what every pinned
	// fact on this surface describes.
	if !newTestClient(t, nil, Options{}).AuthDeploymentSupported() {
		t.Fatal("an unset deployment selector was refused")
	}

	// Darwin intercepts through the launcher shim exactly as Linux does; only
	// Windows lacks a PATH-shadowable launch and is refused before any child.
	if !newTestClient(t, nil, Options{TestOnlyAuthLoginPlatform: darwinPlatform}).AuthDeploymentSupported() {
		t.Fatal("darwin account login was reported as unsupported")
	}

	if newTestClient(t, nil, Options{TestOnlyAuthLoginPlatform: windowsPlatform}).AuthDeploymentSupported() {
		t.Fatal("windows account login was reported as supported")
	}
}

func TestBuildEnvScrubsTheDisclosureVariables(t *testing.T) {
	t.Setenv(scrubbedTracebackEnv, "crash")
	t.Setenv(scrubbedRedactionEnv, "1")

	env := BuildEnv(map[string]string{scrubbedRedactionEnv: "1", scrubbedTracebackEnv: "all"}, "")
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if isScrubbedEnv(key) {
			t.Fatalf("spawn env kept %q", entry)
		}
	}
}

func TestAuthLoginArgs(t *testing.T) {
	bare := newTestClient(t, nil, Options{}).authLoginArgs()
	if bare[len(bare)-1] != authLoginSubcommand || slices.Contains(bare, ampArgSettingsFile) {
		t.Fatalf("bare login args = %v", bare)
	}

	configured := newTestClient(t, nil, Options{SettingsFile: "/s.json"}).authLoginArgs()
	if !slices.Contains(configured, ampArgSettingsFile) || !slices.Contains(configured, "/s.json") {
		t.Fatalf("configured login args = %v", configured)
	}

	// The mode and MCP flags never ride the login command: neither selects a
	// model turn and both would be rejected by a command that takes no turn.
	full := newTestClient(t, nil, Options{SettingsFile: "/s.json", Mode: "high", MCPConfigPath: "/m.json"}).authLoginArgs()
	if slices.Contains(full, "-m") || slices.Contains(full, ampArgMCPConfig) {
		t.Fatalf("login args carry turn flags: %v", full)
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

	// Close is idempotent and reports the same result to every caller.
	if first, second := login.Close(), login.Close(); !errors.Is(second, first) && first != second {
		t.Fatalf("Close is not idempotent: %v then %v", first, second)
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

// stubWriteCloser accepts every write and fails its close, which is the one
// stdin outcome a real pipe will not produce on demand.
type stubWriteCloser struct{ closeErr error }

func (s stubWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (s stubWriteCloser) Close() error                { return s.closeErr }

func TestAuthLoginSubmitReportsAFailedStdinClose(t *testing.T) {
	want := errors.New("close refused")
	login := &AuthLogin{stdin: stubWriteCloser{closeErr: want}}

	if err := login.Submit(t.Context(), "pasted"); !errors.Is(err, want) {
		t.Fatalf("Submit = %v, want %v", err, want)
	}
}

func TestStartAuthLoginRejectsAnUnusableLaunch(t *testing.T) {
	if _, err := newTestClient(t, nil, Options{CLIPath: ""}).StartAuthLogin(cancelledContext()); err == nil {
		t.Fatal("a cancelled context started a login")
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

func TestStartAuthLoginReportsAStartFailure(t *testing.T) {
	client := newTestClient(t, nil, Options{CLIPath: filepath.Join(t.TempDir(), "missing-amp"), Cwd: t.TempDir()})
	if _, err := client.StartAuthLogin(t.Context()); err == nil {
		t.Fatal("a missing binary started a login")
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

func TestAuthLoginSettlementAndCloseResidualBranches(t *testing.T) {
	wantWait := errors.New("wait refused")
	wantSettle := errors.New("settle refused")
	process := newCoverageNativeProcess()
	process.wait = func(context.Context) (NativeResult, error) { return NativeResult{ExitCode: 2}, wantWait }
	login := &AuthLogin{
		process:  process,
		waitDone: make(chan struct{}),
		settle: func(context.Context) error {
			return wantSettle
		},
	}
	login.waitNative(t.Context())
	if !errors.Is(login.waitErr, wantWait) || !errors.Is(login.waitErr, wantSettle) {
		t.Fatalf("waitNative error = %v", login.waitErr)
	}

	cleaned := false
	settled := make(chan struct{})
	close(settled)
	login = &AuthLogin{
		process:  newCoverageNativeProcess(),
		stdin:    &coverageWriteCloser{},
		stdout:   io.NopCloser(strings.NewReader("")),
		stderr:   io.NopCloser(strings.NewReader("")),
		waitDone: settled,
		cleanup: func() error {
			cleaned = true

			return nil
		},
	}
	if err := login.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !cleaned {
		t.Fatal("Close skipped cleanup after a settled boundary")
	}

	timeoutCtx, cancel := context.WithCancel(t.Context())
	cancel()
	login = &AuthLogin{
		process:  newCoverageNativeProcess(),
		stdout:   io.NopCloser(strings.NewReader("")),
		stderr:   io.NopCloser(strings.NewReader("")),
		waitDone: make(chan struct{}),
	}
	if err := login.closeWithContext(timeoutCtx); !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("timed-out close = %v", err)
	}
}

func TestAuthLoginIncompleteCloseCancelsAndJoinsWaiterWithoutCleanup(t *testing.T) {
	process := newContextBoundCoverageProcess()
	waitCtx, stopWaiting := context.WithCancel(context.Background())
	settled := false
	cleaned := false
	login := &AuthLogin{
		process:  process,
		stdin:    &coverageWriteCloser{},
		stdout:   io.NopCloser(strings.NewReader("")),
		stderr:   io.NopCloser(strings.NewReader("")),
		waitDone: make(chan struct{}),
		waitStop: stopWaiting,
		settle: func(context.Context) error {
			settled = true

			return nil
		},
		cleanup: func() error {
			cleaned = true

			return nil
		},
	}
	go login.waitNative(waitCtx)
	<-process.waitStarted

	closeCtx, cancelClose := context.WithCancel(t.Context())
	closeDone := make(chan error, 1)
	go func() { closeDone <- login.closeWithContext(closeCtx) }()

	select {
	case <-process.waitExited:
		t.Fatal("login waiter exited before the close lifetime expired")
	default:
	}

	cancelClose()
	err := <-closeDone
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("closeWithContext = %v, want cancelled incomplete containment", err)
	}
	<-process.waitExited
	if settled {
		t.Fatal("detached wait reclaimed the auth residence")
	}
	if cleaned {
		t.Fatal("incomplete close cleaned the auth residence")
	}
}

func TestAuthLoginIncompleteCloseCancelsAndJoinsSettlementWithoutCleanup(t *testing.T) {
	process := newCoverageNativeProcess()
	waitCtx, stopWaiting := context.WithCancel(context.Background())
	settleStarted := make(chan struct{})
	settleExited := make(chan struct{})
	cleaned := false
	login := &AuthLogin{
		process:  process,
		stdin:    &coverageWriteCloser{},
		stdout:   io.NopCloser(strings.NewReader("")),
		stderr:   io.NopCloser(strings.NewReader("")),
		waitDone: make(chan struct{}),
		waitStop: stopWaiting,
		settle: func(ctx context.Context) error {
			close(settleStarted)
			<-ctx.Done()
			close(settleExited)

			return ctx.Err()
		},
		cleanup: func() error {
			cleaned = true

			return nil
		},
	}
	go login.waitNative(waitCtx)
	<-settleStarted

	closeCtx, cancelClose := context.WithCancel(t.Context())
	closeDone := make(chan error, 1)
	go func() { closeDone <- login.closeWithContext(closeCtx) }()
	cancelClose()

	err := <-closeDone
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrContainmentIncomplete) {
		t.Fatalf("closeWithContext = %v, want cancelled incomplete containment", err)
	}
	<-settleExited
	if cleaned {
		t.Fatal("incomplete settlement cleaned the auth residence")
	}
}

func TestAuthLoginPanicHandlerRuns(t *testing.T) {
	recovered := make(chan any, 1)
	login := &AuthLogin{onPanic: func(_ context.Context, _ string, value any) { recovered <- value }}

	func() {
		defer login.recoverGoroutine(t.Context(), "probe")

		panic("boom")
	}()

	if value := <-recovered; value != "boom" {
		t.Fatalf("recovered = %v", value)
	}

	// A login with no handler leaves the panic to propagate, which is what the
	// nil branch means.
	bare := &AuthLogin{}

	func() {
		defer bare.recoverGoroutine(t.Context(), "probe")
	}()
}
