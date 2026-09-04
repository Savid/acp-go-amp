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
	"sync"
	"testing"
)

const helperLoginURL = "https://ampcode.com/auth/cli-login?authToken=deadbeef"

func TestAuthSettingsDocumentAssertsTheFlagFalse(t *testing.T) {
	if !strings.Contains(string(AuthSettingsDocument()), authNativeSecretsSetting) {
		t.Fatalf("settings document does not name the namespaced key: %q", AuthSettingsDocument())
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

// stubWriteCloser accepts every write and fails its close, which is the one
// stdin outcome a real pipe will not produce on demand.
type stubWriteCloser struct{ closeErr error }

func (s stubWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (s stubWriteCloser) Close() error                { return s.closeErr }

func TestAuthLoginStreamCloseAcceptsReleasedDescriptors(t *testing.T) {
	alreadyClosed := &os.PathError{Op: "close", Path: "|0", Err: os.ErrClosed}
	if err := closeAuthLoginStream(stubWriteCloser{closeErr: alreadyClosed}); err != nil {
		t.Fatalf("already-closed stream = %v", err)
	}

	want := errors.New("close refused")
	if err := closeAuthLoginStream(stubWriteCloser{closeErr: want}); !errors.Is(err, want) {
		t.Fatalf("independent close error = %v, want %v", err, want)
	}
}

type authWorkerReadCloser struct {
	readStarted chan struct{}
	closeCalled chan struct{}
	release     chan struct{}
	readOnce    sync.Once
	closeOnce   sync.Once
	closeOrder  *[]string
	name        string
}

func newAuthWorkerReadCloser(name string, closeOrder *[]string) *authWorkerReadCloser {
	return &authWorkerReadCloser{
		readStarted: make(chan struct{}),
		closeCalled: make(chan struct{}),
		release:     make(chan struct{}),
		closeOrder:  closeOrder,
		name:        name,
	}
}

func (r *authWorkerReadCloser) Read([]byte) (int, error) {
	r.readOnce.Do(func() { close(r.readStarted) })
	<-r.release

	return 0, io.EOF
}

func (r *authWorkerReadCloser) Close() error {
	r.closeOnce.Do(func() {
		*r.closeOrder = append(*r.closeOrder, r.name)
		close(r.closeCalled)
	})

	return nil
}

type authPanicReadCloser struct{}

func (authPanicReadCloser) Read([]byte) (int, error) { panic("read panic") }
func (authPanicReadCloser) Close() error             { return nil }

func closedAuthWorkerDone() chan struct{} {
	done := make(chan struct{})
	close(done)

	return done
}

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

func TestStartAuthLoginReportsAStartFailure(t *testing.T) {
	t.Run("discovery", func(t *testing.T) {
		client := newTestClient(t, nil, Options{CLIPath: filepath.Join(t.TempDir(), "missing-amp"), Cwd: t.TempDir()})
		if _, err := client.StartAuthLogin(t.Context()); err == nil {
			t.Fatal("a missing binary started a login")
		}
	})

	t.Run("managed start", func(t *testing.T) {
		want := errors.New("managed start refused")
		started := false
		client := newTestClient(t, nil, Options{
			CLIPath: "amp",
			Cwd:     t.TempDir(),
			StartNative: func(context.Context, NativeRequest) (NativeProcess, error) {
				started = true

				return nil, want
			},
		})

		_, err := client.StartAuthLogin(t.Context())

		// Windows materialises no launcher shim, so a brokered login is refused
		// on the platform fact before any native child exists. That refusal is
		// this platform's whole answer here and it stands in front of the start
		// failure every other platform reports.
		if runtime.GOOS == windowsPlatform {
			if !errors.Is(err, ErrBrowserLaunchUnsupported) || started {
				t.Fatalf("StartAuthLogin = %v (started %t), want the unneutralisable-browser refusal", err, started)
			}

			return
		}

		if !errors.Is(err, want) {
			t.Fatalf("StartAuthLogin = %v, want %v", err, want)
		}
	})
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
		process:    newCoverageNativeProcess(),
		stdin:      &coverageWriteCloser{},
		stdout:     io.NopCloser(strings.NewReader("")),
		stderr:     io.NopCloser(strings.NewReader("")),
		stdoutDone: closedAuthWorkerDone(),
		stderrDone: closedAuthWorkerDone(),
		waitDone:   settled,
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
		process:    newCoverageNativeProcess(),
		stdout:     io.NopCloser(strings.NewReader("")),
		stderr:     io.NopCloser(strings.NewReader("")),
		stdoutDone: closedAuthWorkerDone(),
		stderrDone: closedAuthWorkerDone(),
		waitDone:   make(chan struct{}),
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
		process:    process,
		stdin:      &coverageWriteCloser{},
		stdout:     io.NopCloser(strings.NewReader("")),
		stderr:     io.NopCloser(strings.NewReader("")),
		stdoutDone: closedAuthWorkerDone(),
		stderrDone: closedAuthWorkerDone(),
		waitDone:   make(chan struct{}),
		waitStop:   stopWaiting,
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
		process:    process,
		stdin:      &coverageWriteCloser{},
		stdout:     io.NopCloser(strings.NewReader("")),
		stderr:     io.NopCloser(strings.NewReader("")),
		stdoutDone: closedAuthWorkerDone(),
		stderrDone: closedAuthWorkerDone(),
		waitDone:   make(chan struct{}),
		waitStop:   stopWaiting,
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

func TestAuthLoginCloseJoinsPipeWorkers(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		incomplete bool
	}{
		{name: "terminal"},
		{name: "incomplete wait", incomplete: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			closeOrder := []string{}
			stdout := newAuthWorkerReadCloser("stdout", &closeOrder)
			stderr := newAuthWorkerReadCloser("stderr", &closeOrder)
			waitDone := make(chan struct{})
			login := &AuthLogin{
				process:    newCoverageNativeProcess(),
				stdin:      &coverageWriteCloser{},
				stdout:     stdout,
				stderr:     stderr,
				stdoutDone: make(chan struct{}),
				stderrDone: make(chan struct{}),
				waitDone:   waitDone,
				url:        make(chan string, 1),
			}

			closeCtx := t.Context()
			if testCase.incomplete {
				var cancel context.CancelFunc

				closeCtx, cancel = context.WithCancel(t.Context())
				cancel()
				login.waitStop = func() { close(waitDone) }
			} else {
				close(waitDone)
			}

			go login.readStdout(t.Context())
			go login.drainStderr(t.Context())
			<-stdout.readStarted
			<-stderr.readStarted

			closeDone := make(chan error, 1)
			go func() { closeDone <- login.closeWithContext(closeCtx) }()
			<-stdout.closeCalled
			<-stderr.closeCalled

			if len(closeOrder) != 2 || closeOrder[0] != "stdout" || closeOrder[1] != "stderr" {
				t.Fatalf("stream close order = %v", closeOrder)
			}

			select {
			case err := <-closeDone:
				t.Fatalf("Close returned before pipe workers completed: %v", err)
			default:
			}

			close(stdout.release)
			close(stderr.release)
			err := <-closeDone
			if testCase.incomplete {
				if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrContainmentIncomplete) {
					t.Fatalf("Close = %v, want canceled incomplete containment", err)
				}
			} else if err != nil {
				t.Fatalf("Close = %v", err)
			}
		})
	}
}

func TestAuthLoginPipeWorkerCompletionIncludesPanicCallback(t *testing.T) {
	callbackStarted := make(chan string, 2)
	callbackRelease := make(chan struct{})
	login := &AuthLogin{
		stdout:     authPanicReadCloser{},
		stderr:     authPanicReadCloser{},
		stdoutDone: make(chan struct{}),
		stderrDone: make(chan struct{}),
		url:        make(chan string, 1),
		onPanic: func(_ context.Context, name string, recovered any) {
			if recovered != "read panic" {
				t.Errorf("recovered = %v", recovered)
			}

			callbackStarted <- name
			<-callbackRelease
		},
	}

	go login.readStdout(t.Context())
	go login.drainStderr(t.Context())
	names := map[string]bool{
		<-callbackStarted: true,
		<-callbackStarted: true,
	}
	if !names["amp login stdout reader"] || !names["amp login stderr drain"] {
		t.Fatalf("panic callbacks = %v", names)
	}

	select {
	case <-login.stdoutDone:
		t.Fatal("stdout worker completed before its panic callback")
	default:
	}
	select {
	case <-login.stderrDone:
		t.Fatal("stderr worker completed before its panic callback")
	default:
	}

	close(callbackRelease)
	<-login.stdoutDone
	<-login.stderrDone
}

func TestAuthLoginPipeWorkersCloseOwnedSignals(t *testing.T) {
	login := &AuthLogin{
		stdout:     io.NopCloser(strings.NewReader("")),
		stderr:     io.NopCloser(strings.NewReader("")),
		stdoutDone: make(chan struct{}),
		stderrDone: make(chan struct{}),
		url:        make(chan string, 1),
	}

	login.readStdout(t.Context())
	login.drainStderr(t.Context())
	<-login.stdoutDone
	<-login.stderrDone
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
