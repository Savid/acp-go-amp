package amp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

const (
	authLoginSubcommand = "login"

	authSecretsDirName = "amp"
	// authSecretsFileName holds the flat single-key object amp writes its
	// account credential into under XDG_DATA_HOME.
	authSecretsFileName = "secrets.json"
	// authSecretEntryKey is the only entry of that object this package reads.
	// Every other key belongs to amp and is dropped rather than forwarded.
	//nolint:gosec // G101 false positive: this is amp's store key name, not a credential.
	authSecretEntryKey = "apiKey@https://ampcode.com/"

	// authNativeSecretsSetting moves the account credential from the file store
	// to the OS keystore, where the item is keyed by hostname alone. Only this
	// exact namespaced spelling resolves — the bare dotted key and both nested
	// forms are ignored — and it is read at global scope, which is the settings
	// file this package writes.
	authNativeSecretsSetting = "amp.experimental.cli.nativeSecretsStorage.enabled"

	// AuthAPIKeyEnv carries the Amp credential. It is the one operation value
	// every authenticated command needs, wherever that command was started
	// from.
	//
	//nolint:gosec // G101 false positive: this is an environment variable name, not a credential.
	AuthAPIKeyEnv        = "AMP_API_KEY"
	authHeadlessOAuthEnv = "AMP_HEADLESS_OAUTH"

	// AuthURLHost is the only host a relayed authorization URL may name.
	AuthURLHost = "ampcode.com"

	// AuthDeploymentEnv selects which Amp deployment a command talks to. It is
	// honoured by every ordinary command, and it is the reason the brokering
	// legs refuse: AuthURLHost and authSecretEntryKey are both measured facts
	// about the default deployment, so a login run against another one prints a
	// URL this package will not relay and writes a store entry under a key it
	// does not read.
	AuthDeploymentEnv = "AMP_URL"

	dataHomeEnv = "XDG_DATA_HOME"

	authURLScanLimit = 64 * 1024
)

// authSettingsDocument asserts the native-secrets flag false. The wrapper owns
// the settings file it points amp at, and no managed file can override this
// value: the admin layer wins only for an explicit "admin" scope or a scopeless
// read, while this flag is read at "global" scope.
var authSettingsDocument = []byte("{\n  \"" + authNativeSecretsSetting + "\": false\n}\n")

var (
	authReadFile  = os.ReadFile
	errAuthNoURL  = errors.New("amp login printed no authorization URL")
	errAuthSecret = errors.New("amp account secret is not a non-empty string")
)

// AuthSettingsDocument returns the settings body every session writes.
func AuthSettingsDocument() []byte {
	return authSettingsDocument
}

// AuthSecretsPath is the exact file the credential leg reads. Nothing else
// under the data home is read: `amp login` launches a browser that writes its
// own profile there, so the root holds more than this package put in it.
func AuthSecretsPath(dataHome string) string {
	return filepath.Join(dataHome, authSecretsDirName, authSecretsFileName)
}

// AuthReadSecret reads the single account entry out of the isolated data home.
// A missing file and a missing entry are both absence; a present entry that is
// not a non-empty string is a decode failure.
func AuthReadSecret(dataHome string) (string, bool, error) {
	contents, err := authReadFile(AuthSecretsPath(dataHome))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}

		return "", false, fmt.Errorf("read amp secrets: %w", err)
	}

	var secrets map[string]json.RawMessage
	if err := json.Unmarshal(contents, &secrets); err != nil {
		return "", false, fmt.Errorf("decode amp secrets: %w", err)
	}

	raw, ok := secrets[authSecretEntryKey]
	if !ok {
		return "", false, nil
	}

	var secret string
	if err := json.Unmarshal(raw, &secret); err != nil || secret == "" {
		return "", false, errAuthSecret
	}

	return secret, true, nil
}

// AuthSecretPresent probes the named slot for non-empty presence.
func AuthSecretPresent(dataHome string) (bool, error) {
	_, present, err := AuthReadSecret(dataHome)

	return present, err
}

// AuthDeploymentSupported reports whether the login child this client would
// start has both a browser-safe platform boundary and the deployment every
// pinned fact on this surface was measured against. Windows is refused before
// a login child exists: its system loader opens URLs through images no PATH
// entry can shadow. Darwin and Linux both intercept through the launcher
// shim, so their gate is the per-executable audit at login start.
func (c *Client) AuthDeploymentSupported() bool {
	platform := runtime.GOOS
	if c.options.TestOnlyAuthLoginPlatform != "" {
		platform = c.options.TestOnlyAuthLoginPlatform
	}

	if platform == windowsPlatform {
		return false
	}

	environment, err := authLoginEnv(c, c.options.Env, c.options.Cwd)
	if err != nil {
		return false
	}

	value, ok := environmentMap(environment)[AuthDeploymentEnv]
	if !ok || value == "" {
		return true
	}

	parsed, err := url.Parse(value)

	return err == nil && parsed.Host == AuthURLHost
}

// AuthLogin is one running `amp login`. It owns the child's containment
// boundary, its pipes, and the single authorization URL it printed.
type AuthLogin struct {
	process    NativeProcess
	stdin      io.WriteCloser
	stdout     io.ReadCloser
	stderr     io.ReadCloser
	stdoutDone chan struct{}
	stderrDone chan struct{}
	waitDone   chan struct{}
	waitErr    error
	result     NativeResult
	waitStop   context.CancelFunc
	url        chan string
	dataHome   string
	settle     func(context.Context) error
	cleanup    func() error

	stdinOnce sync.Once
	stdinErr  error
	closeOnce sync.Once
	closeErr  error
	onPanic   func(ctx context.Context, name string, recovered any)
}

// StartAuthLogin launches the hosted paste-back login in the client's isolated
// data home. AMP_API_KEY is removed: with one set, `amp login` copies the
// ambient value into the store and exits without starting a flow at all, which
// would hand an environment-supplied credential back as a brokered one.
// AMP_HEADLESS_OAUTH affects MCP OAuth, not this account-login path. Darwin
// and Linux run behind a browser-launcher shim; platforms and executables
// without a provable interception boundary fail before the login child is
// constructed.
func (c *Client) StartAuthLogin(ctx context.Context) (*AuthLogin, error) {
	environment, err := authLoginEnv(c, c.options.Env, c.options.Cwd)
	if err != nil {
		return nil, err
	}

	path, err := c.authLoginSafety(ctx)
	if err != nil {
		return nil, err
	}

	cwd := c.options.Cwd
	if cwd == "" {
		cwd, err = getwd()
		if err != nil {
			return nil, fmt.Errorf("get working directory: %w", err)
		}
	}

	if c.options.BrowserShim == "" {
		return nil, fmt.Errorf("amp login: %w", ErrBrowserLaunchUnsupported)
	}

	environment = browserShimEnviron(environment, c.options.BrowserShim)

	process, err := c.startNative(ctx, NativeRequest{
		Executable: path, Arguments: c.authLoginArgs(), Environment: environment, WorkingDirectory: cwd,
	})
	if err != nil {
		return nil, fmt.Errorf("amp login: %w", err)
	}

	// The residence is the data home the containment boundary settled on, not
	// the one this package asked for: a boundary that redirects the child's
	// roots rewrites it. The launch publishes that environment, so the harvest
	// depends on a stated hand-off rather than on cmd still being the object the
	// boundary mutated.
	dataHome := environmentMap(environment)[dataHomeEnv]

	waitCtx, stopWaiting := context.WithCancel(context.Background())
	login := &AuthLogin{
		process:    process,
		stdin:      process.Stdin(),
		stdout:     process.Stdout(),
		stderr:     process.Stderr(),
		stdoutDone: make(chan struct{}),
		stderrDone: make(chan struct{}),
		waitDone:   make(chan struct{}),
		waitStop:   stopWaiting,
		url:        make(chan string, 1),
		dataHome:   dataHome,
		onPanic:    c.options.OnGoroutinePanic,
		settle:     c.options.AfterNativeWait,
		cleanup:    c.options.CleanupResidence,
	}

	go login.readStdout(ctx)
	go login.drainStderr(ctx)
	go login.waitNative(waitCtx)

	return login, nil
}

// CheckAuthLoginSafety audits the exact executable this client would launch
// without constructing a command or allocating its browser residence.
func (c *Client) CheckAuthLoginSafety(ctx context.Context) error {
	_, err := c.authLoginSafety(ctx)

	return err
}

func (c *Client) authLoginSafety(ctx context.Context) (string, error) {
	path, err := c.resolveExecutable(ctx, c.options.Cwd)
	if err != nil {
		return "", err
	}

	if err := c.checkAuthLoginSafety(path); err != nil {
		return "", fmt.Errorf("amp login: %w", err)
	}

	return path, nil
}

func (c *Client) authLoginArgs() []string {
	args := []string{ampArgNoIDE, ampArgNoColor, ampArgNoNotifications}
	if c.options.SettingsFile != "" {
		args = append(args, ampArgSettingsFile, c.options.SettingsFile)
	}

	return append(args, authLoginSubcommand)
}

// authLoginEnv builds the login child's environment: the session's own values
// plus the headless override, with the ambient API key removed however it
// arrived.
func authLoginEnv(client *Client, base map[string]string, cwd string) ([]string, error) {
	managed := map[string]string{authHeadlessOAuthEnv: "1"}

	env, err := client.buildEnvironment(composeEnvironmentMaps(base, managed), cwd)
	if err != nil {
		return nil, err
	}

	kept := make([]string, 0, len(env))

	for _, entry := range env {
		if key, _, ok := strings.Cut(entry, "="); ok && key == AuthAPIKeyEnv {
			continue
		}

		kept = append(kept, entry)
	}

	return kept, nil
}

func composeEnvironmentMaps(phases ...map[string]string) map[string]string {
	out := map[string]string{}

	for _, phase := range phases {
		for key, value := range phase {
			out[key] = value
		}
	}

	return out
}

func (l *AuthLogin) recoverGoroutine(ctx context.Context, name string) {
	if l.onPanic == nil {
		return
	}

	if recovered := recover(); recovered != nil {
		l.onPanic(ctx, name, recovered)
	}
}

// readStdout publishes the first line that is a valid authorization URL and
// keeps draining afterwards so the child never blocks on a full pipe. The URL
// is validated as a URL before anything else looks at the line, so output the
// harness wraps or reflows yields no URL rather than the wrong bytes.
func (l *AuthLogin) readStdout(ctx context.Context) {
	defer closeAuthLoginWorker(l.stdoutDone)
	defer l.recoverGoroutine(ctx, "amp login stdout reader")
	defer close(l.url)

	scanner := bufio.NewScanner(l.stdout)
	scanner.Buffer(make([]byte, 0, 4096), authURLScanLimit)

	found := false

	for scanner.Scan() {
		if found {
			continue
		}

		candidate := strings.TrimSpace(scanner.Text())
		if !authLoginURL(candidate) {
			continue
		}

		found = true

		l.url <- candidate
	}
}

// drainStderr consumes the child's stderr and forwards none of it. A native
// login failure line can quote the value the owner pasted.
func (l *AuthLogin) drainStderr(ctx context.Context) {
	defer closeAuthLoginWorker(l.stderrDone)
	defer l.recoverGoroutine(ctx, "amp login stderr drain")

	_, _ = io.Copy(io.Discard, l.stderr)
}

// StartAuthLogin always supplies worker signals; nil admits only narrow manual
// values that did not start a worker.
func closeAuthLoginWorker(done chan struct{}) {
	if done != nil {
		close(done)
	}
}

func joinAuthLoginWorker(done <-chan struct{}) {
	if done != nil {
		<-done
	}
}

func authLoginURL(candidate string) bool {
	parsed, err := url.Parse(candidate)

	return err == nil && parsed.Scheme == "https" && parsed.Host == AuthURLHost
}

// DataHome reports where this login's credential actually lands. It is the
// session's isolated data home on a platform whose containment boundary leaves
// the environment alone, and the per-command generation root on one that
// redirects it — so the residence is read off the child that wrote it rather
// than assumed from the session.
func (l *AuthLogin) DataHome() string {
	return l.dataHome
}

// URL waits for the authorization URL the child printed.
func (l *AuthLogin) URL(ctx context.Context) (string, error) {
	select {
	case value, ok := <-l.url:
		if !ok {
			return "", errAuthNoURL
		}

		return value, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Submit hands the owner's pasted value to the child and waits for it to
// settle. The value carries the account credential in the clear, so it is
// written straight through and retained nowhere.
func (l *AuthLogin) Submit(ctx context.Context, input string) error {
	if _, err := l.stdin.Write([]byte(input + "\n")); err != nil {
		return fmt.Errorf("submit amp login input: %w", err)
	}

	if err := l.closeStdin(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for amp login: %w", ctx.Err())
	case <-l.waitDone:
		if l.waitErr != nil || l.result.ExitCode != 0 || l.result.Signal != 0 || l.result.Revoked {
			return fmt.Errorf("amp login: %w", l.waitErr)
		}
	}

	return nil
}

// Settled reports whether the child has exited without blocking on it, which is
// how a login that completed on its own is discovered.
func (l *AuthLogin) Settled() (bool, error) {
	select {
	case <-l.waitDone:
		if l.waitErr != nil || l.result.ExitCode != 0 || l.result.Signal != 0 || l.result.Revoked {
			return true, fmt.Errorf("amp login: %w", l.waitErr)
		}

		return true, nil
	default:
		return false, nil
	}
}

func (l *AuthLogin) waitNative(ctx context.Context) {
	l.result, l.waitErr = l.process.Wait(ctx)

	l.waitErr = markDetachedWaitIncomplete(l.waitErr)
	if l.settle != nil && !errors.Is(l.waitErr, ErrContainmentIncomplete) {
		l.waitErr = errors.Join(l.waitErr, markDetachedWaitIncomplete(l.settle(ctx)))
	}

	close(l.waitDone)
}

func (l *AuthLogin) closeStdin() error {
	l.stdinOnce.Do(func() {
		if err := l.stdin.Close(); err != nil {
			l.stdinErr = fmt.Errorf("close amp login stdin: %w", err)
		}
	})

	return l.stdinErr
}

// Close terminates the login child through the selected containment boundary
// and releases every descriptor. An interrupted child's exit status is an
// expected outcome and is not reported as a failure.
func (l *AuthLogin) Close() error {
	l.closeOnce.Do(func() {
		_ = l.closeStdin()
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultCloseWait)
		l.closeErr = l.closeWithContext(closeCtx)

		cancel()
	})

	return l.closeErr
}

func (l *AuthLogin) closeWithContext(closeCtx context.Context) error {
	revokeErr := l.process.Revoke(closeCtx)

	var (
		waitErr error
		result  NativeResult
	)

	settled := false

	select {
	case <-l.waitDone:
		waitErr = l.waitErr
		result = l.result
		settled = true
	case <-closeCtx.Done():
		waitErr = errors.Join(closeCtx.Err(), ErrContainmentIncomplete)

		if l.waitStop != nil {
			l.waitStop()
			<-l.waitDone
		}
	}

	if settled && (result.Revoked || result.Signal != 0) && !errors.Is(waitErr, ErrContainmentIncomplete) {
		waitErr = nil
	}

	var cleanupErr error
	if l.cleanup != nil && !errors.Is(waitErr, ErrContainmentIncomplete) {
		cleanupErr = l.cleanup()
	}

	stdoutErr := l.stdout.Close()
	stderrErr := l.stderr.Close()

	joinAuthLoginWorker(l.stdoutDone)
	joinAuthLoginWorker(l.stderrDone)

	return errors.Join(
		revokeErr,
		waitErr,
		stdoutErr,
		stderrErr,
		cleanupErr,
	)
}
