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
	"os/exec"
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
	authOpenPipe  = os.Pipe
	errAuthNoURL  = errors.New("amp login printed no authorization URL")
	errAuthSecret = errors.New("amp account secret is not a non-empty string")
)

// AuthSettingsDocument returns the settings body every session writes.
func AuthSettingsDocument() []byte {
	return authSettingsDocument
}

// AuthFileStoreAsserted reports whether the settings file still resolves the
// native-secrets flag to false. An absent, malformed, or true value is not an
// assertion, and the caller fails closed on it rather than reading a store it
// cannot prove is authoritative.
func AuthFileStoreAsserted(settingsFile string) (bool, error) {
	contents, err := authReadFile(settingsFile)
	if err != nil {
		return false, fmt.Errorf("read amp settings file: %w", err)
	}

	var settings map[string]json.RawMessage
	if err := json.Unmarshal(contents, &settings); err != nil {
		return false, fmt.Errorf("decode amp settings file: %w", err)
	}

	raw, ok := settings[authNativeSecretsSetting]
	if !ok {
		return false, nil
	}

	var enabled bool
	if err := json.Unmarshal(raw, &enabled); err != nil {
		return false, nil //nolint:nilerr // a non-boolean value is not an assertion, and this reports assertion rather than decode success.
	}

	return !enabled, nil
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
// pinned fact on this surface was measured against. Unsupported platforms and
// unmeasured deployments are refused before a login child exists.
func (c *Client) AuthDeploymentSupported() bool {
	platform := runtime.GOOS
	if c.options.TestOnlyAuthLoginPlatform != "" {
		platform = c.options.TestOnlyAuthLoginPlatform
	}

	if platform == darwinPlatform || platform == windowsPlatform {
		return false
	}

	environment, err := authLoginEnv(c.options.Isolation, c.options.OrdinaryEnvironment, c.options.Env, c.options.Cwd)
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
	tree     *processTree
	stdin    io.WriteCloser
	stdout   io.ReadCloser
	stderr   io.ReadCloser
	wait     *commandWait
	url      chan string
	dataHome string
	shim     *browserShim

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
// AMP_HEADLESS_OAUTH affects MCP OAuth, not this account-login path. Linux runs
// behind a browser-launcher shim; platforms without a provable interception
// boundary fail before the login child is constructed.
func (c *Client) StartAuthLogin(ctx context.Context) (*AuthLogin, error) {
	environment, err := authLoginEnv(c.options.Isolation, c.options.OrdinaryEnvironment, c.options.Env, c.options.Cwd)
	if err != nil {
		return nil, err
	}

	path, err := c.resolveExecutable(ctx, c.options.Cwd)
	if err != nil {
		return nil, err
	}

	if compatibilityErr := c.checkAuthLoginCompatibility(path); compatibilityErr != nil {
		return nil, fmt.Errorf("amp login: %w", compatibilityErr)
	}

	cmd := commandContext(context.Background(), path, c.authLoginArgs()...)

	cmd.Dir = c.options.Cwd
	if cmd.Dir == "" {
		cmd.Dir, err = getwd()
		if err != nil {
			return nil, fmt.Errorf("get working directory: %w", err)
		}
	}

	shim, err := newBrowserShim(c.options.ScratchParent)
	if err != nil {
		return nil, fmt.Errorf("amp login: %w", err)
	}

	if handoffErr := handoffGeneratedNativeTree(shim.dir, c.options.Isolation); handoffErr != nil {
		return nil, errors.Join(fmt.Errorf("amp login browser shim: %w", handoffErr), shim.remove())
	}

	cmd.Env = shim.environ(environment)

	launch, err := c.prepareProcessLaunch(ctx, cmd)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("amp login: %w", err), shim.remove())
	}

	// The residence is the data home the containment boundary settled on, not
	// the one this package asked for: a boundary that redirects the child's
	// roots rewrites it. The launch publishes that environment, so the harvest
	// depends on a stated hand-off rather than on cmd still being the object the
	// boundary mutated.
	dataHome := environmentMap(launch.nativeEnv)[dataHomeEnv]

	pipes, err := newAuthLoginPipes()
	if err != nil {
		return nil, errors.Join(err, launch.close(), shim.remove())
	}

	cmd = launch.cmd
	cmd.Stdin = pipes.stdinReader
	cmd.Stdout = pipes.stdoutWriter
	cmd.Stderr = pipes.stderrWriter
	cmd.WaitDelay = defaultCloseKillAfter

	tree, err := startProcessTree(launch)
	if err != nil {
		pipes.closeAll()

		return nil, errors.Join(fmt.Errorf("start amp login: %w", err), shim.remove())
	}

	pipes.closeChildSide()

	login := &AuthLogin{
		tree:     tree,
		stdin:    pipes.stdin,
		stdout:   pipes.stdout,
		stderr:   pipes.stderr,
		wait:     tree.commandWait(),
		url:      make(chan string, 1),
		dataHome: dataHome,
		shim:     shim,
		onPanic:  c.options.OnGoroutinePanic,
	}

	go login.readStdout(ctx)
	go login.drainStderr(ctx)

	return login, nil
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
func authLoginEnv(isolation *ProcessIsolation, ordinary, base map[string]string, cwd string) ([]string, error) {
	managed := map[string]string{authHeadlessOAuthEnv: "1"}

	var (
		env []string
		err error
	)

	if isolation != nil {
		env, err = buildIsolatedEnvironment(isolation, cwd, base, managed)
	} else {
		env, err = buildEnvironment(cwd, ordinary, base, managed)
	}

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

type authLoginPipes struct {
	stdinReader  *os.File
	stdin        *os.File
	stdout       *os.File
	stdoutWriter *os.File
	stderr       *os.File
	stderrWriter *os.File
}

func newAuthLoginPipes() (*authLoginPipes, error) {
	pipes := &authLoginPipes{}

	stdinReader, stdin, err := authOpenPipe()
	if err != nil {
		return nil, fmt.Errorf("create amp login stdin: %w", err)
	}

	pipes.stdinReader, pipes.stdin = stdinReader, stdin

	stdout, stdoutWriter, err := authOpenPipe()
	if err != nil {
		pipes.closeAll()

		return nil, fmt.Errorf("create amp login stdout: %w", err)
	}

	pipes.stdout, pipes.stdoutWriter = stdout, stdoutWriter

	stderr, stderrWriter, err := authOpenPipe()
	if err != nil {
		pipes.closeAll()

		return nil, fmt.Errorf("create amp login stderr: %w", err)
	}

	pipes.stderr, pipes.stderrWriter = stderr, stderrWriter

	return pipes, nil
}

func (p *authLoginPipes) closeChildSide() {
	for _, file := range []*os.File{p.stdinReader, p.stdoutWriter, p.stderrWriter} {
		if file != nil {
			_ = file.Close()
		}
	}
}

func (p *authLoginPipes) closeAll() {
	p.closeChildSide()

	for _, file := range []*os.File{p.stdin, p.stdout, p.stderr} {
		if file != nil {
			_ = file.Close()
		}
	}
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
	defer l.recoverGoroutine(ctx, "amp login stderr drain")

	_, _ = io.Copy(io.Discard, l.stderr)
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

	waitErr, completed := l.wait.await(ctx)
	if !completed {
		return fmt.Errorf("wait for amp login: %w", waitErr)
	}

	if waitErr != nil {
		return fmt.Errorf("amp login: %w", waitErr)
	}

	return nil
}

// Settled reports whether the child has exited without blocking on it, which is
// how a login that completed on its own is discovered.
func (l *AuthLogin) Settled() (bool, error) {
	select {
	case <-l.wait.done:
		if l.wait.err != nil {
			return true, fmt.Errorf("amp login: %w", l.wait.err)
		}

		return true, nil
	default:
		return false, nil
	}
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
		killErr := l.tree.kill()
		containmentErr := processTreeTerminateAndWait(l.tree, defaultCloseWait)

		if ProcessContainmentComplete(containmentErr) {
			waitCtx, cancel := context.WithTimeout(context.Background(), commandWaitTimeout)
			if _, completed := l.wait.await(waitCtx); !completed {
				containmentErr = errors.Join(containmentErr,
					fmt.Errorf("%w: wait for amp login close", ErrProcessContainmentIncomplete))
			}

			cancel()
		}

		l.closeErr = errors.Join(
			authExpected(killErr),
			containmentErr,
			l.closeStdin(),
			l.stdout.Close(),
			l.stderr.Close(),
			l.shim.remove(),
		)
	})

	return l.closeErr
}

// authExpected drops the error a kill against an already-exited child returns.
func authExpected(err error) error {
	if err == nil || errors.Is(err, os.ErrProcessDone) || errors.Is(err, exec.ErrNotFound) {
		return nil
	}

	return err
}
