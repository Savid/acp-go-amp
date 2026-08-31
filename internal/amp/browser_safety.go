package amp

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
)

// ErrBrowserLaunchUnsupported reports a brokered login refused before any
// native child exists: the platform, or the resolved executable's own
// account-login shape, gives the launcher shim nothing it can prove.
var ErrBrowserLaunchUnsupported = errors.New("browser launch cannot be neutralised on this platform")

const (
	darwinPlatform              = "darwin"
	linuxPlatform               = "linux"
	windowsPlatform             = "windows"
	darwinDirectBrowserLauncher = "/usr/bin/open"
	authLoginInspectionBytes    = 64 * 1024
	authLoginInspectionOverlap  = 8 * 1024
)

var (
	authLoginFallback = []byte("If your browser does not open automatically")
	browserOpener     = regexp.MustCompile(`(?s)async function ([A-Za-z_$][A-Za-z0-9_$]*)\([^)]{1,128}\)\{try\{.{0,1000}?case"darwin":await ([A-Za-z_$][A-Za-z0-9_$]*)\("open",\[[^]]{1,128}\],[^)]{1,128}\);break;default:await ([A-Za-z_$][A-Za-z0-9_$]*)\("xdg-open",\[[^]]{1,128}\],[^)]{1,128}\);break`)
	// darwinDirectLauncherLiterals are the source-literal spellings through
	// which embedded application code could name the absolute Darwin launcher.
	// The bare NUL-delimited path also present in every bundle belongs to the
	// runtime's own string table — its editor-open feature — which no
	// account-login call reaches, so only a quoted literal is the
	// application's.
	darwinDirectLauncherLiterals = [][]byte{
		[]byte(`"` + darwinDirectBrowserLauncher + `"`),
		[]byte(`'` + darwinDirectBrowserLauncher + `'`),
		[]byte("`" + darwinDirectBrowserLauncher + "`"),
	}
	linuxDirectBrowserLaunchers = [][]byte{
		[]byte("/usr/bin/xdg-open"),
		[]byte("/usr/local/bin/xdg-open"),
		[]byte("/bin/xdg-open"),
		[]byte("/usr/bin/gio"),
		[]byte("/bin/gio"),
		[]byte("/usr/bin/sensible-browser"),
		[]byte("/usr/bin/x-www-browser"),
		[]byte("/usr/bin/www-browser"),
	}
)

type authLoginBinaryInspection struct {
	digest               [sha256.Size]byte
	darwinDirectLauncher bool
	linuxDirectLauncher  string
	accountLoginUsesPath bool
	browserOpenerFunc    string
	loginPrefixes        [][]byte
}

// CheckAuthLoginBrowserSafety inspects the installed Amp executable
// without running it. Darwin and Linux are each accepted only when the
// embedded account-login path calls the platform opener whose branch for that
// platform executes a bare PATH-resolved launcher — `open` on Darwin,
// `xdg-open` on Linux — and when no direct launcher for that platform exists:
// a quoted `/usr/bin/open` source literal on Darwin, any known absolute
// launcher path on Linux. Unknown shapes are fingerprinted and refused before
// command construction.
func CheckAuthLoginBrowserSafety(path string) error {
	return checkAuthLoginBrowserSafetyOnPlatform(runtime.GOOS, path)
}

func checkAuthLoginBrowserSafetyOnPlatform(platform string, path string) error {
	return checkAuthLoginBrowserSafety(platform, path, func(path string) (io.ReadCloser, error) {
		return os.Open(path) // #nosec G304 -- path is the already-resolved configured Amp executable.
	})
}

func checkAuthLoginBrowserSafety(goos string, path string, openFile func(string) (io.ReadCloser, error)) error {
	if goos != darwinPlatform && goos != linuxPlatform {
		return nil
	}

	file, err := openFile(path)
	if err != nil {
		return fmt.Errorf("%w: inspect Amp executable: %v", ErrBrowserLaunchUnsupported, err)
	}

	inspection, inspectErr := inspectAuthLoginBinary(file)

	closeErr := file.Close()

	if inspectErr != nil || closeErr != nil {
		return fmt.Errorf("%w: inspect Amp executable: %v", ErrBrowserLaunchUnsupported, errors.Join(inspectErr, closeErr))
	}

	if goos == darwinPlatform {
		if inspection.darwinDirectLauncher {
			return fmt.Errorf("%w: Amp executable sha256:%x names the direct Darwin browser launcher %s in its embedded source", ErrBrowserLaunchUnsupported, inspection.digest, darwinDirectBrowserLauncher)
		}

		if !inspection.accountLoginUsesPath {
			return fmt.Errorf("%w: Amp executable sha256:%x exposes no audited PATH-mediated Darwin account-login launcher", ErrBrowserLaunchUnsupported, inspection.digest)
		}

		return nil
	}

	if inspection.linuxDirectLauncher != "" {
		return fmt.Errorf("%w: Amp executable sha256:%x contains the direct Linux browser launcher %s", ErrBrowserLaunchUnsupported, inspection.digest, inspection.linuxDirectLauncher)
	}

	if !inspection.accountLoginUsesPath {
		return fmt.Errorf("%w: Amp executable sha256:%x exposes no audited PATH-mediated Linux account-login launcher", ErrBrowserLaunchUnsupported, inspection.digest)
	}

	return nil
}

func inspectAuthLoginBinary(reader io.Reader) (authLoginBinaryInspection, error) {
	hash := sha256.New()
	buffer := make([]byte, authLoginInspectionBytes)
	overlap := make([]byte, 0, authLoginInspectionOverlap)
	inspection := authLoginBinaryInspection{}

	for {
		read, err := reader.Read(buffer)
		if read > 0 {
			chunk := buffer[:read]
			_, _ = hash.Write(chunk)

			window := make([]byte, 0, len(overlap)+len(chunk))
			window = append(window, overlap...)
			window = append(window, chunk...)
			inspection.inspectWindow(window)

			keep := min(len(window), cap(overlap))
			overlap = append(overlap[:0], window[len(window)-keep:]...)
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return authLoginBinaryInspection{}, err
		}
	}

	copy(inspection.digest[:], hash.Sum(nil))
	inspection.linkAccountLogin()

	return inspection, nil
}

func (inspection *authLoginBinaryInspection) inspectWindow(window []byte) {
	if !inspection.darwinDirectLauncher {
		for _, literal := range darwinDirectLauncherLiterals {
			if bytes.Contains(window, literal) {
				inspection.darwinDirectLauncher = true

				break
			}
		}
	}

	if inspection.linuxDirectLauncher == "" {
		for _, launcher := range linuxDirectBrowserLaunchers {
			if bytes.Contains(window, launcher) {
				inspection.linuxDirectLauncher = string(launcher)

				break
			}
		}
	}

	if inspection.browserOpenerFunc == "" {
		match := browserOpener.FindSubmatch(window)
		if len(match) == 4 && bytes.Equal(match[2], match[3]) {
			inspection.browserOpenerFunc = string(match[1])
		}
	}

	for remaining := window; len(inspection.loginPrefixes) < 2; {
		marker := bytes.Index(remaining, authLoginFallback)
		if marker < 0 {
			break
		}

		start := max(0, marker-2048)
		prefix := append([]byte(nil), remaining[start:marker]...)
		inspection.loginPrefixes = append(inspection.loginPrefixes, prefix)
		remaining = remaining[marker+len(authLoginFallback):]
	}
}

// linkAccountLogin ties the account-login region to the audited opener: the
// same platform-switch function serves every platform's branch, so one link
// proves the Darwin and Linux launches alike.
func (inspection *authLoginBinaryInspection) linkAccountLogin() {
	if inspection.browserOpenerFunc == "" {
		return
	}

	call := []byte("try{await " + inspection.browserOpenerFunc + "(")
	for _, prefix := range inspection.loginPrefixes {
		if bytes.Contains(prefix, call) {
			inspection.accountLoginUsesPath = true

			return
		}
	}
}
