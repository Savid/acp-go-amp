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

var errBrowserShimUnsupported = errors.New("browser launch cannot be neutralised on this platform")

const (
	darwinPlatform              = "darwin"
	linuxPlatform               = "linux"
	windowsPlatform             = "windows"
	darwinDirectBrowserLauncher = "/usr/bin/open"
	authLoginInspectionBytes    = 64 * 1024
	authLoginInspectionOverlap  = 8 * 1024
)

var (
	linuxAuthLoginFallback      = []byte("If your browser does not open automatically")
	linuxBrowserOpener          = regexp.MustCompile(`(?s)async function ([A-Za-z_$][A-Za-z0-9_$]*)\([^)]{1,128}\)\{try\{.{0,1000}?case"darwin":await ([A-Za-z_$][A-Za-z0-9_$]*)\("open",\[[^]]{1,128}\],[^)]{1,128}\);break;default:await ([A-Za-z_$][A-Za-z0-9_$]*)\("xdg-open",\[[^]]{1,128}\],[^)]{1,128}\);break`)
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
	digest                     [sha256.Size]byte
	darwinDirectLauncher       bool
	linuxDirectLauncher        string
	linuxAccountLoginUsesPath  bool
	linuxBrowserOpenerFunction string
	linuxLoginPrefixes         [][]byte
}

// CheckAuthLoginBrowserCompatibility inspects the installed Amp executable
// without running it. Darwin account login has no headless contract that this
// adapter can prove, so every build is refused. Linux is accepted only when
// the embedded account-login path calls the platform opener whose Linux branch
// executes bare `xdg-open`, and when no known absolute Linux launcher exists.
// Unknown shapes are fingerprinted and refused before command construction.
func CheckAuthLoginBrowserCompatibility(path string) error {
	return checkAuthLoginBrowserCompatibilityOnPlatform(runtime.GOOS, path)
}

func checkAuthLoginBrowserCompatibilityOnPlatform(platform string, path string) error {
	return checkAuthLoginBrowserCompatibility(platform, path, func(path string) (io.ReadCloser, error) {
		return os.Open(path) // #nosec G304 -- path is the already-resolved configured Amp executable.
	})
}

func checkAuthLoginBrowserCompatibility(goos string, path string, openFile func(string) (io.ReadCloser, error)) error {
	if goos != darwinPlatform && goos != linuxPlatform {
		return nil
	}

	file, err := openFile(path)
	if err != nil {
		return fmt.Errorf("%w: inspect Amp executable: %v", errBrowserShimUnsupported, err)
	}

	inspection, inspectErr := inspectAuthLoginBinary(file)

	closeErr := file.Close()

	if inspectErr != nil || closeErr != nil {
		return fmt.Errorf("%w: inspect Amp executable: %v", errBrowserShimUnsupported, errors.Join(inspectErr, closeErr))
	}

	if goos == darwinPlatform {
		if inspection.darwinDirectLauncher {
			return fmt.Errorf("%w: Amp executable sha256:%x contains the direct Darwin browser launcher %s", errBrowserShimUnsupported, inspection.digest, darwinDirectBrowserLauncher)
		}

		return fmt.Errorf("%w: Amp executable sha256:%x exposes no audited headless account-login contract", errBrowserShimUnsupported, inspection.digest)
	}

	if inspection.linuxDirectLauncher != "" {
		return fmt.Errorf("%w: Amp executable sha256:%x contains the direct Linux browser launcher %s", errBrowserShimUnsupported, inspection.digest, inspection.linuxDirectLauncher)
	}

	if !inspection.linuxAccountLoginUsesPath {
		return fmt.Errorf("%w: Amp executable sha256:%x exposes no audited PATH-mediated Linux account-login launcher", errBrowserShimUnsupported, inspection.digest)
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
	inspection.linkLinuxAccountLogin()

	return inspection, nil
}

func (inspection *authLoginBinaryInspection) inspectWindow(window []byte) {
	inspection.darwinDirectLauncher = inspection.darwinDirectLauncher || bytes.Contains(window, []byte(darwinDirectBrowserLauncher))

	if inspection.linuxDirectLauncher == "" {
		for _, launcher := range linuxDirectBrowserLaunchers {
			if bytes.Contains(window, launcher) {
				inspection.linuxDirectLauncher = string(launcher)

				break
			}
		}
	}

	if inspection.linuxBrowserOpenerFunction == "" {
		match := linuxBrowserOpener.FindSubmatch(window)
		if len(match) == 4 && bytes.Equal(match[2], match[3]) {
			inspection.linuxBrowserOpenerFunction = string(match[1])
		}
	}

	for remaining := window; len(inspection.linuxLoginPrefixes) < 2; {
		marker := bytes.Index(remaining, linuxAuthLoginFallback)
		if marker < 0 {
			break
		}

		start := max(0, marker-2048)
		prefix := append([]byte(nil), remaining[start:marker]...)
		inspection.linuxLoginPrefixes = append(inspection.linuxLoginPrefixes, prefix)
		remaining = remaining[marker+len(linuxAuthLoginFallback):]
	}
}

func (inspection *authLoginBinaryInspection) linkLinuxAccountLogin() {
	if inspection.linuxBrowserOpenerFunction == "" {
		return
	}

	call := []byte("try{await " + inspection.linuxBrowserOpenerFunction + "(")
	for _, prefix := range inspection.linuxLoginPrefixes {
		if bytes.Contains(prefix, call) {
			inspection.linuxAccountLoginUsesPath = true

			return
		}
	}
}
