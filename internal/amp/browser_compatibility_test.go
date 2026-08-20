package amp

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/iotest"
)

type authLoginInspectionFile struct {
	io.Reader
	closeErr error
}

func (f authLoginInspectionFile) Close() error { return f.closeErr }

func TestAuthLoginBrowserCompatibilityIsPlatformBound(t *testing.T) {
	open := func(string) (io.ReadCloser, error) {
		t.Fatal("an uninspected platform consulted the executable")

		return nil, errors.New("unreachable")
	}
	if err := checkAuthLoginBrowserCompatibility("freebsd", "/amp", open); err != nil {
		t.Fatalf("FreeBSD compatibility = %v", err)
	}

	want := errors.New("unreadable")
	open = func(string) (io.ReadCloser, error) { return nil, want }
	if err := checkAuthLoginBrowserCompatibility(darwinPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("Darwin unreadable compatibility = %v", err)
	}

	// A quoted source literal is the application naming the absolute launcher
	// itself, and no audited opener outweighs that.
	open = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(compatibleAmpSource + ` await qj("/usr/bin/open",[T],t)`)), nil
	}
	if err := checkAuthLoginBrowserCompatibility(darwinPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), "direct Darwin browser launcher") {
		t.Fatalf("Darwin direct-launch compatibility = %v", err)
	}

	// The runtime's own NUL-delimited string table names the same path for its
	// editor feature in every bundle; that occurrence is not the application's
	// and does not refuse an otherwise audited build.
	open = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("editor\x00/usr/bin/open\x00--args\x00" + compatibleAmpSource)), nil
	}
	if err := checkAuthLoginBrowserCompatibility(darwinPlatform, "/amp", open); err != nil {
		t.Fatalf("Darwin runtime-table compatibility = %v", err)
	}

	open = func(string) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("unknown amp build")), nil }
	if err := checkAuthLoginBrowserCompatibility(darwinPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), "no audited PATH-mediated Darwin") {
		t.Fatalf("Darwin unknown compatibility = %v", err)
	}

	open = func(string) (io.ReadCloser, error) {
		return authLoginInspectionFile{Reader: strings.NewReader("unknown"), closeErr: want}, nil
	}
	if err := checkAuthLoginBrowserCompatibility(darwinPlatform, "/amp", open); !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("Darwin close compatibility = %v", err)
	}

	open = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(compatibleAmpSource)), nil
	}
	if err := checkAuthLoginBrowserCompatibility(darwinPlatform, "/amp", open); err != nil {
		t.Fatalf("Darwin PATH-mediated compatibility = %v", err)
	}
	if err := checkAuthLoginBrowserCompatibility(linuxPlatform, "/amp", open); err != nil {
		t.Fatalf("Linux PATH-mediated compatibility = %v", err)
	}

	open = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(compatibleAmpSource + "/usr/bin/xdg-open")), nil
	}
	if err := checkAuthLoginBrowserCompatibility(linuxPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), "direct Linux browser launcher") {
		t.Fatalf("Linux direct-launch compatibility = %v", err)
	}

	open = func(string) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("unknown amp build")), nil }
	if err := checkAuthLoginBrowserCompatibility(linuxPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), "no audited PATH-mediated Linux") {
		t.Fatalf("Linux unknown compatibility = %v", err)
	}
}

const compatibleAmpSource = `async function openURL(T,R){try{let platform=currentPlatform(),options={timeout:5000,signal:R};switch(platform){case"win32":await spawn("rundll32",["url.dll,FileProtocolHandler",T],options);break;case"darwin":await spawn("open",[T],options);break;default:await spawn("xdg-open",[T],options);break}}}async function accountLogin(T){let url=makeURL(T);try{await openURL(url,abort.signal)}catch(error){log(error)}write("If your browser does not open automatically")}`

func TestAuthLoginBinaryInspectionIsStreaming(t *testing.T) {
	contents := `prefix "` + darwinDirectBrowserLauncher + `" ` + compatibleAmpSource + "suffix"
	inspection, err := inspectAuthLoginBinary(iotest.OneByteReader(strings.NewReader(contents)))
	if err != nil || !inspection.darwinDirectLauncher || !inspection.accountLoginUsesPath || inspection.digest != sha256.Sum256([]byte(contents)) {
		t.Fatalf("streaming inspection = %#v/%v", inspection, err)
	}

	want := errors.New("read failed")
	if _, err := inspectAuthLoginBinary(iotest.ErrReader(want)); !errors.Is(err, want) {
		t.Fatalf("failed streaming inspection = %v, want %v", err, want)
	}
}

func TestCheckAuthLoginBrowserCompatibilityUsesTheHostPlatform(t *testing.T) {
	path := filepath.Join(t.TempDir(), "amp")
	if err := os.WriteFile(path, []byte("unknown amp build"), 0o700); err != nil {
		t.Fatal(err)
	}

	err := CheckAuthLoginBrowserCompatibility(path)
	if runtime.GOOS == darwinPlatform || runtime.GOOS == linuxPlatform {
		if !errors.Is(err, ErrBrowserLaunchUnsupported) {
			t.Fatalf("%s compatibility = %v", runtime.GOOS, err)
		}

		return
	}

	if err != nil {
		t.Fatalf("uninspected-platform compatibility = %v", err)
	}
}

func TestAuthLoginCompatibilityRefusalPrecedesCommandConstruction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "amp")
	if err := os.WriteFile(path, []byte("not executed"), 0o700); err != nil {
		t.Fatal(err)
	}

	original := commandContext
	commandContext = func(context.Context, string, ...string) *exec.Cmd {
		t.Fatal("a refused brokered login constructed a native command")

		return nil
	}
	t.Cleanup(func() { commandContext = original })

	want := errors.New("incompatible browser launch")
	client := newTestClient(t, nil, Options{CLIPath: path, Cwd: t.TempDir()})
	client.checkAuthLoginCompatibility = func(got string) error {
		if got != path {
			t.Fatalf("compatibility path = %q, want %q", got, path)
		}

		return want
	}

	if _, err := client.StartAuthLogin(t.Context()); !errors.Is(err, want) {
		t.Fatalf("StartAuthLogin = %v, want %v", err, want)
	}
}
