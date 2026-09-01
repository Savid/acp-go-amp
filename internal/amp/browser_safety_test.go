package amp

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
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

func TestAuthLoginBrowserSafetyIsPlatformBound(t *testing.T) {
	open := func(string) (io.ReadCloser, error) {
		t.Fatal("an uninspected platform consulted the executable")

		return nil, errors.New("unreachable")
	}
	if err := checkAuthLoginBrowserSafety("freebsd", "/amp", open); err != nil {
		t.Fatalf("FreeBSD safety audit = %v", err)
	}

	want := errors.New("unreadable")
	open = func(string) (io.ReadCloser, error) { return nil, want }
	if err := checkAuthLoginBrowserSafety(darwinPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("Darwin unreadable safety audit = %v", err)
	}

	// A quoted source literal is the application naming the absolute launcher
	// itself, and no audited opener outweighs that.
	open = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(auditedAmpSource + ` await qj("/usr/bin/open",[T],t)`)), nil
	}
	if err := checkAuthLoginBrowserSafety(darwinPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), "direct Darwin browser launcher") {
		t.Fatalf("Darwin direct-launch safety audit = %v", err)
	}

	// The runtime's own NUL-delimited string table names the same path for its
	// editor feature in every bundle; that occurrence is not the application's
	// and does not refuse an otherwise audited build.
	open = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("editor\x00/usr/bin/open\x00--args\x00" + auditedAmpSource)), nil
	}
	if err := checkAuthLoginBrowserSafety(darwinPlatform, "/amp", open); err != nil {
		t.Fatalf("Darwin runtime-table safety audit = %v", err)
	}

	open = func(string) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("unknown amp build")), nil }
	if err := checkAuthLoginBrowserSafety(darwinPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), "no audited PATH-mediated Darwin") {
		t.Fatalf("Darwin unknown safety audit = %v", err)
	}

	open = func(string) (io.ReadCloser, error) {
		return authLoginInspectionFile{Reader: strings.NewReader("unknown"), closeErr: want}, nil
	}
	if err := checkAuthLoginBrowserSafety(darwinPlatform, "/amp", open); !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("Darwin close safety audit = %v", err)
	}

	open = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(auditedAmpSource)), nil
	}
	if err := checkAuthLoginBrowserSafety(darwinPlatform, "/amp", open); err != nil {
		t.Fatalf("Darwin PATH-mediated safety audit = %v", err)
	}
	if err := checkAuthLoginBrowserSafety(linuxPlatform, "/amp", open); err != nil {
		t.Fatalf("Linux PATH-mediated safety audit = %v", err)
	}

	open = func(string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(auditedAmpSource + "/usr/bin/xdg-open")), nil
	}
	if err := checkAuthLoginBrowserSafety(linuxPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), "direct Linux browser launcher") {
		t.Fatalf("Linux direct-launch safety audit = %v", err)
	}

	open = func(string) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("unknown amp build")), nil }
	if err := checkAuthLoginBrowserSafety(linuxPlatform, "/amp", open); !errors.Is(err, ErrBrowserLaunchUnsupported) || !strings.Contains(err.Error(), "no audited PATH-mediated Linux") {
		t.Fatalf("Linux unknown safety audit = %v", err)
	}
}

const auditedAmpSource = `async function openURL(T,R){try{let platform=currentPlatform(),options={timeout:5000,signal:R};switch(platform){case"win32":await spawn("rundll32",["url.dll,FileProtocolHandler",T],options);break;case"darwin":await spawn("open",[T],options);break;default:await spawn("xdg-open",[T],options);break}}}async function accountLogin(T){let url=makeURL(T);try{await openURL(url,abort.signal)}catch(error){log(error)}write("If your browser does not open automatically")}`

func TestAuthLoginBinaryInspectionIsStreaming(t *testing.T) {
	contents := `prefix "` + darwinDirectBrowserLauncher + `" ` + auditedAmpSource + "suffix"
	inspection, err := inspectAuthLoginBinary(iotest.OneByteReader(strings.NewReader(contents)))
	if err != nil || !inspection.darwinDirectLauncher || !inspection.accountLoginUsesPath || inspection.digest != sha256.Sum256([]byte(contents)) {
		t.Fatalf("streaming inspection = %#v/%v", inspection, err)
	}

	want := errors.New("read failed")
	if _, err := inspectAuthLoginBinary(iotest.ErrReader(want)); !errors.Is(err, want) {
		t.Fatalf("failed streaming inspection = %v, want %v", err, want)
	}
}

func TestCheckAuthLoginBrowserSafetyUsesTheHostPlatform(t *testing.T) {
	path := filepath.Join(t.TempDir(), "amp")
	if err := os.WriteFile(path, []byte("unknown amp build"), 0o700); err != nil {
		t.Fatal(err)
	}

	err := CheckAuthLoginBrowserSafety(path)
	if runtime.GOOS == darwinPlatform || runtime.GOOS == linuxPlatform {
		if !errors.Is(err, ErrBrowserLaunchUnsupported) {
			t.Fatalf("%s safety audit = %v", runtime.GOOS, err)
		}

		return
	}

	if err != nil {
		t.Fatalf("uninspected-platform safety audit = %v", err)
	}
}

func TestAuthLoginSafetyRefusalPrecedesCommandConstruction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "amp")
	if err := os.WriteFile(path, []byte("not executed"), 0o700); err != nil {
		t.Fatal(err)
	}

	for _, managed := range []bool{false, true} {
		t.Run(fmt.Sprintf("managed=%t", managed), func(t *testing.T) {
			want := errors.New("unsafe browser launch")
			options := Options{CLIPath: path, Cwd: t.TempDir()}
			if managed {
				options.StartNative = func(context.Context, NativeRequest) (NativeProcess, error) {
					t.Fatal("managed login started before its safety audit")

					return nil, errors.New("unreachable")
				}
			}

			client := newTestClient(t, nil, options)
			client.checkAuthLoginSafety = func(got string) error {
				if got != path {
					t.Fatalf("safety-audit path = %q, want %q", got, path)
				}

				return want
			}

			if _, err := client.StartAuthLogin(t.Context()); !errors.Is(err, want) {
				t.Fatalf("StartAuthLogin = %v, want %v", err, want)
			}
		})
	}
}
