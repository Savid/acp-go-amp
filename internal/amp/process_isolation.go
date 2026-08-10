package amp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type ProcessIdentityLockCapability interface {
	Duplicate() (*os.File, error)
}

type ProcessIsolation struct {
	UID                      uint32
	GID                      uint32
	BaseEnvironment          map[string]string
	TestOnlyNoCredential     bool
	TestOnlyIdentityLockRoot string
	IdentityLock             ProcessIdentityLockCapability `json:"-"`
	AuthorityDomain          ProcessIdentityLockCapability `json:"-"`
	StandaloneOwnerID        string                        `json:"standaloneOwnerId"`
	StandaloneStateRoot      string                        `json:"standaloneStateRoot"`
}

const (
	envIsolationUID  = adapterPrivateEnvPrefix + "ISOLATION_UID"
	envIsolationGID  = adapterPrivateEnvPrefix + "ISOLATION_GID"
	envIsolationTest = adapterPrivateEnvPrefix + "ISOLATION_TEST_ONLY"
	envValueTrue     = "true"
)

var (
	ordinaryEnvironmentEntries = os.Environ
	ordinaryEnvironmentGetwd   = os.Getwd
)

// CaptureOrdinaryEnvironment returns the portable current-process environment
// used by ordinary launches after removing adapter-private and unsafe values.
func CaptureOrdinaryEnvironment() map[string]string {
	base := map[string]string{}

	for _, entry := range ordinaryEnvironmentEntries() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" || isPrivateAdapterEnv(key) || isScrubbedEnv(key) {
			continue
		}

		base[key] = value
	}

	return base
}

func validateProcessIsolation(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation policy is required")
	}

	if isolation.UID == 0 || isolation.GID == 0 {
		return errors.New("process isolation UID and GID must be nonzero")
	}

	if isolation.BaseEnvironment == nil {
		return errors.New("process isolation base environment is required")
	}

	for key := range isolation.BaseEnvironment {
		if key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0 {
			return fmt.Errorf("process isolation base environment contains invalid key %q", key)
		}

		if isPrivateAdapterEnv(key) || isScrubbedEnv(key) {
			return fmt.Errorf("process isolation base environment contains forbidden key %q", key)
		}
	}

	return validateProcessIsolationPlatform(isolation)
}

func environmentValue(env []string, name string) string {
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key == name {
			return value
		}
	}

	return ""
}

func lookPathInEnvironment(file string, environment []string) (string, error) {
	if file == "" {
		return "", errors.New("executable name is empty")
	}

	if strings.ContainsRune(file, os.PathSeparator) {
		if !filepath.IsAbs(file) {
			return "", fmt.Errorf("executable path %q is not absolute", file)
		}

		return executableFile(file)
	}

	search := environmentValue(environment, "PATH")
	if search == "" {
		return "", fmt.Errorf("executable %q cannot be resolved without policy PATH", file)
	}

	for _, dir := range filepath.SplitList(search) {
		if !filepath.IsAbs(dir) {
			return "", fmt.Errorf("policy PATH entry %q is not absolute", dir)
		}

		if path, err := executableFile(filepath.Join(dir, file)); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in policy PATH", file)
}

func lookPathInOrdinaryEnvironment(file string, environment []string, cwd string) (string, error) {
	if file == "" {
		return "", errors.New("executable name is empty")
	}

	if cwd == "" {
		var err error

		cwd, err = ordinaryEnvironmentGetwd()
		if err != nil {
			return "", fmt.Errorf("get working directory: %w", err)
		}
	}

	resolve := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}

		return filepath.Join(cwd, path)
	}

	if strings.ContainsRune(file, os.PathSeparator) {
		return executableFile(resolve(file))
	}

	for _, dir := range filepath.SplitList(environmentValue(environment, "PATH")) {
		if dir == "" {
			dir = "."
		}

		if path, err := executableFile(filepath.Join(resolve(dir), file)); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in PATH", file)
}

func executableFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return "", fmt.Errorf("%q is not executable", path)
	}

	return path, nil
}

func supervisorEnvironment(native []string, isolation *ProcessIsolation, mode string) ([]string, error) {
	if err := validateProcessIsolation(isolation); err != nil {
		return nil, err
	}

	env := make([]string, 0, len(native)+5)
	for _, entry := range native {
		name, _, ok := strings.Cut(entry, "=")
		if ok && name != adapterSupervisorModeEnv && name != envIsolationUID && name != envIsolationGID &&
			name != envIsolationTest {
			env = append(env, entry)
		}
	}

	return append(env,
		adapterSupervisorModeEnv+"="+mode,
		envIsolationUID+"="+strconv.FormatUint(uint64(isolation.UID), 10),
		envIsolationGID+"="+strconv.FormatUint(uint64(isolation.GID), 10),
		envIsolationTest+"="+strconv.FormatBool(isolation.TestOnlyNoCredential),
	), nil
}

func verifyInheritedProcessIsolation() error {
	uid, uidErr := strconv.ParseUint(os.Getenv(envIsolationUID), 10, 32)
	gid, gidErr := strconv.ParseUint(os.Getenv(envIsolationGID), 10, 32)

	if uidErr != nil || gidErr != nil {
		return errors.New("process isolation bootstrap identity is invalid")
	}

	if os.Getenv(envIsolationTest) == envValueTrue {
		return nil
	}

	return verifyProcessIsolation(&ProcessIsolation{UID: uint32(uid), GID: uint32(gid), BaseEnvironment: map[string]string{}})
}
