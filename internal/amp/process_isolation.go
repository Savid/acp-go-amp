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
		if !ok {
			continue
		}

		key = launchEnvironmentKey(key)
		if key == "" || isPrivateAdapterEnv(key) || isScrubbedEnv(key) {
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

		canonicalKey := launchEnvironmentKey(key)
		if isPrivateAdapterEnv(canonicalKey) || isScrubbedEnv(canonicalKey) {
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

// ordinaryExecutableSearchRules carries the platform facts inherited-process
// executable lookup depends on. Keeping the rules as data lets every host
// exercise Windows lookup while the build-tagged selector still chooses the
// rules used by real launches.
type ordinaryExecutableSearchRules struct {
	pathSeparators     string
	extensions         []string
	requireExecuteBit  bool
	foldEnvironmentKey bool
}

func unixOrdinaryExecutableRules() ordinaryExecutableSearchRules {
	return ordinaryExecutableSearchRules{pathSeparators: string(os.PathSeparator), requireExecuteBit: true}
}

func windowsOrdinaryExecutableRules(environment []string) ordinaryExecutableSearchRules {
	return ordinaryExecutableSearchRules{
		pathSeparators:     `:\/`,
		extensions:         ordinaryWindowsExecutableExtensions(ordinaryEnvironmentValue(environment, "PATHEXT", true)),
		foldEnvironmentKey: true,
	}
}

func ordinaryEnvironmentValue(environment []string, name string, fold bool) string {
	value := ""

	for _, entry := range environment {
		key, candidate, ok := strings.Cut(entry, "=")
		if ok && (key == name || fold && strings.EqualFold(key, name)) {
			value = candidate
		}
	}

	return value
}

const defaultWindowsExecutableExtensions = ".com;.exe;.bat;.cmd"

func ordinaryWindowsExecutableExtensions(value string) []string {
	if value == "" {
		value = defaultWindowsExecutableExtensions
	}

	extensions := make([]string, 0, 4)

	for _, extension := range strings.Split(value, ";") {
		extension = strings.ToLower(strings.TrimSpace(extension))
		if extension == "" {
			continue
		}

		if extension[0] != '.' {
			extension = "." + extension
		}

		extensions = append(extensions, extension)
	}

	return extensions
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
	return lookPathInOrdinaryEnvironmentWithRules(file, environment, cwd, ordinaryExecutableRules(environment))
}

func lookPathInOrdinaryEnvironmentWithRules(
	file string,
	environment []string,
	cwd string,
	rules ordinaryExecutableSearchRules,
) (string, error) {
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

	if strings.ContainsAny(file, rules.pathSeparators) {
		return ordinaryExecutableFile(resolve(file), rules)
	}

	search := ordinaryEnvironmentValue(environment, "PATH", rules.foldEnvironmentKey)
	for _, dir := range filepath.SplitList(search) {
		if dir == "" {
			dir = "."
		}

		if path, err := ordinaryExecutableFile(filepath.Join(resolve(dir), file), rules); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in PATH", file)
}

func ordinaryExecutableFile(path string, rules ordinaryExecutableSearchRules) (string, error) {
	if len(rules.extensions) == 0 {
		return matchOrdinaryExecutableFile(path, rules.requireExecuteBit)
	}

	if filepath.Ext(path) != "" {
		if resolved, err := matchOrdinaryExecutableFile(path, rules.requireExecuteBit); err == nil {
			return resolved, nil
		}
	}

	for _, extension := range rules.extensions {
		if resolved, err := matchOrdinaryExecutableFile(path+extension, rules.requireExecuteBit); err == nil {
			return resolved, nil
		}
	}

	return "", fmt.Errorf("%q has no executable extension", path)
}

func matchOrdinaryExecutableFile(path string, requireExecuteBit bool) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	if !info.Mode().IsRegular() || requireExecuteBit && info.Mode()&0o111 == 0 {
		return "", fmt.Errorf("%q is not executable", path)
	}

	return path, nil
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
