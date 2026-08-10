package amp

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

type ProcessIdentityLockCapability interface {
	Duplicate() (*os.File, error)
}

type ProcessIsolation struct {
	UID             uint32
	GID             uint32
	BaseEnvironment map[string]string
	// Implicit marks the ordinary current-identity launch policy captured when
	// no explicit isolation is configured: UID and GID name the identity the
	// process already runs as, BaseEnvironment is the sanitized ambient capture,
	// and no credential is ever applied. Explicit policies never set it.
	Implicit                 bool
	TestOnlyNoCredential     bool
	TestOnlyIdentityLockRoot string
	IdentityLock             ProcessIdentityLockCapability `json:"-"`
	AuthorityDomain          ProcessIdentityLockCapability `json:"-"`
	StandaloneOwnerID        string                        `json:"standaloneOwnerId"`
	StandaloneStateRoot      string                        `json:"standaloneStateRoot"`
}

const (
	envIsolationUID       = adapterPrivateEnvPrefix + "ISOLATION_UID"
	envIsolationGID       = adapterPrivateEnvPrefix + "ISOLATION_GID"
	envIsolationTest      = adapterPrivateEnvPrefix + "ISOLATION_TEST_ONLY"
	envIsolationImplicit  = adapterPrivateEnvPrefix + "ISOLATION_IMPLICIT"
	envValueTrue          = "true"
	processIsolationLinux = "linux"
)

var (
	processIsolationGOOS = runtime.GOOS

	implicitIsolationEnviron = os.Environ
	implicitIsolationUID     = os.Geteuid
	implicitIsolationGID     = os.Getegid
)

// ImplicitProcessIsolation captures the ordinary current-identity launch
// policy used when the embedder configures no explicit isolation: the identity
// this process already runs as — root or not — and a sanitized snapshot of the
// ambient environment. The capture happens once per call, so every launch built
// from one result sees the same base environment regardless of later ambient
// mutation.
func ImplicitProcessIsolation() *ProcessIsolation {
	base := map[string]string{}

	for _, entry := range implicitIsolationEnviron() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" || isPrivateAdapterEnv(key) || isScrubbedEnv(key) {
			continue
		}

		base[key] = value
	}

	return &ProcessIsolation{
		UID:             implicitIdentityValue(implicitIsolationUID()),
		GID:             implicitIdentityValue(implicitIsolationGID()),
		BaseEnvironment: base,
		Implicit:        true,
	}
}

// implicitIdentityValue maps an effective id onto the 32 bits an isolation
// policy stores. A platform that reports no id (-1) fails closed onto an id no
// launch validation can match.
func implicitIdentityValue(id int) uint32 {
	if id < 0 || id > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(id)
}

// sharedIdentitySupervisorRemedy states what an operator can change when the
// supervisor was asked to launch the native process under the very identity it
// already runs as and the shape it was handed describes something else. There
// is no privilege boundary to cross in that deployment, so the two answers are
// to give the supervisor one, or to describe the launch as what it is.
const sharedIdentitySupervisorRemedy = "run the supervisor as root to isolate the agent identity, " +
	"or launch the agent under the identity the supervisor already holds"

func validateProcessIsolation(isolation *ProcessIsolation) error {
	if isolation == nil {
		return errors.New("process isolation policy is required")
	}

	if isolation.Implicit {
		if isolation.IdentityLock != nil || isolation.AuthorityDomain != nil ||
			isolation.StandaloneOwnerID != "" || isolation.StandaloneStateRoot != "" {
			return errors.New("implicit current-identity launch forbids identity capabilities and standalone owner fields")
		}
	} else if isolation.UID == 0 || isolation.GID == 0 {
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
			name != envIsolationTest && name != envIsolationImplicit {
			env = append(env, entry)
		}
	}

	return append(env,
		adapterSupervisorModeEnv+"="+mode,
		envIsolationUID+"="+strconv.FormatUint(uint64(isolation.UID), 10),
		envIsolationGID+"="+strconv.FormatUint(uint64(isolation.GID), 10),
		envIsolationTest+"="+strconv.FormatBool(isolation.TestOnlyNoCredential),
		envIsolationImplicit+"="+strconv.FormatBool(isolation.Implicit),
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

	// An implicit launch never changed credentials, so the proof is that the
	// bootstrap still runs as the captured identity; the ambient supplementary
	// groups belong to that identity and are not a failure.
	if os.Getenv(envIsolationImplicit) == envValueTrue {
		return validateProcessIsolation(&ProcessIsolation{
			UID: uint32(uid), GID: uint32(gid), BaseEnvironment: map[string]string{}, Implicit: true,
		})
	}

	return verifyProcessIsolation(&ProcessIsolation{UID: uint32(uid), GID: uint32(gid), BaseEnvironment: map[string]string{}})
}
