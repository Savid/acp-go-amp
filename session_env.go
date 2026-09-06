package ampacp

import (
	"maps"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-amp/internal/amp"
)

const (
	ampEnvOptionPath          = "_meta.amp.options." + optionEnvKey
	ampModelOptionPath        = "_meta.amp.options." + optionModelKey
	ampOutputSchemaOptionPath = "_meta.amp.options." + metaOutputSchemaKey

	envNodeOptionsKey = "NODE_OPTIONS"
	envBashEnvKey     = "BASH_ENV"
	envShellEnvKey    = "ENV"
)

// canonicalEnvKey is the environment-variable identity the target platform
// actually resolves names by. Windows matches variable names without regard to
// case, so one uppercase spelling stands for every equal-fold key; every other
// platform treats the exact bytes as the name.
func canonicalEnvKey(key string) string {
	if runtimeGOOS == platformWindows {
		return strings.ToUpper(key)
	}

	return key
}

// composeEnv applies environment phases left to right under the platform key
// identity, so agent base, session overrides, and adapter-managed values form
// one deterministic later-phase-wins chain that carries exactly one entry per
// variable. Keys inside a phase are applied in sorted order, so a map that
// reached here without validation still composes deterministically instead of
// depending on Go map iteration order.
func composeEnv(phases ...map[string]string) map[string]string {
	out := map[string]string{}

	for _, phase := range phases {
		for _, key := range slices.Sorted(maps.Keys(phase)) {
			out[canonicalEnvKey(key)] = phase[key]
		}
	}

	return out
}

// managedSessionEnv is the adapter-owned residence phase. It is applied after
// every caller-supplied phase, so the isolated home and XDG roots a session
// runs in cannot be redirected by an agent or session environment value under
// any spelling.
func managedSessionEnv(home, config, cache, data, state string) map[string]string {
	return map[string]string{
		envHome:          home,
		envXDGConfigHome: config,
		envXDGCacheHome:  cache,
		envXDGDataHome:   data,
		envXDGStateHome:  state,
	}
}

// managedSessionEnvKey reports whether key names an adapter-owned residence
// variable under the target platform's environment identity. Callers cannot
// persist these names: the session residence is rebuilt for each live wrapper,
// so accepting one would write a manifest that recovery must reject.
func managedSessionEnvKey(key string) bool {
	canonical := canonicalEnvKey(key)
	for managed := range managedSessionEnv("", "", "", "", "") {
		if canonicalEnvKey(managed) == canonical {
			return true
		}
	}

	return false
}

// operationEnvNames are the only session-supplied values that reach a child
// which is not a prompt. A session's raw PATH is its prompt carrier and nothing
// else; the credential and the deployment URL are what an authenticated
// one-shot operation — the startup method probes, thread export, thread delete,
// account login — genuinely needs, so they are named here rather than reaching
// those children as part of one undifferentiated session environment.
func operationEnvNames() []string {
	return []string{amp.AuthAPIKeyEnv, amp.AuthDeploymentEnv}
}

// operationSessionEnv is the explicit operation-value phase lifted out of a
// session environment. Keys are read under the platform identity and applied in
// sorted order, so a raw caller map yields the same phase a composed one does.
func operationSessionEnv(env map[string]string) map[string]string {
	wanted := make(map[string]struct{}, len(operationEnvNames()))
	for _, name := range operationEnvNames() {
		wanted[canonicalEnvKey(name)] = struct{}{}
	}

	out := map[string]string{}

	for _, key := range slices.Sorted(maps.Keys(env)) {
		canonical := canonicalEnvKey(key)
		if _, ok := wanted[canonical]; ok {
			out[canonical] = env[key]
		}
	}

	return out
}

func validStoredSessionEnv(env map[string]string) bool {
	if env == nil || validateEnvironment(env) != nil {
		return false
	}

	for key := range env {
		if managedSessionEnvKey(key) {
			return false
		}
	}

	return true
}

// invalidEnvName reports a key that cannot be delivered as an environment
// variable name at all. The native boundary refuses such a key on every phase,
// so the public surface names it while the caller can still act on it.
func invalidEnvName(key string) bool {
	return key == "" || strings.ContainsRune(key, '=') || strings.IndexByte(key, 0) >= 0
}

// ambiguousEnvKeys reports two spellings of one platform environment variable
// that a single caller-supplied map names at once. A Go map carries no order,
// so the value such a map would deliver to the child is unknowable; the
// request is refused rather than resolved by chance.
func ambiguousEnvKeys(env map[string]string) (string, string) {
	seen := make(map[string]string, len(env))

	for _, key := range slices.Sorted(maps.Keys(env)) {
		canonical := canonicalEnvKey(key)
		if previous, ok := seen[canonical]; ok {
			return previous, key
		}

		seen[canonical] = key
	}

	return "", ""
}

// blockedAgentEnvKey reports whether a caller-supplied env key names a
// variable the adapter refuses on every surface: its private namespace under
// every spelling, and the loader, node, and shell injection names under the
// platform identity. PATH is absent on both surfaces: the agent-scoped
// environment establishes the base search path and a session's complete raw
// PATH is amp's one session carrier.
func blockedAgentEnvKey(key string) bool {
	if strings.HasPrefix(strings.ToUpper(key), privateEnvPrefix) {
		return true
	}

	switch name := canonicalEnvKey(key); name {
	case envNodeOptionsKey, envBashEnvKey, envShellEnvKey:
		return true
	default:
		return strings.HasPrefix(name, "LD_") || strings.HasPrefix(name, "DYLD_")
	}
}

// blockedSessionEnvKey additionally refuses the managed residence roots. They
// are rebuilt for each live wrapper, so a session value naming one would write
// a manifest that recovery must reject.
func blockedSessionEnvKey(key string) bool {
	return blockedAgentEnvKey(key) || managedSessionEnvKey(key)
}

// validateSessionEnv checks a session environment in sorted key order, so the
// first refusal is the same on every call. A key that cannot be a variable
// name, a value carrying a NUL, and a blocked name each fail as unsupported at
// the key exactly as the host sent it. Two keys that name one variable under
// the platform identity fail as ambiguous at the later key: a Go map carries
// no order, so the value such a map would deliver is unknowable.
func validateSessionEnv(env map[string]string, path string) error {
	seen := make(map[string]struct{}, len(env))

	for _, key := range slices.Sorted(maps.Keys(env)) {
		if invalidEnvName(key) || strings.IndexByte(env[key], 0) >= 0 || blockedSessionEnvKey(key) {
			return unsupportedField(path + "." + key)
		}

		identity := canonicalEnvKey(key)
		if _, duplicate := seen[identity]; duplicate {
			return ambiguousField(path + "." + key)
		}

		seen[identity] = struct{}{}
	}

	return nil
}

func ambiguousField(path string) error {
	return acp.NewInvalidParams(map[string]any{jsonFieldError: valAmbiguous, jsonFieldField: path})
}

// ValidateAmpSessionMeta reports the refusal a session/new, session/load, or
// session/resume request carrying meta receives from this package's _meta.amp
// parsing, or nil when the vendor namespace is accepted. Refusals that depend
// on how an Agent was constructed are not part of it.
func ValidateAmpSessionMeta(meta map[string]any) error {
	parsed, err := parseSessionMeta(meta)
	if err != nil {
		return err
	}

	return validateAmpSessionOptions(parsed.options)
}
