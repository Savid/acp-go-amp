package ampacp

import (
	"maps"
	"slices"
	"strings"

	"github.com/savid/acp-go-amp/internal/amp"
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

	for key := range managedSessionEnv("", "", "", "", "") {
		if _, exists := env[key]; exists {
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
