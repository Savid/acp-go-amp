package ampacp

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
)

// captureAmbientEnvironment is the seam the adapter's own environment is read
// through. Tests select a fixed environment rather than mutating the process's.
var captureAmbientEnvironment = os.Environ

// ambientEnvironmentEntries is the ordered block ordinary execution inherits
// from: the adapter's own process environment, or the one
// WithAmbientEnvironment supplied in its place, folded in sorted key order.
func ambientEnvironmentEntries(options Options) []string {
	if options.AmbientEnvironment == nil {
		return captureAmbientEnvironment()
	}

	keys := slices.Sorted(maps.Keys(options.AmbientEnvironment))
	entries := make([]string, 0, len(keys))

	for _, key := range keys {
		entries = append(entries, key+"="+options.AmbientEnvironment[key])
	}

	return entries
}

// validateAmbientEnvironment refuses a supplied block whose entries could not
// be environment entries at all. Which names the block then contributes is
// decided by the ordinary inheritance rules, never here.
func validateAmbientEnvironment(env map[string]string) error {
	for _, key := range slices.Sorted(maps.Keys(env)) {
		switch {
		case key == "" || strings.ContainsAny(key, "=\x00"):
			return fmt.Errorf("ambient environment key %q is not a variable name", key)
		case strings.ContainsRune(env[key], '\x00'):
			return fmt.Errorf("ambient environment value for %q contains NUL", key)
		}
	}

	return nil
}
