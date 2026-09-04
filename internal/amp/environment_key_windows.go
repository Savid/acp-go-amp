//go:build windows

package amp

import "strings"

func launchEnvironmentKey(key string) string { return strings.ToUpper(key) }

func ordinaryExecutableRules(environment []string) ordinaryExecutableSearchRules {
	return windowsOrdinaryExecutableRules(environment)
}
