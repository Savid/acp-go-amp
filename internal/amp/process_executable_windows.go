//go:build windows

package amp

import "strings"

func ordinaryExecutableRules(environment []string) ordinaryExecutableSearchRules {
	return windowsOrdinaryExecutableRules(environment)
}

func launchEnvironmentKey(key string) string { return strings.ToUpper(key) }
