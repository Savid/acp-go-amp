//go:build !windows

package amp

func launchEnvironmentKey(key string) string { return key }

func ordinaryExecutableRules([]string) ordinaryExecutableSearchRules {
	return unixOrdinaryExecutableRules()
}
