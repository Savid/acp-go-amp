//go:build !windows

package amp

func ordinaryExecutableRules([]string) ordinaryExecutableSearchRules {
	return unixOrdinaryExecutableRules()
}

func launchEnvironmentKey(key string) string { return key }
