//go:build !linux

package amp

func validateStandaloneIdentityDispositionPlatform(*ProcessIsolation) error { return nil }
