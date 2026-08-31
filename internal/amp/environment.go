package amp

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	ordinaryEnvironmentEntries = os.Environ
	ordinaryEnvironmentGetwd   = os.Getwd
)

func CaptureOrdinaryEnvironment() map[string]string {
	base := map[string]string{}

	for _, entry := range ordinaryEnvironmentEntries() {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}

		key = launchEnvironmentKey(key)
		if key != "" && !isPrivateAdapterEnv(key) && !isScrubbedEnv(key) {
			base[key] = value
		}
	}

	return base
}

func environmentMap(entries []string) map[string]string {
	values := make(map[string]string, len(entries))
	for _, entry := range entries {
		if key, value, ok := strings.Cut(entry, "="); ok {
			values[key] = value
		}
	}

	return values
}

func environmentValue(environment []string, name string) string {
	return ordinaryEnvironmentValue(environment, name, false)
}

type ordinaryExecutableSearchRules struct {
	pathSeparators     string
	extensions         []string
	requireExecuteBit  bool
	foldEnvironmentKey bool
}

func unixOrdinaryExecutableRules() ordinaryExecutableSearchRules {
	return ordinaryExecutableSearchRules{pathSeparators: string(os.PathSeparator), requireExecuteBit: true}
}

func windowsOrdinaryExecutableRules(environment []string) ordinaryExecutableSearchRules { //nolint:unused // Used by the Windows build-tagged selector.
	return ordinaryExecutableSearchRules{pathSeparators: `:\/`, extensions: ordinaryWindowsExecutableExtensions(ordinaryEnvironmentValue(environment, "PATHEXT", true)), foldEnvironmentKey: true}
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

const defaultWindowsExecutableExtensions = ".com;.exe;.bat;.cmd" //nolint:unused // Used by the Windows build-tagged selector.

func ordinaryWindowsExecutableExtensions(value string) []string { //nolint:unused // Used by the Windows build-tagged selector.
	if value == "" {
		value = defaultWindowsExecutableExtensions
	}

	var extensions []string

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

		return matchOrdinaryExecutableFile(file, true)
	}

	search := environmentValue(environment, "PATH")
	if search == "" {
		return "", fmt.Errorf("executable %q cannot be resolved without PATH", file)
	}

	for _, dir := range filepath.SplitList(search) {
		if path, err := matchOrdinaryExecutableFile(filepath.Join(dir, file), true); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("executable %q not found in PATH", file)
}

func lookPathInOrdinaryEnvironment(file string, environment []string, cwd string) (string, error) {
	return lookPathInOrdinaryEnvironmentWithRules(file, environment, cwd, ordinaryExecutableRules(environment))
}

func lookPathInOrdinaryEnvironmentWithRules(file string, environment []string, cwd string, rules ordinaryExecutableSearchRules) (string, error) {
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

	for _, dir := range filepath.SplitList(ordinaryEnvironmentValue(environment, "PATH", rules.foldEnvironmentKey)) {
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
