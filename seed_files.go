package ampacp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	// seedManifestName is the seed-owned ownership manifest, a JSON array of the
	// relative seed paths the wrapper has written into a seed root. It lets a
	// later seed pass tell a seed-managed file (safe to overwrite) apart from an
	// operator-authored file (must never be clobbered).
	seedManifestName = ".seed-manifest.json"
	// seedBackupSuffix names the sidecar that holds the prior on-disk bytes of a
	// managed seed target before the wrapper overwrites it with changed content.
	seedBackupSuffix = ".seed.bak"
)

// writeSeedFiles materializes WithSeedFiles contents under the session's
// resolved native root before any short-lived amp process launches.
//
// Anchor: the per-session isolated HOME (homeDir) that amp runs with, which is
// also the seed root. Each `amp threads` process is started with HOME set to
// this directory (see newAgentSession and internal/amp BuildEnv), so a seeded
// relative path is visible to amp as $HOME/<relpath>. The wrapper's own managed
// settings.json lives under XDG_CONFIG_HOME (configDir/amp/settings.json), a
// sibling of HOME rather than a child of it, so seed files can never overwrite
// the wrapper's required settings.
//
// This is the always-isolated case, not the direct-WithHome case: homeDir is a
// child of the dir returned by mkdirTemp(settingsParent(), ...). When WithHome
// is unset, settingsParent() is empty and mkdirTemp falls back to the OS temp
// dir, so amp never runs in the operator's real HOME or ~/.config/amp. Because
// the wrapper always creates a fresh isolated HOME per session regardless of
// WithHome, seeding needs no WithHome guard and cannot leak into a shared home.
//
// Provenance guard: writes are routed through an ownership manifest
// (.seed-manifest.json, kept in the seed root — $HOME, the sibling of the
// XDG settings.json) so a seed can never clobber a file the wrapper did not
// write. Per relpath: if the target is absent it is written and recorded; if it
// exists and the manifest already owns it, changed bytes are backed up to
// <relpath>.seed.bak before the rewrite (identical bytes are a no-op); if it
// exists but the manifest does not own it (an operator-authored file), the whole
// pass fails closed with the uniform unsupported error and nothing is written.
// The wrapper's fresh per-session HOME means the manifest is normally absent, so
// every seed is a first write and the guard is a no-op by design; it only bites
// if a caller aims two passes at a reused root.
func writeSeedFiles(root string, files map[string]string) error {
	if len(files) == 0 {
		return nil
	}

	keys := make([]string, 0, len(files))
	for key := range files {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	manifest, err := loadSeedManifest(root)
	if err != nil {
		return err
	}
	// Plan and guard every seed before writing anything so a fail-closed
	// rejection leaves all files on disk untouched.
	plans := make([]seedPlan, 0, len(keys))
	for _, key := range keys {
		target, manifestKey, err := resolveSeedPath(root, key)
		if err != nil {
			return err
		}

		current, err := readFile(target)

		exists := err == nil
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("read seed file: %w", err)
		}

		if exists && !manifest[manifestKey] {
			return unsupportedField(fmt.Sprintf("seedFiles[%q]", key))
		}

		plans = append(plans, seedPlan{manifestKey: manifestKey, target: target, current: current, contents: []byte(files[key]), exists: exists})
	}

	for _, plan := range plans {
		if err := applySeedPlan(plan); err != nil {
			return err
		}

		manifest[plan.manifestKey] = true
	}

	return writeSeedManifest(root, manifest)
}

// seedPlan captures a single seed target's guard decision made in the pre-write
// pass so the write pass never re-reads disk to decide backup vs. fresh write.
// manifestKey is the canonical slash-separated ownership key; contents is the
// seed body resolved from the original caller key.
type seedPlan struct {
	manifestKey string
	target      string
	current     []byte
	contents    []byte
	exists      bool
}

// applySeedPlan writes one planned seed. A managed target whose bytes are
// unchanged is a no-op; a managed target whose bytes differ is first backed up
// to <target>.seed.bak; an absent target has its parent directory created.
func applySeedPlan(plan seedPlan) error {
	if plan.exists {
		if bytes.Equal(plan.current, plan.contents) {
			return nil
		}

		if err := writeFile(plan.target+seedBackupSuffix, plan.current, 0o600); err != nil {
			return fmt.Errorf("back up seed file: %w", err)
		}
	} else {
		if err := mkdirAll(filepath.Dir(plan.target), 0o700); err != nil {
			return fmt.Errorf("create seed file parent: %w", err)
		}
	}

	if err := writeFile(plan.target, plan.contents, 0o600); err != nil {
		return fmt.Errorf("write seed file: %w", err)
	}

	return nil
}

// loadSeedManifest reads the ownership manifest from the seed root, returning an
// empty set when it is absent (the common fresh-per-session case).
func loadSeedManifest(root string) (map[string]bool, error) {
	data, err := readFile(filepath.Join(root, seedManifestName))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]bool{}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("read seed manifest: %w", err)
	}

	var paths []string
	if err := json.Unmarshal(data, &paths); err != nil {
		return nil, fmt.Errorf("parse seed manifest: %w", err)
	}

	managed := make(map[string]bool, len(paths))
	for _, path := range paths {
		managed[path] = true
	}

	return managed, nil
}

// writeSeedManifest persists the ownership manifest as a sorted, deterministic
// JSON array of managed relative paths.
func writeSeedManifest(root string, managed map[string]bool) error {
	paths := make([]string, 0, len(managed))
	for path := range managed {
		paths = append(paths, path)
	}

	sort.Strings(paths)

	data, _ := json.Marshal(paths)
	if err := writeFile(filepath.Join(root, seedManifestName), data, 0o600); err != nil {
		return fmt.Errorf("write seed manifest: %w", err)
	}

	return nil
}

// resolveSeedPath confines a seed key to root. Empty keys, absolute paths, and
// any `..` escape (including the root itself) fail closed with the uniform
// unsupported error so bad seeds are rejected at session start. It returns the
// absolute destination and the canonical slash-separated manifest key.
func resolveSeedPath(root, key string) (string, string, error) {
	field := fmt.Sprintf("seedFiles[%q]", key)
	if strings.TrimSpace(key) == "" {
		return "", "", unsupportedField(field)
	}

	if filepath.IsAbs(key) {
		return "", "", unsupportedField(field)
	}

	cleaned := filepath.Clean(key)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", "", unsupportedField(field)
	}

	return filepath.Join(root, cleaned), filepath.ToSlash(cleaned), nil
}
