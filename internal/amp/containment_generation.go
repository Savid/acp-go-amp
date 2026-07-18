package amp

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

const (
	DarwinRuntimeIDEnv   = "ACP_GO_AMP_RUNTIME_ID"
	DarwinScratchRootEnv = "ACP_GO_AMP_SCRATCH_ROOT"
)

type DarwinGeneration struct {
	RuntimeID      string
	ScratchRoot    string
	RecordStarted  func(pid, pgid int) error
	RecordFinished func(complete bool) error
	Release        func(complete bool) error

	finishOnce sync.Once
	finishErr  error
}

var (
	darwinGenerationAbs      = filepath.Abs
	darwinGenerationWalkDir  = filepath.WalkDir
	darwinGenerationRel      = filepath.Rel
	darwinGenerationReadFile = os.ReadFile
	darwinGenerationMkdirAll = os.MkdirAll
	darwinGenerationWrite    = os.WriteFile
)

func (g *DarwinGeneration) prepareCommand(cmd *exec.Cmd, writableRoot string) error {
	if g == nil || cmd == nil {
		return errors.New("darwin containment generation is unavailable")
	}

	home := filepath.Join(g.ScratchRoot, "home")
	config := filepath.Join(g.ScratchRoot, "xdg-config")
	cache := filepath.Join(g.ScratchRoot, "xdg-cache")
	data := filepath.Join(g.ScratchRoot, "xdg-data")

	state := filepath.Join(g.ScratchRoot, "xdg-state")
	for _, path := range []string{home, config, cache, data, state, filepath.Join(config, "amp")} {
		if err := darwinGenerationMkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create Darwin command root: %w", err)
		}
	}

	baseEnv := environmentMap(cmd.Env)

	if writableRoot != "" {
		root, err := darwinGenerationAbs(writableRoot)
		if err != nil {
			return fmt.Errorf("resolve Amp writable root: %w", err)
		}

		for source, destination := range map[string]string{
			baseEnv["HOME"]:            home,
			baseEnv["XDG_CONFIG_HOME"]: config,
		} {
			if source == "" || !pathWithin(root, source) {
				return errors.New("amp writable input is outside the wrapper-owned session root")
			}

			if err := copyRegularTree(source, destination); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}

	for index := 0; index+1 < len(cmd.Args); index++ {
		var destination string

		switch cmd.Args[index] {
		case ampArgSettingsFile:
			destination = filepath.Join(config, "amp", "settings.json")
		case ampArgMCPConfig:
			destination = filepath.Join(g.ScratchRoot, "mcp.json")
		default:
			continue
		}

		data, err := darwinGenerationReadFile(cmd.Args[index+1])
		if errors.Is(err, os.ErrNotExist) {
			cmd.Args[index+1] = destination

			continue
		}

		if err != nil {
			return fmt.Errorf("read Darwin command input: %w", err)
		}
		// #nosec G703 -- destination is wrapper-constructed inside the mode-0700 generation root.
		if err := darwinGenerationWrite(destination, data, 0o600); err != nil {
			return fmt.Errorf("write Darwin command input: %w", err)
		}

		cmd.Args[index+1] = destination
	}

	overrides := map[string]string{
		"HOME":               home,
		"XDG_CONFIG_HOME":    config,
		"XDG_CACHE_HOME":     cache,
		"XDG_DATA_HOME":      data,
		"XDG_STATE_HOME":     state,
		DarwinRuntimeIDEnv:   g.RuntimeID,
		DarwinScratchRootEnv: g.ScratchRoot,
	}
	seen := make(map[string]bool, len(overrides))

	for index, entry := range cmd.Env {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}

		if value, replace := overrides[key]; replace {
			cmd.Env[index] = key + "=" + value
			seen[key] = true
		}
	}

	for key, value := range overrides {
		if !seen[key] {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}

	return nil
}

func environmentMap(entries []string) map[string]string {
	values := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}

	return values
}

func pathWithin(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)

	return err == nil && relative != ".." && relative != "." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func copyRegularTree(source, destination string) error {
	return darwinGenerationWalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		if info.Mode()&os.ModeSymlink != 0 || (!info.Mode().IsDir() && !info.Mode().IsRegular()) {
			return fmt.Errorf("amp writable input contains unsupported file %s", path)
		}

		relative, err := darwinGenerationRel(source, path)
		if err != nil {
			return err
		}

		target := filepath.Join(destination, relative)
		if info.IsDir() {
			return darwinGenerationMkdirAll(target, 0o700)
		}
		// #nosec G122 -- source is inside a wrapper-owned mode-0700 root and every traversed entry is rejected unless regular.
		data, err := darwinGenerationReadFile(path)
		if err != nil {
			return err
		}

		if err := darwinGenerationMkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		// #nosec G703 -- target is derived from a validated relative path below the wrapper-created destination.
		if err := darwinGenerationWrite(target, data, 0o600); err != nil {
			return err
		}

		return nil
	})
}

func (g *DarwinGeneration) started(pid, pgid int) error {
	if g == nil || g.RecordStarted == nil {
		return nil
	}

	return g.RecordStarted(pid, pgid)
}

func (g *DarwinGeneration) finish(complete bool) error {
	if g == nil {
		return nil
	}

	g.finishOnce.Do(func() {
		if g.RecordFinished != nil {
			if err := g.RecordFinished(complete); err != nil {
				g.finishErr = fmt.Errorf("%w: update Darwin containment record: %v", ErrProcessContainmentIncomplete, err)

				return
			}
		}

		if g.Release != nil {
			g.finishErr = errors.Join(g.finishErr, g.Release(complete))
		}
	})

	return g.finishErr
}
