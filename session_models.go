package ampacp

import (
	"context"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
)

const (
	// configMode is the one config option Amp advertises.
	configMode acp.SessionConfigId = "mode"
	// configModel is the model selector id; Amp advertises none and refuses it.
	configModel acp.SessionConfigId = "model"
)

const modeMedium = "medium"

func (s *session) configOptions() []acp.SessionConfigOption {
	s.mu.Lock()
	mode := s.options.Mode
	s.mu.Unlock()

	if mode == "" {
		return []acp.SessionConfigOption{}
	}

	modes := []string{"low", modeMedium, "high", "ultra"}
	if !slices.Contains(modes, mode) {
		modes = append(modes, mode)
	}

	values := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(modes))
	for _, value := range modes {
		values = append(values, acp.SessionConfigSelectOption{Value: acp.SessionConfigValueId(value), Name: value})
	}

	return []acp.SessionConfigOption{{Select: &acp.SessionConfigOptionSelect{Id: configMode, Name: "Mode", Type: "select", Category: new(acp.SessionConfigOptionCategoryMode), CurrentValue: acp.SessionConfigValueId(mode), Options: acp.SessionConfigSelectOptions{Ungrouped: &values}}}}
}

func (s *session) setConfigOption(ctx context.Context, id acp.SessionConfigId, value string) ([]acp.SessionConfigOption, error) {
	if err := s.admissionError(); err != nil {
		return nil, err
	}

	if id != configMode {
		return nil, wire.Unsupported("configId")
	}

	if strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return nil, wire.Unsupported("value")
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return nil, err
	}
	defer release()

	s.mu.Lock()
	old := s.options.Mode
	s.options.Mode = value
	s.mu.Unlock()

	if err := s.commitMirror(ctx); err != nil {
		s.mu.Lock()
		s.options.Mode = old
		s.mu.Unlock()

		return nil, wire.InternalFailure(vendor, "")
	}

	options := s.configOptions()
	if err := s.emit(ctx, acp.SessionUpdate{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: options}}); err != nil {
		return nil, err
	}

	return options, nil
}
