package ampacp

import (
	"maps"
	"slices"
	"strings"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
)

const (
	metaOptionsKey       = "options"
	metaRawEventKey      = "rawEvent"
	metaModelKey         = "model"
	metaEnvKey           = "env"
	metaExtraPathDirsKey = "extraPathDirs"
	metaEnabledKey       = "enabled"
	metaModeKey          = "mode"
)

// AmpOptions is the per-session options struct carried at _meta.amp.options.
type AmpOptions struct {
	// Mode selects a native built-in or plugin mode.
	Mode string `json:"mode,omitempty"`
	// Model is unsupported by Amp; use Mode.
	Model string `json:"model,omitempty"`
	// Env overlays the session's amp process environment.
	Env map[string]string `json:"env,omitempty"`
	// ExtraPathDirs are absolute directories prepended, in order, to the PATH
	// of this session's amp process.
	ExtraPathDirs []string `json:"extraPathDirs,omitempty"`
}

// AmpOption configures AmpOptions values.
type AmpOption func(*AmpOptions)

// NewAmpOptions constructs AmpOptions from functional options.
func NewAmpOptions(opts ...AmpOption) AmpOptions {
	options := AmpOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	return options.clone()
}

// WithAmpModel sets the model field, which Amp refuses at session start: the
// native CLI selects models through its modes.
func WithAmpModel(model string) AmpOption {
	return func(options *AmpOptions) { options.Model = model }
}

// WithAmpEnv configures the session environment overlay.
func WithAmpEnv(env map[string]string) AmpOption {
	cloned := maps.Clone(env)

	return func(options *AmpOptions) { options.Env = maps.Clone(cloned) }
}

// WithAmpExtraPathDirs configures the directories prepended to the session PATH.
func WithAmpExtraPathDirs(dirs ...string) AmpOption {
	cloned := slices.Clone(dirs)

	return func(options *AmpOptions) { options.ExtraPathDirs = slices.Clone(cloned) }
}

// Meta returns exactly {"amp": {"options": {...}}} with the selected fields.
func (options AmpOptions) Meta() map[string]any {
	values := map[string]any{}
	if options.Mode != "" {
		values[metaModeKey] = options.Mode
	}

	if options.Model != "" {
		values[metaModelKey] = options.Model
	}

	if options.Env != nil {
		values[metaEnvKey] = maps.Clone(options.Env)
	}

	if options.ExtraPathDirs != nil {
		values[metaExtraPathDirsKey] = slices.Clone(options.ExtraPathDirs)
	}

	return map[string]any{vendor: map[string]any{metaOptionsKey: values}}
}

func (options AmpOptions) clone() AmpOptions {
	cloned := options
	cloned.Env = maps.Clone(options.Env)
	cloned.ExtraPathDirs = slices.Clone(options.ExtraPathDirs)

	return cloned
}

// ValidateAmpSessionMeta runs the owned-namespace parsing of a session
// lifecycle request's _meta without an Agent and returns the same refusal.
func ValidateAmpSessionMeta(meta map[string]any) error {
	_, err := parseSessionMeta(meta)
	if err != nil {
		return err
	}

	return nil
}

// sessionMeta is what one session lifecycle request's _meta.amp carried.
type sessionMeta struct {
	options   AmpOptions
	rawEvents bool
	// present records which carrier fields the request named, so a load or
	// resume inherits the stored value only for fields it left out.
	presentEnv           bool
	presentExtraPathDirs bool
}

// parseSessionMeta validates the owned _meta.amp namespace of one session
// lifecycle request. Unknown own-namespace keys fail closed; foreign
// namespaces are ignored; the lifecycle literal is refused by name.
func parseSessionMeta(meta map[string]any) (sessionMeta, *acp.RequestError) {
	if refusal := lifecycle.RejectKey(meta); refusal != nil {
		return sessionMeta{}, wire.ParamRefusal(refusal)
	}

	raw, exists := meta[vendor]
	if !exists {
		return sessionMeta{}, nil
	}

	vendorMeta, ok := raw.(map[string]any)
	if !ok {
		return sessionMeta{}, wire.Unsupported("_meta." + vendor)
	}

	parsed := sessionMeta{}

	for key := range vendorMeta {
		switch key {
		case metaOptionsKey, metaRawEventKey:
		default:
			return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + key)
		}
	}

	if rawEvent, ok := vendorMeta[metaRawEventKey]; ok {
		values, ok := rawEvent.(map[string]any)
		if !ok {
			return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + metaRawEventKey)
		}

		for key, item := range values {
			enabled, ok := item.(bool)
			if key != metaEnabledKey || !ok {
				return sessionMeta{}, wire.Unsupported("_meta." + vendor + "." + metaRawEventKey + "." + key)
			}

			parsed.rawEvents = enabled
		}
	}

	rawOptions, hasOptions := vendorMeta[metaOptionsKey]
	if !hasOptions {
		return parsed, nil
	}

	values, isObject := rawOptions.(map[string]any)
	if !isObject {
		return sessionMeta{}, wire.Unsupported(wire.MetaOptionPath(vendor, ""))
	}

	options, err := parseAmpOptions(values)
	if err != nil {
		return sessionMeta{}, err
	}

	parsed.options = options
	_, parsed.presentEnv = values[metaEnvKey]
	_, parsed.presentExtraPathDirs = values[metaExtraPathDirsKey]

	return parsed, nil
}

func parseAmpOptions(values map[string]any) (AmpOptions, *acp.RequestError) {
	options := AmpOptions{}

	for key, item := range values {
		switch key {
		case metaModeKey:
			value, ok := item.(string)
			if !ok || value == "" {
				return AmpOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.Mode = value
		case metaEnvKey:
			env, err := wire.StringMapOption(item, wire.MetaOptionPath(vendor, key))
			if err != nil {
				return AmpOptions{}, err
			}

			options.Env = env
		case metaExtraPathDirsKey:
			dirs, err := wire.StringSliceOption(item, wire.MetaOptionPath(vendor, key))
			if err != nil {
				return AmpOptions{}, err
			}

			options.ExtraPathDirs = dirs

		default:
			return AmpOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
		}
	}

	return options, validateAmpOptions(options)
}

func validateAmpOptions(options AmpOptions) *acp.RequestError {
	if strings.ContainsRune(options.Mode, '\x00') || (options.Mode != "" && strings.TrimSpace(options.Mode) == "") {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaModeKey))
	}

	return wire.ValidateSessionEnvironment(options.Env, options.ExtraPathDirs, wire.MetaOptionPath(vendor, ""))
}

// WithAmpMode selects a native built-in or plugin mode.
func WithAmpMode(mode string) AmpOption { return func(o *AmpOptions) { o.Mode = mode } }
