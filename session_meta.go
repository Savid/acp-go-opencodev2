package opencodeacp

import (
	"maps"
	"slices"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

const (
	metaOptionsKey       = "options"
	metaRawEventKey      = "rawEvent"
	metaModelKey         = "model"
	metaEnvKey           = "env"
	metaExtraPathDirsKey = "extraPathDirs"
	metaEffortKey        = "effort"
	metaEnabledKey       = "enabled"

	metaModeKey       = "mode"
	metaPermissionKey = "permission"
)

// OpenCodeOptions is the per-session options struct carried at _meta.opencode.options.
type OpenCodeOptions struct {
	// Mode selects a native agent.
	Mode string `json:"mode,omitempty"`
	// Permission selects ask, allow, or deny for native tools.
	Permission string `json:"permission,omitempty"`
	// Model selects the opencode model for this session as "provider/id".
	Model string `json:"model,omitempty"`
	// Env overlays the session's opencode process environment.
	Env map[string]string `json:"env,omitempty"`
	// ExtraPathDirs are absolute directories prepended, in order, to the PATH
	// of this session's opencode process.
	ExtraPathDirs []string `json:"extraPathDirs,omitempty"`
	// Effort is a reasoning-level value passed unchanged to opencode.
	Effort string `json:"effort,omitempty"`
}

// OpenCodeOption configures OpenCodeOptions values.
type OpenCodeOption func(*OpenCodeOptions)

// NewOpenCodeOptions constructs OpenCodeOptions from functional options.
func NewOpenCodeOptions(opts ...OpenCodeOption) OpenCodeOptions {
	options := OpenCodeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	return options.clone()
}

// WithOpenCodeModel configures the session model as "provider/id".
func WithOpenCodeModel(model string) OpenCodeOption {
	return func(options *OpenCodeOptions) { options.Model = model }
}

// WithOpenCodeEnv configures the session environment overlay.
func WithOpenCodeEnv(env map[string]string) OpenCodeOption {
	cloned := maps.Clone(env)

	return func(options *OpenCodeOptions) { options.Env = maps.Clone(cloned) }
}

// WithOpenCodeExtraPathDirs configures the directories prepended to the session PATH.
func WithOpenCodeExtraPathDirs(dirs ...string) OpenCodeOption {
	cloned := slices.Clone(dirs)

	return func(options *OpenCodeOptions) { options.ExtraPathDirs = slices.Clone(cloned) }
}

// WithOpenCodeEffort configures the reasoning level passed to opencode.
func WithOpenCodeEffort(level string) OpenCodeOption {
	return func(options *OpenCodeOptions) { options.Effort = level }
}

// Meta returns exactly {"opencode": {"options": {...}}} with the selected fields.
func (options OpenCodeOptions) Meta() map[string]any {
	values := map[string]any{}
	if options.Mode != "" {
		values[metaModeKey] = options.Mode
	}

	if options.Permission != "" {
		values[metaPermissionKey] = options.Permission
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

	if options.Effort != "" {
		values[metaEffortKey] = options.Effort
	}

	return map[string]any{vendor: map[string]any{metaOptionsKey: values}}
}

func (options OpenCodeOptions) clone() OpenCodeOptions {
	cloned := options
	cloned.Env = maps.Clone(options.Env)
	cloned.ExtraPathDirs = slices.Clone(options.ExtraPathDirs)

	return cloned
}

// ValidateOpenCodeSessionMeta runs the owned-namespace parsing of a session
// lifecycle request's _meta without an Agent and returns the same refusal.
func ValidateOpenCodeSessionMeta(meta map[string]any) error {
	_, err := parseSessionMeta(meta)
	if err != nil {
		return err
	}

	return nil
}

// sessionMeta is what one session lifecycle request's _meta.opencode carried.
type sessionMeta struct {
	options   OpenCodeOptions
	rawEvents bool
	// present records which carrier fields the request named, so a load or
	// resume inherits the stored value only for fields it left out.
	presentEnv           bool
	presentExtraPathDirs bool
}

// parseSessionMeta validates the owned _meta.opencode namespace of one session
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

	options, err := parseOpenCodeOptions(values)
	if err != nil {
		return sessionMeta{}, err
	}

	parsed.options = options
	_, parsed.presentEnv = values[metaEnvKey]
	_, parsed.presentExtraPathDirs = values[metaExtraPathDirsKey]

	return parsed, nil
}

func parseOpenCodeOptions(values map[string]any) (OpenCodeOptions, *acp.RequestError) {
	options := OpenCodeOptions{}

	for key, item := range values {
		switch key {
		case metaModeKey, metaPermissionKey:
			value, ok := item.(string)
			if !ok || value == "" {
				return OpenCodeOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			if key == metaModeKey {
				options.Mode = value
			} else {
				options.Permission = value
			}
		case metaModelKey:
			model, ok := item.(string)
			if !ok || model == "" {
				return OpenCodeOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.Model = model
		case metaEnvKey:
			env, err := wire.StringMapOption(item, wire.MetaOptionPath(vendor, key))
			if err != nil {
				return OpenCodeOptions{}, err
			}

			options.Env = env
		case metaExtraPathDirsKey:
			dirs, err := wire.StringSliceOption(item, wire.MetaOptionPath(vendor, key))
			if err != nil {
				return OpenCodeOptions{}, err
			}

			options.ExtraPathDirs = dirs
		case metaEffortKey:
			level, ok := item.(string)
			if !ok || level == "" {
				return OpenCodeOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
			}

			options.Effort = level
		default:
			return OpenCodeOptions{}, wire.Unsupported(wire.MetaOptionPath(vendor, key))
		}
	}

	return options, validateOpenCodeOptions(options)
}

func validateOpenCodeOptions(options OpenCodeOptions) *acp.RequestError {
	if options.Model != "" {
		if err := opencode.ModelSelectionShapeError(options.Model); err != nil {
			return wire.Unsupported(wire.MetaOptionPath(vendor, metaModelKey))
		}
	}

	if options.Permission != "" && !slices.Contains([]string{"ask", "allow", "deny"}, options.Permission) {
		return wire.Unsupported(wire.MetaOptionPath(vendor, metaPermissionKey))
	}

	return wire.ValidateSessionEnvironment(options.Env, options.ExtraPathDirs, wire.MetaOptionPath(vendor, ""))
}

// WithOpenCodeMode selects a native agent.
func WithOpenCodeMode(mode string) OpenCodeOption {
	return func(o *OpenCodeOptions) { o.Mode = mode }
}

// WithOpenCodePermission selects native tool permission behavior.
func WithOpenCodePermission(permission string) OpenCodeOption {
	return func(o *OpenCodeOptions) { o.Permission = permission }
}
