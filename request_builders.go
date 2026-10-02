package opencodeacp

import (
	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

// WithSessionOpenCodeOptions merges opencode-specific options into _meta.opencode.options.
func WithSessionOpenCodeOptions(options OpenCodeOptions) wire.SessionRequestOption {
	return wire.WithSessionMetaValue(options.Meta())
}

// WithSessionRawEvents toggles raw opencode event emission for the session.
func WithSessionRawEvents(enabled bool) wire.SessionRequestOption {
	return wire.WithSessionMetaValue(map[string]any{
		vendor: map[string]any{metaRawEventKey: map[string]any{metaEnabledKey: enabled}},
	})
}

// SetModelRequest constructs a model selector update as "provider/id".
func SetModelRequest(sessionID acp.SessionId, model string) acp.SetSessionConfigOptionRequest {
	return wire.SetConfigOptionRequest(sessionID, configModel, acp.SessionConfigValueId(model))
}
