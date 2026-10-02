package opencodeacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateOpenCodeSessionMeta(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateOpenCodeSessionMeta(nil))
	require.NoError(t, ValidateOpenCodeSessionMeta(NewOpenCodeOptions(WithOpenCodeEnv(map[string]string{"A": "1"})).Meta()))
	require.Error(t, ValidateOpenCodeSessionMeta(map[string]any{"opencode": "x"}))
}
