package opencodeacp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSessionEnvironmentAndPathReachTheHarness(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	dir := t.TempDir()
	session := h.newSession(WithSessionOpenCodeOptions(NewOpenCodeOptions(
		WithOpenCodeEnv(map[string]string{"CUSTOM": "one"}),
		WithOpenCodeExtraPathDirs(dir),
	)))

	_, err := h.prompt(session.SessionId, "ENV", nil)
	require.NoError(t, err)

	var carrier struct {
		Env           map[string]string `json:"env"`
		ExtraPathDirs []string          `json:"extraPathDirs"`
	}
	require.NoError(t, json.Unmarshal([]byte(agentText(h.rec.snapshot())), &carrier))
	require.Equal(t, map[string]string{"CUSTOM": "one"}, carrier.Env)
	require.Equal(t, []string{dir}, carrier.ExtraPathDirs)
}

func TestSelectedAgentAndEffortReachTheHarness(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	session := h.newSession(WithSessionOpenCodeOptions(NewOpenCodeOptions(
		WithOpenCodeMode("plan"), WithOpenCodeEffort("high"),
	)))

	_, err := h.prompt(session.SessionId, "AGENT", nil)
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "agent:plan variant:high")
}
