package opencodeacp

import (
	"encoding/json"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

// negotiatedAnswer decodes the lifecycle answer the initialize response carries.
func negotiatedAnswer(t *testing.T, response acp.InitializeResponse) lifecycle.Negotiated {
	t.Helper()

	raw, err := json.Marshal(response.Meta[wire.LifecycleKey])
	require.NoError(t, err)

	var negotiated lifecycle.Negotiated
	require.NoError(t, json.Unmarshal(raw, &negotiated))

	return negotiated
}

// sessionFrames renders one session's recorded notifications as the payloads
// the host received, in delivery order.
func sessionFrames(t *testing.T, updates []acp.SessionNotification, sessionID acp.SessionId) []json.RawMessage {
	t.Helper()

	var frames []json.RawMessage

	for _, update := range updates {
		if update.SessionId != sessionID {
			continue
		}

		encoded, err := json.Marshal(update)
		require.NoError(t, err)
		frames = append(frames, encoded)
	}

	return frames
}

func idleTransitions(updates []acp.SessionNotification, sessionID acp.SessionId) int {
	count := 0

	for _, update := range updates {
		if update.SessionId != sessionID {
			continue
		}

		envelope, _ := update.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)

		if event["type"] == "state_update" && event["state"] == "idle" {
			count++
		}
	}

	return count
}

// TestOrdinaryContentAttributesToTheForeground proves the contract's
// attribution rule over a recorded stream: every content update arrives
// while the foreground is live, and no vendor namespace hints a turn or
// message.
func TestOrdinaryContentAttributesToTheForeground(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	negotiated := negotiatedAnswer(t, h.initialize(withLifecycle()))
	session := h.newSession()

	for n, text := range []string{"HELLO"} {
		_, err := h.prompt(session.SessionId, text, promptMeta(n+1))
		require.NoError(t, err)
	}

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		return idleTransitions(updates, session.SessionId) >= 1
	})

	updates := h.rec.snapshot()
	require.NotZero(t, contentUpdates(updates, session.SessionId), "the stream carried content to attribute")
	require.NoError(t, lifecycle.CheckAttribution(negotiated, sessionFrames(t, updates, session.SessionId)))
}

func contentUpdates(updates []acp.SessionNotification, sessionID acp.SessionId) int {
	count := 0

	for _, update := range updates {
		if update.SessionId == sessionID && (update.Update.AgentMessageChunk != nil || update.Update.ToolCall != nil) {
			count++
		}
	}

	return count
}
