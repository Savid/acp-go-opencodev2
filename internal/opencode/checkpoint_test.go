package opencode

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func checkpointExport(t *testing.T, messages string) Export {
	t.Helper()
	var value Export
	require.NoError(t, json.Unmarshal([]byte(`{"info":{"id":"ses_root","time":{"created":1}},"messages":[`+messages+`],"extension":{"number":9007199254740993}}`), &value))

	return value
}

func TestExecutionPrefix(t *testing.T) {
	t.Parallel()
	const saved = `{"id":"msg_user","type":"user","text":"hello","unknown":9007199254740993}, {"id":"msg_done","type":"idle","outcome":"interrupted"}`
	value := checkpointExport(t, saved+`, {"id":"msg_later","type":"user","text":"next"}`)
	event := Event{ID: "evt_done", Type: "session.execution.interrupted", Data: json.RawMessage(`{"sessionID":"ses_root"}`)}
	prefix, err := value.ExecutionPrefix(event)
	require.NoError(t, err)
	require.Len(t, prefix.Messages, 2)
	require.Equal(t, value.Messages[0].Raw, prefix.Messages[0].Raw)
	require.Equal(t, value.Messages[1].Raw, prefix.Messages[1].Raw)
	require.Contains(t, string(prefix.Raw), `"unknown":9007199254740993`)
	require.Contains(t, string(prefix.Raw), `"extension":{"number":9007199254740993}`)
	require.NotContains(t, string(prefix.Raw), "msg_later")
	var decoded Export
	require.NoError(t, json.Unmarshal(prefix.Raw, &decoded))
	require.Len(t, decoded.Messages, 2)
	for _, invalid := range []Event{
		{ID: "evt_missing", Type: event.Type, Data: event.Data},
		{ID: event.ID, Type: "session.execution.succeeded", Data: event.Data},
		{ID: event.ID, Type: event.Type, Data: json.RawMessage(`{"sessionID":"ses_other"}`)},
		{ID: "done", Type: event.Type, Data: event.Data},
	} {
		_, err = value.ExecutionPrefix(invalid)
		require.Error(t, err)
	}
	child, err := value.CompletedPrefix()
	require.NoError(t, err)
	require.Equal(t, prefix.Raw, child.Raw)
	child, err = checkpointExport(t, `{"id":"msg_first","type":"user"}`).CompletedPrefix()
	require.NoError(t, err)
	require.Empty(t, child.Messages)
	require.Contains(t, string(child.Raw), `"messages":[]`)
}

func TestContainsHistory(t *testing.T) {
	t.Parallel()
	const user = `{"id":"msg_user","type":"user","text":"hello","unknown":9007199254740993}`
	const idle = `{"id":"msg_done","type":"idle","outcome":"interrupted"}`
	saved := checkpointExport(t, user+`,`+idle)
	for _, test := range []struct {
		name     string
		messages string
		want     bool
	}{
		{"unchanged", user + `,` + idle, true},
		{"successor", user + `,` + idle + `,{"id":"msg_next","type":"user"}`, true},
		{"completed shell", user + `,{"id":"msg_shell","type":"shell","status":"exited"},` + idle, true},
		{"completed assistant", user + `,{"id":"msg_assistant","type":"assistant","time":{"completed":2}},` + idle, true},
		{"completed compaction", user + `,{"id":"msg_compaction","type":"compaction","status":"completed"},` + idle, true},
		{"inserted user", user + `,{"id":"msg_other","type":"user"},` + idle, false},
		{"changed saved message", strings.Replace(user, "hello", "changed", 1) + `,` + idle, false},
		{"changed large number", strings.Replace(user, "9007199254740993", "9007199254740992", 1) + `,` + idle, false},
		{"reordered", idle + `,` + user, false},
		{"missing", user, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, checkpointExport(t, test.messages).ContainsHistory(saved))
		})
	}
	other := saved
	other.Info.ID = "ses_other"
	require.False(t, other.ContainsHistory(saved))
	other = saved
	other.Info.ParentID = "ses_parent"
	require.False(t, other.ContainsHistory(saved))
}
