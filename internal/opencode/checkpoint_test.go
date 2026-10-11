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
	require.NoError(t, json.Unmarshal([]byte(`{"info":{"id":"ses_root","time":{"created":1}},"messages":[`+messages+`]}`), &value))

	return value
}

func TestExecutionPrefix(t *testing.T) {
	t.Parallel()
	const saved = `{"id":"msg_user","type":"user","text":"hello","unknown":9007199254740993}, {"id":"msg_done","type":"idle","outcome":"interrupted"}`
	value := checkpointExport(t, saved+`, {"id":"msg_later","type":"user","text":"next"}`)
	event := Event{ID: "evt_done", Type: "session.execution.interrupted", SessionID: "ses_root"}
	prefix, err := value.ExecutionPrefix(event)
	require.NoError(t, err)
	require.Len(t, prefix.Messages, 2)
	require.JSONEq(t, `{"id":"msg_user","type":"user","text":"hello","unknown":9007199254740993}`, string(prefix.Messages[0].Raw))
	require.Contains(t, string(prefix.Messages[0].Raw), `"unknown":9007199254740993`)
	require.Equal(t, value.Messages[1].Raw, prefix.Messages[1].Raw)
	encoded, err := json.Marshal(prefix)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "msg_later")
	var decoded Export
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.Messages, 2)
	require.Equal(t, "ses_root", decoded.Info.ID)
	for _, invalid := range []Event{
		{ID: "evt_missing", Type: event.Type, SessionID: event.SessionID},
		{ID: event.ID, Type: "session.execution.succeeded", SessionID: event.SessionID},
		{ID: event.ID, Type: event.Type, SessionID: "ses_other"},
		{ID: "done", Type: event.Type, SessionID: event.SessionID},
	} {
		_, err = value.ExecutionPrefix(invalid)
		require.Error(t, err)
	}
	_, err = value.ExecutionPrefix(Event{ID: "evt_missing", Type: event.Type, SessionID: event.SessionID})
	require.ErrorIs(t, err, ErrMarkerMissing)
	require.Equal(t, prefix.Messages, value.CompletedPrefix().Messages)
	child := checkpointExport(t, `{"id":"msg_first","type":"user"}`).CompletedPrefix()
	require.Empty(t, child.Messages)
	encoded, err = json.Marshal(child)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"messages":[]`)
}

func TestDecodeEvent(t *testing.T) {
	t.Parallel()
	frame := []byte(`{"id":"evt_1","type":"form.created","data":{"form":{"sessionID":"ses_form"}}}`)
	event, err := DecodeEvent(frame)
	require.NoError(t, err)
	require.Equal(t, "ses_form", event.SessionID)
	require.Equal(t, frame, []byte(event.Raw))
	event, err = DecodeEvent([]byte(`{"id":"evt_2","type":"session.text.delta","data":{"sessionID":"ses_text"}}`))
	require.NoError(t, err)
	require.Equal(t, "ses_text", event.SessionID)
	_, err = DecodeEvent([]byte(`{`))
	require.Error(t, err)
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
