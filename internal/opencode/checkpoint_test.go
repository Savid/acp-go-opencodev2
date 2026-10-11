package opencode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestExecutionMarker(t *testing.T) {
	t.Parallel()
	id, outcome, err := ExecutionMarker(Event{ID: "evt_done", Type: "session.execution.interrupted", SessionID: "ses_root"})
	require.NoError(t, err)
	require.Equal(t, "msg_done", id)
	require.Equal(t, "interrupted", outcome)
	for _, invalid := range []Event{
		{ID: "evt_done", Type: "session.execution.started", SessionID: "ses_root"},
		{ID: "evt_done", Type: "session.execution.succeeded"},
		{ID: "done", Type: "session.execution.succeeded", SessionID: "ses_root"},
	} {
		_, _, err = ExecutionMarker(invalid)
		require.Error(t, err)
	}
}

func TestStreamExport(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "missing") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"type":"not_found","message":"gone"}`))

			return
		}
		_, _ = w.Write([]byte(`{"data":{"extra":[1,{"a":2}],"messages":[{"id":"msg_a","type":"user","n":9007199254740993},{"id":"msg_b","type":"idle"}],"info":{"id":"ses_root"}},"tail":true}`))
	}))
	defer server.Close()
	client := NewClient()
	client.URL = server.URL
	var info json.RawMessage
	var messages []string
	err := client.StreamExport(t.Context(), "ses_root", func(raw json.RawMessage) error {
		info = raw

		return nil
	}, func(raw []byte) error {
		messages = append(messages, string(raw))

		return nil
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"ses_root"}`, string(info))
	require.Equal(t, []string{`{"id":"msg_a","type":"user","n":9007199254740993}`, `{"id":"msg_b","type":"idle"}`}, messages)
	err = client.StreamExport(t.Context(), "missing", func(json.RawMessage) error { return nil }, func([]byte) error { return nil })
	require.True(t, IsMissing(err))
}

func TestDecodeEvent(t *testing.T) {
	t.Parallel()
	frame := []byte(`{"id":"evt_1","type":"form.created","created":7,"data":{"form":{"sessionID":"ses_form"}}}`)
	event, err := DecodeEvent(frame)
	require.NoError(t, err)
	require.Equal(t, "ses_form", event.SessionID)
	require.Equal(t, "evt_1", event.ID)
	require.Equal(t, "form.created", event.Type)
	require.EqualValues(t, 7, event.Created)
	require.Equal(t, frame, []byte(event.Raw))
	require.JSONEq(t, `{"form":{"sessionID":"ses_form"}}`, string(event.Data))
	require.Same(t, &frame[len(frame)-len(event.Data)-1], &event.Data[0], "the data is a slice of the frame, not a copy")
	_, err = DecodeEvent([]byte(`{"id":"evt_1","type":"x","data":{}} {}`))
	require.Error(t, err)
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
