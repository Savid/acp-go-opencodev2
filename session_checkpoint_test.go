package opencodeacp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
	"github.com/stretchr/testify/require"
)

func TestExecutionSnapshotCutsAtMarker(t *testing.T) {
	for _, mode := range []string{"growing", "changing tail", "changing child", "completed child", "missing marker", "wrong outcome", "export failure", "active child", "delivered inbox", "held marker"} {
		t.Run(mode, func(t *testing.T) {
			a, s, _ := newDirectSession(t)
			response, err := a.Prompt(t.Context(), wire.TextPromptRequest(s.id, "HELLO"))
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			rt := s.binding
			native := *rt.client
			exported, err := native.Export(t.Context(), s.nativeID)
			require.NoError(t, err)
			var record sessionRecord
			before, _, err := sessionlog.Load(t.Context(), a.store, string(s.id), &record)
			require.NoError(t, err)
			marker := exported.Messages[len(exported.Messages)-1]
			require.Equal(t, "idle", marker.Type)
			terminal := opencode.Event{ID: strings.Replace(marker.ID, "msg_", "evt_", 1), Type: "session.execution.succeeded", SessionID: s.nativeID}
			if mode == "active child" {
				child := createNativeChild(t, rt, s.nativeID, s.cwd)
				require.NoError(t, native.Do(t.Context(), s.cwd, http.MethodPost, opencode.SessionPath(child.ID)+"/prompt", map[string]any{"id": opencode.NewMessageID(), "text": "SLOW"}, nil))
				defer func() { _ = native.Interrupt(t.Context(), child.ID) }()
			}
			target, err := url.Parse(native.URL)
			require.NoError(t, err)
			forward := httputil.NewSingleHostReverseProxy(target)
			childID := ""
			if mode == "changing child" || mode == "completed child" {
				child := createNativeChild(t, rt, s.nativeID, s.cwd)
				childID = child.ID
			}
			var reads atomic.Int32
			var inboxRead atomic.Bool
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "delivered inbox" && strings.HasSuffix(r.URL.Path, "/inbox") && inboxRead.CompareAndSwap(false, true) {
					fakeData(w, []map[string]any{{"id": "msg_delivered", "sessionID": s.nativeID, "type": "synthetic", "payload": map[string]any{"text": "delivered"}, "delivery": "queue", "time": map[string]any{"created": 1}}})

					return
				}
				if mode == "active child" || !strings.HasSuffix(r.URL.Path, "/export") {
					forward.ServeHTTP(w, r)

					return
				}
				count := int(reads.Add(1))
				if childID != "" && strings.Contains(r.URL.Path, childID) {
					messages := []opencode.NativeMessage{{ID: "msg_child_live", Type: roleAssistant, Text: fmt.Sprintf("child-%d", count)}}
					if mode == "completed child" {
						messages = append([]opencode.NativeMessage{{ID: "msg_child_saved", Type: roleAssistant, Text: "saved"}, {ID: "msg_child_idle", Type: "idle", Outcome: "succeeded"}}, messages...)
					}
					fakeData(w, fakeExport{Info: opencode.NativeSession{ID: childID, ParentID: s.nativeID}, Messages: messages})

					return
				}
				if mode == "export failure" {
					http.Error(w, "export unavailable", http.StatusServiceUnavailable)

					return
				}
				fakeData(w, json.RawMessage(checkpointExport(mode, count, exported)))
			}))
			defer proxy.Close()
			rt.client.URL = proxy.URL
			if mode != "held marker" {
				// Read the whole history again, as the first commit of a mirror does.
				s.mirror.Reset()
			}
			err = s.commitMirror(t.Context(), rt, &terminal)
			rt.client.URL = native.URL
			rows, _, loadErr := sessionlog.Load(t.Context(), a.store, string(s.id), &record)
			require.NoError(t, loadErr)
			if mode == "missing marker" || mode == "wrong outcome" || mode == "export failure" {
				require.Error(t, err)
				require.Equal(t, before, rows, "an unread history must not replace the durable generation")
				if mode != "export failure" {
					require.ErrorContains(t, err, "native execution marker missing")
					require.EqualValues(t, markerAttempts, reads.Load())
				}

				return
			}
			require.NoError(t, err)
			messages := storedMessages(t, rows, record, s.nativeID)
			require.Len(t, messages, len(exported.Messages))
			require.Equal(t, marker.ID, messages[len(messages)-1].ID)
			if mode == "delivered inbox" {
				require.Len(t, record.PendingInputs[s.nativeID], 1)
			} else {
				require.Empty(t, record.PendingInputs)
			}
			if mode == "completed child" {
				require.Len(t, storedMessages(t, rows, record, childID), 2)
			}
			if mode == "changing child" {
				require.Empty(t, storedMessages(t, rows, record, childID))
			}
			if mode == "held marker" {
				require.Equal(t, before, rows, "a marker the store holds adds nothing")
			}
			if mode != "active child" {
				exports := 1
				if childID != "" {
					exports = 2
				}
				require.EqualValues(t, exports, reads.Load(), "a commit exports each session once")
			}
		})
	}
}

func checkpointExport(mode string, count int, exported opencode.Export) []byte {
	value := fakeExport{Info: exported.Info}
	for _, message := range exported.Messages {
		native, _ := message.Decode()
		value.Messages = append(value.Messages, native)
	}
	switch mode {
	case "missing marker":
		value.Messages = value.Messages[:len(value.Messages)-1]
	case "wrong outcome":
		value.Messages[len(value.Messages)-1].Outcome = "failed"
	case "changing tail":
		value.Messages = append(value.Messages, opencode.NativeMessage{ID: "msg_live", Type: roleAssistant, Text: fmt.Sprintf("live-%d", count)})
	case "growing":
		for i := range count {
			value.Messages = append(value.Messages, opencode.NativeMessage{ID: fmt.Sprintf("msg_later_%d", i), Type: "synthetic", Text: "successor"})
		}
		value.Info.Cost = float64(count)
		value.Info.Time.Updated = int64(count)
	}
	raw, _ := json.Marshal(value)
	if mode == "delivered inbox" {
		var encoded map[string]json.RawMessage
		_ = json.Unmarshal(raw, &encoded)
		messages := make([]json.RawMessage, 0, len(value.Messages)+1)
		_ = json.Unmarshal(encoded["messages"], &messages)
		message, _ := json.Marshal(map[string]any{"id": "msg_delivered", "type": "synthetic", "text": "delivered", "time": map[string]any{"created": 1}})
		messages = append(messages, message)
		encoded["messages"], _ = json.Marshal(messages)
		raw, _ = json.Marshal(encoded)
	}

	return raw
}

// storedMessages returns one session's messages from a stored generation.
func storedMessages(t *testing.T, rows [][]byte, record sessionRecord, id string) []opencode.Message {
	t.Helper()
	graph, err := decodeStored(rows, record)
	require.NoError(t, err)
	for i := range graph {
		if graph[i].Info.ID == id {
			return graph[i].Messages
		}
	}

	return nil
}

// BenchmarkTurnCommit measures one turn's commit of a long synthetic
// transcript through the native export: append streams the export and appends
// the turn's two messages; republish reads and republishes every message.
func BenchmarkTurnCommit(b *testing.B) {
	for _, mode := range []string{"append", "republish"} {
		b.Run(mode, func(b *testing.B) {
			text := strings.Repeat("x", 2048)
			messages := make([]opencode.NativeMessage, 0, 4096)
			for len(messages) < 2000 {
				messages = append(messages, benchMessage(len(messages), text))
			}
			var body atomic.Pointer[[]byte]
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/export"):
					_, _ = w.Write(*body.Load())
				case r.URL.Path == "/api/session/active":
					_, _ = w.Write([]byte(`{"data":{}}`))
				default:
					_, _ = w.Write([]byte(`{"data":[]}`))
				}
			}))
			defer server.Close()
			client := opencode.NewClient()
			client.URL = server.URL
			a := NewAgent()
			b.Cleanup(func() { _ = a.Close() })
			s := &session{agent: a, id: "ses_bench", nativeID: "ses_bench", cwd: "/work"}
			s.mirror = sessionlog.NewMirror(a.store, "ses_bench")
			rt := &binding{client: client}
			publish := func() {
				encoded, err := json.Marshal(map[string]any{"data": fakeExport{Info: opencode.NativeSession{ID: "ses_bench"}, Messages: messages}})
				require.NoError(b, err)
				body.Store(&encoded)
			}
			publish()
			var last graphTail
			record := func(sessionlog.Tail) (any, error) { return s.record(), nil }
			require.NoError(b, s.mirror.Commit(b.Context(), s.mirrorSource(b.Context(), rt, nil, &last), record))
			for b.Loop() {
				b.StopTimer()
				messages = append(messages, benchMessage(len(messages), text), benchMessage(len(messages)+1, text))
				publish()
				if mode == "republish" {
					s.mirror.Reset()
				}
				b.StartTimer()
				require.NoError(b, s.mirror.Commit(b.Context(), s.mirrorSource(b.Context(), rt, nil, &last), record))
			}
		})
	}
}

func benchMessage(index int, text string) opencode.NativeMessage {
	message := opencode.NativeMessage{ID: fmt.Sprintf("msg_%06d", index), Type: roleAssistant, Content: []opencode.Content{{Type: fieldText, Text: text}}}
	message.Time.Created = int64(index)

	return message
}
