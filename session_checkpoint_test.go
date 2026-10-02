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

func TestExecutionSnapshotVerifiesPrefix(t *testing.T) {
	for _, mode := range []string{"growing", "changing tail", "changing child", "completed child", "missing marker", "wrong outcome", "changed message", "changed inbox", "export failure", "active child", "delivered inbox", "changed delivered inbox"} {
		t.Run(mode, func(t *testing.T) {
			a, s, _ := newDirectSession(t)
			response, err := a.Prompt(t.Context(), wire.TextPromptRequest(s.id, "HELLO"))
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			rt := s.runtime
			native := *rt.client
			exported, err := native.Export(t.Context(), s.nativeID)
			require.NoError(t, err)
			var record sessionRecord
			before, _, err := sessionlog.Load(t.Context(), a.store, string(s.id), &record)
			require.NoError(t, err)
			marker := exported.Messages[len(exported.Messages)-1]
			require.Equal(t, "idle", marker.Type)
			terminal := opencode.Event{ID: strings.Replace(marker.ID, "msg_", "evt_", 1), Type: "session.execution.succeeded", Data: json.RawMessage(`{"sessionID":"` + s.nativeID + `"}`)}
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
			var firstRaw []byte
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(mode, "delivered inbox") && strings.HasSuffix(r.URL.Path, "/inbox") && inboxRead.CompareAndSwap(false, true) {
					fakeData(w, []map[string]any{{"id": "msg_delivered", "sessionID": s.nativeID, "type": "synthetic", "payload": map[string]any{"text": "delivered"}, "delivery": "queue", "time": map[string]any{"created": 1}}})

					return
				}
				if mode == "changed inbox" && strings.HasSuffix(r.URL.Path, "/inbox") {
					fakeData(w, []map[string]any{{"id": fmt.Sprintf("msg_input_%d", reads.Load()), "sessionID": s.nativeID, "type": "synthetic", "payload": map[string]any{"text": "pending"}, "delivery": "queue", "time": map[string]any{"created": 1}}})

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
				raw := checkpointExport(mode, count, exported)
				if count == 1 {
					firstRaw = raw
				}
				fakeData(w, json.RawMessage(raw))
			}))
			defer proxy.Close()
			rt.client.URL = proxy.URL
			err = s.commitMirror(t.Context(), rt, &terminal)
			rt.client.URL = native.URL
			rows, _, loadErr := sessionlog.Load(t.Context(), a.store, string(s.id), &record)
			require.NoError(t, loadErr)
			if mode == "growing" || mode == "changing tail" || mode == "changing child" || mode == "completed child" || mode == "active child" || mode == "delivered inbox" {
				require.NoError(t, err)
				messages := nativeMessages(rows, s.nativeID)
				require.Len(t, messages, len(exported.Messages))
				require.Equal(t, marker.ID, messages[len(messages)-1].ID)
				if mode == "delivered inbox" {
					require.Len(t, record.PendingInputs[s.nativeID], 1)
				} else {
					require.Empty(t, record.PendingInputs)
				}
				if mode == "completed child" {
					require.Len(t, nativeMessages(rows, childID), 2)
				}
				if mode == "changing child" {
					require.Empty(t, nativeMessages(rows, childID))
				}
				if mode == "growing" || mode == "changing tail" || mode == "delivered inbox" {
					require.NotEqual(t, firstRaw, rows[0])
					require.EqualValues(t, 2, reads.Load())
				}
			} else {
				require.Error(t, err)
				require.Equal(t, before, rows, "unverified state must not replace the durable generation")
				requireSnapshotFailure(t, mode, err, reads.Load())
			}
		})
	}
}

func requireSnapshotFailure(t *testing.T, mode string, err error, reads int32) {
	t.Helper()
	switch mode {
	case "missing marker", "wrong outcome":
		require.ErrorContains(t, err, "native execution marker missing")
		require.EqualValues(t, snapshotAttempts*2, reads)
	case "changed inbox", "changed delivered inbox":
		require.EqualValues(t, 2, reads, "a retry must not forget the mismatched input")
	}
}

func checkpointExport(mode string, count int, exported opencode.Export) []byte {
	value := fakeExport{Info: exported.Info, Messages: append([]opencode.NativeMessage(nil), exported.Messages...)}
	switch mode {
	case "missing marker":
		value.Messages = value.Messages[:len(value.Messages)-1]
	case "wrong outcome":
		value.Messages[len(value.Messages)-1].Outcome = "failed"
	case "changed message":
		value.Messages[0].Text = fmt.Sprintf("changed-%d", count)
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
	if strings.Contains(mode, "delivered inbox") {
		var encoded map[string]json.RawMessage
		_ = json.Unmarshal(raw, &encoded)
		messages := make([]json.RawMessage, 0, len(value.Messages)+1)
		_ = json.Unmarshal(encoded["messages"], &messages)
		text := "delivered"
		if mode == "changed delivered inbox" {
			text = "changed"
		}
		message, _ := json.Marshal(map[string]any{"id": "msg_delivered", "type": "synthetic", "text": text, "time": map[string]any{"created": 1}})
		messages = append(messages, message)
		encoded["messages"], _ = json.Marshal(messages)
		raw, _ = json.Marshal(encoded)
	}

	return raw
}
