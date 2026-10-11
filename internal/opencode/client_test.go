package opencode

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadAddress(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		output string
		want   string
	}{
		{name: "native address after noise", output: "startup notice\nserver listening on http://127.0.0.1:49152\n", want: "http://127.0.0.1:49152"},
		{name: "missing address", output: "startup failed\n"},
		{name: "unbound port", output: "server listening on http://127.0.0.1:0\n"},
		{name: "foreign address", output: "server listening on http://192.0.2.1:49152\n"},
		{name: "URL credentials", output: "server listening on http://user:password@127.0.0.1:49152\n"},
		{name: "oversized line", output: strings.Repeat("x", 64<<10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := NewClient()
			err := client.ReadAddress(strings.NewReader(tc.output))
			if tc.want == "" {
				require.Error(t, err)
				require.Empty(t, client.URL)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, client.URL)
			}
		})
	}
}

func TestClientRequiresJSON(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<!doctype html><title>Web page</title>"))
	}))
	defer server.Close()
	client := NewClient()
	client.URL = server.URL
	for _, out := range []any{nil, &map[string]any{}} {
		err := client.Do(t.Context(), "", http.MethodPost, "/api/session/missing/prompt", nil, out)
		var protocol *ProtocolError
		require.ErrorAs(t, err, &protocol)
	}
}

func TestExportBoundsEachMessageNotTheWhole(t *testing.T) {
	t.Parallel()
	text := strings.Repeat("x", 200)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		messages := make([]string, 0, 20)
		for i := range 20 {
			messages = append(messages, `{"id":"msg_`+strconv.Itoa(i)+`","type":"user","text":"`+text+`"}`)
		}
		if strings.Contains(r.URL.Path, "large") {
			messages = append(messages, `{"id":"msg_large","type":"user","text":"`+strings.Repeat("y", 2048)+`"}`)
		}
		_, _ = w.Write([]byte(`{"data":{"info":{"id":"ses_root"},"messages":[` + strings.Join(messages, ",") + `]}}`))
	}))
	defer server.Close()
	client := NewClient()
	client.URL = server.URL
	client.bodyLimit = 1024
	export, err := client.Export(t.Context(), "ses_root")
	require.NoError(t, err, "an export past the response bound still reads")
	require.Len(t, export.Messages, 20)
	require.Equal(t, "ses_root", export.Info.ID)
	require.Equal(t, "msg_19", export.Messages[19].ID)
	_, err = client.Export(t.Context(), "large")
	require.ErrorContains(t, err, "message exceeds size limit")
}
