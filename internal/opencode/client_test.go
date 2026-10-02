package opencode

import (
	"net/http"
	"net/http/httptest"
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
