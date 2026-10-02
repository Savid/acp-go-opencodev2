package opencode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestUsageAccessVerifiesNativeRoute(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, base, pkg, headers, override, connection, key, reason, variants, selectedVariant, credentialFields string }{
		{name: "native credential", key: "native-key"},
		{name: "environment", connection: `{"type":"env","name":"NATIVE_KEY"}`, key: "env-key"},
		{name: "active key overrides settings", override: `,"apiKey":"configured-key"`, key: "native-key"},
		{name: "settings without connection", connection: "null", override: `,"apiKey":"configured-key"`, key: "configured-key"},
		{name: "oauth overrides settings", connection: `{"type":"credential","id":"credential","method":"oauth"}`, override: `,"apiKey":"configured-key"`, reason: wire.AccountUsageNotReported},
		{name: "credential configuration", credentialFields: `,"configuration":{"baseURL":"https://proxy.invalid/v1"}`, reason: wire.AccountUsageNotReported},
		{name: "variant route", selectedVariant: "low", variants: `[{"id":"low","settings":{"baseURL":"https://proxy.invalid/v1"}}]`, reason: wire.AccountUsageNotReported},
		{name: "variant authentication", selectedVariant: "low", variants: `[{"id":"low","headers":{"Authorization":"different"}}]`, reason: wire.AccountUsageNotReported},
		{name: "variant key", selectedVariant: "low", connection: "null", variants: `[{"id":"low","settings":{"apiKey":"variant-key"}}]`, key: "variant-key"},
		{name: "nested header merge", selectedVariant: "low", override: `,"headers":{"Authorization":"different"}`, variants: `[{"id":"low","settings":{"headers":{"x-title":"test"}}}]`, reason: wire.AccountUsageNotReported},
		{name: "missing variant", selectedVariant: "missing", reason: wire.AccountUsageNotReported},
		{name: "unselected variant", variants: `[{"id":"low","settings":{"baseURL":"https://proxy.invalid/v1"}}]`, key: "native-key"},
		{name: "proxy", base: "https://proxy.invalid/v1", reason: wire.AccountUsageNotReported},
		{name: "organization-scoped route", headers: `{"x-opencode-org-id":"organization"}`, reason: wire.AccountUsageNotReported},
		{name: "auth header", headers: `{"Authorization":"different"}`, reason: wire.AccountUsageNotReported},
		{name: "oauth", connection: `{"type":"credential","id":"credential","method":"oauth"}`, reason: wire.AccountUsageNotReported},
		{name: "empty setting retains active key", override: `,"apiKey":""`, key: "native-key"},
		{name: "missing credential", connection: "null", reason: wire.AccountUsageNotAuthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base, pkg, headers, connection := tc.base, tc.pkg, tc.headers, tc.connection
			if base == "" {
				base = "https://opencode.ai/zen/go/v1"
			}
			if pkg == "" {
				pkg = "@opencode/ai/providers/anthropic"
			}
			if headers == "" {
				headers = `{}`
			}
			if connection == "" {
				connection = `{"type":"credential","id":"credential","method":"key"}`
			}
			variants := tc.variants
			if variants == "" {
				variants = "[]"
			}
			connections := "[" + connection + "]"
			if connection == "null" {
				connections = "[]"
			}
			var ready atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if !ready.Load() && (r.URL.Path == "/api/provider" || r.URL.Path == "/api/model") {
					_, _ = w.Write([]byte(`{"data":[]}`))

					return
				}
				var body string
				switch r.URL.Path {
				case "/api/provider":
					body = `{"data":[{"id":"opencode-go","integrationID":"opencode-go","package":"@opencode/ai/providers/openai-compatible","settings":{"baseURL":"https://opencode.ai/zen/go/v1"}}]}`
				case "/api/model":
					body = `{"data":[{"id":"model","providerID":"opencode-go","package":"` + pkg + `","settings":{"baseURL":"` + base + `"` + tc.override + `},"headers":` + headers + `,"variants":` + variants + `}]}`
				case "/api/integration":
					ready.Store(true)
					body = `{"data":[{"id":"opencode-go","connections":` + connections + `}]}`
				case "/api/credential":
					body = `{"data":[{"id":"credential","value":{"type":"key","key":"native-key"` + tc.credentialFields + `}}]}`
				default:
					w.WriteHeader(404)

					return
				}
				_ = json.NewEncoder(w).Encode(json.RawMessage(body))
			}))
			defer server.Close()
			client := &Client{URL: server.URL, http: server.Client()}
			access, err := client.UsageAccess(t.Context(), "/workspace", "opencode-go", ModelRef{ProviderID: "opencode-go", ID: "model", Variant: tc.selectedVariant}, func(name string) (string, bool) { return "env-key", name == "NATIVE_KEY" })
			require.NoError(t, err)
			require.Equal(t, tc.reason, access.Reason)
			require.Equal(t, tc.key, access.APIKey)
		})
	}
}

func TestGatewaySelectedVariantEndpoint(t *testing.T) {
	for _, variant := range []string{"low", "high", "", "default"} {
		t.Run("variant="+variant, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				var body string
				switch r.URL.Path {
				case "/api/provider":
					body = `{"data":[{"id":"broker","settings":{"apiKey":"broker-key"}}]}`
				case "/api/model":
					body = `{"data":[{"providerID":"broker","id":"model","variants":[{"id":"low","settings":{"baseURL":"https://gateway.invalid/v1"}},{"id":"high","settings":{}}]}]}`
				case "/api/config":
					body = `[{"info":{"providers":{"broker":{"models":{"model":{"variants":[{"id":"low","settings":{"baseURL":"https://gateway.invalid/v1"}}]}}}}}}]`
				case "/api/integration":
					body = `{"data":[]}`
				default:
					w.WriteHeader(http.StatusNotFound)

					return
				}
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			client := &Client{URL: server.URL, http: server.Client()}
			routes, err := client.UsageGateways(t.Context(), "/workspace", ModelRef{ProviderID: "broker", ID: "model", Variant: variant}, func(string) (string, bool) { return "", false })
			require.NoError(t, err)
			if variant != "low" {
				require.Empty(t, routes)

				return
			}
			require.Len(t, routes, 1)
			require.Equal(t, "https://gateway.invalid/v1", routes[0].BaseURL)
			require.Equal(t, "broker-key", routes[0].Token)
		})
	}
}
