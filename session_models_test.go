package opencodeacp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	acp "github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

// commandCatalog returns the names of the first advertised command catalog.
func commandCatalog(t *testing.T, h *harness) []string {
	t.Helper()

	var names []string

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		for _, update := range updates {
			catalog := update.Update.AvailableCommandsUpdate
			if catalog == nil {
				continue
			}

			names = make([]string, 0, len(catalog.AvailableCommands))
			for _, command := range catalog.AvailableCommands {
				names = append(names, command.Name)
			}

			return true
		}

		return false
	})

	return names
}

// configOption returns the advertised select with the given id.
func configOption(options []acp.SessionConfigOption, id acp.SessionConfigId) *acp.SessionConfigOptionSelect {
	for index := range options {
		if selected := options[index].Select; selected != nil && selected.Id == id {
			return selected
		}
	}

	return nil
}

func TestCommandCatalogIsSanitizedAndRoutes(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	require.Equal(t, []string{"inspect"}, commandCatalog(t, h),
		"only names the shared sanitizer accepts are advertised")

	_, err := h.prompt(session.SessionId, "/inspect the tree", nil)
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "command:inspect args:the tree")

	_, err = h.prompt(session.SessionId, "/group/nested the tree", nil)
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "hello /group/nested the tree",
		"a name the catalog filtered out is plain prompt text")
}

func TestSetConfigOptionAppliesAndRefuses(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()

	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	models := configOption(session.ConfigOptions, configModel)
	require.NotNil(t, models)
	require.Equal(t, acp.SessionConfigValueId("fake/vision"), models.CurrentValue)
	require.NotNil(t, configOption(session.ConfigOptions, configMode))
	require.Nil(t, configOption(session.ConfigOptions, configEffort), "effort is advertised only while set")

	applied, err := h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configModel, "fake/text"))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("fake/text"), configOption(applied.ConfigOptions, configModel).CurrentValue)

	applied, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configEffort, "high"))
	require.NoError(t, err)

	effort := configOption(applied.ConfigOptions, configEffort)
	require.NotNil(t, effort)
	require.Equal(t, acp.SessionConfigValueId("high"), effort.CurrentValue)

	_, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, "bogus", "x"))
	require.Equal(t, "configId", requestErrorData(t, err)["field"])

	_, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configModel, ""))
	require.Equal(t, fieldValue, requestErrorData(t, err)["field"])

	_, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configModel, "nomodel"))
	require.Equal(t, fieldValue, requestErrorData(t, err)["field"])

	require.Equal(t, "fake/text", storedRecord(t, store, session.SessionId).Model, "an applied value is committed")
}

func TestConfigChangeUsesOnlyAddressedSelector(t *testing.T) {
	a, s, _ := newDirectSession(t)
	rt := s.runtime
	target, err := url.Parse(rt.client.URL)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	var modelCalls, agentCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			switch {
			case strings.HasSuffix(r.URL.Path, "/model"):
				modelCalls.Add(1)
			case strings.HasSuffix(r.URL.Path, "/agent"):
				agentCalls.Add(1)
			}
		}
		forward.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	rt.client.URL = proxy.URL
	_, err = a.SetSessionConfigOption(t.Context(), wire.SetConfigOptionRequest(s.id, configModel, "fake/text"))
	require.NoError(t, err)
	require.EqualValues(t, 1, modelCalls.Load())
	require.Zero(t, agentCalls.Load())
	_, err = a.SetSessionConfigOption(t.Context(), wire.SetConfigOptionRequest(s.id, configMode, "plan"))
	require.NoError(t, err)
	require.EqualValues(t, 1, modelCalls.Load())
	require.EqualValues(t, 1, agentCalls.Load())
}

func TestFailedNativeConfigChangeRebindsCommittedSelection(t *testing.T) {
	for _, tc := range []struct {
		name  string
		id    acp.SessionConfigId
		value string
	}{
		{"model", configModel, "fake/text"}, {"effort", configEffort, "high"}, {"mode", configMode, "plan"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _ := newDirectSession(t)
			rt := s.runtime
			before := s.record()
			target, err := url.Parse(rt.client.URL)
			require.NoError(t, err)
			forward := httputil.NewSingleHostReverseProxy(target)
			var fail atomic.Bool
			fail.Store(true)
			forward.ModifyResponse = func(response *http.Response) error {
				if response.Request.Method == http.MethodPost && (strings.HasSuffix(response.Request.URL.Path, "/model") || strings.HasSuffix(response.Request.URL.Path, "/agent")) && fail.CompareAndSwap(true, false) {
					response.StatusCode = http.StatusInternalServerError
				}

				return nil
			}
			proxy := httptest.NewServer(forward)
			defer proxy.Close()
			rt.client.URL = proxy.URL
			_, err = a.SetSessionConfigOption(t.Context(), wire.SetConfigOptionRequest(s.id, tc.id, acp.SessionConfigValueId(tc.value)))
			require.Equal(t, "opencode_internal_failure", requestErrorData(t, err)["error"])
			require.False(t, rt.alive())
			require.Equal(t, before.Model, s.record().Model)
			require.Equal(t, before.Mode, s.record().Mode)
			require.Equal(t, before.Effort, s.record().Effort)
			stored := storedRecord(t, a.store, s.id)
			require.Equal(t, before.Model, stored.Model)
			require.Equal(t, before.Mode, stored.Mode)
			require.Equal(t, before.Effort, stored.Effort)
			replacement, err := s.ensureRuntime(t.Context())
			require.NoError(t, err)
			native, err := replacement.client.Session(t.Context(), s.nativeID)
			require.NoError(t, err)
			require.Equal(t, before.Model, native.Model.ProviderID+"/"+native.Model.ID)
			require.Equal(t, before.Mode, native.Agent)
			require.Equal(t, before.Effort, native.Model.Variant)
		})
	}
}

func TestNativeDefaultsAndChangedSelectionSurviveRebind(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	server, err := a.ensureRuntime(t.Context())
	require.NoError(t, err)
	target, err := url.Parse(server.client.URL)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.ModifyResponse = func(response *http.Response) error {
		if response.Request.URL.Path == "/api/agent" {
			_ = response.Body.Close()
			body := `{"data":[{"id":"plan","name":"plan","mode":"primary"},{"id":"build","name":"build","mode":"primary"}]}`
			response.Body = io.NopCloser(strings.NewReader(body))
			response.ContentLength = int64(len(body))
			response.Header.Del("Content-Length")
		}

		return nil
	}
	proxy := httptest.NewServer(forward)
	defer proxy.Close()
	server.client.URL = proxy.URL
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir(), WithSessionOpenCodeOptions(NewOpenCodeOptions(WithOpenCodeEffort("low")))))
	require.NoError(t, err)
	require.Equal(t, acp.SessionConfigValueId("plan"), configOption(created.ConfigOptions, configMode).CurrentValue)
	require.Equal(t, acp.SessionConfigValueId("fake/vision"), configOption(created.ConfigOptions, configModel).CurrentValue)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	native, err := s.runtime.client.Session(t.Context(), s.nativeID)
	require.NoError(t, err)
	require.Equal(t, "low", native.Model.Variant)
	require.Equal(t, "plan", native.Agent)
	_, err = a.SetSessionConfigOption(t.Context(), wire.SetConfigOptionRequest(s.id, configEffort, "high"))
	require.NoError(t, err)
	_, err = a.SetSessionConfigOption(t.Context(), wire.SetConfigOptionRequest(s.id, configMode, "build"))
	require.NoError(t, err)
	s.stopRuntime(t.Context(), s.runtime)
	s.fenceStream()
	rt, err := s.ensureRuntime(t.Context())
	require.NoError(t, err)
	native, err = rt.client.Session(t.Context(), s.nativeID)
	require.NoError(t, err)
	require.Equal(t, "high", native.Model.Variant)
	require.Equal(t, "build", native.Agent)
}
