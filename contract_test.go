package opencodeacp

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

func TestInitializeShape(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithInputHandoffRoot(t.TempDir()))
	resp := h.initialize(func(request *acp.InitializeRequest) {
		request.ClientCapabilities.PositionEncodings = []acp.PositionEncodingKind{acp.PositionEncodingKindUtf8}
	})

	require.Empty(t, resp.AuthMethods)
	require.Nil(t, resp.Meta)
	require.True(t, resp.AgentCapabilities.LoadSession)
	require.True(t, resp.AgentCapabilities.PromptCapabilities.Image)
	require.True(t, resp.AgentCapabilities.PromptCapabilities.EmbeddedContext)
	require.False(t, resp.AgentCapabilities.McpCapabilities.Http)
	require.Nil(t, resp.AgentCapabilities.SessionCapabilities.Fork)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.Close)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.Delete)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.List)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.Resume)
	require.NotNil(t, resp.AgentCapabilities.SessionCapabilities.AdditionalDirectories)
	require.Equal(t, acp.PositionEncodingKindUtf8, *resp.AgentCapabilities.PositionEncoding)

	meta := resp.AgentCapabilities.Meta
	require.Contains(t, meta, "opencode")
	require.Contains(t, meta, wire.MediaEnvelopeKey)
	require.Contains(t, meta, wire.HandoffKey)

	envelope, _ := meta[wire.MediaEnvelopeKey].(map[string]any)
	require.EqualValues(t, 6291456, envelope["maxBytes"])
	require.EqualValues(t, 6291456, envelope["maxPromptBytes"])
	require.EqualValues(t, 0, envelope["maxDimension"])
	require.Equal(t, []any{"image/png", "image/jpeg", "image/gif", "image/webp"}, envelope["imageFormats"])
	require.Equal(t, []any{}, envelope["documentFormats"])

	vendorMeta, _ := meta["opencode"].(map[string]any)
	require.NotContains(t, vendorMeta, "structuredOutput")
	store, _ := vendorMeta["sessionStore"].(map[string]any)
	require.Equal(t, SessionStoreFormat, store["format"])
}

func TestInitializeWithoutHandoffOmitsAdvertisement(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	resp := h.initialize()
	require.NotContains(t, resp.AgentCapabilities.Meta, wire.HandoffKey)
	require.Equal(t, acp.PositionEncodingKindUtf16, *resp.AgentCapabilities.PositionEncoding)
}

func TestInitializeLifecycleAnswer(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	resp := h.initialize(withLifecycle())

	answer, ok := resp.Meta[wire.LifecycleKey].(map[string]any)
	require.True(t, ok)
	require.EqualValues(t, 1, answer["version"])
	require.Equal(t, true, answer["updatesOutsidePrompt"])
	require.Equal(t, []any{}, answer["activityKinds"])
	require.NotContains(t, resp.AgentCapabilities.Meta, wire.LifecycleKey)
}

func TestInitializeLifecycleStrictness(t *testing.T) {
	t.Parallel()

	cases := map[string]any{
		"string version": map[string]any{"version": "1"},
		"fraction":       map[string]any{"version": json.Number("1.0")},
		"other version":  map[string]any{"version": 2},
		"unknown member": map[string]any{"version": 1, "extra": true},
		"non-object":     true,
	}

	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			meta := map[string]any{wire.LifecycleKey: value}

			_, err := h.conn.Initialize(h.ctx(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: meta})
			require.Equal(t, -32602, requestErrorCode(t, err))

			data := requestErrorData(t, err)
			require.Equal(t, "unsupported", data[wire.FieldError])
			require.Contains(t, data["field"], wire.LifecycleKey)
		})
	}
}

func TestProtocolAdmission(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	_, err := h.conn.CallExtension(h.ctx(), "_opencode/anything", map[string]any{})
	require.Equal(t, -32601, requestErrorCode(t, err))

	_, err = h.conn.SetSessionMode(h.ctx(), acp.SetSessionModeRequest{SessionId: "x", ModeId: "plan"})
	require.Equal(t, -32601, requestErrorCode(t, err))

	_, err = h.conn.Authenticate(h.ctx(), acp.AuthenticateRequest{MethodId: "oauth"})
	require.Equal(t, -32602, requestErrorCode(t, err))
	require.Equal(t, "oauth", requestErrorData(t, err)["methodId"])

	_, err = h.conn.Logout(h.ctx(), acp.LogoutRequest{})
	require.Equal(t, -32601, requestErrorCode(t, err))
}

func TestSessionMetaStrictness(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		meta  map[string]any
		field string
	}{
		{"unknown own key", map[string]any{"opencode": map[string]any{"bogus": 1}}, "_meta.opencode.bogus"},
		{"unknown option", map[string]any{"opencode": map[string]any{"options": map[string]any{"bogus": 1}}}, "_meta.opencode.options.bogus"},
		{"bad model", map[string]any{"opencode": map[string]any{"options": map[string]any{"model": "nomodel"}}}, "_meta.opencode.options.model"},
		{"bad permission", map[string]any{"opencode": map[string]any{"options": map[string]any{"permission": "bogus"}}}, "_meta.opencode.options.permission"},
		{"relative path dir", map[string]any{"opencode": map[string]any{"options": map[string]any{"extraPathDirs": []any{"rel"}}}}, "_meta.opencode.options.extraPathDirs[0]"},
		{"bad env name", map[string]any{"opencode": map[string]any{"options": map[string]any{"env": map[string]any{"A=B": "x"}}}}, "_meta.opencode.options.env.A=B"},
		{"lifecycle literal", map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}, `_meta["` + wire.LifecycleKey + `"]`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.initialize()

			request := wire.NewSessionRequest(t.TempDir())
			request.Meta = tc.meta

			_, err := h.conn.NewSession(h.ctx(), request)
			require.Equal(t, -32602, requestErrorCode(t, err))

			data := requestErrorData(t, err)
			require.Equal(t, "unsupported", data[wire.FieldError])
			require.Equal(t, tc.field, data["field"])
		})
	}
}

func TestForeignMetaIgnored(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	session := h.newSession(wire.WithSessionMeta(map[string]any{"other": map[string]any{"x": 1}, "traceparent": "00-1-2-01"}))
	require.NotEmpty(t, session.SessionId)
}

func TestUniformRejections(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()

	_, err := h.conn.NewSession(h.ctx(), acp.NewSessionRequest{Cwd: "relative", McpServers: []acp.McpServer{}})
	require.Equal(t, fieldCwd, requestErrorData(t, err)["field"])

	var server acp.McpServer
	require.NoError(t, json.Unmarshal([]byte(`{"type":"stdio","name":"x","command":"x","args":[],"env":[]}`), &server))

	_, err = h.conn.NewSession(h.ctx(), acp.NewSessionRequest{Cwd: t.TempDir(), McpServers: []acp.McpServer{server}})
	require.Equal(t, "mcpServers", requestErrorData(t, err)["field"])

	session := h.newSession()

	_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId))
	require.Equal(t, "prompt", requestErrorData(t, err)["field"])

	_, err = h.conn.Prompt(h.ctx(), wire.PromptRequest(session.SessionId, acp.ContentBlock{Audio: &acp.ContentBlockAudio{Data: "x", MimeType: "audio/wav"}}))
	require.Equal(t, "prompt", requestErrorData(t, err)["field"])

	_, err = h.conn.Prompt(h.ctx(), wire.TextPromptRequest("00000000-0000-4000-8000-000000000000", "hi"))
	require.Equal(t, -32602, requestErrorCode(t, err))
	require.Equal(t, "unknown session", requestErrorData(t, err)[wire.FieldError])

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest("00000000-0000-4000-8000-000000000000")))
}

func TestPromptCorrelationGate(t *testing.T) {
	t.Parallel()

	t.Run("required when negotiated", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		h.initialize(withLifecycle())
		session := h.newSession()

		_, err := h.prompt(session.SessionId, "HELLO", nil)
		data := requestErrorData(t, err)
		require.Equal(t, "missing", data[wire.FieldError])
		require.Equal(t, `_meta["`+wire.LifecycleKey+`"]`, data["field"])

		_, err = h.prompt(session.SessionId, "HELLO", map[string]any{wire.LifecycleKey: map[string]any{"version": 1, "submission": map[string]any{"submissionId": "", "clientNonce": "n"}}})
		data = requestErrorData(t, err)
		require.Equal(t, "unsupported", data[wire.FieldError])
		require.Contains(t, data["field"], "submissionId")
	})

	t.Run("refused when omitted", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		h.initialize()
		session := h.newSession()

		_, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
		data := requestErrorData(t, err)
		require.Equal(t, "unsupported", data[wire.FieldError])
		require.Equal(t, `_meta["`+wire.LifecycleKey+`"]`, data["field"])
	})

	t.Run("fraction version refused over the wire", func(t *testing.T) {
		t.Parallel()

		h := newHarness(t)
		h.initialize(withLifecycle())
		session := h.newSession()

		_, err := h.prompt(session.SessionId, "HELLO", map[string]any{wire.LifecycleKey: map[string]any{"version": json.Number("1.0"), "submission": map[string]any{"submissionId": "s", "clientNonce": "n"}}})
		require.Equal(t, "unsupported", requestErrorData(t, err)[wire.FieldError])
	})
}

func TestInvalidOptionsVerdict(t *testing.T) {
	t.Parallel()

	cases := map[string]Option{
		"home":              WithHome("relative/home"),
		"defaultModel":      WithDefaultModel("no-provider"),
		"configuredModels":  WithConfiguredModels([]string{"fake/a", "fake/a"}),
		"env":               WithEnv(map[string]string{"": "x"}),
		"imageLimits":       WithImageLimits(ImageLimits{MaxInputBytesPerImage: -1}),
		"concurrencyLimits": WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: -1}),
		"inputHandoffRoot":  WithInputHandoffRoot("relative"),
	}

	for field, option := range cases {
		t.Run(field, func(t *testing.T) {
			t.Parallel()

			agent := NewAgent(testOptions(t, option)...)
			t.Cleanup(func() { _ = agent.Close() })

			_, err := agent.Initialize(context.Background(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
			require.Equal(t, -32603, requestErrorCode(t, err))

			data := requestErrorData(t, err)
			require.Equal(t, "opencode_invalid_options", data[wire.FieldError])
			require.Equal(t, field, data["field"])

			_, err = agent.NewSession(context.Background(), wire.NewSessionRequest(t.TempDir()))
			require.Equal(t, "opencode_invalid_options", requestErrorData(t, err)[wire.FieldError])
		})
	}
}

func TestPromptBackpressure(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	done := make(chan error, 1)

	go func() {
		_, err := h.prompt(session.SessionId, "SLOW", promptMeta(1))
		done <- err
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEvents(updates)) >= 3 })

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, -32600, requestErrorCode(t, err))
	require.Equal(t, "backpressure", requestErrorData(t, err)[wire.FieldError])
	require.Equal(t, "session_prompt", requestErrorData(t, err)["limit"])

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.NoError(t, <-done)
}

func TestActiveSessionLimit(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
	h.initialize()
	h.newSession()

	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Equal(t, "backpressure", requestErrorData(t, err)[wire.FieldError])
	require.Equal(t, "active_sessions", requestErrorData(t, err)["limit"])

	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
}

func TestClosedAgentRefusesRequests(t *testing.T) {
	t.Parallel()

	agent := NewAgent(testOptions(t)...)
	require.NoError(t, agent.Close())
	require.NoError(t, agent.Close())

	t.Run("Initialize", func(t *testing.T) {
		t.Parallel()

		_, err := agent.Initialize(t.Context(), acp.InitializeRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("Authenticate", func(t *testing.T) {
		t.Parallel()

		_, err := agent.Authenticate(t.Context(), acp.AuthenticateRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("Logout", func(t *testing.T) {
		t.Parallel()

		_, err := agent.Logout(t.Context(), acp.LogoutRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("NewSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.NewSession(t.Context(), acp.NewSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("LoadSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.LoadSession(t.Context(), acp.LoadSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("ResumeSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.ResumeSession(t.Context(), acp.ResumeSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("ListSessions", func(t *testing.T) {
		t.Parallel()

		_, err := agent.ListSessions(t.Context(), acp.ListSessionsRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("Prompt", func(t *testing.T) {
		t.Parallel()

		_, err := agent.Prompt(t.Context(), acp.PromptRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("CloseSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.CloseSession(t.Context(), acp.CloseSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("UnstableDeleteSession", func(t *testing.T) {
		t.Parallel()

		_, err := agent.UnstableDeleteSession(t.Context(), acp.UnstableDeleteSessionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("SetSessionConfigOption", func(t *testing.T) {
		t.Parallel()

		_, err := agent.SetSessionConfigOption(t.Context(), acp.SetSessionConfigOptionRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("SetSessionMode", func(t *testing.T) {
		t.Parallel()

		_, err := agent.SetSessionMode(t.Context(), acp.SetSessionModeRequest{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("extension", func(t *testing.T) {
		t.Parallel()

		_, err := agent.HandleExtensionMethod(t.Context(), "_unknown/read", json.RawMessage(`{}`))
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("cancel", func(t *testing.T) {
		t.Parallel()

		err := agent.Cancel(t.Context(), acp.CancelNotification{})
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})

	t.Run("account usage", func(t *testing.T) {
		t.Parallel()

		_, err := agent.HandleExtensionMethod(t.Context(), AccountUsageMethod, json.RawMessage(`{}`))
		require.Equal(t, -32600, requestErrorCode(t, err))
		require.Equal(t, map[string]any{"error": "agent closed"}, requestErrorData(t, err))
	})
}

func TestNegativeClientCallLimitReturnsOptionsError(t *testing.T) {
	t.Parallel()
	agent := NewAgent(WithConcurrencyLimits(ConcurrencyLimits{MaxConcurrentClientCalls: -1}))
	defer agent.Close()
	_, err := agent.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.Equal(t, "opencode_invalid_options", requestErrorData(t, err)[wire.FieldError])
}

// Form elicitation is relayed only when the client advertises form support,
// in every shape the capability can take; without it the native question is
// answered with no client call.
func TestElicitationCapabilityGating(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, json string
		form       bool
	}{
		{"omitted", `{}`, false},
		{"empty", `{"elicitation":{}}`, false},
		{"form", `{"elicitation":{"form":{}}}`, true},
		{"url", `{"elicitation":{"url":{}}}`, false},
		{"both", `{"elicitation":{"form":{},"url":{}}}`, true},
		{"null", `{"elicitation":{"form":null,"url":null}}`, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)

			var calls atomic.Int32

			h.rec.mu.Lock()
			h.rec.elicit = func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
				calls.Add(1)

				return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: map[string]any{"0": "blue"}}}, nil
			}
			h.rec.mu.Unlock()

			h.initialize(func(request *acp.InitializeRequest) {
				require.NoError(t, json.Unmarshal([]byte(tc.json), &request.ClientCapabilities))
			})
			session := h.newSession()

			response, err := h.prompt(session.SessionId, "QUESTION", nil)
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)

			expected := int32(0)
			if tc.form {
				expected = 1
			}

			require.Equal(t, expected, calls.Load())
		})
	}
}

// Lines the server writes to its stdout and stderr never reach the ACP
// stream.
func TestNativeNoiseCannotCorruptACPStdout(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "NOISE", nil)
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "hello NOISE")

	for _, update := range h.rec.snapshot() {
		encoded, marshalErr := json.Marshal(update)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(encoded), "not a json record at all")
		require.NotContains(t, string(encoded), "chatter on stderr")
	}

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, list.Sessions, 1)
}
