package opencodeacp

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/wire"
)

func TestSessionRequestBuilders(t *testing.T) {
	t.Parallel()

	request := wire.NewSessionRequest("/w", wire.WithSessionAdditionalDirectories("/a"), wire.WithSessionMeta(map[string]any{"host": 1}), WithSessionRawEvents(true), WithSessionOpenCodeOptions(NewOpenCodeOptions(WithOpenCodeModel("p/m"))))
	require.Equal(t, "/w", request.Cwd)
	require.Equal(t, []acp.McpServer{}, request.McpServers)
	require.Equal(t, []string{"/a"}, request.AdditionalDirectories)
	require.Equal(t, 1, request.Meta["host"])
	require.Equal(t, map[string]any{"options": map[string]any{"model": "p/m"}, "rawEvent": map[string]any{"enabled": true}}, request.Meta["opencode"])

	load := wire.LoadSessionRequest("s1", "/w")
	require.Equal(t, acp.SessionId("s1"), load.SessionId)
	require.Equal(t, []acp.McpServer{}, load.McpServers)

	resume := wire.ResumeSessionRequest("s1", "/w", WithSessionOpenCodeOptions(NewOpenCodeOptions(WithOpenCodeEffort("low"))))
	require.Equal(t, []acp.McpServer{}, resume.McpServers)
	require.NotNil(t, resume.Meta["opencode"])

	require.Equal(t, acp.SessionId("s1"), wire.DeleteSessionRequest("s1").SessionId)
	require.Equal(t, acp.SessionId("s1"), wire.CancelRequest("s1").SessionId)
	require.Len(t, wire.TextPromptRequest("s1", "hi").Prompt, 1)
	require.NotNil(t, wire.PromptRequest("s1").Prompt)
	require.Equal(t, configModel, SetModelRequest("s1", "p/m").ValueId.ConfigId)

	list := wire.ListSessionsRequest(wire.WithListSessionsCwd("/w"), wire.WithListSessionsCursor("c"), wire.WithListSessionsMeta(map[string]any{"k": "v"}))
	require.Equal(t, "/w", *list.Cwd)
	require.Equal(t, "c", *list.Cursor)
	require.Equal(t, "v", list.Meta["k"])
}

func TestBuildersRejectReservedMeta(t *testing.T) {
	t.Parallel()

	for _, literal := range wire.ReservedLiterals {
		require.Panics(t, func() { wire.WithSessionMeta(map[string]any{literal: 1}) })
		require.Panics(t, func() { wire.WithListSessionsMeta(map[string]any{literal: 1}) })
	}
}

func TestMetadataClonesTypedEnvironment(t *testing.T) {
	env := map[string]string{"SESSION_KEY": "original"}
	meta := map[string]any{vendor: map[string]any{"options": map[string]any{"env": env}}}
	option := wire.WithSessionMeta(meta)
	first := wire.NewSessionRequest(t.TempDir(), option)
	env["SESSION_KEY"] = "caller changed"
	second := wire.NewSessionRequest(t.TempDir(), option)
	for _, request := range []map[string]any{first.Meta, second.Meta} {
		parsed, err := parseSessionMeta(request)
		require.Nil(t, err)
		require.Equal(t, "original", parsed.options.Env["SESSION_KEY"], "a caller mutation reached a session environment the builder already captured")
	}
}
