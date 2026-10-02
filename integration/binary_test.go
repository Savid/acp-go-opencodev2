//go:build integration

package integration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	opencodeacp "github.com/savid/acp-go-opencodev2"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
	"github.com/stretchr/testify/require"
)

// TestNativePersistence exercises native creation, import, and delete without
// model calls.
func TestNativePersistence(t *testing.T) {
	if os.Getenv("ACP_GO_OPENCODEV2_RUN_INTEGRATION") != "1" {
		t.Skip("set ACP_GO_OPENCODEV2_RUN_INTEGRATION=1")
	}
	executable := harnessPath(t)
	store := acpcore.NewInMemorySessionStore()
	a := opencodeacp.NewAgent(opencodeacp.WithExecutablePath(executable), opencodeacp.WithHome(t.TempDir()), opencodeacp.WithSessionStore(store), opencodeacp.WithEnv(map[string]string{"OPENCODE_API_KEY": ""}))
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	cwd := t.TempDir()
	pluginDir := filepath.Join(cwd, ".opencode", "plugins")
	require.NoError(t, os.MkdirAll(pluginDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(pluginDir, "noop.ts"), []byte(`export default {
 id: "acp-noop-test",
 async setup(ctx) {
  await new Promise(resolve => setTimeout(resolve, 250))
  await ctx.model.transform(editor => {
   for (const model of editor.list()) {
    editor.update(model.providerID, model.id, draft => { draft.limit.context = 65536 })
   }
  })
  await ctx.agent.transform(editor => { editor.update("review", agent => { agent.mode = "primary" }); editor.default("review") })
  await ctx.command.transform(editor => editor.add({name: "acpnoop", description: "Complete without model work", execute: async () => {}}))
 }
}`), 0600))
	// A large carrier must survive native export and import into an empty home.
	marker := strings.Repeat("history-payload-", 8192)
	options := opencodeacp.NewOpenCodeOptions(opencodeacp.WithOpenCodeEnv(map[string]string{"HISTORY_PROBE": marker}))
	session, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd, opencodeacp.WithSessionOpenCodeOptions(options)))
	require.NoError(t, err)
	require.NotEmpty(t, session.SessionId)
	selectedAgent := ""
	var models *acp.SessionConfigSelectOptionsUngrouped
	for _, option := range session.ConfigOptions {
		if option.Select != nil && option.Select.Id == "mode" {
			selectedAgent = string(option.Select.CurrentValue)
		}
		if option.Select != nil && option.Select.Id == "model" {
			models = option.Select.Options.Ungrouped
		}
	}
	require.Equal(t, "review", selectedAgent)
	require.NotNil(t, models)
	require.NotEmpty(t, *models)
	for _, model := range *models {
		meta, ok := model.Meta["opencode"].(map[string]any)
		require.True(t, ok)
		require.Equal(t, float64(65536), meta["contextWindow"])
	}
	response, err := a.Prompt(t.Context(), wire.TextPromptRequest(session.SessionId, "/acpnoop"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	rows, err := store.Load(t.Context(), string(session.SessionId))
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	var mirrored bytes.Buffer
	for _, row := range rows[""] {
		mirrored.Write(row)
	}
	require.Contains(t, mirrored.String(), marker)
	raw, err := a.HandleExtensionMethod(t.Context(), opencodeacp.AccountUsageMethod, json.RawMessage(`{"sessionId":"`+string(session.SessionId)+`","providerId":"opencode-go"}`))
	require.NoError(t, err)
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	var usage wire.AccountUsageResponse
	require.NoError(t, json.Unmarshal(encoded, &usage))
	require.NoError(t, usage.Validate())
	require.Equal(t, wire.AccountUsageUnavailable(wire.AccountUsageNotAuthenticated), usage, "an isolated home holds no OpenCode Go account")
	require.NoError(t, a.Close())
	b := opencodeacp.NewAgent(opencodeacp.WithExecutablePath(executable), opencodeacp.WithHome(t.TempDir()), opencodeacp.WithSessionStore(store))
	t.Cleanup(func() { _ = b.Close() })
	_, err = b.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	_, err = b.LoadSession(t.Context(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	assertStoredDirectory(t, store, session.SessionId, cwd)
	_, err = b.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	relocated := t.TempDir()
	_, err = b.ResumeSession(t.Context(), wire.ResumeSessionRequest(session.SessionId, relocated))
	require.NoError(t, err)
	assertStoredDirectory(t, store, session.SessionId, relocated)
	_, err = b.UnstableDeleteSession(t.Context(), wire.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err)
	list, err := b.ListSessions(t.Context(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
	_, err = b.LoadSession(t.Context(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.Error(t, err)
	require.NoError(t, b.Close())
}
func TestNativeContinuation(t *testing.T) {
	if os.Getenv("ACP_GO_OPENCODEV2_RUN_LIVE_TOKENS") != "1" {
		t.Skip("live token gate")
	}
	home, cwd := nativeHome(t), t.TempDir()
	store := acpcore.NewInMemorySessionStore()
	opts := []opencodeacp.Option{opencodeacp.WithHome(home), opencodeacp.WithSessionStore(store)}
	if model := os.Getenv("ACP_GO_OPENCODEV2_MODEL"); model != "" {
		opts = append(opts, opencodeacp.WithDefaultModel(model))
	}
	h := newHarness(t, opts...)
	h.initialize(withLifecycle())
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, opencodeacp.WithSessionOpenCodeOptions(opencodeacp.NewOpenCodeOptions(opencodeacp.WithOpenCodeEffort("low")))))
	require.NoError(t, err)
	response, err := h.prompt(session.SessionId, "Remember the project slug apricot-orbit. Reply with exactly apricot-orbit and nothing else. Do not use tools.", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.Contains(t, agentText(h.rec.snapshot()), "apricot-orbit")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	h.stop()
	command := exec.CommandContext(h.ctx(), harnessPath(t), "run", "--standalone", "--format", "json", "--session", nativeSessionID(t, session.Meta), "Remember the release label cobalt-lantern. Reply with the project slug and release label, and nothing else. Do not use tools.")
	if model := os.Getenv("ACP_GO_OPENCODEV2_MODEL"); model != "" {
		command.Args = append(command.Args, "--model", model+"#low")
	}
	command.WaitDelay = 2 * time.Second
	var stderr bytes.Buffer
	command.Stderr = &stderr
	command.Dir = cwd
	command.Env = nativeEnvironment(home)
	data, err := command.Output()
	require.NoError(t, err, "native resume: %s", stderr.String())
	require.Contains(t, string(data), "apricot-orbit")
	require.Contains(t, string(data), "cobalt-lantern")
	restored := newHarness(t, opts...)
	restored.initialize(withLifecycle())
	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "apricot-orbit")
	require.Contains(t, agentText(restored.rec.snapshot()), "cobalt-lantern")
	_, err = restored.prompt(session.SessionId, "What project slug and release label did we choose? Do not use tools.", promptMeta(2))
	require.NoError(t, err)
	require.Contains(t, agentText(restored.rec.snapshot()), "apricot-orbit")
	require.Contains(t, agentText(restored.rec.snapshot()), "cobalt-lantern")
	_, err = restored.conn.CloseSession(restored.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	restored.stop()
	imported := newHarness(t, opencodeacp.WithHome(nativeHome(t)), opencodeacp.WithSessionStore(store))
	imported.initialize(withLifecycle())
	_, err = imported.conn.LoadSession(imported.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(imported.rec.snapshot()), "cobalt-lantern")
	_, err = imported.prompt(session.SessionId, "What project slug and release label did we choose? Do not use tools.", promptMeta(3))
	require.NoError(t, err)
	require.Contains(t, agentText(imported.rec.snapshot()), "apricot-orbit")
	require.Contains(t, agentText(imported.rec.snapshot()), "cobalt-lantern")
}

func nativeEnvironment(home string) []string {
	env := os.Environ()
	for key, subdir := range map[string]string{"XDG_DATA_HOME": "data", "XDG_CONFIG_HOME": "config", "XDG_CACHE_HOME": "cache", "XDG_STATE_HOME": "state"} {
		env = append(env, key+"="+filepath.Join(home, subdir))
	}

	return env
}
func nativeHome(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}
func TestNativeCallbacksPathAndCancellation(t *testing.T) {
	if os.Getenv("ACP_GO_OPENCODEV2_RUN_LIVE_TOKENS") != "1" {
		t.Skip("live token gate")
	}
	home, cwd := nativeHome(t), t.TempDir()
	directories := []string{t.TempDir(), t.TempDir()}
	for index, dir := range directories {
		script := "#!/bin/sh\nprintf '%s\\n' 'marker-" + strconv.Itoa(index) + "'\nprintf 'ACP_PATH=%s\\n' \"$PATH\"\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, "acpgogo-native-probe"), []byte(script), 0700))
	}
	h := newHarness(t, opencodeacp.WithHome(home), opencodeacp.WithDefaultModel(os.Getenv("ACP_GO_OPENCODEV2_MODEL")))
	var questions atomic.Int32
	h.rec.elicit = func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
		questions.Add(1)

		return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: map[string]any{"q0": "cobalt"}}}, nil
	}
	h.initialize(withLifecycle(), withFormElicitation())
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, opencodeacp.WithSessionRawEvents(true), opencodeacp.WithSessionOpenCodeOptions(opencodeacp.NewOpenCodeOptions(opencodeacp.WithOpenCodeExtraPathDirs(directories[0]), opencodeacp.WithOpenCodeEffort("low")))))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(cwd, "remove-me.txt"), []byte("test-only"), 0600))
	response, err := h.prompt(session.SessionId, "Use the shell tool to run exactly: rm remove-me.txt. Do not use any other tool. Reply DONE when finished.", promptMeta(1))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.NoFileExists(t, filepath.Join(cwd, "remove-me.txt"))
	h.rec.mu.Lock()
	permissions := len(h.rec.permissions)
	raw := len(h.rec.raw)
	h.rec.mu.Unlock()

	require.Positive(t, permissions)
	require.Positive(t, raw)
	_, err = h.prompt(session.SessionId, "Use the question tool to ask one question: Which color? Offer cobalt and silver. After receiving my answer, reply with that color. Do not use any other tool.", promptMeta(2))
	require.NoError(t, err)
	require.EqualValues(t, 1, questions.Load())
	for index, dir := range directories {
		if index > 0 {
			_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
			require.NoError(t, err)
			_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(session.SessionId, cwd, opencodeacp.WithSessionRawEvents(true), opencodeacp.WithSessionOpenCodeOptions(opencodeacp.NewOpenCodeOptions(opencodeacp.WithOpenCodeExtraPathDirs(dir)))))
			require.NoError(t, err)
		}
		before := len(h.rec.snapshot())
		_, err = h.prompt(session.SessionId, "Use the shell tool to run the exact command acpgogo-native-probe. Do not set PATH, use an absolute command path, or run other commands. Reply DONE after it runs.", promptMeta(index+3))
		require.NoError(t, err)
		output := toolText(h.rec.snapshot()[before:])
		require.Contains(t, output, "marker-"+strconv.Itoa(index))
		require.Contains(t, output, "ACP_PATH="+dir+string(os.PathListSeparator))
		if index > 0 {
			require.NotContains(t, output, directories[0])
		}
	}
	done := make(chan acp.PromptResponse, 1)
	failed := make(chan error, 1)
	beforeCancel := len(h.rec.snapshot())
	h.rec.mu.Lock()
	rawBeforeCancel := len(h.rec.raw)
	h.rec.mu.Unlock()
	go func() {
		response, promptErr := h.prompt(session.SessionId, "Use the shell tool to run exactly sleep 60 in the foreground, with a timeout of 90000 milliseconds. Reply DONE when it finishes.", promptMeta(5))
		done <- response
		failed <- promptErr
	}()
	started := func() bool {
		h.rec.mu.Lock()
		defer h.rec.mu.Unlock()
		for _, frame := range h.rec.raw[rawBeforeCancel:] {
			var payload struct {
				Event struct {
					Type string `json:"type"`
					Data struct {
						Metadata map[string]any `json:"metadata"`
					} `json:"data"`
				} `json:"event"`
			}
			if json.Unmarshal(frame, &payload) == nil && payload.Event.Type == "session.tool.progress" && payload.Event.Data.Metadata["shellID"] != nil {
				return true
			}
		}
		return false
	}
	require.Eventually(t, func() bool { return started() || len(failed) > 0 }, 45*time.Second, 25*time.Millisecond)
	if !started() {
		t.Fatalf("native command did not start: response=%+v error=%v text=%s tools=%s", <-done, <-failed, agentText(h.rec.snapshot()[beforeCancel:]), toolText(h.rec.snapshot()[beforeCancel:]))
	}
	require.NoError(t, h.conn.Cancel(h.ctx(), acp.CancelNotification{SessionId: session.SessionId}))
	require.NoError(t, <-failed)
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)
	_, err = h.conn.UnstableDeleteSession(h.ctx(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	list, err := h.conn.ListSessions(h.ctx(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
}

func toolText(updates []acp.SessionNotification) string {
	var text strings.Builder
	for _, update := range updates {
		if tool := update.Update.ToolCallUpdate; tool != nil {
			for _, item := range tool.Content {
				if item.Content != nil && item.Content.Content.Text != nil {
					text.WriteString(item.Content.Content.Text.Text)
				}
			}
		}
	}

	return text.String()
}

func nativeSessionID(t *testing.T, meta map[string]any) string {
	t.Helper()
	binding, ok := meta["opencode"].(map[string]any)
	require.True(t, ok)
	id, ok := binding["nativeSessionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func TestNativeImageReplayAndAccountUsage(t *testing.T) {
	if os.Getenv("ACP_GO_OPENCODEV2_RUN_LIVE_TOKENS") != "1" {
		t.Skip("live token gate")
	}
	store := acpcore.NewInMemorySessionStore()
	cwd := t.TempDir()
	model := os.Getenv("ACP_GO_OPENCODEV2_MODEL")
	h := newHarness(t, opencodeacp.WithHome(nativeHome(t)), opencodeacp.WithSessionStore(store), opencodeacp.WithDefaultModel(model))
	h.initialize(withLifecycle())
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, opencodeacp.WithSessionOpenCodeOptions(opencodeacp.NewOpenCodeOptions(opencodeacp.WithOpenCodeEffort("low")))))
	require.NoError(t, err)
	if strings.HasPrefix(model, "opencode-go/") {
		raw, readErr := h.conn.CallExtension(h.ctx(), opencodeacp.AccountUsageMethod, map[string]any{"sessionId": created.SessionId, "providerId": "opencode-go"})
		require.NoError(t, readErr)
		var report wire.AccountUsageResponse
		require.NoError(t, json.Unmarshal(raw, &report))
		require.NoError(t, report.Validate())
		if !report.Available {
			require.Equal(t, wire.AccountUsageNotReported, report.Reason, "authenticated routes without a matching account reader must remain unreported")
		}
	}
	raster := image.NewRGBA(image.Rect(0, 0, 16, 16))
	for y := range 16 {
		for x := range 16 {
			raster.SetRGBA(x, y, color.RGBA{R: 255, A: 255})
		}
	}
	var buffer bytes.Buffer
	require.NoError(t, png.Encode(&buffer, raster))
	encoded := base64.StdEncoding.EncodeToString(buffer.Bytes())
	request := wire.PromptRequest(created.SessionId, acp.TextBlock("Briefly describe this image. Do not use tools."), acp.ImageBlock(encoded, "image/png"))
	request.Meta = promptMeta(1)
	response, err := h.conn.Prompt(h.ctx(), request)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.NotEmpty(t, agentText(h.rec.snapshot()))
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	h.stop()
	imported := newHarness(t, opencodeacp.WithHome(nativeHome(t)), opencodeacp.WithSessionStore(store))
	imported.initialize(withLifecycle())
	_, err = imported.conn.LoadSession(imported.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	var replay []string
	for _, notification := range imported.rec.snapshot() {
		if chunk := notification.Update.UserMessageChunk; chunk != nil && chunk.Content.Image != nil {
			replay = append(replay, chunk.Content.Image.Data)
		}
	}
	require.Equal(t, []string{encoded}, replay)
}

func assertStoredDirectory(t *testing.T, store acpcore.SessionStore, id acp.SessionId, directory string) {
	t.Helper()
	rows, err := store.Load(t.Context(), string(id))
	require.NoError(t, err)
	require.NotEmpty(t, rows[""])
	var snapshot struct {
		Info struct {
			Location struct {
				Directory string `json:"directory"`
			} `json:"location"`
		} `json:"info"`
	}
	require.NoError(t, json.Unmarshal(rows[""][0], &snapshot))
	require.Equal(t, directory, snapshot.Info.Location.Directory)
}

func storedPendingInputs(t *testing.T, store acpcore.SessionStore, id acp.SessionId) map[string][]json.RawMessage {
	t.Helper()
	var record map[string]json.RawMessage
	_, found, err := sessionlog.Load(t.Context(), store, string(id), &record)
	require.NoError(t, err)
	require.True(t, found)
	var pending map[string][]map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(record["pendingInputs"], &pending))
	result := map[string][]json.RawMessage{}
	for sessionID, entries := range pending {
		for _, entry := range entries {
			delete(entry, "time")
			encoded, err := json.Marshal(entry)
			require.NoError(t, err)
			result[sessionID] = append(result[sessionID], encoded)
		}
	}
	return result
}

func TestNativePendingPlanInputs(t *testing.T) {
	if os.Getenv("ACP_GO_OPENCODEV2_RUN_INTEGRATION") != "1" {
		t.Skip("set ACP_GO_OPENCODEV2_RUN_INTEGRATION=1")
	}
	executable, cwd := harnessPath(t), t.TempDir()
	store := acpcore.NewInMemorySessionStore()
	newAgent := func() *opencodeacp.Agent {
		a := opencodeacp.NewAgent(opencodeacp.WithExecutablePath(executable), opencodeacp.WithHome(t.TempDir()), opencodeacp.WithSessionStore(store), opencodeacp.WithEnv(map[string]string{"OPENCODE_API_KEY": ""}))
		t.Cleanup(func() { _ = a.Close() })
		_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
		require.NoError(t, err)
		return a
	}
	a := newAgent()
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = a.SetSessionConfigOption(t.Context(), wire.SetConfigOptionRequest(created.SessionId, "mode", "plan"))
	require.NoError(t, err)
	pending := storedPendingInputs(t, store, created.SessionId)
	require.Len(t, pending[string(created.SessionId)], 1)
	require.NoError(t, a.Close())
	b := newAgent()
	_, err = b.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = b.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	require.Equal(t, pending, storedPendingInputs(t, store, created.SessionId))
	_, err = b.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	_, err = b.ResumeSession(t.Context(), wire.ResumeSessionRequest(created.SessionId, t.TempDir()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "backpressure")
	require.Equal(t, pending, storedPendingInputs(t, store, created.SessionId))
	require.NoError(t, b.Close())
}

func TestNativeCompletedCheckpointWithActiveChildAndShell(t *testing.T) {
	if os.Getenv("ACP_GO_OPENCODEV2_RUN_INTEGRATION") != "1" {
		t.Skip("set ACP_GO_OPENCODEV2_RUN_INTEGRATION=1")
	}
	config, requests, finish := localStreamingModel(t)
	executable, scratch, endpoint := nativeEndpoint(t)
	home := t.TempDir()
	store := acpcore.NewInMemorySessionStore()
	options := []opencodeacp.Option{executable, scratch, opencodeacp.WithHome(home), opencodeacp.WithSessionStore(store), opencodeacp.WithSeedFiles(map[string]string{"opencode.json": config}), opencodeacp.WithDefaultModel("probe/probe")}
	a := opencodeacp.NewAgent(options...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	cwd := t.TempDir()
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	native := endpoint()
	id := nativeSessionID(t, created.Meta)
	done := make(chan acp.PromptResponse, 1)
	failed := make(chan error, 1)
	go func() {
		response, promptErr := a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "root"))
		failed <- promptErr
		done <- response
	}()
	select {
	case <-requests:
	case <-time.After(5 * time.Second):
		t.Fatal("root did not reach provider")
	}
	var child struct {
		Data opencode.NativeSession `json:"data"`
	}
	require.NoError(t, native.Do(t.Context(), cwd, http.MethodPost, "/api/session", map[string]any{"parentID": id, "location": opencode.Location{Directory: cwd}, "model": opencode.ModelRef{ProviderID: "probe", ID: "probe"}}, &child))
	require.NoError(t, native.Do(t.Context(), cwd, http.MethodPost, opencode.SessionPath(child.Data.ID)+"/prompt", map[string]any{"id": opencode.NewMessageID(), "text": "child"}, nil))
	select {
	case <-requests:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not reach provider")
	}
	started, release := filepath.Join(t.TempDir(), "started"), filepath.Join(t.TempDir(), "release")
	shellID := opencode.NewMessageID()
	shellDone := make(chan error, 1)
	go func() {
		shellDone <- native.Do(t.Context(), cwd, http.MethodPost, opencode.SessionPath(id)+"/shell", map[string]any{"id": shellID, "command": "touch " + started + "; while [ ! -f " + release + " ]; do sleep 0.01; done; printf SHELL_DONE"}, nil)
	}()
	t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(started)

		return statErr == nil
	}, 5*time.Second, 10*time.Millisecond)
	var transcript struct {
		Data []opencode.NativeMessage `json:"data"`
	}
	require.NoError(t, native.Do(t.Context(), "", http.MethodGet, opencode.SessionPath(id)+"/message?order=asc", nil, &transcript))
	require.True(t, slices.ContainsFunc(transcript.Data, func(message opencode.NativeMessage) bool { return message.ID == shellID }))
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	select {
	case response := <-done:
		require.NoError(t, <-failed)
		require.Equal(t, acp.StopReasonCancelled, response.StopReason)
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation waited for the active child")
	}
	generation, err := store.Load(t.Context(), string(created.SessionId))
	require.NoError(t, err)
	var saved opencode.Export
	require.NoError(t, json.Unmarshal(generation[""][0], &saved))
	require.Equal(t, "idle", saved.Messages[len(saved.Messages)-1].Type)
	require.False(t, slices.ContainsFunc(saved.Messages, func(message opencode.NativeMessage) bool { return message.ID == shellID }))
	for _, row := range generation[""][1:] {
		var descendant opencode.Export
		require.NoError(t, json.Unmarshal(row, &descendant))
		require.Empty(t, descendant.Messages)
	}
	var active struct {
		Data map[string]opencode.NativeSessionStatus `json:"data"`
	}
	require.NoError(t, native.Do(t.Context(), "", http.MethodGet, "/api/session/active", nil, &active))
	require.Contains(t, active.Data, child.Data.ID)
	require.NoError(t, os.WriteFile(release, nil, 0600))
	require.NoError(t, <-shellDone)
	require.NoError(t, native.Interrupt(t.Context(), child.Data.ID))
	require.NoError(t, native.Wait(t.Context(), child.Data.ID))
	completed, err := native.Export(t.Context(), id)
	require.NoError(t, err)
	shellIndex := slices.IndexFunc(completed.Messages, func(message opencode.NativeMessage) bool { return message.ID == shellID })
	markerIndex := slices.IndexFunc(completed.Messages, func(message opencode.NativeMessage) bool {
		return message.ID == saved.Messages[len(saved.Messages)-1].ID
	})
	require.GreaterOrEqual(t, shellIndex, 0)
	require.Less(t, shellIndex, markerIndex)
	require.True(t, completed.ContainsHistory(saved))
	finish()
	response, err := a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "followup"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.NoError(t, a.Close())
	for _, restoreHome := range []string{home, t.TempDir()} {
		restored := opencodeacp.NewAgent(slices.Concat(options, []opencodeacp.Option{opencodeacp.WithHome(restoreHome), opencodeacp.WithSessionStore(copyGeneration(t, generation, created.SessionId))})...)
		t.Cleanup(func() { _ = restored.Close() })
		_, err = restored.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
		require.NoError(t, err)
		_, err = restored.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, cwd))
		require.NoError(t, err)
		require.NoError(t, restored.Close())
	}
}
