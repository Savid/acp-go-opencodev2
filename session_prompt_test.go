package opencodeacp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	stdimage "image"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

func promptRaster(t *testing.T) string {
	t.Helper()

	var b bytes.Buffer
	require.NoError(t, png.Encode(&b, stdimage.NewRGBA(stdimage.Rect(0, 0, 1, 1))))

	return base64.StdEncoding.EncodeToString(b.Bytes())
}
func TestImageInputOrder(t *testing.T) {
	s := &session{agent: NewAgent()}
	raster := acp.ImageBlock(promptRaster(t), "image/png")
	_, err := s.mapPrompt(t.Context(), []acp.ContentBlock{acp.TextBlock("before"), raster, acp.TextBlock("after")})
	require.Equal(t, "prompt", requestErrorData(t, err)["field"])
	mapped, err := s.mapPrompt(t.Context(), []acp.ContentBlock{acp.TextBlock("before"), raster, raster})
	require.NoError(t, err)
	_, request := s.promptRequest(mapped, "msg_1")
	data, err := json.Marshal(request)
	require.NoError(t, err)
	var native struct {
		Text  string           `json:"text"`
		Files []map[string]any `json:"files"`
	}
	require.NoError(t, json.Unmarshal(data, &native))
	require.Equal(t, "before", native.Text)
	require.Len(t, native.Files, 2)
}

// A turn is the session's before the prompt has anything to dispatch, so a
// session/cancel that lands while opencode is still being relaunched ends it
// there: the prompt answers cancelled, opencode never receives the turn, and
// the lifecycle stream carries nothing for it.
func TestPromptCancelledWhileRelaunching(t *testing.T) {
	t.Parallel()

	held := filepath.Join(t.TempDir(), "relaunch-held")
	h := newHarness(t, WithEnv(map[string]string{fakeOpenCodeEnv: "1", fakeOpenCodeEnvResumeHold: held}))
	h.initialize(withLifecycle())
	session := h.newSession()

	// The server dies mid-turn, so the next prompt starts a replacement and
	// looks the session up on it; the replacement never answers that lookup.
	_, err := h.prompt(session.SessionId, "CRASH", promptMeta(1))
	require.Equal(t, "process_exit", requestErrorData(t, err)["cause"])

	before := eventTypes(lifecycleEvents(h.rec.snapshot()))

	type result struct {
		resp acp.PromptResponse
		err  error
	}

	done := make(chan result, 1)

	go func() {
		resp, promptErr := h.prompt(session.SessionId, "HELLO", promptMeta(2))
		done <- result{resp, promptErr}
	}()

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(held)

		return statErr == nil
	}, testTimeout, time.Millisecond)

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))

	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, acp.StopReasonCancelled, got.resp.StopReason)
	require.Equal(t, before, eventTypes(lifecycleEvents(h.rec.snapshot())),
		"a prompt opencode never received opens no incarnation and publishes no acceptance")
}

// A $/cancel_request ends only the addressed handler's context: the turn it
// was driving stays the session's, completes successfully once, and the
// session keeps serving prompts.
func TestCancelRequestSettlesTheOriginalRequestOnce(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	permissionCtx, releasePermission := context.WithCancel(t.Context())
	defer releasePermission()
	entered := make(chan struct{}, 1)
	answer := h.rec.answer
	h.rec.answer = func(request acp.RequestPermissionRequest) acp.RequestPermissionResponse {
		entered <- struct{}{}
		<-permissionCtx.Done()

		return answer(request)
	}
	h.initialize(withLifecycle())
	session := h.newSession()

	request := wire.TextPromptRequest(session.SessionId, "PERMISSION")
	request.Meta = promptMeta(1)
	failed := make(chan error, 1)

	go func() {
		response, err := h.conn.Prompt(h.ctx(), request)
		if err == nil && response.StopReason != acp.StopReasonEndTurn {
			err = errors.New("request cancellation ended the native turn")
		}
		failed <- err
	}()

	select {
	case <-entered:
	case <-h.ctx().Done():
		t.Fatal("native permission request did not arrive")
	}
	require.NoError(t, h.input.cancelPrompt())

	_, busyErr := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "backpressure", requestErrorData(t, busyErr)["error"])
	releasePermission()
	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool {
		return slices.Contains(eventTypes(lifecycleEvents(updates)), "state_update:idle")
	})

	idles := 0

	for _, update := range h.rec.snapshot() {
		envelope, _ := update.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["type"] == "state_update" && event["state"] == "idle" {
			idles++
			require.Equal(t, "success", event["outcome"])
			require.Equal(t, string(acp.StopReasonEndTurn), event["stopReason"])
		}
	}

	require.Equal(t, 1, idles, "the turn the cancelled request started settles exactly once")
	require.NoError(t, <-failed)

	resp, err := h.prompt(session.SessionId, "HELLO", promptMeta(3))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, resp.StopReason)
}

// Handler cancellation can precede native dispatch; only session cancellation owns the turn.
func TestPromptOwnsCancellationBeforeDispatch(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(newRecorder(), nil)
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	response, err := a.Prompt(ctx, wire.TextPromptRequest(created.SessionId, "HELLO"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
}

func TestNativeExecutionErrorPreservesProviderDetails(t *testing.T) {
	t.Parallel()
	var data eventData
	require.NoError(t, json.Unmarshal([]byte(`{"error":{"type":"rate_limit","message":"account rate limit","status":429}}`), &data))
	verdict := judgeCycle(&cycle{state: cycleState{stopReason: stopReasonError, nativeError: data.Error}}, nil, false)
	failure := requestErrorData(t, verdict.failure)
	require.EqualValues(t, 429, failure["statusCode"])
	require.Equal(t, "rate_limit", failure["providerCode"])
	require.Equal(t, "account rate limit", failure["message"])
}

func TestCommandWithoutExecutionCompletes(t *testing.T) {
	a, s, _ := newDirectSession(t)
	rt := s.runtime
	originalURL, _ := url.Parse(rt.client.URL)
	forward := httputil.NewSingleHostReverseProxy(originalURL)
	accepted := make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/command") {
			w.WriteHeader(http.StatusNoContent)
			close(accepted)

			return
		}
		forward.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	rt.client.URL = proxy.URL
	done := make(chan error, 1)
	go func() { _, err := a.Prompt(t.Context(), wire.TextPromptRequest(s.id, "/inspect")); done <- err }()
	<-accepted
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		rt.cancel()
		<-done
		t.Fatal("native command returned 204 with no execution, but ACP prompt stayed pending")
	}
}
func TestCancelBeforeNativeAcceptance(t *testing.T) {
	a, s, _ := newDirectSession(t)
	rt := s.runtime
	originalURL, _ := url.Parse(rt.client.URL)
	forward := httputil.NewSingleHostReverseProxy(originalURL)
	entered := make(chan struct{})
	release := make(chan struct{})
	interrupted := make(chan struct{}, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/prompt") {
			close(entered)
			<-release
		}
		forward.ServeHTTP(w, r)
		if strings.HasSuffix(r.URL.Path, "/interrupt") {
			select {
			case interrupted <- struct{}{}:
			default:
			}
		}
	}))
	defer proxy.Close()
	rt.client.URL = proxy.URL
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.client.Interrupt(ctx, s.nativeID)
	}()
	done := make(chan acp.PromptResponse, 1)
	failed := make(chan error, 1)
	go func() {
		response, err := a.Prompt(t.Context(), wire.TextPromptRequest(s.id, "SLOW"))
		failed <- err
		done <- response
	}()
	<-entered
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(s.id)))
	<-interrupted
	close(release)
	select {
	case response := <-done:
		require.NoError(t, <-failed)
		require.Equal(t, acp.StopReasonCancelled, response.StopReason)
	case <-time.After(8 * time.Second):
		t.Fatal("cancelled prompt did not finish")
	}
	var active struct {
		Data map[string]opencode.NativeSessionStatus `json:"data"`
	}
	require.NoError(t, rt.client.Do(t.Context(), "", http.MethodGet, "/api/session/active", nil, &active))
	require.NotContains(t, active.Data, s.nativeID, "cancel reported success but native execution remained active")
}

func TestCommandCompletionWaitsForStream(t *testing.T) {
	a, s, _ := newDirectSession(t)
	rec := newRecorder()
	a.attach(rec, nil)
	rt := s.runtime
	originalURL, err := url.Parse(rt.client.URL)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(originalURL)
	var submitted atomic.Bool
	observed := make(chan struct{})
	var observe sync.Once
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/export") {
			fakeData(w, fakeExport{Info: opencode.NativeSession{ID: s.nativeID}, Messages: []opencode.NativeMessage{{ID: "msg_command_end", Type: "idle", Outcome: "succeeded"}}})

			return
		}
		if strings.HasSuffix(r.URL.Path, "/command") {
			submitted.Store(true)
			w.WriteHeader(http.StatusNoContent)

			return
		}
		if r.URL.Path == opencode.SessionPath(s.nativeID) && r.Method == http.MethodGet && submitted.Load() {
			native := opencode.NativeSession{ID: s.nativeID}
			native.Time.Idle = 100
			fakeData(w, native)
			observe.Do(func() { close(observed) })

			return
		}
		forward.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	rt.client.URL = proxy.URL
	done := make(chan error, 1)
	go func() { _, err := a.Prompt(t.Context(), wire.TextPromptRequest(s.id, "/inspect")); done <- err }()
	<-observed
	select {
	case err := <-done:
		t.Fatalf("command completed before its output stream: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	for _, event := range []opencode.Event{
		{Type: eventExecutionStarted, Created: 90, Data: json.RawMessage(`{}`)},
		{Type: "session.text.ended", Created: 95, Data: json.RawMessage(`{"assistantMessageID":"msg_command","ordinal":0,"text":"command output"}`)},
		{ID: "evt_command_end", Type: "session.execution.succeeded", Created: 100, Data: json.RawMessage(`{"sessionID":"` + s.nativeID + `"}`)},
	} {
		rt.events <- event
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		rt.cancel()
		<-done
		t.Fatal("command did not complete after its output stream")
	}
	require.Equal(t, "command output", agentText(rec.snapshot()))
}

func TestCommandControlBeforeExecution(t *testing.T) {
	for _, tc := range []struct{ name, event, data, method, answer string }{
		{"form", eventFormCreated, `{"form":{"id":"form_command","title":"Choose","fields":[{"key":"choice","type":"string","title":"Choice"}]}}`, http.MethodPost, `"choice":"blue"`},
		{"unsupported form", eventFormCreated, `{"form":{"id":"form_command","fields":[{"key":"choice","type":"external"}]}}`, http.MethodDelete, ""},
		{"unowned permission", eventPermissionAsked, `{"id":"permission_command","action":"shell","source":{"type":"command","id":"inspect"}}`, http.MethodPost, `"decision":"reject"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAgent(testOptions(t)...)
			t.Cleanup(func() { _ = a.Close() })
			rec := newRecorder()
			a.attach(rec, nil)
			request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
			withLifecycle()(&request)
			withFormElicitation()(&request)
			_, err := a.Initialize(t.Context(), request)
			require.NoError(t, err)
			rec.elicit = func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
				return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: map[string]any{"choice": "blue"}}}, nil
			}
			created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err)
			s, err := a.session(t.Context(), created.SessionId)
			require.NoError(t, err)
			rt := s.runtime
			target, err := url.Parse(rt.client.URL)
			require.NoError(t, err)
			forward := httputil.NewSingleHostReverseProxy(target)
			answered := make(chan struct{})
			type reply struct{ method, body string }
			replies := make(chan reply, 1)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/command") {
					rt.events <- opencode.Event{Type: tc.event, Data: json.RawMessage(tc.data)}
					select {
					case <-answered:
						w.WriteHeader(http.StatusNoContent)
					case <-time.After(3 * time.Second):
						w.WriteHeader(http.StatusInternalServerError)
					}

					return
				}
				if strings.Contains(r.URL.Path, "/form/form_command") || strings.Contains(r.URL.Path, "/permission/permission_command") {
					body, _ := io.ReadAll(r.Body)
					replies <- reply{method: r.Method, body: string(body)}
					w.WriteHeader(http.StatusNoContent)
					close(answered)

					return
				}
				forward.ServeHTTP(w, r)
			}))
			defer proxy.Close()
			rt.client.URL = proxy.URL
			prompt := wire.TextPromptRequest(s.id, "/inspect")
			prompt.Meta = promptMeta(1)
			response, err := a.Prompt(t.Context(), prompt)
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			select {
			case response := <-replies:
				require.Equal(t, tc.method, response.method)
				require.Contains(t, response.body, tc.answer)
			default:
				t.Fatal("command completed without resolving its control request")
			}
		})
	}
}

func TestCompletedExecutionSurvivesNativeSuccessor(t *testing.T) {
	for _, mode := range []string{"export", "interrupt", "queued", "command", "store failure", "success"} {
		t.Run(mode, func(t *testing.T) {
			checkNativeSuccessor(t, mode)
		})
	}
}

func checkNativeSuccessor(t *testing.T, mode string) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "queued-wake")
	options := testOptions(t, WithEnv(map[string]string{fakeOpenCodeEnv: "1", fakeOpenCodeEnvQueuedWake: marker, "GORACE": os.Getenv("GORACE") + " atexit_sleep_ms=0"}))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	trace := &traceLog{}
	store := &nativeFixtureStore{SessionStore: acpcore.NewInMemorySessionStore(), trace: trace}
	rec := &nativeFixtureRecorder{recorder: newRecorder(), trace: trace}
	a := NewAgent(append(options, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(rec, nil)
	initialized, err := a.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
	require.NoError(t, err)
	created, err := a.NewSession(ctx, wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(ctx, created.SessionId)
	require.NoError(t, err)
	rt := s.runtime
	native := *rt.client
	target, err := url.Parse(native.URL)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	var armed, woke atomic.Bool
	wakeErrors := make(chan error, 1)
	wake := func() {
		if !armed.Load() || !woke.CompareAndSwap(false, true) {
			return
		}
		wakeErr := native.Wait(ctx, s.nativeID)
		if wakeErr == nil {
			wakeErr = native.Do(ctx, s.cwd, http.MethodPost, opencode.SessionPath(s.nativeID)+"/prompt", map[string]any{"id": opencode.NewMessageID(), "text": "SLOW"}, nil)
		}
		wakeErrors <- wakeErr
	}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode != "interrupt" && mode != "queued" && strings.HasSuffix(r.URL.Path, "/export") {
			wake()
		}
		forward.ServeHTTP(w, r)
		if mode == "interrupt" && strings.HasSuffix(r.URL.Path, "/interrupt") {
			wake()
		}
	}))
	t.Cleanup(proxy.Close)
	rt.client.URL = proxy.URL
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = native.Interrupt(cleanupCtx, s.nativeID)
	})
	trace.reset()
	type result struct {
		response acp.PromptResponse
		err      error
	}
	done := make(chan result, 1)
	prompt := "SLOW"
	if mode == "command" {
		prompt = "/inspect SLOW"
	}
	if mode == "success" {
		prompt = "HELLO"
		armed.Store(true)
	}
	request := wire.TextPromptRequest(s.id, prompt)
	request.Meta = promptMeta(1)
	go func() {
		response, promptErr := a.Prompt(ctx, request)
		trace.add("response")
		done <- result{response, promptErr}
	}()
	if mode != "success" {
		require.Eventually(t, func() bool {
			s.mu.Lock()
			current := s.turn
			s.mu.Unlock()

			return current != nil && s.turnAccepted(current)
		}, 3*time.Second, time.Millisecond)
		if mode == "queued" {
			require.NoError(t, os.WriteFile(marker, nil, 0600))
		}
		store.fail.Store(mode == "store failure")
		armed.Store(true)
		require.NoError(t, a.Cancel(ctx, wire.CancelRequest(s.id)))
	}
	var got result
	select {
	case got = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("completed turn waited for its native successor")
	}
	if mode != "queued" {
		require.NoError(t, <-wakeErrors)
	}
	if mode == "store failure" {
		require.Equal(t, "session mirror commit failed", requestErrorData(t, got.err)[wire.FieldMessage])
		require.False(t, rt.alive())
		require.NotContains(t, trace.snapshot(), "idle")
		store.fail.Store(false)

		return
	}
	require.NoError(t, got.err)
	expected := acp.StopReasonCancelled
	if mode == "success" {
		expected = acp.StopReasonEndTurn
	}
	require.Equal(t, expected, got.response.StopReason)
	require.True(t, rt.alive())
	requireCommittedBeforeResponse(t, trace.snapshot())
	var record sessionRecord
	rows, found, err := sessionlog.Load(ctx, store, string(s.id), &record)
	require.NoError(t, err)
	require.True(t, found)
	markers := 0
	for _, m := range nativeMessages(rows, s.nativeID) {
		if m.Type == "idle" {
			markers++
		}
	}
	require.Equal(t, 1, markers, "foreground marker must be durable while its successor is running")
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		return s.cycle != nil && s.turn == nil
	}, 3*time.Second, time.Millisecond)
	var active struct {
		Data map[string]opencode.NativeSessionStatus `json:"data"`
	}
	require.NoError(t, native.Do(ctx, "", http.MethodGet, "/api/session/active", nil, &active))
	require.Contains(t, active.Data, s.nativeID, "settlement must leave successor work running")
	if mode == "queued" {
		waitCtx, stop := context.WithTimeout(ctx, 25*time.Millisecond)
		require.ErrorIs(t, native.Wait(waitCtx, s.nativeID), context.DeadlineExceeded)
		stop()
	}
	require.NoError(t, native.Interrupt(ctx, s.nativeID))
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		return s.cycle == nil
	}, 3*time.Second, time.Millisecond)
	rows, found, err = sessionlog.Load(ctx, store, string(s.id), &record)
	require.NoError(t, err)
	require.True(t, found)
	markers = 0
	for _, m := range nativeMessages(rows, s.nativeID) {
		if m.Type == "idle" {
			markers++
		}
	}
	require.Equal(t, 2, markers)
	activity := false
	for _, event := range lifecycleEvents(rec.snapshot()) {
		if event["type"] == "state_update" && event["cause"] == "activity" {
			activity = true
		}
	}
	require.True(t, activity)
	next := wire.TextPromptRequest(s.id, "HELLO")
	next.Meta = promptMeta(2)
	responseNext, err := a.Prompt(ctx, next)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, responseNext.StopReason)
	require.NoError(t, lifecycle.CheckAttribution(negotiatedAnswer(t, initialized), sessionFrames(t, rec.snapshot(), s.id)))
	require.NoError(t, a.Close())
	restored := NewAgent(append(options, WithSessionStore(store), WithHome(t.TempDir()))...)
	t.Cleanup(func() { _ = restored.Close() })
	_, err = restored.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	_, err = restored.LoadSession(ctx, wire.LoadSessionRequest(s.id, s.cwd))
	require.NoError(t, err)
}

func requireCommittedBeforeResponse(t *testing.T, entries []string) {
	t.Helper()
	commit, idle, response := -1, -1, -1
	for i, entry := range entries {
		switch entry {
		case "commit":
			if commit < 0 {
				commit = i
			}
		case "idle":
			if idle < 0 {
				idle = i
			}
		case "response":
			response = i
		}
	}
	require.Greater(t, idle, commit)
	require.GreaterOrEqual(t, commit, 0)
	require.Greater(t, response, idle)
}

func TestShutdownInterruptEndsBindingWithoutSnapshot(t *testing.T) {
	a, s, _ := newDirectSession(t)
	rt := s.runtime
	target, err := url.Parse(rt.client.URL)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	var snapshots atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/export") || strings.HasSuffix(r.URL.Path, "/wait") {
			snapshots.Add(1)
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	rt.client.URL = proxy.URL
	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(s.id, "SHUTDOWN"))
	data := requestErrorData(t, err)
	require.Equal(t, wire.CauseTransport, data[wire.FieldCause])
	require.NotEqual(t, "session mirror commit failed", data[wire.FieldMessage])
	require.Zero(t, snapshots.Load())
	require.False(t, rt.alive())
}
