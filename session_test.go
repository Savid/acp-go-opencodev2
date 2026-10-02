package opencodeacp

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	acp "github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func TestSharedRuntimeAndFreshHomeRestore(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	cwd := t.TempDir()
	first, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	second, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	one, err := a.session(t.Context(), first.SessionId)
	require.NoError(t, err)
	two, err := a.session(t.Context(), second.SessionId)
	require.NoError(t, err)
	require.Same(t, one.runtime.server, two.runtime.server)
	response, err := a.Prompt(t.Context(), wire.TextPromptRequest(first.SessionId, "remember"))
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	_, err = a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: first.SessionId})
	require.NoError(t, err)
	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(second.SessionId, "peer"))
	require.NoError(t, err)
	require.NoError(t, a.Close())
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(first.SessionId, cwd))
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "hello remember")
	_, err = h.prompt(first.SessionId, "restored", nil)
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "hello restored")
}
func TestNativeCallbacksHaveToolIdentity(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize(withLifecycle(), withFormElicitation())
	h.rec.elicit = func(request acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
		require.NotNil(t, request.Form)
		require.Contains(t, request.Form.RequestedSchema.Properties, "0")

		return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: map[string]any{"0": "blue"}}}, nil
	}
	session := h.newSession(WithSessionRawEvents(true))
	for i, text := range []string{"PERMISSION", "QUESTION"} {
		_, err := h.prompt(session.SessionId, text, promptMeta(i))
		require.NoError(t, err)
	}
	h.rec.mu.Lock()
	permissions := append([]acp.RequestPermissionRequest(nil), h.rec.permissions...)
	raw := len(h.rec.raw)
	h.rec.mu.Unlock()
	require.Len(t, permissions, 1)
	require.Positive(t, raw)
	callID := permissions[0].ToolCall.ToolCallId
	found := false
	for _, notification := range h.rec.snapshot() {
		if call := notification.Update.ToolCall; call != nil && call.ToolCallId == callID {
			found = true
		}
	}
	require.True(t, found, "permission references an emitted native tool")
	events := lifecycleEvents(h.rec.snapshot())
	count := 0
	for _, event := range events {
		if event["type"] == "action_update" {
			count++
		}
	}
	require.Equal(t, 4, count)
}
func TestEnvironmentOptionsSurviveRestore(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	cwd := t.TempDir()
	dir := filepath.Join(t.TempDir(), "bin")
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	options := NewOpenCodeOptions(WithOpenCodeEnv(map[string]string{"PATH": "/usr/bin", "CUSTOM": "one"}), WithOpenCodeExtraPathDirs(dir), WithOpenCodeMode("plan"), WithOpenCodePermission("allow"), WithOpenCodeEffort("high"))
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, WithSessionOpenCodeOptions(options)))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "ENV", nil)
	require.NoError(t, err)
	require.Contains(t, agentText(h.rec.snapshot()), "CUSTOM")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId), Subpath: sessionlog.ConfigSubpath})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var record sessionRecord
	require.NoError(t, json.Unmarshal(rows[0], &record))
	require.Equal(t, "plan", record.Mode)
	require.Equal(t, "high", record.Effort)
	require.Equal(t, "allow", record.Permission)
	require.Equal(t, options.Env, record.Env)
	require.Equal(t, []string{dir}, record.ExtraPathDirs)
}
func TestRuntimeDeathRebindsPeers(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t, WithLogger(slog.Default()))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	one, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	two, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	old := a.runtime
	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(one.SessionId, "CRASH"))
	require.Error(t, err)
	require.Equal(t, "process_exit", requestErrorData(t, err)["cause"])
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = a.Prompt(ctx, wire.TextPromptRequest(two.SessionId, "peer survived"))
	require.NoError(t, err)
	require.NotSame(t, old, a.runtime)
	_, err = a.Prompt(ctx, wire.TextPromptRequest(one.SessionId, "first survived"))
	require.NoError(t, err)
}

func TestEphemeralSessionRecoversAfterRuntimeLoss(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"prompt", "load", "resume"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			store := &countingStore{SessionStore: acpcore.NewInMemorySessionStore()}
			a := NewAgent(testOptions(t, WithSessionStore(store))...)
			t.Cleanup(func() { _ = a.Close() })
			rec := newRecorder()
			a.attach(rec, nil)
			_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
			require.NoError(t, err)
			cwd := t.TempDir()
			created, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd, ephemeralMeta()))
			require.NoError(t, err)
			_, err = a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "before"))
			require.NoError(t, err)
			s, err := a.session(t.Context(), created.SessionId)
			require.NoError(t, err)
			original := s.runtime
			history, err := original.client.Export(t.Context(), s.nativeID)
			require.NoError(t, err)
			peer, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir(), ephemeralMeta()))
			require.NoError(t, err)
			_, err = a.Prompt(t.Context(), wire.TextPromptRequest(peer.SessionId, "CRASH"))
			require.Error(t, err)
			<-original.done
			switch operation {
			case "load":
				_, err = a.LoadSession(t.Context(), wire.LoadSessionRequest(s.id, cwd))
			case "resume":
				_, err = a.ResumeSession(t.Context(), wire.ResumeSessionRequest(s.id, cwd))
			case "prompt":
				_, err = a.Prompt(t.Context(), wire.TextPromptRequest(s.id, "after"))
			}
			require.NoError(t, err)
			restored, err := a.session(t.Context(), s.id)
			require.NoError(t, err)
			require.NotSame(t, original, restored.runtime)
			require.True(t, restored.runtime.alive())
			after, err := restored.runtime.client.Export(t.Context(), restored.nativeID)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(after.Messages), len(history.Messages))
			require.Equal(t, history.Messages, after.Messages[:len(history.Messages)])
			listed, err := a.ListSessions(t.Context(), wire.ListSessionsRequest())
			require.NoError(t, err)
			require.Empty(t, listed.Sessions)
			_, err = a.UnstableDeleteSession(t.Context(), wire.DeleteSessionRequest(s.id))
			require.NoError(t, err)
			require.Zero(t, store.replaces.Load())
			require.Zero(t, store.deletes.Load())
		})
	}
}

func TestImageReplaySurvivesNativeFileRemoval(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, WithSessionRawEvents(true)))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "IMAGE", nil)
	require.NoError(t, err)
	var original string
	for _, notification := range h.rec.snapshot() {
		if update := notification.Update.ToolCallUpdate; update != nil {
			for _, content := range update.Content {
				if content.Content != nil && content.Content.Content.Image != nil {
					original = content.Content.Content.Image.Data
				}
			}
		}
	}
	require.NotEmpty(t, original)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(cwd, "output.png")))
	restored := newHarness(t, WithSessionStore(store))
	restored.initialize()
	_, err = restored.conn.LoadSession(restored.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err)
	var replayed string
	for _, notification := range restored.rec.snapshot() {
		if update := notification.Update.ToolCallUpdate; update != nil {
			for _, content := range update.Content {
				if content.Content != nil && content.Content.Content.Image != nil {
					replayed = content.Content.Content.Image.Data
				}
			}
		}
	}
	require.Equal(t, original, replayed)
}

// The SDK cancels a prompt's request context when the next prompt for the same
// session arrives; the refused peer prompt leaves the turn in flight running to
// its own end.
func TestRefusedPeerPromptLeavesTheLiveTurn(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle(), withFormElicitation())

	entered := make(chan struct{})
	release := make(chan struct{})
	h.rec.elicit = func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
		close(entered)
		<-release

		return acp.UnstableCreateElicitationResponse{Accept: &acp.UnstableCreateElicitationAccept{Content: map[string]any{"0": "blue"}}}, nil
	}

	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)
	failed := make(chan error, 1)

	go func() {
		response, err := h.prompt(session.SessionId, "QUESTION", promptMeta(1))
		done <- response
		failed <- err
	}()

	<-entered

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "backpressure", requestErrorData(t, err)[wire.FieldError])

	close(release)
	require.NoError(t, <-failed)
	require.Equal(t, acp.StopReasonEndTurn, (<-done).StopReason)
}

// An explicit session/cancel ends the turn in flight, and its terminal idle
// reports the cancelled outcome.
func TestSessionCancelEndsTheTurnAsCancelled(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize(withLifecycle())
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)
	failed := make(chan error, 1)

	go func() {
		response, err := h.prompt(session.SessionId, "SLOW", promptMeta(1))
		done <- response
		failed <- err
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(lifecycleEvents(updates)) >= 3 })

	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.NoError(t, <-failed)
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)

	cancelled := false

	for _, event := range lifecycleEvents(h.rec.snapshot()) {
		if event["type"] == "state_update" && event["state"] == "idle" && event["outcome"] == "cancelled" {
			cancelled = true
		}
	}

	require.True(t, cancelled, "the turn's terminal idle reports the cancelled outcome")
}

// While the pump runs a generation's lifecycle tail the session still holds the
// binding, so a prompt that arrives in that window joins the departing pump and
// relaunches instead of dispatching on a binding no pump is left to settle.
func TestDepartingBindingIsNeverHandedToAPrompt(t *testing.T) {
	t.Parallel()

	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)

	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	require.NotNil(t, rt)

	// A registered dialog holds the tail at callbacks.Wait, so the generation
	// stays in the window the fence is about until the test releases it.
	var once sync.Once

	entered := make(chan struct{})

	release, registered := s.registerDialog("held-tail", func(error) { once.Do(func() { close(entered) }) })
	require.True(t, registered)
	t.Cleanup(release)

	rt.cancel()

	select {
	case <-entered:
	case <-time.After(testTimeout):
		t.Fatal("the ended generation never reached its lifecycle tail")
	}

	s.mu.Lock()
	bound := s.runtime == rt
	s.mu.Unlock()
	require.True(t, bound, "the tail runs before the binding is cleared")
	require.False(t, rt.alive(), "a binding whose generation ended routes nothing")

	settled := make(chan error, 1)

	go func() {
		_, promptErr := a.Prompt(context.WithoutCancel(t.Context()), wire.TextPromptRequest(created.SessionId, "after the tail"))
		settled <- promptErr
	}()

	select {
	case <-settled:
		t.Fatal("the prompt settled on the binding whose generation was still ending")
	case <-time.After(200 * time.Millisecond):
	}

	release()

	select {
	case promptErr := <-settled:
		require.NoError(t, promptErr, "the prompt runs on the incarnation that replaced the departed one")
	case <-time.After(testTimeout):
		t.Fatal("the prompt never settled: it was dispatched on a departed binding")
	}
}

func TestEndedBindingCannotCancelReplacementPrompt(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s := a.sessions[created.SessionId]
	old := s.runtime
	old.server.mu.Lock()
	unlock := sync.OnceFunc(old.server.mu.Unlock)
	defer unlock()
	old.cancel()
	type result struct {
		response acp.PromptResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, promptErr := a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "replacement prompt"))
		done <- result{response: response, err: promptErr}
	}()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		return s.turn != nil
	}, testTimeout, time.Millisecond)
	unlock()
	select {
	case answered := <-done:
		require.NoError(t, answered.err)
		require.Equal(t, acp.StopReasonEndTurn, answered.response.StopReason, "old binding cleanup cancelled a prompt that had not bound to it")
	case <-time.After(testTimeout):
		t.Fatal("replacement prompt did not settle")
	}
}

// A close that lands while a relaunch is still waiting for the server leaves
// the session closed and binds nothing to it, so the session loads cleanly
// afterwards.
func TestCloseDuringRelaunchBindsNothing(t *testing.T) {
	t.Parallel()

	held := filepath.Join(t.TempDir(), "start-held")
	h := newHarness(t, WithEnv(map[string]string{fakeOpenCodeEnv: "1", fakeOpenCodeEnvStartHold: held}))
	h.initialize(withLifecycle())
	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	_, err = h.prompt(session.SessionId, "CRASH", promptMeta(1))
	require.Equal(t, "process_exit", requestErrorData(t, err)["cause"])
	require.NoError(t, os.WriteFile(held+".armed", nil, 0o600))

	relaunched := make(chan error, 1)

	go func() {
		_, configErr := h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configModel, "fake/text"))
		relaunched <- configErr
	}()

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(held)

		return statErr == nil
	}, testTimeout, time.Millisecond)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	require.NoError(t, os.Remove(held))

	err = <-relaunched
	require.Equal(t, "unknown session", requestErrorData(t, err)[wire.FieldError], "the relaunch answers for the session that closed under it")

	_, err = h.prompt(session.SessionId, "HELLO", promptMeta(2))
	require.Equal(t, "unknown session", requestErrorData(t, err)[wire.FieldError])

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.NoError(t, err, "nothing from the abandoned relaunch stays bound to the native session")
}

// An empty executable path resolves the opencode binary from the base PATH.
func TestEmptyExecutablePathResolvesOpenCodeFromPath(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	require.NoError(t, os.Symlink(os.Args[0], filepath.Join(base, "opencode")))

	h := newHarness(t, WithExecutablePath(""), WithEnv(map[string]string{fakeOpenCodeEnv: "1", "PATH": base}))
	h.initialize(withLifecycle())
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "HELLO", promptMeta(1))
	require.NoError(t, err)
}

// A harness that cannot start is the native_start class of the internal failure.
func TestNativeStartFailureClass(t *testing.T) {
	t.Parallel()

	h := newHarness(t, WithExecutablePath(filepath.Join(t.TempDir(), "missing-opencode")))
	h.initialize()

	_, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Equal(t, -32603, requestErrorCode(t, err))
	require.Equal(t, map[string]any{wire.FieldError: "opencode_internal_failure", wire.FieldClass: "native_start"}, requestErrorData(t, err))
}

func TestCloseJoinsFirstMirrorAndFencesOpening(t *testing.T) {
	t.Parallel()
	for _, agentClose := range []bool{false, true} {
		name := "close_session"
		if agentClose {
			name = "close_agent"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := &commitBarrier{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan acpcore.SessionKey, 1), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(store.release) })
			t.Cleanup(release)
			store.block.Store(true)
			a := NewAgent(testOptions(t, WithSessionStore(store))...)
			t.Cleanup(func() { _ = a.Close() })
			rec := newRecorder()
			a.attach(rec, nil)
			request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
			withLifecycle()(&request)
			_, err := a.Initialize(t.Context(), request)
			require.NoError(t, err)
			cwd := t.TempDir()
			created := make(chan error, 1)
			go func() {
				_, createErr := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
				created <- createErr
			}()
			var key acpcore.SessionKey
			select {
			case key = <-store.entered:
			case <-time.After(testTimeout):
				t.Fatal("creation did not reach its first mirror")
			}
			s, err := a.session(t.Context(), acp.SessionId(key.SessionID))
			require.NoError(t, err)
			s.mu.Lock()
			rt := s.runtime
			s.mu.Unlock()
			closed := make(chan error, 1)
			go func() {
				if agentClose {
					closed <- a.Close()

					return
				}
				_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: acp.SessionId(key.SessionID)})
				closed <- err
			}()
			require.Eventually(t, func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()

				return s.closing
			}, testTimeout, time.Millisecond)
			select {
			case err := <-closed:
				t.Fatalf("close returned while the first mirror was blocked: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			release()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(testTimeout):
				t.Fatal("close did not join creation")
			}
			select {
			case err := <-created:
				require.Error(t, err, "a closing session must refuse its opening publication")
			case <-time.After(testTimeout):
				t.Fatal("creation did not release its gate before cleanup")
			}
			require.False(t, s.lc.Active())
			before := len(rec.snapshot())
			require.Error(t, s.openStream(t.Context(), rt))
			require.Len(t, rec.snapshot(), before, "closed session published commands or a lifecycle snapshot")
			require.False(t, s.lc.Active())
		})
	}
}

func TestOpeningRejectsReplacedNativeGeneration(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	stale := s.runtime
	s.mu.Unlock()
	transport, meta := prepareOpeningResponse(t)
	a.attach(rec, transport)
	require.NoError(t, a.scheduleOpen(transport.RequestContext(t.Context(), meta), s))
	s.stopRuntime(t.Context(), stale)
	require.False(t, s.lc.Active())
	fresh, err := s.ensureRuntime(t.Context())
	require.NoError(t, err)
	require.NotSame(t, stale, fresh)
	require.True(t, s.lc.Active())
	before := len(rec.snapshot())
	require.Error(t, s.openStream(t.Context(), stale))
	require.Len(t, rec.snapshot(), before, "stale deferred opening published on the replacement generation")
	require.True(t, s.lc.Active(), "stale opening fenced the replacement stream")
	finishOpeningResponse(t, transport, s.id)
	require.Len(t, rec.snapshot(), before, "stale hook published on the replacement generation")
	current, err := a.session(t.Context(), s.id)
	require.NoError(t, err)
	require.Same(t, s, current)
	require.True(t, s.lc.Active(), "stale hook closed the replacement stream")
}

// openingCallbackClient exercises a synchronous embedded callback into admission.
type openingCallbackClient struct {
	*recorder
	agent *Agent
}

func (c *openingCallbackClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if err := c.agent.Cancel(ctx, acp.CancelNotification{SessionId: notification.SessionId}); err != nil {
		return err
	}

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestOpeningAllowsSynchronousSessionCallback(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(&openingCallbackClient{recorder: rec, agent: a}, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	require.NotEmpty(t, rec.snapshot())
}

func prepareOpeningResponse(t *testing.T) (*wire.Transport, map[string]any) {
	t.Helper()
	transport := wire.NewTransport(strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"session/new\",\"params\":{}}\n"), io.Discard)
	t.Cleanup(transport.Close)
	transport.Start()
	inbound, err := io.ReadAll(transport.Reader())
	require.NoError(t, err)

	var frame struct {
		Params acp.NewSessionRequest `json:"params"`
	}

	require.NoError(t, json.Unmarshal(inbound, &frame))

	return transport, frame.Params.Meta
}

func finishOpeningResponse(t *testing.T, transport *wire.Transport, id acp.SessionId) {
	t.Helper()
	_, err := transport.Writer().Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	require.NoError(t, transport.AwaitSession(ctx, id))
}

func TestDeferredOpeningFailureDetachesSession(t *testing.T) {
	t.Parallel()
	for _, ephemeral := range []bool{false, true} {
		name := "stored"
		if ephemeral {
			name = "ephemeral"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a := NewAgent(testOptions(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))...)
			t.Cleanup(func() { _ = a.Close() })
			request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
			withLifecycle()(&request)
			_, err := a.Initialize(t.Context(), request)
			require.NoError(t, err)
			transport, meta := prepareOpeningResponse(t)
			a.attach(&firstOpenFailureClient{recorder: newRecorder()}, transport)
			newRequest := wire.NewSessionRequest(t.TempDir())
			newRequest.Meta = meta
			if ephemeral {
				newRequest.Meta = wire.SessionMeta{Ephemeral: true}.Apply(meta)
			}
			created, err := a.NewSession(t.Context(), newRequest)
			require.NoError(t, err)
			a.mu.Lock()
			s := a.sessions[created.SessionId]
			a.mu.Unlock()
			require.NotNil(t, s)
			finishOpeningResponse(t, transport, created.SessionId)
			require.False(t, s.lc.Active())
			a.mu.Lock()
			_, installed := a.sessions[created.SessionId]
			a.mu.Unlock()
			require.False(t, installed, "failed deferred publication retained the active slot")
			s.mu.Lock()
			closed := s.closing
			s.mu.Unlock()
			require.True(t, closed)
		})
	}
}

type cancellingBackgroundClient struct {
	*recorder
	agent          *Agent
	terminalOnly   bool
	terminalCalled bool
}

func (c *cancellingBackgroundClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if c.terminalOnly {
		envelope, _ := notification.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["state"] != "idle" {
			return c.recorder.SessionUpdate(ctx, notification)
		}
		c.terminalCalled = true
	}
	done := make(chan error, 1)
	go func() { done <- c.agent.Cancel(ctx, wire.CancelRequest(notification.SessionId)) }()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		return context.DeadlineExceeded
	}
}

func TestBackgroundPublicationAllowsCancelCallback(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	a.attach(&cancellingBackgroundClient{recorder: rec, agent: a}, nil)
	s.openAgentCycle(t.Context(), rt)
	a.attach(rec, nil)
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	require.NoError(t, s.cycleFailure(c))
	require.True(t, s.cycleCancelled(c))
	s.settleAgentCycle(t.Context(), rt, c)
}

func TestCancelAgentOriginResolvesDialogsAndSettlesCancelled(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	s.openAgentCycle(t.Context(), rt)
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	dialogCtx, cancelDialog := context.WithCancelCause(t.Context())
	defer cancelDialog(nil)
	unregister, admitted := s.registerDialog("permission", cancelDialog)
	require.True(t, admitted)
	require.NoError(t, s.lc.ActionPending(t.Context(), c.Cycle, "permission", lifecycle.ActionPermission))
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	require.ErrorIs(t, context.Cause(dialogCtx), errDialogCancelled)
	unregister()
	lateCtx, cancelLate := context.WithCancelCause(t.Context())
	defer cancelLate(nil)
	release, admitted := s.registerDialog("late", cancelLate)
	require.False(t, admitted)
	release()
	require.ErrorIs(t, context.Cause(lateCtx), errDialogCancelled)
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(created.SessionId)))
	prompt := wire.TextPromptRequest(created.SessionId, "HELLO")
	prompt.Meta = promptMeta(1)
	_, err = a.Prompt(t.Context(), prompt)
	require.Equal(t, "backpressure", requestErrorData(t, err)["error"])
	s.settleAgentCycle(t.Context(), rt, c)
	s.mu.Lock()
	active := s.cycle
	s.mu.Unlock()
	require.Nil(t, active)
	cancelled := false
	for _, notification := range rec.snapshot() {
		envelope, _ := notification.Meta[wire.LifecycleKey].(map[string]any)
		event, _ := envelope["event"].(map[string]any)
		if event["type"] == "state_update" && event["state"] == "idle" && event["outcome"] == "cancelled" {
			cancelled = true
		}
	}
	require.True(t, cancelled)
}

func TestTerminalPublicationCannotInterruptNextCycle(t *testing.T) {
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	rec := newRecorder()
	a.attach(rec, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()
	terminalClient := &cancellingBackgroundClient{recorder: rec, agent: a, terminalOnly: true}
	a.attach(terminalClient, nil)
	s.openAgentCycle(t.Context(), rt)
	s.mu.Lock()
	c := s.cycle
	s.mu.Unlock()
	require.NotNil(t, c)
	require.NoError(t, s.cycleFailure(c))
	require.False(t, s.cycleCancelled(c))
	s.settleAgentCycle(t.Context(), rt, c)
	require.False(t, s.cycleCancelled(c))
	require.True(t, terminalClient.terminalCalled)
	s.callbacks.Wait()
	a.attach(rec, nil)
}

// TestCloseCommitsAnAgentCycleBeforeTerminalizing proves close commits owed
// state before it terminalizes an open agent-origin cycle: a failed commit
// fences with no terminal idle, and a clean commit emits the cancelled idle.
func TestCloseCommitsAnAgentCycleBeforeTerminalizing(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		name := "committed"
		if failCommit {
			name = "failed commit"
		}

		t.Run(name, func(t *testing.T) {
			store := &nativeFixtureStore{SessionStore: acpcore.NewInMemorySessionStore(), trace: &traceLog{}}
			a := NewAgent(testOptions(t, WithSessionStore(store))...)
			t.Cleanup(func() { _ = a.Close() })
			rec := newRecorder()
			a.attach(rec, nil)
			request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
			withLifecycle()(&request)
			_, err := a.Initialize(t.Context(), request)
			require.NoError(t, err)
			created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err)
			s, err := a.session(t.Context(), created.SessionId)
			require.NoError(t, err)

			s.mu.Lock()
			rt := s.runtime
			s.mu.Unlock()

			require.NoError(t, s.commitMirror(t.Context(), rt, nil), "a first mirror makes the session persisted")
			s.openAgentCycle(t.Context(), rt)
			s.mu.Lock()
			require.NotNil(t, s.cycle)
			s.mu.Unlock()

			store.fail.Store(failCommit)
			closeErr := s.close(t.Context())

			terminalIdle := false
			for _, notification := range rec.snapshot() {
				envelope, _ := notification.Meta[wire.LifecycleKey].(map[string]any)
				event, _ := envelope["event"].(map[string]any)
				if event["type"] == "state_update" && event["state"] == "idle" {
					if _, ok := event["outcome"]; ok {
						terminalIdle = true
					}
				}
			}

			if failCommit {
				require.Error(t, closeErr, "a failed commit fails the close")
				require.False(t, terminalIdle, "a failed commit leaves no terminal idle")

				return
			}

			require.NoError(t, closeErr)
			require.True(t, terminalIdle, "a clean commit terminalizes the agent cycle")
		})
	}
}

// A delete that arrives while the opening publication is still queued behind
// the establishing response waits for that publication: the stream opens with
// its snapshot, then the close fences it, and nothing answers unknown session.
func TestDeleteAwaitsDeferredOpening(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	transport, meta := prepareOpeningResponse(t)
	rec := newRecorder()
	a.attach(rec, transport)
	newRequest := wire.NewSessionRequest(t.TempDir())
	newRequest.Meta = wire.SessionMeta{Ephemeral: true}.Apply(meta)
	created, err := a.NewSession(t.Context(), newRequest)
	require.NoError(t, err)
	a.mu.Lock()
	s := a.sessions[created.SessionId]
	a.mu.Unlock()
	require.NotNil(t, s)
	deleted := make(chan error, 1)
	go func() {
		_, deleteErr := a.UnstableDeleteSession(t.Context(), wire.DeleteSessionRequest(created.SessionId))
		deleted <- deleteErr
	}()
	select {
	case err := <-deleted:
		t.Fatalf("delete returned before the establishing response was written: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.Empty(t, rec.snapshot(), "publication preceded the establishing response")
	finishOpeningResponse(t, transport, created.SessionId)
	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(testTimeout):
		t.Fatal("delete did not complete after the opening publication")
	}
	types := eventTypes(lifecycleEvents(rec.snapshot()))
	require.NotEmpty(t, types, "delete fenced the stream before its opening publication")
	require.Equal(t, "lifecycle_snapshot", types[0], "stream did not open with its snapshot")
	require.False(t, s.lc.Active())
	a.mu.Lock()
	_, installed := a.sessions[created.SessionId]
	a.mu.Unlock()
	require.False(t, installed)
}

func TestUsageIgnoresReplayedResponses(t *testing.T) {
	t.Parallel()

	for _, script := range []string{"REPLAY"} {
		t.Run(script, func(t *testing.T) {
			t.Parallel()

			h := newHarness(t)
			h.initialize()
			session := h.newSession()

			resp, err := h.prompt(session.SessionId, script, nil)
			require.NoError(t, err)
			require.Equal(t, []acp.SessionUsageUpdate{
				callReport(fakeContextWindow, 1175, 100, 1000, 50, 25),
			}, usageUpdates(h.rec.snapshot()))
			require.Equal(t, 100, resp.Usage.InputTokens)
			require.Equal(t, 20, resp.Usage.OutputTokens)
			require.Equal(t, 1000, *resp.Usage.CachedReadTokens)
			require.Equal(t, 50, *resp.Usage.CachedWriteTokens)
			require.Equal(t, 5, *resp.Usage.ThoughtTokens)
			require.Equal(t, 1175, resp.Usage.TotalTokens)
		})
	}
}
