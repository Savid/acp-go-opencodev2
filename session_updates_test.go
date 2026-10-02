package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
	"github.com/stretchr/testify/require"
)

// traceLog orders the mirror commits and lifecycle states the fixture observes.
// The session pump and the test both write to it.
type traceLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *traceLog) add(entry string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, entry)
}

func (l *traceLog) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = nil
}

func (l *traceLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.entries...)
}

type nativeFixtureStore struct {
	acpcore.SessionStore
	trace *traceLog
	fail  atomic.Bool
}

func (s *nativeFixtureStore) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("fixture mirror failure")
	}
	if err := s.SessionStore.Replace(ctx, key, replacements); err != nil {
		return err
	}
	s.trace.add("commit")

	return nil
}

type nativeFixtureRecorder struct {
	*recorder
	trace *traceLog
}

func (r *nativeFixtureRecorder) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if envelope, ok := notification.Meta[wire.LifecycleKey].(map[string]any); ok {
		if event, ok := envelope["event"].(map[string]any); ok && event["type"] == "state_update" {
			state, _ := event["state"].(string)
			r.trace.add(state)
		}
	}
	if notification.Update.AgentMessageChunk != nil || notification.Update.UserMessageChunk != nil {
		r.trace.add("message")
	}

	return r.recorder.SessionUpdate(ctx, notification)
}

func TestCapturedNativeAgentOrigin(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		name := "committed"
		if failCommit {
			name = "failed commit"
		}
		t.Run(name, func(t *testing.T) {
			trace := &traceLog{}
			store := &nativeFixtureStore{SessionStore: acpcore.NewInMemorySessionStore(), trace: trace}
			rec := &nativeFixtureRecorder{recorder: newRecorder(), trace: trace}
			a := NewAgent(testOptions(t, WithSessionStore(store))...)
			t.Cleanup(func() { _ = a.Close() })
			a.attach(rec, nil)
			initResponse, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
			require.NoError(t, err)
			created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err)
			s, err := a.session(t.Context(), created.SessionId)
			require.NoError(t, err)
			s.mu.Lock()
			rt := s.runtime
			require.Nil(t, s.turn)
			s.mu.Unlock()
			data, err := os.ReadFile("testdata/native/agent-origin.json")
			require.NoError(t, err)
			data = []byte(strings.ReplaceAll(string(data), "fixture-session", string(s.id)))
			var frames []json.RawMessage
			require.NoError(t, json.Unmarshal(data, &frames))
			var terminal opencode.Event
			require.NoError(t, json.Unmarshal(frames[len(frames)-1], &terminal))
			native, err := rt.client.Export(t.Context(), s.nativeID)
			require.NoError(t, err)
			marker := opencode.NativeMessage{ID: strings.Replace(terminal.ID, "evt_", "msg_", 1), Type: "idle", Outcome: "succeeded"}
			var fixtureActive atomic.Bool
			fixtureActive.Store(true)
			target, err := url.Parse(rt.client.URL)
			require.NoError(t, err)
			forward := httputil.NewSingleHostReverseProxy(target)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if fixtureActive.Load() && strings.HasSuffix(r.URL.Path, "/export") {
					fakeData(w, fakeExport{Info: native.Info, Messages: []opencode.NativeMessage{marker}})

					return
				}
				forward.ServeHTTP(w, r)
			}))
			t.Cleanup(proxy.Close)
			rt.client.URL = proxy.URL
			trace.reset()
			store.fail.Store(failCommit)
			for _, frame := range frames {
				var envelope opencode.Event
				require.NoError(t, json.Unmarshal(frame, &envelope))
				rt.events <- envelope
			}
			require.Eventually(t, func() bool {
				if failCommit {
					select {
					case <-rt.done:
						return true
					default:
						return false
					}
				}
				s.mu.Lock()
				settled := s.cycle == nil
				s.mu.Unlock()
				entries := trace.snapshot()

				return settled && len(entries) > 0 && entries[len(entries)-1] == "idle"
			}, testTimeout, time.Millisecond)
			fixtureActive.Store(false)
			require.NotEmpty(t, trace.snapshot())
			require.Equal(t, "running", trace.snapshot()[0])
			for _, event := range lifecycleEvents(rec.snapshot()) {
				if event["type"] == "state_update" {
					require.Equal(t, "activity", event["cause"])
				}
				require.NotEqual(t, "prompt_accepted", event["type"])
			}
			chunks := responseChunks(rec.snapshot(), created.SessionId)
			require.NotEmpty(t, chunks)
			for _, chunk := range chunks {
				require.Nil(t, chunk.messageID)
			}
			reports := usageUpdates(rec.snapshot())
			require.Len(t, reports, 1)
			require.Equal(t, 6+20+5147, reports[0].Used)
			require.NotContains(t, reports[0].Meta[wire.CallUsageKey], "responseId")
			require.NotEmpty(t, s.nativeMessages)
			s.mu.Lock()
			require.Nil(t, s.cycle)
			s.mu.Unlock()
			require.NoError(t, lifecycle.CheckAttribution(negotiatedAnswer(t, initResponse), sessionFrames(t, rec.snapshot(), created.SessionId)))
			if !failCommit {
				entries := trace.snapshot()
				require.Equal(t, []string{"commit", "idle"}, entries[len(entries)-2:])

				return
			}

			require.NotContains(t, trace.snapshot(), "idle")
			require.False(t, s.lc.Active(), "a failed mirror commit fences the incarnation")

			// The fence is terminal, so the incarnation's binding ends with it.
			select {
			case <-rt.done:
			case <-time.After(testTimeout):
				t.Fatal("a failed agent-cycle commit left the binding in place")
			}

			store.fail.Store(false)

			next := wire.TextPromptRequest(s.id, "after the fence")
			next.Meta = promptMeta(1)

			_, err = a.Prompt(t.Context(), next)
			require.NoError(t, err)

			streams := lifecycleStreams(rec.snapshot())
			require.Len(t, streams, 2, "the next prompt opens a new incarnation on a fresh binding")
			require.NotEqual(t, streams[0], streams[1])
		})
	}
}

func TestAssistantTextIsAppendOnly(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	_, err := h.prompt(session.SessionId, "STREAM", nil)
	require.NoError(t, err)
	require.Equal(t, "abcdef", agentText(h.rec.snapshot()),
		"a finalized text that extends the streamed prefix delivers only the suffix")
}

func TestToolInputArrivesAfterPending(t *testing.T) {
	rec := newRecorder()
	a := NewAgent()
	t.Cleanup(func() { _ = a.Close() })
	a.attach(rec, nil)
	s := &session{agent: a, id: "tool-session"}
	state := cycleState{}
	for _, status := range []string{"pending", "running", "completed"} {
		input := map[string]any{}
		if status != "pending" {
			input["command"] = "pwd"
		}
		require.NoError(t, s.emitTool(t.Context(), &state, "call", "shell", opencode.ToolState{Status: status, Input: input, Content: []opencode.Content{{Type: "text", Text: "/work"}}}))
	}
	started := 0
	var inputs []any
	for _, n := range rec.snapshot() {
		if n.Update.ToolCall != nil {
			started++
		}
		if u := n.Update.ToolCallUpdate; u != nil {
			if input, ok := u.RawInput.(map[string]any); ok && input["command"] != nil {
				inputs = append(inputs, input["command"])
			}
		}
	}
	require.Equal(t, 1, started)
	require.Equal(t, []any{"pwd", "pwd"}, inputs)
}

// fakeContextWindow is the fake catalog's context window for the session's
// model.
const fakeContextWindow = 32000

// TestUsageFollowsEachResponse proves every model call of a turn reports the
// context it left occupied, never the running sum, while the prompt response
// carries the turn's summed consumption.
func TestUsageFollowsEachResponse(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "MULTI", nil)
	require.NoError(t, err)

	require.Equal(t, []acp.SessionUsageUpdate{
		callReport(fakeContextWindow, 1120, 100, 1000, 0, 20),
		callReport(fakeContextWindow, 1200, 50, 1120, 0, 30),
		callReport(fakeContextWindow, 1250, 40, 1200, 0, 10),
	}, usageUpdates(h.rec.snapshot()))
	require.NotNil(t, resp.Usage)
	require.Equal(t, 190, resp.Usage.InputTokens)
	require.Equal(t, 60, resp.Usage.OutputTokens)
	require.Equal(t, 3320, *resp.Usage.CachedReadTokens)
	require.Equal(t, 3570, resp.Usage.TotalTokens)
}

// TestUsageFollowsSteeredResponses proves the calls opencode makes for a
// message a native client added mid-turn report inside the turn and count
// toward its consumption, and that the added message's generation ends with
// the turn.
func TestUsageFollowsSteeredResponses(t *testing.T) {
	t.Parallel()

	rec := newRecorder()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(rec, nil)
	initResponse, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber, Meta: map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	request := wire.TextPromptRequest(created.SessionId, "STEER")
	request.Meta = promptMeta(1)
	resp, err := a.Prompt(t.Context(), request)
	require.NoError(t, err)

	require.Equal(t, []acp.SessionUsageUpdate{
		callReport(fakeContextWindow, 1120, 100, 1000, 0, 20),
		callReport(fakeContextWindow, 1200, 50, 1120, 0, 30),
		callReport(fakeContextWindow, 1250, 40, 1200, 0, 10),
	}, usageUpdates(rec.snapshot()))
	require.Equal(t, 3570, resp.Usage.TotalTokens)
	require.NoError(t, lifecycle.CheckAttribution(negotiatedAnswer(t, initResponse), sessionFrames(t, rec.snapshot(), created.SessionId)))
}

// TestSteeredUsageArrivesInsideTheTurn proves a steered call reports as it
// finishes, while the turn is still running.
func TestSteeredUsageArrivesInsideTheTurn(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)

	go func() {
		resp, _ := h.prompt(session.SessionId, "STEERSLOW", nil)
		done <- resp
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(usageUpdates(updates)) == 2 })
	require.Equal(t, []acp.SessionUsageUpdate{
		callReport(fakeContextWindow, 1120, 100, 1000, 0, 20),
		callReport(fakeContextWindow, 1200, 50, 1120, 0, 30),
	}, usageUpdates(h.rec.snapshot()))
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)
}

// TestUnusableResponsesReportNoUsage proves a failed attempt opencode retries
// inside the call reports nothing of its own.
func TestUnusableResponsesReportNoUsage(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "FLAKY", nil)
	require.NoError(t, err)
	require.Equal(t, []acp.SessionUsageUpdate{callReport(fakeContextWindow, 1120, 100, 1000, 0, 20)}, usageUpdates(h.rec.snapshot()))
	require.Equal(t, 1120, resp.Usage.TotalTokens)
}

// TestUsageAfterCompaction proves the compaction summary's own call never
// stands in for the session's context: the next figure is the compacted one.
// The summary still counts toward the turn's consumption.
func TestUsageAfterCompaction(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	resp, err := h.prompt(session.SessionId, "COMPACT", nil)
	require.NoError(t, err)
	require.Equal(t, []acp.SessionUsageUpdate{
		callReport(fakeContextWindow, 1120, 100, 1000, 0, 20),
		callReport(fakeContextWindow, 320, 300, 0, 0, 20),
	}, usageUpdates(h.rec.snapshot()))
	require.Equal(t, 1120+1320+320, resp.Usage.TotalTokens)
}

func TestCancelledTurnReportsNoUsageAfterCancel(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	session := h.newSession()

	done := make(chan acp.PromptResponse, 1)

	go func() {
		resp, _ := h.prompt(session.SessionId, "STEPSLOW", nil)
		done <- resp
	}()

	h.rec.waitFor(t, func(updates []acp.SessionNotification) bool { return len(usageUpdates(updates)) == 1 })
	require.NoError(t, h.conn.Cancel(h.ctx(), wire.CancelRequest(session.SessionId)))
	require.Equal(t, acp.StopReasonCancelled, (<-done).StopReason)

	require.Equal(t, []acp.SessionUsageUpdate{callReport(fakeContextWindow, 1120, 100, 1000, 0, 20)}, usageUpdates(h.rec.snapshot()),
		"the call that finished after the cancel reports nothing")
}

func TestFailedCompactionCountsConsumption(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	session := h.newSession()
	response, err := h.prompt(session.SessionId, "COMPACTFAIL", nil)
	require.NoError(t, err)
	require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
	require.NotNil(t, response.Usage)
	require.Equal(t, 1320, response.Usage.TotalTokens)
	require.Empty(t, usageUpdates(h.rec.snapshot()))
}

func TestCancelDuringUsageCostRead(t *testing.T) {
	t.Parallel()
	a, s, _ := newDirectSession(t)
	rec := newRecorder()
	a.attach(rec, nil)
	rt := s.runtime
	target, err := url.Parse(rt.client.URL)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	var once sync.Once
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == opencode.SessionPath(s.nativeID) {
			once.Do(func() {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
				}
			})
		}
		forward.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	defer unblock()
	client := opencode.NewClient()
	client.URL, client.Password = proxy.URL, rt.client.Password
	rt.client = client
	done := make(chan error, 1)
	go func() {
		_, promptErr := a.Prompt(t.Context(), wire.TextPromptRequest(s.id, "hello"))
		done <- promptErr
	}()
	select {
	case <-entered:
	case <-time.After(testTimeout):
		t.Fatal("usage cost read did not start")
	}
	require.NoError(t, a.Cancel(t.Context(), wire.CancelRequest(s.id)))
	unblock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(testTimeout):
		t.Fatal("cancelled prompt did not finish")
	}
	require.Empty(t, usageUpdates(rec.snapshot()))
}

// responseChunk is one agent message or thought chunk as a client sees it.
type responseChunk struct {
	thought   bool
	text      string
	messageID *string
}

// responseChunks returns the agent message and thought chunks of one session
// in delivery order.
func responseChunks(updates []acp.SessionNotification, id acp.SessionId) []responseChunk {
	var chunks []responseChunk

	for _, update := range updates {
		if update.SessionId != id {
			continue
		}

		if chunk := update.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
			chunks = append(chunks, responseChunk{text: chunk.Content.Text.Text, messageID: chunk.MessageId})
		}

		if chunk := update.Update.AgentThoughtChunk; chunk != nil && chunk.Content.Text != nil {
			chunks = append(chunks, responseChunk{thought: true, text: chunk.Content.Text.Text, messageID: chunk.MessageId})
		}
	}

	return chunks
}

// TestCallTokens proves one call's native tokens give the context it left
// occupied and its breakdown: opencode's input is already uncached, and its
// reasoning joins the output. A call without any token is unknown.
func TestCallTokens(t *testing.T) {
	t.Parallel()

	tokens := func(input, output, reasoning, read, write float64) opencode.NativeTokens {
		value := opencode.NativeTokens{Input: input, Output: output, Reasoning: reasoning}
		value.Cache.Read, value.Cache.Write = read, write

		return value
	}

	for name, tc := range map[string]struct {
		tokens                     opencode.NativeTokens
		used                       int
		input, read, write, output int
		known                      bool
	}{
		"every component":              {tokens(1, 2, 3, 4, 5), 15, 1, 4, 5, 5, true},
		"cache reads, no cache writes": {tokens(3041, 72, 24, 7424, 0), 10561, 3041, 7424, 0, 96, true},
		"no cache reported":            {tokens(9524, 57, 17, 0, 0), 9598, 9524, 0, 0, 74, true},
		"reasoning only":               {tokens(0, 0, 7, 0, 0), 7, 0, 0, 0, 7, true},
		"replayed from a cache":        {opencode.NativeTokens{}, 0, 0, 0, 0, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.used, contextTokens(tc.tokens))

			call := callUsage(tc.tokens)
			require.Equal(t, wire.CallUsage{InputTokens: &tc.input, CachedReadTokens: &tc.read, CachedWriteTokens: &tc.write, OutputTokens: &tc.output}, call)
			require.Equal(t, tc.known, call.Known())
		})
	}
}
