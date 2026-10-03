package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakeOpenCodeEnv) == "1" {
		os.Exit(runFakeOpenCode(os.Args[1:]))
	}

	os.Exit(m.Run())
}

const testTimeout = 20 * time.Second

// testOptions configures an agent that launches the test binary as opencode, with
// an isolated opencode home and scratch directory.
func testOptions(t *testing.T, extra ...Option) []Option {
	t.Helper()

	options := make([]Option, 0, 5+len(extra))
	options = append(options,
		WithExecutablePath(os.Args[0]),
		WithEnv(map[string]string{fakeOpenCodeEnv: "1", "GORACE": os.Getenv("GORACE") + " atexit_sleep_ms=0"}),
		WithHome(filepath.Join(t.TempDir(), "home")),
		WithScratchDir(filepath.Join(t.TempDir(), "scratch")),
		WithLogger(slog.New(slog.DiscardHandler)),
	)

	return append(options, extra...)
}

// nativeWork counts the native operations the fake has recorded in path.
func nativeWork(t *testing.T, path string) int {
	t.Helper()

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}

	require.NoError(t, err)

	return strings.Count(string(data), "\n")
}

// recorder is the ACP client the tests observe the agent through.
type recorder struct {
	mu          sync.Mutex
	updates     []acp.SessionNotification
	raw         []json.RawMessage
	permissions []acp.RequestPermissionRequest
	answer      func(acp.RequestPermissionRequest) acp.RequestPermissionResponse
	elicit      func(acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error)
	changed     chan struct{}
}

var (
	_ acp.Client                 = (*recorder)(nil)
	_ acp.ExtensionMethodHandler = (*recorder)(nil)
)

func newRecorder() *recorder {
	return &recorder{
		changed: make(chan struct{}, 1),
		answer: func(acp.RequestPermissionRequest) acp.RequestPermissionResponse {
			return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(approvalOnce)}
		},
	}
}

func (r *recorder) signal() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *recorder) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	r.mu.Lock()
	r.updates = append(r.updates, params)
	r.mu.Unlock()
	r.signal()

	return nil
}

func (r *recorder) RequestPermission(_ context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	r.mu.Lock()
	r.permissions = append(r.permissions, params)
	answer := r.answer
	r.mu.Unlock()
	r.signal()

	return answer(params), nil
}

func (r *recorder) UnstableCreateElicitation(_ context.Context, params acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error) {
	r.mu.Lock()
	elicit := r.elicit
	r.mu.Unlock()

	if elicit == nil {
		return acp.UnstableCreateElicitationResponse{}, errors.New("no elicitation handler")
	}

	return elicit(params)
}

func (r *recorder) HandleExtensionMethod(_ context.Context, method string, params json.RawMessage) (any, error) {
	if method == RawEventMethod {
		r.mu.Lock()
		r.raw = append(r.raw, append(json.RawMessage(nil), params...))
		r.mu.Unlock()
		r.signal()
	}

	return map[string]any{}, nil
}

func (*recorder) NotifyExtension(context.Context, string, any) error { return nil }

func (*recorder) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, errors.New("unsupported")
}

func (*recorder) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, errors.New("unsupported")
}

func (*recorder) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, errors.New("unsupported")
}

func (*recorder) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, errors.New("unsupported")
}

func (*recorder) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, errors.New("unsupported")
}

// snapshot returns the notifications recorded so far.
func (r *recorder) snapshot() []acp.SessionNotification {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]acp.SessionNotification(nil), r.updates...)
}

// waitFor blocks until condition holds over the recorded notifications.
func (r *recorder) waitFor(t *testing.T, condition func([]acp.SessionNotification) bool) {
	t.Helper()

	deadline := time.After(testTimeout)

	for {
		if condition(r.snapshot()) {
			return
		}

		select {
		case <-r.changed:
		case <-deadline:
			t.Fatalf("condition not met; %d notifications recorded", len(r.snapshot()))
		}
	}
}

// harness serves an agent over pipes to a recording client.
type harness struct {
	input *requestWriter
	t     *testing.T
	conn  *acp.ClientSideConnection
	rec   *recorder
}

func newHarness(t *testing.T, extra ...Option) *harness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	clientReader, agentWriter := io.Pipe()
	agentReader, clientWriter := io.Pipe()
	rec := newRecorder()
	served := make(chan error, 1)

	go func() { served <- Serve(ctx, agentReader, agentWriter, testOptions(t, extra...)...) }()

	input := &requestWriter{Writer: clientWriter}
	conn := acp.NewClientSideConnection(rec, input, clientReader)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	h := &harness{t: t, conn: conn, rec: rec, input: input}

	t.Cleanup(func() {
		cancel()
		_ = clientWriter.Close()

		select {
		case <-served:
		case <-time.After(testTimeout):
			t.Error("Serve did not return")
		}
	})

	return h
}

func (h *harness) ctx() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	h.t.Cleanup(cancel)

	return ctx
}

func (h *harness) initialize(opts ...func(*acp.InitializeRequest)) acp.InitializeResponse {
	h.t.Helper()

	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	for _, opt := range opts {
		opt(&request)
	}

	resp, err := h.conn.Initialize(h.ctx(), request)
	require.NoError(h.t, err)

	return resp
}

func withLifecycle() func(*acp.InitializeRequest) {
	return func(request *acp.InitializeRequest) {
		request.Meta = map[string]any{wire.LifecycleKey: map[string]any{"version": 1}}
	}
}

func withFormElicitation() func(*acp.InitializeRequest) {
	return func(request *acp.InitializeRequest) {
		request.ClientCapabilities.Elicitation = &acp.ElicitationCapabilities{Form: &acp.ElicitationFormCapabilities{}}
	}
}

func (h *harness) newSession(opts ...wire.SessionRequestOption) acp.NewSessionResponse {
	h.t.Helper()

	resp, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(h.t.TempDir(), opts...))
	require.NoError(h.t, err)

	return resp
}

func (h *harness) prompt(sessionID acp.SessionId, text string, meta map[string]any) (acp.PromptResponse, error) {
	h.t.Helper()

	request := wire.TextPromptRequest(sessionID, text)
	request.Meta = meta

	return h.conn.Prompt(h.ctx(), request)
}

// promptMeta stamps the lifecycle prompt correlation.
func promptMeta(n int) map[string]any {
	return map[string]any{wire.LifecycleKey: map[string]any{
		"version": 1, "submission": map[string]any{"submissionId": fmt.Sprintf("sub-%d", n), "clientNonce": fmt.Sprintf("non-%d", n)},
	}}
}

// requestErrorData decodes the data member of a JSON-RPC error.
func requestErrorData(t *testing.T, err error) map[string]any {
	t.Helper()

	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)

	data, ok := reqErr.Data.(map[string]any)
	if !ok {
		encoded, marshalErr := json.Marshal(reqErr.Data)
		require.NoError(t, marshalErr)
		require.NoError(t, json.Unmarshal(encoded, &data))
	}

	return data
}

func requestErrorCode(t *testing.T, err error) int {
	t.Helper()

	var reqErr *acp.RequestError
	require.ErrorAs(t, err, &reqErr)

	return reqErr.Code
}

// agentText concatenates streamed agent message text.
func agentText(updates []acp.SessionNotification) string {
	var text strings.Builder

	for _, update := range updates {
		if chunk := update.Update.AgentMessageChunk; chunk != nil && chunk.Content.Text != nil {
			text.WriteString(chunk.Content.Text.Text)
		}
	}

	return text.String()
}

// usageUpdates returns the recorded usage_update payloads in delivery order,
// without the variant discriminator the wire decoding fills in, and with
// their meta in its wire form.
func usageUpdates(updates []acp.SessionNotification) []acp.SessionUsageUpdate {
	var usage []acp.SessionUsageUpdate

	for _, update := range updates {
		if payload := update.Update.UsageUpdate; payload != nil {
			usage = append(usage, acp.SessionUsageUpdate{Size: payload.Size, Used: payload.Used, Cost: payload.Cost, Meta: wireMeta(payload.Meta)})
		}
	}

	return usage
}

// wireMeta is meta as a client decodes it.
func wireMeta(meta map[string]any) map[string]any {
	if meta == nil {
		return nil
	}

	data, err := json.Marshal(meta)
	if err != nil {
		panic(err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		panic(err)
	}

	return decoded
}

// callReport is the usage_update reporting one call's response: the context
// it left occupied and its breakdown of uncached input, cache reads, cache
// writes, and output with reasoning.
func callReport(size, used, input, read, write, output int) acp.SessionUsageUpdate {
	call := wire.CallUsage{InputTokens: &input, CachedReadTokens: &read, CachedWriteTokens: &write, OutputTokens: &output}

	return acp.SessionUsageUpdate{Size: size, Used: used, Cost: &acp.Cost{Currency: "USD"}, Meta: wireMeta(call.Apply(nil))}
}

// lifecycleEvents extracts the lifecycle envelopes in delivery order.
func lifecycleEvents(updates []acp.SessionNotification) []map[string]any {
	events := make([]map[string]any, 0)

	for _, update := range updates {
		envelope, ok := update.Meta[wire.LifecycleKey].(map[string]any)
		if !ok {
			continue
		}

		event, _ := envelope["event"].(map[string]any)
		events = append(events, event)
	}

	return events
}

func loadEntries(ctx context.Context, store acpcore.SessionStore, key acpcore.SessionKey) ([]acpcore.SessionStoreEntry, error) {
	generation, err := store.Load(ctx, key.SessionID)

	return generation[key.Subpath], err
}

// storedRecord decodes the committed session configuration.
func storedRecord(t *testing.T, store acpcore.SessionStore, id acp.SessionId) sessionRecord {
	t.Helper()

	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(id), Subpath: sessionlog.ConfigSubpath})
	require.NoError(t, err)
	require.Len(t, rows, 1)

	var record sessionRecord
	require.NoError(t, json.Unmarshal(rows[0], &record))

	return record
}

// lifecycleStreams lists the incarnation identities the session published, in
// order, one entry per incarnation.
// eventTypes renders each lifecycle event as its type, with the state or
// action state appended where one exists.
func eventTypes(events []map[string]any) []string {
	types := make([]string, 0, len(events))
	for _, event := range events {
		kind, _ := event["type"].(string)
		state, _ := event["state"].(string)

		if action, ok := event["action"].(map[string]any); ok {
			state, _ = action["state"].(string)
		}

		if state != "" {
			kind += ":" + state
		}

		types = append(types, kind)
	}

	return types
}

func lifecycleStreams(updates []acp.SessionNotification) []string {
	streams := make([]string, 0)

	for _, update := range updates {
		envelope, ok := update.Meta[wire.LifecycleKey].(map[string]any)
		if !ok {
			continue
		}

		id, _ := envelope["streamId"].(string)
		if len(streams) == 0 || streams[len(streams)-1] != id {
			streams = append(streams, id)
		}
	}

	return streams
}

// requestWriter retains the wire JSON-RPC id of the latest prompt so a test
// can cancel that request without also sending session/cancel.
type requestWriter struct {
	io.Writer
	mu       sync.Mutex
	promptID json.RawMessage
}

func (w *requestWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var frame struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if json.Unmarshal(data, &frame) == nil && frame.Method == acp.AgentMethodSessionPrompt {
		w.promptID = frame.ID
	}

	return w.Writer.Write(data)
}

func (w *requestWriter) cancelPrompt() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "$/cancel_request", "params": map[string]any{"requestId": w.promptID}})
	if err != nil {
		return err
	}
	_, err = w.Writer.Write(append(data, '\n'))

	return err
}

func newDirectSession(t *testing.T) (*Agent, *session, string) {
	t.Helper()
	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	cwd := t.TempDir()
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	return a, a.sessions[created.SessionId], cwd
}
