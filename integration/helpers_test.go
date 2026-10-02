//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	opencodeacp "github.com/savid/acp-go-opencodev2"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"

	"github.com/savid/acp-go-core/wire"
)

const testTimeout = 120 * time.Second
const permissionOptionAllow acp.PermissionOptionId = "once"

// harnessPath is the installed opencode the tier drives. ACP_GO_OPENCODEV2_HARNESS_PATH
// points it at a build outside PATH; an absent binary skips the tier.
func harnessPath(t *testing.T) string {
	t.Helper()

	selector := os.Getenv("ACP_GO_OPENCODEV2_HARNESS_PATH")
	if selector == "" {
		selector = "opencode"
	}

	resolved, err := exec.LookPath(selector)
	if err != nil {
		t.Skipf("opencode not installed: %v", err)
	}

	return resolved
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
			return acp.RequestPermissionResponse{Outcome: acp.NewRequestPermissionOutcomeSelected(permissionOptionAllow)}
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
	if method == opencodeacp.RawEventMethod {
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

// harness serves an agent over pipes to a recording client.
type harness struct {
	t        *testing.T
	conn     *acp.ClientSideConnection
	rec      *recorder
	stopOnce sync.Once
	stop     func()
}

func newHarness(t *testing.T, extra ...opencodeacp.Option) *harness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	clientReader, agentWriter := io.Pipe()
	agentReader, clientWriter := io.Pipe()
	rec := newRecorder()
	served := make(chan error, 1)

	opts := append([]opencodeacp.Option{opencodeacp.WithExecutablePath(harnessPath(t))}, extra...)

	go func() { served <- opencodeacp.Serve(ctx, agentReader, agentWriter, opts...) }()

	conn := acp.NewClientSideConnection(rec, clientWriter, clientReader)
	conn.SetLogger(slog.New(slog.DiscardHandler))

	h := &harness{t: t, conn: conn, rec: rec}

	h.stop = func() {
		h.stopOnce.Do(func() {
			cancel()
			_ = clientWriter.Close()
			select {
			case <-served:
			case <-time.After(testTimeout):
				t.Error("opencodeacp.Serve did not return")
			}
		})
	}
	t.Cleanup(h.stop)

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
