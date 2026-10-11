package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
	"github.com/stretchr/testify/require"
)

func TestMirrorRefusesWhileChildSessionRuns(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	cwd := t.TempDir()
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	b := s.binding
	s.mu.Unlock()
	var child = createNativeChild(t, b, s.nativeID, cwd)

	promptCtx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		request := map[string]any{"id": opencode.NewMessageID(), "text": "SLOW"}
		done <- b.client.Do(promptCtx, cwd, http.MethodPost, opencode.SessionPath(child.ID)+"/prompt", request, nil)
	}()
	require.Eventually(t, func() bool {
		var statuses struct {
			Data map[string]opencode.NativeSessionStatus `json:"data"`
		}
		if b.client.Do(t.Context(), cwd, http.MethodGet, "/api/session/active", nil, &statuses) != nil {
			return false
		}

		return statuses.Data[child.ID].Type == "running"
	}, 5*time.Second, 10*time.Millisecond)

	require.ErrorContains(t, s.commitMirror(t.Context(), b, nil), "native session is still running")

	require.NoError(t, b.client.Interrupt(t.Context(), child.ID))
	require.NoError(t, b.client.Wait(t.Context(), child.ID))
	stop()
	<-done
	require.NoError(t, s.commitMirror(t.Context(), b, nil))
	var record sessionRecord
	rows, found, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	require.True(t, found)
	exports, err := decodeStored(rows, record)
	require.NoError(t, err)
	childRows := 0
	for _, item := range exports {
		if item.Info.ID == child.ID {
			childRows++
		}
	}
	require.Equal(t, 1, childRows)
}

func TestMirrorIncludesDescendantsAndExcludesUnrelatedSessions(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	cwd := t.TempDir()
	root, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	s, err := a.session(t.Context(), root.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	b := s.binding
	s.mu.Unlock()
	parent := s.nativeID
	want := make([]string, 1, 3)
	want[0] = parent
	for range 2 {
		var child = createNativeChild(t, b, parent, cwd)
		want = append(want, child.ID)
		parent = child.ID
	}
	unrelated, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	require.NoError(t, s.commitMirror(t.Context(), b, nil))
	var record sessionRecord
	rows, found, err := sessionlog.Load(t.Context(), store, string(root.SessionId), &record)
	require.NoError(t, err)
	require.True(t, found)
	exports, err := decodeStored(rows, record)
	require.NoError(t, err)
	graph := map[string]bool{}
	for _, item := range exports {
		graph[item.Info.ID] = true
	}
	require.Len(t, graph, len(want))
	for _, id := range want {
		require.Contains(t, graph, id)
	}
	require.NotContains(t, graph, string(unrelated.SessionId))
}

// A commit the session cannot attempt, and one with no complete native
// snapshot to replace, both fail rather than report a success the store does
// not hold.
func TestMirrorCommitRefusesWhatItCannotAttempt(t *testing.T) {
	t.Parallel()

	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)

	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)

	require.Error(t, s.commitMirror(t.Context(), nil, nil),
		"a commit with no binding to read the native history through cannot be attempted")

	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "FORGET"))
	data := requestErrorData(t, err)
	require.Equal(t, vendor+"_"+wire.TokenTurnFailed, data[wire.FieldError],
		"a turn whose native history vanished is not durable")
	require.Equal(t, wire.CauseTransport, data[wire.FieldCause])
	require.Equal(t, "session mirror commit failed", data[wire.FieldMessage])
}

func TestRestoreRefusesMissingImageArtifact(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()

	cwd := t.TempDir()

	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	_, err = h.prompt(session.SessionId, "IMAGE", nil)
	require.NoError(t, err)

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)

	rows, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(session.SessionId)})
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	record := storedRecord(t, store, session.SessionId)
	require.NotEmpty(t, record.Artifacts, "the native image is captured beside the rows")

	stored := make([][]byte, 0, len(rows))
	for _, row := range rows {
		stored = append(stored, row)
	}

	uncaptured := record
	uncaptured.Artifacts = nil

	require.NoError(t, sessionlog.NewMirror(store, string(session.SessionId)).Commit(t.Context(), sessionlog.Rows(stored), recordOf(uncaptured)))

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.Equal(t, vendor+"_"+wire.TokenRestoreFailed, requestErrorData(t, err)[wire.FieldError],
		"a local image the record no longer captures refuses the restore")
}

// Residual native state with no store entry is neither listed nor adopted.
func TestResidualNativeStateIsNeverAdopted(t *testing.T) {
	t.Parallel()

	a := NewAgent(testOptions(t)...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	server, err := a.ensureRuntime(t.Context())
	require.NoError(t, err)
	cwd := t.TempDir()
	var orphan struct {
		Data opencode.NativeSession `json:"data"`
	}
	require.NoError(t, server.client.Do(t.Context(), "", http.MethodPost, "/api/session", map[string]any{"location": opencode.Location{Directory: cwd}}, &orphan))
	require.NotEmpty(t, orphan.Data.ID)
	list, err := a.ListSessions(t.Context(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
	_, err = a.LoadSession(t.Context(), wire.LoadSessionRequest(acp.SessionId(orphan.Data.ID), cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)[wire.FieldError])
	_, err = a.ResumeSession(t.Context(), wire.ResumeSessionRequest(acp.SessionId(orphan.Data.ID), cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)[wire.FieldError])
}

type recoveryFaultStore struct {
	acpcore.SessionStore
	fail atomic.Bool
}

func (s *recoveryFaultStore) Replace(ctx context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("injected store failure")
	}

	return s.SessionStore.Replace(ctx, main, replacements)
}

func (s *recoveryFaultStore) Append(ctx context.Context, main acpcore.SessionKey, base int, entries []acpcore.SessionStoreEntry, replacements []acpcore.SessionStoreReplacement) error {
	if s.fail.Load() {
		return errors.New("injected store failure")
	}

	return s.SessionStore.Append(ctx, main, base, entries, replacements)
}

func TestNativeBindingSurvivesLoadAndResume(t *testing.T) {
	t.Parallel()
	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	var record sessionRecord
	rows, found, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, record.NativeSessionID)
	require.Equal(t, wire.NativeSessionMeta(vendor, record.NativeSessionID), created.Meta)
	id := acp.SessionId("acp-conversation-independent-of-native-id")
	record.SessionID = string(id)
	require.NoError(t, sessionlog.NewMirror(store, string(id)).Commit(t.Context(), sessionlog.Rows(rows), recordOf(record)))
	require.NoError(t, store.Delete(t.Context(), acpcore.SessionKey{SessionID: string(created.SessionId)}))

	before := len(h.rec.snapshot())
	loaded, err := h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(id, cwd))
	require.NoError(t, err)
	require.Equal(t, wire.NativeSessionMeta(vendor, record.NativeSessionID), loaded.Meta)
	_, err = h.prompt(id, "HELLO", nil)
	require.NoError(t, err)
	for _, update := range h.rec.snapshot()[before:] {
		require.Equal(t, id, update.SessionId)
	}
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, id, listed.Sessions[0].SessionId)
	require.Equal(t, loaded.Meta, listed.Sessions[0].Meta)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: id})
	require.NoError(t, err)
	listed, err = h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, listed.Sessions, 1)
	require.Equal(t, loaded.Meta, listed.Sessions[0].Meta)
	resumed, err := h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(id, cwd))
	require.NoError(t, err)
	require.Equal(t, loaded.Meta, resumed.Meta)
	_, err = h.prompt(id, "HELLO", nil)
	require.NoError(t, err)
	var after sessionRecord
	_, found, err = sessionlog.Load(t.Context(), store, string(id), &after)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, record.NativeSessionID, after.NativeSessionID)
	require.Equal(t, string(id), after.SessionID)
}

func TestFailedConfigChangeDoesNotReachTheNextCommit(t *testing.T) {
	t.Parallel()
	store := &recoveryFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	session := h.newSession()
	var before sessionRecord
	_, found, err := sessionlog.Load(t.Context(), store, string(session.SessionId), &before)
	require.NoError(t, err)
	require.True(t, found)
	store.fail.Store(true)
	_, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configModel, "fake/text"))
	require.Error(t, err)
	store.fail.Store(false)
	_, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(session.SessionId, configEffort, "high"))
	require.NoError(t, err)
	var after sessionRecord
	_, found, err = sessionlog.Load(t.Context(), store, string(session.SessionId), &after)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, before.Model, after.Model)
}

type firstMirrorFailureStore struct {
	acpcore.SessionStore
	calls atomic.Int32
}

func (s *firstMirrorFailureStore) Replace(ctx context.Context, key acpcore.SessionKey, rows []acpcore.SessionStoreReplacement) error {
	if s.calls.Add(1) == 1 {
		return errors.New("initial mirror unavailable")
	}

	return s.SessionStore.Replace(ctx, key, rows)
}

func TestFailedNewSessionDoesNotPersistDuringCleanup(t *testing.T) {
	t.Parallel()
	store := &firstMirrorFailureStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	response, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	require.Empty(t, response.SessionId)
	rows, err := store.ListSessions(h.ctx())
	require.NoError(t, err)
	require.Empty(t, rows)
	listed, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, listed.Sessions)
}

type firstOpenFailureClient struct {
	*recorder
	failed atomic.Bool
}

func (c *firstOpenFailureClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if c.failed.CompareAndSwap(false, true) {
		return errors.New("initial publication unavailable")
	}

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestFailedSessionOpenReleasesActiveSlot(t *testing.T) {
	t.Parallel()
	a := NewAgent(testOptions(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))...)
	t.Cleanup(func() { _ = a.Close() })
	a.attach(&firstOpenFailureClient{recorder: newRecorder()}, nil)
	request := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
	withLifecycle()(&request)
	_, err := a.Initialize(t.Context(), request)
	require.NoError(t, err)
	first, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.Error(t, err)
	require.Empty(t, first.SessionId)
	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
}

type blockedLoadStore struct {
	acpcore.SessionStore
	block            atomic.Bool
	entered, release chan struct{}
}

func (s *blockedLoadStore) Load(ctx context.Context, sessionID string) (map[string][]acpcore.SessionStoreEntry, error) {
	rows, err := s.SessionStore.Load(ctx, sessionID)
	if err == nil && s.block.CompareAndSwap(true, false) {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return rows, err
}

func TestDeleteWinsAgainstPreparedLoad(t *testing.T) {
	t.Parallel()
	store := &blockedLoadStore{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan struct{}), release: make(chan struct{})}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(session.SessionId, "HELLO", nil)
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: session.SessionId})
	require.NoError(t, err)
	store.block.Store(true)
	done := make(chan error, 1)
	ctx := h.ctx()
	go func() {
		_, loadErr := h.conn.LoadSession(ctx, wire.LoadSessionRequest(session.SessionId, cwd))
		done <- loadErr
	}()
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("load did not reach stored configuration")
	}
	_, err = h.conn.UnstableDeleteSession(h.ctx(), acp.UnstableDeleteSessionRequest{SessionId: session.SessionId})
	close(store.release)
	require.NoError(t, err)
	require.Equal(t, "unknown session", requestErrorData(t, <-done)["error"])
	list, err := h.conn.ListSessions(h.ctx(), acp.ListSessionsRequest{})
	require.NoError(t, err)
	require.Empty(t, list.Sessions)
}

// countingStore counts the writes a session makes to the store.
type countingStore struct {
	acpcore.SessionStore
	replaces atomic.Int32
	deletes  atomic.Int32
}

func (s *countingStore) Replace(ctx context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	s.replaces.Add(1)

	return s.SessionStore.Replace(ctx, main, replacements)
}

func (s *countingStore) Delete(ctx context.Context, key acpcore.SessionKey) error {
	s.deletes.Add(1)

	return s.SessionStore.Delete(ctx, key)
}

func ephemeralMeta() wire.SessionRequestOption {
	return wire.WithSessionMeta(wire.SessionMeta{Ephemeral: true}.Apply(nil))
}

func TestEphemeralSessionNeverReachesTheStore(t *testing.T) {
	t.Parallel()

	store := &countingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()

	ephemeral, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, ephemeralMeta()))
	require.NoError(t, err)
	_, err = h.prompt(ephemeral.SessionId, "probe", nil)
	require.NoError(t, err)
	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions, "a live ephemeral session is never listed")
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: ephemeral.SessionId})
	require.NoError(t, err)
	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(ephemeral.SessionId))
	require.NoError(t, err)

	require.Zero(t, store.replaces.Load(), "an ephemeral session is never mirrored")
	require.Zero(t, store.deletes.Load(), "an ephemeral session leaves no tombstone")
	list, err = h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)

	durable, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = h.prompt(durable.SessionId, "kept", nil)
	require.NoError(t, err)
	require.Positive(t, store.replaces.Load(), "a session the host did not mark ephemeral still mirrors")
}

func TestEphemeralSessionMetaIsRefusedWhereItHasNoMeaning(t *testing.T) {
	t.Parallel()

	h := newHarness(t)
	h.initialize()
	cwd := t.TempDir()
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd, ephemeralMeta()))
	require.Equal(t, "_meta."+wire.SessionMetaKey, requestErrorData(t, err)[wire.FieldField])
	_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd, ephemeralMeta()))
	require.Equal(t, "_meta."+wire.SessionMetaKey, requestErrorData(t, err)[wire.FieldField])

	unknown := wire.WithSessionMeta(map[string]any{wire.SessionMetaKey: map[string]any{"persist": false}})
	_, err = h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd, unknown))
	require.Equal(t, "_meta."+wire.SessionMetaKey+".persist", requestErrorData(t, err)[wire.FieldField])
}

func createNativeChild(t *testing.T, b *binding, parent, cwd string) opencode.NativeSession {
	t.Helper()
	info, err := b.client.Session(t.Context(), parent)
	require.NoError(t, err)
	info.ID = opencode.NewID("ses_")
	info.ParentID = parent
	body := map[string]any{"info": info, "messages": []any{}}
	require.NoError(t, b.client.Do(t.Context(), "", http.MethodPost, "/api/experimental/session/import", body, nil))

	return info
}

func TestRestoreVerifiesPromotedPendingInput(t *testing.T) {
	input := opencode.InboxInput{ID: "msg_input", SessionID: "ses_native", Type: "synthetic", Delivery: "steer", Payload: json.RawMessage(`{"text":"remember","description":"note","metadata":{"source":"test"}}`)}
	for _, tc := range []struct {
		name, message string
		refused       bool
	}{
		{"promoted", `{"id":"msg_input","type":"synthetic","text":"remember","description":"note","metadata":{"source":"test"},"time":{"created":1}}`, false},
		{"text changed", `{"id":"msg_input","type":"synthetic","text":"changed","description":"note","metadata":{"source":"test"},"time":{"created":1}}`, true},
		{"description changed", `{"id":"msg_input","type":"synthetic","text":"remember","description":"changed","metadata":{"source":"test"},"time":{"created":1}}`, true},
		{"metadata changed", `{"id":"msg_input","type":"synthetic","text":"remember","description":"note","metadata":{"source":"changed"},"time":{"created":1}}`, true},
		{"type changed", `{"id":"msg_input","type":"user","text":"remember","description":"note","metadata":{"source":"test"},"time":{"created":1}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message, err := opencode.ParseMessage([]byte(tc.message))
			require.NoError(t, err)
			err = verifyPendingInputs([]opencode.InboxInput{input}, nil, []opencode.Message{message})
			if tc.refused {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
	require.NoError(t, verifyPendingInputs([]opencode.InboxInput{input}, []opencode.InboxInput{input}, nil))
	require.Error(t, verifyPendingInputs([]opencode.InboxInput{input}, nil, nil))
	record := sessionRecord{SessionID: "ses_native", NativeSessionID: "ses_native", Cwd: t.TempDir(), Sessions: []json.RawMessage{json.RawMessage(`{"id":"ses_native"}`)}, PendingInputs: map[string][]opencode.InboxInput{"ses_native": {input, input}}}
	require.ErrorContains(t, record.validate("ses_native"), "duplicate native pending input")
}

func TestMirrorFailureFencesTurnAndAllowsRetry(t *testing.T) {
	t.Parallel()

	for _, prompt := range []string{"HELLO", "ERROR"} {
		t.Run(prompt, func(t *testing.T) {
			t.Parallel()

			store := &recoveryFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
			h := newHarness(t, WithSessionStore(store))
			h.initialize(withLifecycle())
			created := h.newSession()
			before, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			store.fail.Store(true)
			t.Cleanup(func() { store.fail.Store(false) })
			response, err := h.prompt(created.SessionId, prompt, promptMeta(1))
			require.Empty(t, response.StopReason)
			require.Equal(t, -32603, requestErrorCode(t, err))
			data := requestErrorData(t, err)
			require.Equal(t, "opencode_turn_failed", data["error"])
			if prompt == "ERROR" {
				require.Equal(t, "provider", data["cause"])
				require.Equal(t, "account rate limit", data["message"])
				require.EqualValues(t, 429, data["statusCode"])
				require.Equal(t, "rate_limit", data["providerCode"])
			} else {
				require.Equal(t, "transport", data["cause"])
				require.Equal(t, "session mirror commit failed", data["message"])
			}
			after, err := store.Load(t.Context(), string(created.SessionId))
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update:running"}, eventTypes(lifecycleEvents(h.rec.snapshot())))

			store.fail.Store(false)
			response, err = h.prompt(created.SessionId, "HELLO", promptMeta(2))
			require.NoError(t, err)
			require.Equal(t, acp.StopReasonEndTurn, response.StopReason)
			require.Equal(t, []string{"lifecycle_snapshot", "prompt_accepted", "state_update:running", "lifecycle_snapshot", "prompt_accepted", "state_update:running", "state_update:idle"}, eventTypes(lifecycleEvents(h.rec.snapshot())))
			var streams []string
			for _, update := range h.rec.snapshot() {
				envelope, ok := update.Meta[wire.LifecycleKey].(map[string]any)
				if !ok {
					continue
				}
				event, ok := envelope["event"].(map[string]any)
				if ok && event["type"] == "lifecycle_snapshot" {
					id, ok := envelope["streamId"].(string)
					require.True(t, ok)
					streams = append(streams, id)
				}
			}
			require.Len(t, streams, 2)
			require.NotEqual(t, streams[0], streams[1])
		})
	}
}

// writeCountingStore records the main-record rows each write carries.
type writeCountingStore struct {
	acpcore.SessionStore
	mu       sync.Mutex
	replaced []int
	appended []int
	configs  int
}

func (s *writeCountingStore) Replace(ctx context.Context, main acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	s.mu.Lock()
	for _, replacement := range replacements {
		if replacement.Key == main {
			s.replaced = append(s.replaced, len(replacement.Entries))
		}
	}
	s.mu.Unlock()

	return s.SessionStore.Replace(ctx, main, replacements)
}

func (s *writeCountingStore) Append(ctx context.Context, main acpcore.SessionKey, base int, entries []acpcore.SessionStoreEntry, replacements []acpcore.SessionStoreReplacement) error {
	s.mu.Lock()
	s.appended = append(s.appended, len(entries))
	s.configs += len(replacements)
	s.mu.Unlock()

	return s.SessionStore.Append(ctx, main, base, entries, replacements)
}

func (s *writeCountingStore) writes() (replaced, appended []int, configs int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.replaced), slices.Clone(s.appended), s.configs
}

func TestMirrorAppendsOnlyNewRows(t *testing.T) {
	t.Parallel()
	store := &writeCountingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	h := newHarness(t, WithSessionStore(store))
	h.initialize()
	cwd := t.TempDir()
	created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	for range 3 {
		_, err = h.prompt(created.SessionId, "HELLO", nil)
		require.NoError(t, err)
	}
	replaced, appended, configs := store.writes()
	require.Equal(t, []int{0}, replaced, "only the first commit publishes the whole generation")
	require.Len(t, appended, 3)
	total := 0
	for _, rows := range appended {
		require.Positive(t, rows)
		total += rows
	}
	stored, err := loadEntries(t.Context(), store, acpcore.SessionKey{SessionID: string(created.SessionId)})
	require.NoError(t, err)
	require.Len(t, stored, total, "each turn appends exactly the rows it added")
	require.Equal(t, 3, configs)

	_, err = h.conn.SetSessionConfigOption(h.ctx(), wire.SetConfigOptionRequest(created.SessionId, configMode, "plan"))
	require.NoError(t, err)
	_, appended, _ = store.writes()
	require.Equal(t, 0, appended[len(appended)-1], "a configuration change rewrites no transcript rows")

	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: created.SessionId})
	require.NoError(t, err)
	before := len(h.rec.snapshot())
	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd))
	require.NoError(t, err)
	replayed := 0
	for _, update := range h.rec.snapshot()[before:] {
		if update.Update.UserMessageChunk != nil {
			replayed++
		}
	}
	require.Equal(t, 3, replayed, "the appended rows restore the whole conversation")
	_, err = h.prompt(created.SessionId, "HELLO", nil)
	require.NoError(t, err)
	replaced, appended, _ = store.writes()
	require.Equal(t, []int{0}, replaced, "a restored session continues its stored generation")
	require.Positive(t, appended[len(appended)-1])
}

func TestMirrorContinuesDescendantHistory(t *testing.T) {
	t.Parallel()
	store := &writeCountingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	cwd := t.TempDir()
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)
	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "HELLO"))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	s.mu.Lock()
	b := s.binding
	s.mu.Unlock()
	child := createNativeChild(t, b, s.nativeID, cwd)
	promptCtx, stop := context.WithCancel(t.Context())
	defer stop()
	running := make(chan error, 1)
	go func() {
		running <- b.client.Do(promptCtx, cwd, http.MethodPost, opencode.SessionPath(child.ID)+"/prompt", map[string]any{"id": opencode.NewMessageID(), "text": "SLOW"}, nil)
	}()
	require.Eventually(t, func() bool {
		var statuses struct {
			Data map[string]opencode.NativeSessionStatus `json:"data"`
		}

		return b.client.Do(t.Context(), cwd, http.MethodGet, "/api/session/active", nil, &statuses) == nil && statuses.Data[child.ID].Type == "running"
	}, testTimeout, 10*time.Millisecond)

	stored := func() map[string][]opencode.Message {
		var record sessionRecord
		rows, found, loadErr := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
		require.NoError(t, loadErr)
		require.True(t, found)
		graph, decodeErr := decodeStored(rows, record)
		require.NoError(t, decodeErr)
		messages := map[string][]opencode.Message{}
		for i := range graph {
			messages[graph[i].Info.ID] = graph[i].Messages
		}

		return messages
	}
	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "HELLO"))
	require.NoError(t, err)
	require.Empty(t, stored()[child.ID], "a running descendant commits nothing past its latest completed execution")

	require.NoError(t, b.client.Interrupt(t.Context(), child.ID))
	require.NoError(t, b.client.Wait(t.Context(), child.ID))
	stop()
	<-running
	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "HELLO"))
	require.NoError(t, err)
	messages := stored()
	require.NotEmpty(t, messages[child.ID], "the descendant's finished execution follows in the next commit")
	require.Equal(t, "idle", messages[child.ID][len(messages[child.ID])-1].Type)
	native, err := b.client.Export(t.Context(), child.ID)
	require.NoError(t, err)
	require.Len(t, messages[child.ID], len(native.Messages), "no descendant message is skipped")
	replaced, _, _ := store.writes()
	require.Equal(t, []int{0}, replaced, "descendant history appends without republishing")
}

func TestMirrorRepublishesALateSettledMessage(t *testing.T) {
	t.Parallel()
	store := &writeCountingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "HELLO"))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	b := s.binding
	native := *b.client
	target, err := url.Parse(native.URL)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	late := opencode.NativeMessage{ID: "msg_late_shell", Type: "shell"}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/export") {
			forward.ServeHTTP(w, r)

			return
		}
		exported, exportErr := native.Export(r.Context(), s.nativeID)
		require.NoError(t, exportErr)
		value := fakeExport{Info: exported.Info}
		for i, message := range exported.Messages {
			if i == len(exported.Messages)-1 {
				value.Messages = append(value.Messages, late)
			}
			decoded, decodeErr := message.Decode()
			require.NoError(t, decodeErr)
			value.Messages = append(value.Messages, decoded)
		}
		fakeData(w, value)
	}))
	defer proxy.Close()
	b.client.URL = proxy.URL
	require.NoError(t, s.commitMirror(t.Context(), b, nil))
	b.client.URL = native.URL
	replaced, _, _ := store.writes()
	require.Len(t, replaced, 2, "a message settling before mirrored ones republishes the generation")
	var record sessionRecord
	rows, _, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	messages := storedMessages(t, rows, record, s.nativeID)
	require.Equal(t, late.ID, messages[len(messages)-2].ID, "the republished history keeps native order")
}

func TestFailedCommitKeepsTheSessionsNativeState(t *testing.T) {
	t.Parallel()
	store := &recoveryFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	sentinel := []json.RawMessage{json.RawMessage(`{"id":"sentinel"}`)}
	s.mu.Lock()
	s.nativeInfos = sentinel
	s.title = "changed"
	s.mu.Unlock()
	store.fail.Store(true)
	require.Error(t, s.commitMirror(t.Context(), s.binding, nil))
	s.mu.Lock()
	require.Equal(t, sentinel, s.nativeInfos, "a failed commit leaves the session's native state")
	s.mu.Unlock()
	store.fail.Store(false)
	require.NoError(t, s.commitMirror(t.Context(), s.binding, nil))
	s.mu.Lock()
	require.NotEqual(t, sentinel, s.nativeInfos)
	s.mu.Unlock()
	record := storedRecord(t, store, created.SessionId)
	require.Len(t, record.Sessions, 1)
}

func TestMirrorRepublishesAHeldMessageThatChanged(t *testing.T) {
	t.Parallel()
	store := &writeCountingStore{SessionStore: acpcore.NewInMemorySessionStore()}
	a := NewAgent(testOptions(t, WithSessionStore(store))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	created, err := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	_, err = a.Prompt(t.Context(), wire.TextPromptRequest(created.SessionId, "HELLO"))
	require.NoError(t, err)
	s, err := a.session(t.Context(), created.SessionId)
	require.NoError(t, err)
	b := s.binding
	native := *b.client
	target, err := url.Parse(native.URL)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/export") {
			forward.ServeHTTP(w, r)

			return
		}
		exported, exportErr := native.Export(r.Context(), s.nativeID)
		require.NoError(t, exportErr)
		value := fakeExport{Info: exported.Info}
		for _, message := range exported.Messages {
			decoded, decodeErr := message.Decode()
			require.NoError(t, decodeErr)
			if decoded.Type == roleAssistant {
				decoded.Text = "completed since its capture"
			}
			value.Messages = append(value.Messages, decoded)
		}
		fakeData(w, value)
	}))
	defer proxy.Close()
	b.client.URL = proxy.URL
	require.NoError(t, s.commitMirror(t.Context(), b, nil))
	b.client.URL = native.URL
	replaced, _, _ := store.writes()
	require.Len(t, replaced, 2, "a held message whose bytes changed republishes the generation")
	var record sessionRecord
	rows, _, err := sessionlog.Load(t.Context(), store, string(created.SessionId), &record)
	require.NoError(t, err)
	changed := false
	for _, message := range storedMessages(t, rows, record, s.nativeID) {
		changed = changed || strings.Contains(string(message.Raw), "completed since its capture")
	}
	require.True(t, changed, "the store holds the message as it is now")
}
