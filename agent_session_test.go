package opencodeacp

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

type commitBarrier struct {
	acpcore.SessionStore
	block   atomic.Bool
	entered chan acpcore.SessionKey
	release chan struct{}
}

func (s *commitBarrier) Replace(ctx context.Context, key acpcore.SessionKey, replacements []acpcore.SessionStoreReplacement) error {
	if s.block.CompareAndSwap(true, false) {
		s.entered <- key
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.SessionStore.Replace(ctx, key, replacements)
}
func TestEstablishmentExcludesPrompt(t *testing.T) {
	for _, phase := range []string{"new", "cold_load"} {
		t.Run(phase, func(t *testing.T) {
			store := &commitBarrier{SessionStore: acpcore.NewInMemorySessionStore(), entered: make(chan acpcore.SessionKey, 1), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(store.release) })
			h := newHarness(t, WithSessionStore(store))
			h.initialize()
			t.Cleanup(release)
			cwd := t.TempDir()
			var id acp.SessionId
			if phase == "cold_load" {
				created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
				require.NoError(t, err)
				id = created.SessionId
				_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: id})
				require.NoError(t, err)
			}
			store.block.Store(true)
			done := make(chan error, 1)
			ctx := h.ctx()
			go func() {
				if phase == "new" {
					_, err := h.conn.NewSession(ctx, wire.NewSessionRequest(cwd))
					done <- err
				} else {
					_, err := h.conn.LoadSession(ctx, wire.LoadSessionRequest(id, cwd))
					done <- err
				}
			}()
			select {
			case key := <-store.entered:
				id = acp.SessionId(key.SessionID)
			case <-ctx.Done():
				t.Fatal("establishment never reached commit")
			}
			// An empty prompt cannot dispatch native work, but admission must still reject
			// it as busy before parsing content while establishment holds the session.
			_, err := h.conn.Prompt(ctx, wire.PromptRequest(id))
			data := requestErrorData(t, err)
			release()
			require.NoError(t, <-done)
			require.Equal(t, "session_prompt", data["limit"], "establishing session admitted a prompt into content validation: %v", data)
		})
	}
}

func TestSessionDeleteHidesTheSession(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()

	cwd := t.TempDir()
	session, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
	require.NoError(t, err)

	_, err = h.prompt(session.SessionId, "remember", nil)
	require.NoError(t, err)

	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err)

	generation, err := store.Load(h.ctx(), string(session.SessionId))
	require.NoError(t, err)
	require.Nil(t, generation, "delete writes a tombstone over the whole generation")

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Empty(t, list.Sessions)

	_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(session.SessionId, cwd))
	require.Equal(t, "unknown session", requestErrorData(t, err)[wire.FieldError])

	_, err = h.conn.UnstableDeleteSession(h.ctx(), wire.DeleteSessionRequest(session.SessionId))
	require.NoError(t, err, "delete is idempotent")
}

func TestListSessionsCoversLiveAndStoredSessions(t *testing.T) {
	t.Parallel()

	store := acpcore.NewInMemorySessionStore()
	h := newHarness(t, WithSessionStore(store))
	h.initialize()

	cold, warm := t.TempDir(), t.TempDir()

	first, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cold))
	require.NoError(t, err)
	_, err = h.conn.CloseSession(h.ctx(), acp.CloseSessionRequest{SessionId: first.SessionId})
	require.NoError(t, err)

	second, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(warm))
	require.NoError(t, err)

	list, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest())
	require.NoError(t, err)
	require.Len(t, list.Sessions, 2)

	filtered, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest(wire.WithListSessionsCwd(cold)))
	require.NoError(t, err)
	require.Len(t, filtered.Sessions, 1)
	require.Equal(t, first.SessionId, filtered.Sessions[0].SessionId)
	require.Equal(t, cold, filtered.Sessions[0].Cwd)

	live, err := h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest(wire.WithListSessionsCwd(warm)))
	require.NoError(t, err)
	require.Len(t, live.Sessions, 1)
	require.Equal(t, second.SessionId, live.Sessions[0].SessionId)

	_, err = h.conn.ListSessions(h.ctx(), wire.ListSessionsRequest(wire.WithListSessionsCwd("relative")))
	require.Equal(t, fieldCwd, requestErrorData(t, err)["field"])
}

func TestFailedRestoreCloseReleasesSlot(t *testing.T) {
	for _, method := range []string{acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume} {
		t.Run(method, func(t *testing.T) {
			store := &recoveryFaultStore{SessionStore: acpcore.NewInMemorySessionStore()}
			h := newHarness(t, WithSessionStore(store), WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}))
			h.initialize()
			cwd := t.TempDir()
			created, err := h.conn.NewSession(h.ctx(), wire.NewSessionRequest(cwd))
			require.NoError(t, err)
			before, err := store.Load(h.ctx(), string(created.SessionId))
			require.NoError(t, err)
			option := wire.WithSessionMetaValue(map[string]any{"opencode": map[string]any{"options": map[string]any{"env": map[string]string{"RESTORE_TEST": "changed"}}}})
			store.fail.Store(true)
			if method == acp.AgentMethodSessionLoad {
				_, err = h.conn.LoadSession(h.ctx(), wire.LoadSessionRequest(created.SessionId, cwd, option))
			} else {
				_, err = h.conn.ResumeSession(h.ctx(), wire.ResumeSessionRequest(created.SessionId, cwd, option))
			}
			store.fail.Store(false)
			require.Error(t, err, "store failure must fail restore")
			after, err := store.Load(h.ctx(), string(created.SessionId))
			require.NoError(t, err)
			require.Equal(t, before, after, "failed teardown must retain the durable generation")
			_, err = h.conn.NewSession(h.ctx(), wire.NewSessionRequest(t.TempDir()))
			require.NoError(t, err, "a failed restore-close leaked its active-session slot")
		})
	}
}

// An establishment holds its active-session slot through its native start, so
// a concurrent establishment is refused without native work, and a failed
// start frees the slot for the next one.
func TestEstablishmentHoldsActiveSlot(t *testing.T) {
	t.Parallel()

	held, work := filepath.Join(t.TempDir(), "start-held"), filepath.Join(t.TempDir(), "native-work")
	require.NoError(t, os.WriteFile(held+".armed", nil, 0o600))
	a := NewAgent(testOptions(t, WithConcurrencyLimits(ConcurrencyLimits{MaxActiveSessions: 1}),
		WithEnv(map[string]string{fakeOpenCodeEnv: "1", "GORACE": os.Getenv("GORACE") + " atexit_sleep_ms=0", fakeOpenCodeEnvStartHold: held, fakeOpenCodeEnvNativeWork: work}))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, startErr := a.NewSession(ctx, wire.NewSessionRequest(t.TempDir()))
		first <- startErr
	}()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(held)

		return statErr == nil
	}, testTimeout, time.Millisecond)

	second := make(chan error, 1)
	go func() {
		_, startErr := a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
		second <- startErr
	}()
	select {
	case err = <-second:
		require.Equal(t, "backpressure", requestErrorData(t, err)[wire.FieldError])
		require.Equal(t, "active_sessions", requestErrorData(t, err)["limit"])
		require.Equal(t, 1, nativeWork(t, work), "the refused establishment did native work")
	case <-time.After(testTimeout):
		t.Fatal("a concurrent establishment waited on the held native start")
	}

	cancel()
	select {
	case err = <-first:
		require.Equal(t, map[string]any{wire.FieldError: "opencode_internal_failure", wire.FieldClass: "native_start"}, requestErrorData(t, err))
	case <-time.After(testTimeout):
		t.Fatal("the held establishment did not fail")
	}

	require.NoError(t, os.Remove(held+".armed"))
	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err, "a failed establishment kept its active-session slot")
}

// blockedOpeningClient keeps the first publication in progress until released.
type blockedOpeningClient struct {
	*recorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockedOpeningClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestRestoreWaitsForPreviousOpening(t *testing.T) {
	t.Parallel()
	for _, method := range []string{acp.AgentMethodSessionLoad, acp.AgentMethodSessionResume} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			a := NewAgent(testOptions(t)...)
			t.Cleanup(func() { _ = a.Close() })
			client := &blockedOpeningClient{recorder: newRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(client.release) })
			t.Cleanup(release)
			transport, meta := prepareOpeningResponse(t)
			a.attach(client, transport)
			initialize := acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber}
			withLifecycle()(&initialize)
			_, err := a.Initialize(t.Context(), initialize)
			require.NoError(t, err)
			cwd := t.TempDir()
			request := wire.NewSessionRequest(cwd)
			request.Meta = meta
			created, err := a.NewSession(t.Context(), request)
			require.NoError(t, err)
			_, err = transport.Writer().Write([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"))
			require.NoError(t, err)
			select {
			case <-client.entered:
			case <-time.After(testTimeout):
				t.Fatal("initial opening did not reach the client")
			}

			restored := make(chan error, 1)
			go func() {
				if method == acp.AgentMethodSessionLoad {
					_, restoreErr := a.LoadSession(t.Context(), wire.LoadSessionRequest(created.SessionId, cwd))
					restored <- restoreErr

					return
				}

				_, restoreErr := a.ResumeSession(t.Context(), wire.ResumeSessionRequest(created.SessionId, cwd))
				restored <- restoreErr
			}()

			select {
			case restoreErr := <-restored:
				t.Fatalf("restore completed before the previous opening: %v", restoreErr)
			case <-time.After(100 * time.Millisecond):
			}

			release()
			select {
			case restoreErr := <-restored:
				require.NoError(t, restoreErr, "completed opening must release restore admission")
			case <-time.After(testTimeout):
				t.Fatal("restore did not continue after the previous opening")
			}
			s, err := a.session(t.Context(), created.SessionId)
			require.NoError(t, err)
			require.True(t, s.lc.Active())
		})
	}
}

func TestRestoreAfterRuntimeLoss(t *testing.T) {
	for _, load := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "load"}[load], func(t *testing.T) {
			a, s, cwd := newDirectSession(t)
			original := s.runtime
			original.server.close()
			<-original.done
			if load {
				_, err := a.LoadSession(t.Context(), wire.LoadSessionRequest(s.id, cwd))
				require.NoError(t, err)
			} else {
				_, err := a.ResumeSession(t.Context(), wire.ResumeSessionRequest(s.id, cwd))
				require.NoError(t, err)
			}
			restored, err := a.session(t.Context(), s.id)
			require.NoError(t, err)
			restored.mu.Lock()
			rebound := restored.runtime
			restored.mu.Unlock()
			require.NotNil(t, rebound, "load/resume reported success but left the session unbound")
			require.NotSame(t, original, rebound)
			require.True(t, rebound.alive())
		})
	}
}
func TestRestoreChangesNativeCwd(t *testing.T) {
	a, s, oldcwd := newDirectSession(t)
	_, err := a.CloseSession(t.Context(), acp.CloseSessionRequest{SessionId: s.id})
	require.NoError(t, err)
	newcwd := t.TempDir()
	_, err = a.ResumeSession(t.Context(), wire.ResumeSessionRequest(s.id, newcwd))
	require.NoError(t, err)
	restored := a.sessions[s.id]
	native, err := restored.runtime.client.Session(t.Context(), s.nativeID)
	require.NoError(t, err)
	require.Equal(t, newcwd, restored.cwd)
	require.NotEqual(t, oldcwd, native.Location.Directory, "resumed session still executes in its old directory")
	require.Equal(t, newcwd, native.Location.Directory)
}

func TestRestoreRefusesPoisonedSession(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"load", "resume"} {
		for _, state := range []string{"live", "ended", "changed carrier"} {
			t.Run(operation+"/"+state, func(t *testing.T) {
				t.Parallel()
				a, s, cwd := newDirectSession(t)
				rt := s.runtime
				s.poisonSession(t.Context(), "native_session_identity_drift")
				if state == "ended" {
					rt.cancel()
					<-rt.done
				}
				if state == "changed carrier" {
					cwd = t.TempDir()
				}
				var err error
				if operation == "load" {
					_, err = a.LoadSession(t.Context(), wire.LoadSessionRequest(s.id, cwd))
				} else {
					_, err = a.ResumeSession(t.Context(), wire.ResumeSessionRequest(s.id, cwd))
				}
				require.Equal(t, "opencode_session_poisoned", requestErrorData(t, err)[wire.FieldError])
				active, err := a.session(t.Context(), s.id)
				require.NoError(t, err)
				require.Same(t, s, active)
			})
		}
	}
}
