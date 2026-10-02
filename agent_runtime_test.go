package opencodeacp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
	"github.com/stretchr/testify/require"
)

func TestStartupRetriesStalledHealthRequest(t *testing.T) {
	t.Parallel()
	held := filepath.Join(t.TempDir(), "health-request")
	a := NewAgent(testOptions(t, WithEnv(map[string]string{fakeOpenCodeEnv: "1", fakeOpenCodeEnvHealthHold: held}))...)
	t.Cleanup(func() { require.NoError(t, a.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := a.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)
	created, err := a.NewSession(ctx, wire.NewSessionRequest(t.TempDir()))
	require.NoError(t, err)
	require.NotEmpty(t, created.SessionId)
	require.FileExists(t, held)
}

func TestStartupCancellationBeforeAddress(t *testing.T) {
	t.Parallel()

	held := filepath.Join(t.TempDir(), "startup-held")
	require.NoError(t, os.WriteFile(held+".armed", nil, 0o600))
	a := NewAgent(testOptions(t, WithEnv(map[string]string{fakeOpenCodeEnv: "1", fakeOpenCodeEnvStartHold: held}))...)
	t.Cleanup(func() { require.NoError(t, a.Close()) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := a.startRuntime(ctx)
		finished <- err
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(held)

		return err == nil
	}, testTimeout, time.Millisecond)
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled startup did not join its native process and stdout reader")
	}
}

// A seed file the adapter cannot own is refused as an invalid option naming
// seedFiles, not reported as a native start failure.
func TestInvalidSeedFileRefusesTheStart(t *testing.T) {
	t.Parallel()

	a := NewAgent(testOptions(t, WithSeedFiles(map[string]string{"../outside.json": "{}"}))...)
	t.Cleanup(func() { _ = a.Close() })

	_, err := a.Initialize(t.Context(), acp.InitializeRequest{ProtocolVersion: acp.ProtocolVersionNumber})
	require.NoError(t, err)

	_, err = a.NewSession(t.Context(), wire.NewSessionRequest(t.TempDir()))
	require.Equal(t, -32602, requestErrorCode(t, err))

	data := requestErrorData(t, err)
	require.Equal(t, wire.VerdictUnsupported, data[wire.FieldError])
	require.Equal(t, "seedFiles", data[wire.FieldField])
}

func TestLockedHomeMustNotBeSeeded(t *testing.T) {
	home := t.TempDir()
	agent := NewAgent(testOptions(t, WithHome(home), WithSeedFiles(map[string]string{"seed-config.txt": "replacement"}))...)
	t.Cleanup(func() { _ = agent.Close() })
	env, err := agent.environment(nil, nil).Build()
	require.NoError(t, err)
	lookup := func(key string) (string, bool) { return process.Lookup(env, key) }
	config := opencode.ConfigDir(lookup)
	require.NoError(t, process.WriteSeedFiles(config, map[string]string{"seed-config.txt": "original"}))
	held, err := process.LockFile(filepath.Join(opencode.DataDir(lookup), ".acp-go-opencode.lock"))
	require.NoError(t, err)
	defer held.Close()
	_, err = agent.startRuntime(t.Context())
	require.Error(t, err)
	t.Logf("refused runtime: %v", err)
	content, err := os.ReadFile(filepath.Join(config, "seed-config.txt"))
	require.NoError(t, err)
	require.Equal(t, "original", string(content), "startup mutated the locked home's config before refusing its lock")
}

func TestStartupDeathRetainsStderr(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "dying-opencode")
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\nprintf 'FATAL_NATIVE_START\n' >&2\nexit 7\n"), 0700))
	a := NewAgent(testOptions(t, WithExecutablePath(executable))...)
	t.Cleanup(func() { _ = a.Close() })
	_, err := a.startRuntime(t.Context())
	require.ErrorContains(t, err, "FATAL_NATIVE_START")
}

func TestUnboundDescendantDialogsFailClosed(t *testing.T) {
	t.Parallel()
	for _, child := range []bool{true, false} {
		for _, form := range []bool{true, false} {
			t.Run(fmt.Sprintf("child=%t/form=%t", child, form), func(t *testing.T) {
				t.Parallel()
				var replies atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.Method == http.MethodGet {
						parent := "foreign"
						if child {
							parent = "parent"
						}
						if strings.HasSuffix(r.URL.Path, "/foreign") {
							parent = ""
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"id": "child", "parentID": parent}})

						return
					}
					replies.Add(1)
					if form {
						require.Equal(t, http.MethodDelete, r.Method)
					} else {
						var body map[string]string
						require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
						require.Equal(t, "reject", body["decision"])
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				defer server.Close()
				client := opencode.NewClient()
				client.URL = server.URL
				rt := &runtime{client: client, bindings: map[string]*binding{"parent": {}}}
				event := opencode.Event{Type: eventPermissionAsked, Data: json.RawMessage(`{"sessionID":"child","id":"permission"}`)}
				if form {
					event.Type = eventFormCreated
					event.Data = json.RawMessage(`{"form":{"sessionID":"child","id":"form"}}`)
				}
				rt.rejectChildDialog(t.Context(), event)
				if child {
					require.EqualValues(t, 1, replies.Load())
				} else {
					require.Zero(t, replies.Load())
				}
			})
		}
	}
}
