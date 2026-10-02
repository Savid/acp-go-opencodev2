package opencodeacp

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"time"

	acp "github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

func (s *session) launch(ctx context.Context) (*binding, error) {
	server, err := s.agent.ensureRuntime(ctx)
	if err != nil {
		return nil, err
	}

	readCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	rt := &binding{server: server, client: server.client, cancel: cancel, ending: readCtx.Done(), bound: make(chan struct{}), done: make(chan struct{}), events: make(chan opencode.Event, 256), results: make(chan nativePromptResult, 1)}

	s.mu.Lock()
	closing := s.closing

	if !closing {
		s.runtime = rt
	}
	s.mu.Unlock()

	// A close that began during this launch has already sampled the binding it
	// releases, so one bound now would outlive the session.
	if closing {
		cancel()

		return nil, wire.UnknownSession()
	}

	go s.pump(readCtx, rt)

	return rt, nil
}

func (s *session) startFailure(ctx context.Context, err error) error {
	s.agent.log.ErrorContext(ctx, "opencode session start failed", slog.String("reason", err.Error()))

	return wire.InternalFailure(vendor, internalClassNativeStart)
}

func (s *session) configureRuntime(ctx context.Context, rt *binding, selection OpenCodeOptions, expectID string) error {
	ctx, cancel := context.WithTimeout(ctx, serverStartupTimeout)
	defer cancel()

	var native opencode.NativeSession

	if expectID == "" {
		body := map[string]any{"location": opencode.Location{Directory: s.cwd}, "metadata": s.carrierMetadata(nil), "permissions": s.permissionRules()}
		if selection.Mode != "" {
			body["agent"] = selection.Mode
		}

		if selection.Model != "" {
			p, m, _ := strings.Cut(selection.Model, "/")
			body["model"] = map[string]string{"providerID": p, fieldID: m, "variant": selection.Effort}
		}

		var created struct {
			Data opencode.NativeSession `json:"data"`
		}
		if err := rt.client.Do(ctx, "", http.MethodPost, "/api/session", body, &created); err != nil {
			return s.startFailure(ctx, err)
		}

		native = created.Data
		s.nativeID = native.ID
		s.id = acp.SessionId(native.ID)
	} else {
		var err error

		native, err = rt.client.Session(ctx, expectID)
		if err != nil {
			return s.agent.restoreRefused(ctx, s.id, err)
		}
	}

	if native.ID == "" || (expectID != "" && native.ID != expectID) {
		return s.startFailure(ctx, errors.New("native session identity missing or changed"))
	}

	var err error

	native, err = s.configureLocation(ctx, rt, native)
	if err != nil {
		return err
	}

	if err := rt.client.Do(ctx, s.cwd, http.MethodPatch, opencode.SessionPath(native.ID), map[string]any{"metadata": s.carrierMetadata(native.Metadata), "permissions": s.permissionRules()}, nil); err != nil {
		return s.startFailure(ctx, err)
	}

	s.mu.Lock()

	s.model = selection.Model
	if s.model == "" && native.Model.ID != "" {
		s.model = native.Model.ProviderID + "/" + native.Model.ID
	}

	s.mode = selection.Mode
	if s.mode == "" {
		s.mode = native.Agent
	}

	s.effort = selection.Effort
	if selection.Model == "" && s.effort == "" {
		s.effort = native.Model.Variant
	}

	s.title = native.Title
	s.updatedAt = time.UnixMilli(native.Time.Updated).UTC().Format(time.RFC3339)
	s.mu.Unlock()

	if err := s.refreshCatalogs(ctx, rt); err != nil {
		return s.startFailure(ctx, err)
	}

	if err := s.configureEnvironment(ctx, rt); err != nil {
		return err
	}

	for {
		if err := opencode.CheckPlugin(rt.server.root, s.cwd); err == nil {
			break
		}

		select {
		case <-ctx.Done():
			return s.startFailure(ctx, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}

	if err := s.refreshCatalogs(ctx, rt); err != nil {
		return s.startFailure(ctx, err)
	}

	if err := s.resolveSelectionDefaults(ctx, rt); err != nil {
		return s.startFailure(ctx, err)
	}

	if err := s.applySelection(ctx, rt); err != nil {
		return s.startFailure(ctx, err)
	}

	rt.server.mu.Lock()
	defer rt.server.mu.Unlock()

	if existing := rt.server.bindings[native.ID]; existing != nil && existing != rt {
		return wire.Backpressure(limitSessionRestore)
	}

	if !rt.server.alive() {
		return wire.RuntimeUnavailable(vendor)
	}

	rt.server.bindings[native.ID] = rt

	return nil
}

func (s *session) carrierMetadata(base map[string]any) map[string]any {
	result := wire.CloneMap(base)
	if result == nil {
		result = map[string]any{}
	}

	env := maps.Clone(s.options.Env)
	if env == nil {
		env = map[string]string{}
	}

	result[opencode.CarrierKey] = map[string]any{"sessionID": s.nativeID, metaEnvKey: env, metaExtraPathDirsKey: append([]string{}, s.options.ExtraPathDirs...)}

	return result
}

func (s *session) permissionRules() []map[string]string {
	permission := s.options.Permission
	if permission == "" {
		permission = "ask"
	}

	return []map[string]string{{"action": "*", "resource": "*", "effect": permission}}
}

func (s *session) configureEnvironment(ctx context.Context, rt *binding) error {
	merged := s.agent.environment(s.options.Env, nil)
	merged.ExtraPathDirs = s.options.ExtraPathDirs

	env, err := merged.Build()
	if err != nil {
		return s.startFailure(ctx, err)
	}

	variables := map[string]string{}

	for _, entry := range env {
		key, value, _ := strings.Cut(entry, "=")
		variables[key] = value
	}

	for _, key := range []string{"OPENCODE_SERVER_PASSWORD", "OPENCODE_CONFIG_CONTENT", "OPENCODE_SERVER_USERNAME"} {
		if value, ok := process.Lookup(rt.server.environment, key); ok {
			variables[key] = value
		}
	}

	if err := rt.client.Do(ctx, "", http.MethodPut, opencode.SessionPath(s.nativeID)+"/environment", map[string]any{"variables": variables}, nil); err != nil {
		return s.startFailure(ctx, err)
	}

	return nil
}

func (s *session) configureLocation(ctx context.Context, rt *binding, native opencode.NativeSession) (opencode.NativeSession, error) {
	var statuses struct {
		Data map[string]opencode.NativeSessionStatus `json:"data"`
	}
	if err := rt.client.Do(ctx, s.cwd, http.MethodGet, "/api/session/active", nil, &statuses); err != nil {
		return opencode.NativeSession{}, s.startFailure(ctx, err)
	}

	if status := statuses.Data[native.ID].Type; status != "" && status != statusIdle {
		return opencode.NativeSession{}, wire.Backpressure(limitSessionRestore)
	}

	if native.Location.Directory != s.cwd {
		inbox, err := rt.client.Inbox(ctx, native.ID)
		if err != nil {
			return opencode.NativeSession{}, s.agent.restoreRefused(ctx, s.id, err)
		}

		if len(inbox) != 0 {
			return opencode.NativeSession{}, wire.Backpressure(limitSessionRestore)
		}

		if err = rt.client.Move(ctx, native.ID, s.cwd); err != nil {
			return opencode.NativeSession{}, s.agent.restoreRefused(ctx, s.id, err)
		}

		native, err = rt.client.Session(ctx, native.ID)
		if err != nil {
			return opencode.NativeSession{}, s.agent.restoreRefused(ctx, s.id, err)
		}

		if native.Location.Directory != s.cwd {
			return opencode.NativeSession{}, s.agent.restoreRefused(ctx, s.id, errors.New("native session directory differs from requested directory"))
		}
	}

	return native, nil
}
