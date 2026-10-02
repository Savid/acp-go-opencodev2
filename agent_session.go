package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/wire"
)

const (
	limitActiveSessions = "active_sessions"
	limitSessionRestore = "session_restore"
)

// sessionStart carries the validated inputs of one session-establishing
// request.
type sessionStart struct {
	cwd                   string
	additionalDirectories []string
	meta                  sessionMeta
	// ephemeral is the host's statement that it deletes this session without
	// needing it back, so the store never sees it.
	ephemeral bool
}

func (a *Agent) validateStart(params sessionStart, mcpServers []acp.McpServer, meta map[string]any) (sessionStart, error) {
	if a.optionErr != nil {
		return sessionStart{}, a.optionErr
	}

	parsed, err := parseSessionMeta(meta)
	if err != nil {
		return sessionStart{}, err
	}

	if !filepath.IsAbs(params.cwd) {
		return sessionStart{}, wire.Unsupported(fieldCwd)
	}

	for index, dir := range params.additionalDirectories {
		if !filepath.IsAbs(dir) {
			return sessionStart{}, wire.Unsupported("additionalDirectories[" + strconv.Itoa(index) + "]")
		}
	}

	if len(mcpServers) > 0 {
		return sessionStart{}, wire.Unsupported("mcpServers")
	}

	if err := a.ensureOpen(); err != nil {
		return sessionStart{}, err
	}

	params.meta = parsed

	return params, nil
}

// newSession builds the session value for one establishing request; the
// native identity arrives once opencode reports it.
func (a *Agent) newSession(start sessionStart) *session {
	s := &session{
		agent:                 a,
		cwd:                   start.cwd,
		additionalDirectories: slices.Clone(start.additionalDirectories),
		options:               start.meta.options.clone(),
		ephemeral:             start.ephemeral,
		gate:                  make(chan struct{}, 1),
	}

	return s
}

// install publishes a configured session under its ACP id.
func (a *Agent) install(ctx context.Context, s *session) error {
	a.mu.Lock()

	var refusal error

	switch {
	case a.closed:
		refusal = wire.AgentClosed()
	case a.deleted[s.id]:
		refusal = wire.UnknownSession()
	case a.sessions[s.id] != nil:
		refusal = wire.InternalFailure(vendor, internalClassNativeStart)
	case len(a.sessions) >= a.options.ConcurrencyLimits.MaxActiveSessions:
		refusal = wire.Backpressure(limitActiveSessions)
	}

	if refusal == nil {
		a.sessions[s.id] = s

		if s.ephemeral {
			a.ephemeral[s.id] = true
		}
	}
	a.mu.Unlock()

	if refusal != nil {
		return refusal
	}

	a.observe.AddActiveSession(ctx, 1)

	return nil
}

// scheduleOpen defers the opening publication behind the establishing
// response on a served connection, and runs it inline for an embedded host.
func (a *Agent) scheduleOpen(ctx context.Context, s *session) error {
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

	if t := a.transportRef(); t != nil {
		return t.RegisterHook(ctx, s.id, func(hookCtx context.Context) {
			// Establishment has committed; the gate keeps failure cleanup on its source.
			release := wire.HoldSessionGate(s.gate)
			defer release()

			err := s.openStream(hookCtx, rt)
			if err == nil || errors.Is(err, errSessionClosing) {
				return
			}

			s.mu.Lock()
			current := !s.closing && s.runtime == rt
			s.mu.Unlock()

			if current {
				// close joins the gate, so the hold ends before the ladder runs.
				release()

				_ = s.close(context.WithoutCancel(hookCtx))
				a.detach(hookCtx, s)
			}

			a.log.ErrorContext(hookCtx, "publish session open failed",
				slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))
		})
	}

	if err := s.openStream(ctx, rt); err != nil {
		if errors.Is(err, errSessionClosing) {
			return s.closingRefusal()
		}

		return err
	}

	return nil
}

// NewSession creates and starts a opencode session.
func (a *Agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (resp acp.NewSessionResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.NewSessionResponse{}, openErr
	}

	if transport := a.transportRef(); transport != nil {
		ctx = transport.RequestContext(ctx, params.Meta)
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionNew)
	defer func() { finish(err) }()

	start, err := a.validateStart(sessionStart{cwd: params.Cwd, additionalDirectories: params.AdditionalDirectories}, params.McpServers, params.Meta)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	hostMeta, refusal := wire.DecodeSessionMeta(params.Meta)
	if refusal != nil {
		return acp.NewSessionResponse{}, refusal
	}

	start.ephemeral = hostMeta.Ephemeral

	s := a.newSession(start)

	rt, err := s.launch(ctx)
	if err != nil {
		return acp.NewSessionResponse{}, err
	}

	selection := s.options
	if selection.Model == "" {
		selection.Model = a.options.DefaultModel
	}

	if err := s.configureRuntime(ctx, rt, selection, ""); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return acp.NewSessionResponse{}, err
	}

	s.rawEvents = wire.NewRawEvents(vendor, string(s.id), vendor, start.meta.rawEvents)

	release := wire.HoldSessionGate(s.gate)
	defer release()

	if err := a.install(ctx, s); err != nil {
		release()

		_ = s.close(context.WithoutCancel(ctx))

		return acp.NewSessionResponse{}, err
	}

	if err := s.commitMirror(ctx, rt, nil); err != nil {
		a.log.ErrorContext(ctx, "initial mirror commit failed",
			slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))
		release()

		_ = s.close(context.WithoutCancel(ctx))
		a.detach(ctx, s)

		return acp.NewSessionResponse{}, wire.InternalFailure(vendor, "")
	}

	if err := a.scheduleOpen(ctx, s); err != nil {
		release()

		_ = s.close(context.WithoutCancel(ctx))
		a.detach(ctx, s)

		return acp.NewSessionResponse{}, err
	}

	return acp.NewSessionResponse{Meta: wire.NativeSessionMeta(vendor, s.nativeID), SessionId: s.id, ConfigOptions: s.configOptions()}, nil
}

// LoadSession restores a session and replays its history.
func (a *Agent) LoadSession(ctx context.Context, params acp.LoadSessionRequest) (resp acp.LoadSessionResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.LoadSessionResponse{}, openErr
	}

	if transport := a.transportRef(); transport != nil {
		ctx = transport.RequestContext(ctx, params.Meta)
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionLoad)
	defer func() { finish(err) }()

	s, release, err := a.restore(ctx, params.SessionId, sessionStart{cwd: params.Cwd, additionalDirectories: params.AdditionalDirectories}, params.McpServers, params.Meta, true)
	if err != nil {
		return acp.LoadSessionResponse{}, err
	}

	defer release()

	return acp.LoadSessionResponse{Meta: wire.NativeSessionMeta(vendor, s.nativeID), ConfigOptions: s.configOptions()}, nil
}

// ResumeSession restores a session without replaying its history.
func (a *Agent) ResumeSession(ctx context.Context, params acp.ResumeSessionRequest) (resp acp.ResumeSessionResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.ResumeSessionResponse{}, openErr
	}

	if transport := a.transportRef(); transport != nil {
		ctx = transport.RequestContext(ctx, params.Meta)
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionResume)
	defer func() { finish(err) }()

	s, release, err := a.restore(ctx, params.SessionId, sessionStart{cwd: params.Cwd, additionalDirectories: params.AdditionalDirectories}, params.McpServers, params.Meta, false)
	if err != nil {
		return acp.ResumeSessionResponse{}, err
	}

	defer release()

	return acp.ResumeSessionResponse{Meta: wire.NativeSessionMeta(vendor, s.nativeID), ConfigOptions: s.configOptions()}, nil
}

// restore is the shared load and resume path. A live session whose carrier
// matches is reused; a changed carrier closes it and re-prepares from the
// store; a cold session is hydrated from the store and started.
func (a *Agent) restore(
	ctx context.Context,
	sessionID acp.SessionId,
	params sessionStart,
	mcpServers []acp.McpServer,
	meta map[string]any,
	replay bool,
) (*session, func(), error) {
	if sessionID == "" {
		return nil, nil, wire.UnknownSession()
	}

	if refusal := wire.RefuseSessionMeta(meta); refusal != nil {
		return nil, nil, refusal
	}

	start, err := a.validateStart(params, mcpServers, meta)
	if err != nil {
		return nil, nil, err
	}

	if transport := a.transportRef(); transport != nil {
		if awaitErr := transport.AwaitSession(ctx, sessionID); awaitErr != nil {
			return nil, nil, awaitErr
		}
	}

	releaseRestore, err := a.restores.Acquire(sessionID)
	if err != nil {
		return nil, nil, err
	}
	defer releaseRestore()

	if refusal := wire.CheckSessionID(sessionID); refusal != nil {
		return nil, nil, refusal
	}

	a.mu.Lock()
	deleted := a.deleted[sessionID]
	active := a.sessions[sessionID]
	a.mu.Unlock()

	if deleted {
		return nil, nil, wire.UnknownSession()
	}

	if active != nil {
		release, gateErr := active.acquireGate(limitSessionRestore)
		if gateErr != nil {
			return nil, nil, gateErr
		}

		if admissionErr := active.admissionError(); admissionErr != nil {
			release()

			return nil, nil, admissionErr
		}

		active.mu.Lock()
		rt := active.runtime
		active.mu.Unlock()

		if sameCarrier(active, start) {
			return a.restoreActive(ctx, active, replay, release)
		}

		if rt != nil && !rt.alive() {
			active.stopRuntime(context.WithoutCancel(ctx), rt)
		}

		release()

		closeErr := active.close(ctx)

		a.detach(ctx, active)

		if closeErr != nil {
			return nil, nil, wire.RestoreFailed(vendor)
		}
	}

	stored, err := a.loadStored(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}

	if !stored.found {
		return nil, nil, wire.UnknownSession()
	}

	start.meta.options = inheritCarrier(start.meta, stored.record)
	if start.additionalDirectories == nil {
		start.additionalDirectories = slices.Clone(stored.record.AdditionalDirectories)
	}

	s := a.newSession(start)
	s.id = sessionID
	s.nativeID = stored.record.NativeSessionID
	s.rawEvents = wire.NewRawEvents(vendor, string(sessionID), vendor, start.meta.rawEvents)
	s.title = storedTitle(stored.record.NativeSessionID, stored.rows)

	rt, err := s.launch(ctx)
	if err != nil {
		return nil, nil, err
	}

	rows, err := s.hydrate(ctx, rt, stored)
	if err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, nil, err
	}

	if err := s.configureRuntime(ctx, rt, s.options, s.nativeID); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), rt)

		return nil, nil, err
	}

	release := wire.HoldSessionGate(s.gate)

	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	if err := a.install(ctx, s); err != nil {
		release()

		_ = s.close(context.WithoutCancel(ctx))

		return nil, nil, err
	}

	if err := s.commitMirror(ctx, rt, nil); err != nil {
		release()

		_ = s.close(context.WithoutCancel(ctx))
		a.detach(ctx, s)

		return nil, nil, a.restoreRefused(ctx, sessionID, err)
	}

	if replay {
		if err := s.replay(ctx, rows); err != nil {
			release()

			_ = s.close(context.WithoutCancel(ctx))
			a.detach(ctx, s)

			return nil, nil, err
		}
	}

	if err := a.scheduleOpen(ctx, s); err != nil {
		release()

		_ = s.close(context.WithoutCancel(ctx))
		a.detach(ctx, s)

		return nil, nil, err
	}

	transferred = true

	return s, release, nil
}

// restoreActive keeps the session's foreground held through recovery and replay.
func (a *Agent) restoreActive(ctx context.Context, s *session, replay bool, release func()) (*session, func(), error) {
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

	recovered := rt == nil || !rt.alive()

	transferred := false
	defer func() {
		if !transferred {
			if recovered && rt != nil {
				s.stopRuntime(context.WithoutCancel(ctx), rt)
			}

			release()
		}
	}()

	if recovered {
		var err error

		rt, err = s.recoverRuntime(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if err := s.commitMirror(ctx, rt, nil); err != nil {
		return nil, nil, a.restoreRefused(ctx, s.id, err)
	}

	if replay {
		var rows [][]byte

		var err error

		if s.ephemeral {
			rows, err = s.snapshotRows(ctx, rt, nil)
		} else {
			var stored storedSession

			stored, err = a.loadStored(ctx, s.id)
			rows = stored.rows
		}

		if err != nil {
			return nil, nil, err
		}

		if err := s.replay(ctx, rows); err != nil {
			return nil, nil, err
		}
	}

	if recovered {
		if err := a.scheduleOpen(ctx, s); err != nil {
			return nil, nil, err
		}
	}

	transferred = true

	return s, release, nil
}

// sameCarrier reports whether a restore names the configuration the live
// session already runs under. A field the request omits inherits.
func sameCarrier(s *session, start sessionStart) bool {
	record := s.record()
	if record.Cwd != start.cwd {
		return false
	}

	if start.additionalDirectories != nil && !slices.Equal(record.AdditionalDirectories, start.additionalDirectories) {
		return false
	}

	current := inheritCarrier(sessionMeta{}, record)
	requested := inheritCarrier(start.meta, record)

	return reflect.DeepEqual(current.Meta(), requested.Meta())
}

// inheritCarrier fills the fields a restore omitted from the stored record.
func inheritCarrier(meta sessionMeta, record sessionRecord) OpenCodeOptions {
	options := meta.options.clone()

	if !meta.presentEnv {
		options.Env = maps.Clone(record.Env)
	}

	if !meta.presentExtraPathDirs {
		options.ExtraPathDirs = slices.Clone(record.ExtraPathDirs)
	}

	if options.Model == "" {
		options.Model = record.Model
	}

	if options.Effort == "" {
		options.Effort = record.Effort
	}

	if options.Mode == "" {
		options.Mode = record.Mode
	}

	if options.Permission == "" {
		options.Permission = record.Permission
	}

	return options
}

// ListSessions lists live sessions and stored sessions, newest first.
func (a *Agent) ListSessions(ctx context.Context, params acp.ListSessionsRequest) (resp acp.ListSessionsResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.ListSessionsResponse{}, openErr
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionList)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.ListSessionsResponse{}, wire.ParamRefusal(refusal)
	}

	filter := ""
	if params.Cwd != nil {
		filter = strings.TrimSpace(*params.Cwd)
	}

	if filter != "" && !filepath.IsAbs(filter) {
		return acp.ListSessionsResponse{}, wire.Unsupported(fieldCwd)
	}

	a.mu.Lock()
	active := make([]*session, 0, len(a.sessions))

	for id, s := range a.sessions {
		if !a.deleted[id] && !s.ephemeral && (filter == "" || filter == s.cwd) {
			active = append(active, s)
		}
	}
	a.mu.Unlock()

	sessions := make([]acp.SessionInfo, 0, len(active))
	seen := make(map[acp.SessionId]struct{}, len(active))

	for _, s := range active {
		sessions = append(sessions, s.sessionInfo())
		seen[s.id] = struct{}{}
	}

	listCtx, cancel := context.WithTimeout(ctx, acpcore.SessionStoreTimeout)
	defer cancel()

	listCtx, finishList := a.observe.StartSessionStore(listCtx, "list")
	summaries, err := a.store.ListSessions(listCtx)
	finishList(err)

	if err != nil {
		return acp.ListSessionsResponse{}, wire.InternalFailure(vendor, "")
	}

	for _, summary := range summaries {
		id := acp.SessionId(summary.SessionID)
		if _, ok := seen[id]; ok || a.deletedSession(id) {
			continue
		}

		stored, loadErr := a.loadStored(ctx, id)
		if loadErr != nil {
			return acp.ListSessionsResponse{}, wire.InternalFailure(vendor, "")
		}

		cwd := stored.record.Cwd

		if filter != "" && cwd != filter {
			continue
		}

		title := storedTitle(stored.record.NativeSessionID, stored.rows)
		updatedAt := time.UnixMilli(summary.UpdatedAtUnixMilli).UTC().Format(time.RFC3339)

		sessions = append(sessions, acp.SessionInfo{
			Meta:                  wire.NativeSessionMeta(vendor, stored.record.NativeSessionID),
			SessionId:             id,
			Cwd:                   cwd,
			AdditionalDirectories: slices.Clone(stored.record.AdditionalDirectories),
			Title:                 &title,
			UpdatedAt:             &updatedAt,
		})
		seen[id] = struct{}{}
	}

	page, next, err := wire.PaginateSessions(sessions, params.Cursor)
	if err != nil {
		return acp.ListSessionsResponse{}, err
	}

	return acp.ListSessionsResponse{Sessions: page, NextCursor: next}, nil
}

// Prompt sends one turn to opencode and streams updates until it settles.
func (a *Agent) Prompt(ctx context.Context, params acp.PromptRequest) (resp acp.PromptResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.PromptResponse{}, openErr
	}

	s, err := a.session(ctx, params.SessionId)
	if err != nil {
		return acp.PromptResponse{}, err
	}

	var raw json.RawMessage
	if t := a.transportRef(); t != nil {
		raw = t.TakeRawPrompt(params.SessionId, params.Meta)
	}

	ctx, finish := a.observe.StartPrompt(ctx, params.Meta, s.currentModel())
	defer func() { finish(observer.PromptResultFrom(resp, err, s.currentModel(), "")) }()

	return s.prompt(ctx, params, raw)
}

func (s *session) currentModel() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.model
}

// Cancel interrupts the session's in-flight turn. It is wire-silent on an
// unknown session or with no turn in flight.
func (a *Agent) Cancel(ctx context.Context, params acp.CancelNotification) (err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return openErr
	}

	_, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionCancel)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return wire.ParamRefusal(refusal)
	}

	s, err := a.session(ctx, params.SessionId)
	if err != nil {
		return nil
	}

	s.cancel(ctx)

	return nil
}

// CloseSession runs the shutdown ladder for one session.
func (a *Agent) CloseSession(ctx context.Context, params acp.CloseSessionRequest) (resp acp.CloseSessionResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.CloseSessionResponse{}, openErr
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionClose)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.CloseSessionResponse{}, wire.ParamRefusal(refusal)
	}

	s, err := a.session(ctx, params.SessionId)
	if err != nil {
		return acp.CloseSessionResponse{}, err
	}

	closeErr := s.close(ctx)
	a.detach(ctx, s)

	if closeErr != nil {
		return acp.CloseSessionResponse{}, wire.InternalFailure(vendor, "")
	}

	return acp.CloseSessionResponse{}, nil
}

// UnstableDeleteSession tombstones the session first, then closes any live
// session with the same id. Native state stays in opencode's home.
func (a *Agent) UnstableDeleteSession(ctx context.Context, params acp.UnstableDeleteSessionRequest) (resp acp.UnstableDeleteSessionResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.UnstableDeleteSessionResponse{}, openErr
	}

	ctx, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodSessionDelete)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.UnstableDeleteSessionResponse{}, wire.ParamRefusal(refusal)
	}

	deleteCtx, cancel := context.WithTimeout(ctx, acpcore.SessionStoreTimeout)
	defer cancel()

	if refusal := wire.CheckSessionID(params.SessionId); refusal != nil {
		return acp.UnstableDeleteSessionResponse{}, refusal
	}

	// The opening publication precedes the close that fences it.
	if transport := a.transportRef(); transport != nil {
		if err := transport.AwaitSession(ctx, params.SessionId); err != nil {
			return acp.UnstableDeleteSessionResponse{}, err
		}
	}

	a.mu.Lock()
	ephemeral := a.ephemeral[params.SessionId]
	a.mu.Unlock()

	// An ephemeral session was never written, so the store has nothing to
	// tombstone for it.
	if !ephemeral {
		if err := a.store.Delete(deleteCtx, acpcore.SessionKey{SessionID: string(params.SessionId)}); err != nil {
			return acp.UnstableDeleteSessionResponse{}, wire.InternalFailure(vendor, "")
		}
	}

	a.mu.Lock()
	a.deleted[params.SessionId] = true
	delete(a.ephemeral, params.SessionId)
	s := a.sessions[params.SessionId]
	a.mu.Unlock()

	if s == nil {
		return acp.UnstableDeleteSessionResponse{}, nil
	}

	closeErr := s.close(ctx)
	a.detach(ctx, s)

	if closeErr != nil {
		return acp.UnstableDeleteSessionResponse{}, wire.InternalFailure(vendor, "")
	}

	return acp.UnstableDeleteSessionResponse{}, nil
}

// SetSessionConfigOption applies one select value.
func (a *Agent) SetSessionConfigOption(ctx context.Context, params acp.SetSessionConfigOptionRequest) (resp acp.SetSessionConfigOptionResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.SetSessionConfigOptionResponse{}, openErr
	}

	var meta map[string]any

	switch {
	case params.ValueId != nil:
		meta = params.ValueId.Meta
	case params.Boolean != nil:
		meta = params.Boolean.Meta
	}

	ctx, finish := a.observe.StartACP(ctx, meta, acp.AgentMethodSessionSetConfigOption)
	defer func() { finish(err) }()

	if refusal := lifecycle.RejectKey(meta); refusal != nil {
		return acp.SetSessionConfigOptionResponse{}, wire.ParamRefusal(refusal)
	}

	if params.ValueId == nil {
		return acp.SetSessionConfigOptionResponse{}, wire.Unsupported(fieldType)
	}

	s, err := a.session(ctx, params.ValueId.SessionId)
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	options, err := s.setConfigOption(ctx, params.ValueId.ConfigId, string(params.ValueId.Value))
	if err != nil {
		return acp.SetSessionConfigOptionResponse{}, err
	}

	return acp.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}

// session resolves an addressed id to its live session. A deleted id is
// indistinguishable from one that never existed.
func (a *Agent) session(ctx context.Context, sessionID acp.SessionId) (*session, error) {
	if refusal := wire.CheckSessionID(sessionID); refusal != nil {
		return nil, refusal
	}

	a.mu.Lock()

	if a.closed {
		a.mu.Unlock()

		return nil, wire.AgentClosed()
	}

	s := a.sessions[sessionID]
	if s == nil || a.deleted[sessionID] {
		a.mu.Unlock()

		return nil, wire.UnknownSession()
	}

	a.mu.Unlock()

	if transport := a.transportRef(); transport != nil {
		if err := transport.AwaitSession(ctx, sessionID); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (a *Agent) deletedSession(id acp.SessionId) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.deleted[id]
}

// detach removes a session from the active map while it still resolves to
// this exact session.
func (a *Agent) detach(ctx context.Context, s *session) {
	a.mu.Lock()

	current := a.sessions[s.id] == s
	if current {
		delete(a.sessions, s.id)
	}
	a.mu.Unlock()

	if current {
		a.observe.AddActiveSession(ctx, -1)
	}
}
