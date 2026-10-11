package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

// sessionRecord carries the accepted session configuration and the native
// information of every session in the graph, parent first, beside the
// mirrored native messages.
type sessionRecord struct {
	SessionID             string                           `json:"sessionId"`
	NativeSessionID       string                           `json:"nativeSessionId"`
	Title                 string                           `json:"title,omitempty"`
	Cwd                   string                           `json:"cwd"`
	AdditionalDirectories []string                         `json:"additionalDirectories,omitempty"`
	Env                   map[string]string                `json:"env,omitempty"`
	ExtraPathDirs         []string                         `json:"extraPathDirs,omitempty"`
	Model                 string                           `json:"model,omitempty"`
	Effort                string                           `json:"effort,omitempty"`
	Mode                  string                           `json:"mode,omitempty"`
	Permission            string                           `json:"permission,omitempty"`
	Sessions              []json.RawMessage                `json:"sessions"`
	Artifacts             map[string]imageArtifact         `json:"artifacts,omitempty"`
	PendingInputs         map[string][]opencode.InboxInput `json:"pendingInputs,omitempty"`
}

func (s *session) record() sessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sessionRecord{SessionID: string(s.id), NativeSessionID: s.nativeID, Title: s.title, Cwd: s.cwd, AdditionalDirectories: slices.Clone(s.additionalDirectories), Env: maps.Clone(s.options.Env), ExtraPathDirs: slices.Clone(s.options.ExtraPathDirs), Model: s.model, Effort: s.effort, Mode: s.mode, Permission: s.options.Permission, Sessions: slices.Clone(s.nativeInfos), Artifacts: cloneArtifacts(s.artifacts), PendingInputs: clonePendingInputs(s.pendingInputs)}
}

func (r sessionRecord) validate(id string) error {
	if !validNativeSessionID(r.NativeSessionID) || r.SessionID != id || !filepath.IsAbs(r.Cwd) || len(r.Sessions) == 0 {
		return errors.New("invalid session record")
	}

	for _, dir := range r.AdditionalDirectories {
		if !filepath.IsAbs(dir) {
			return errors.New("invalid additional directory")
		}
	}

	for id, inputs := range r.PendingInputs {
		seen := make(map[string]bool, len(inputs))
		for _, input := range inputs {
			if seen[input.ID] {
				return errors.New("duplicate native pending input")
			}

			seen[input.ID] = true
			if err := input.Validate(id); err != nil {
				return err
			}
		}
	}

	for _, artifact := range r.Artifacts {
		if artifact.Refusal != "" {
			if _, ok := (&image.OutputError{Reason: artifact.Refusal}).Guidance(); !ok {
				return errors.New("invalid stored image refusal")
			}

			continue
		}

		_, mime, _, refusal := image.DecodeInline(artifact.Data, image.FrameClamp)
		if refusal != nil || mime != artifact.MIME {
			return errors.New("invalid stored image artifact")
		}
	}

	_, err := parseSessionMeta(inheritCarrier(sessionMeta{}, r).Meta())
	if err != nil {
		return err
	}

	return nil
}

func validNativeSessionID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}

	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return false
		}
	}

	return true
}

// commitMirror publishes the native graph and its configuration through the
// session's mirror, which appends only the messages the store does not hold.
// A history that no longer continues the mirror is republished whole. An
// ephemeral session commits nothing: opencode's own database holds it until
// the host deletes it.
func (s *session) commitMirror(ctx context.Context, rt *binding, terminal *opencode.Event) error {
	if s.ephemeral {
		return nil
	}

	if rt == nil {
		return errors.New("session has no native binding")
	}

	if terminal == nil {
		if err := rt.client.Wait(ctx, s.nativeID); err != nil {
			return err
		}
	}

	ctx, finish := s.agent.observe.StartSessionStore(ctx, observer.SessionStoreCommit)

	var last graphTail

	source := s.mirrorSource(ctx, rt, terminal, &last)
	record := func(sessionlog.Tail) (any, error) {
		value := s.record()
		value.Sessions = last.infos
		value.PendingInputs = last.inputs

		return value, nil
	}

	err := s.mirror.Commit(ctx, source, record)
	if errors.Is(err, errRepublish) {
		s.mirror.Reset()
		err = s.mirror.Commit(ctx, source, record)
	}

	finish(err)

	if err == nil {
		s.mu.Lock()
		s.persisted = true
		s.pendingInputs = last.inputs
		s.nativeInfos = last.infos
		s.mu.Unlock()
	}

	return err
}

type storedSession struct {
	rows   [][]byte
	graph  []opencode.Export
	record sessionRecord
	found  bool
}

func (a *Agent) loadStored(ctx context.Context, id acp.SessionId) (storedSession, error) {
	ctx, finish := a.observe.StartSessionStore(ctx, observer.SessionStoreLoad)

	var record sessionRecord

	rows, found, err := sessionlog.Load(ctx, a.store, string(id), &record)
	if err == nil && found {
		err = record.validate(string(id))
	}

	var graph []opencode.Export
	if err == nil && found {
		graph, err = decodeStored(rows, record)
	}

	if err == nil && found {
		err = validateStoredImages(graph[0].Messages, record)
	}

	finish(err)

	if err != nil {
		return storedSession{}, wire.RestoreRefused(ctx, a.log, vendor, id, err)
	}

	return storedSession{rows: rows, graph: graph, record: record, found: found}, nil
}

// loadRecord reads a stored session's configuration without its native rows.
func (a *Agent) loadRecord(ctx context.Context, id acp.SessionId) (sessionRecord, bool, error) {
	ctx, finish := a.observe.StartSessionStore(ctx, observer.SessionStoreLoadRecord)

	var record sessionRecord

	found, err := sessionlog.LoadRecord(ctx, a.store, string(id), &record)
	if err == nil && found {
		err = record.validate(string(id))
	}

	finish(err)

	return record, found, err
}

// decodeStored rebuilds the parent-first native graph from the record's
// session information and the mirrored message rows.
func decodeStored(rows [][]byte, record sessionRecord) ([]opencode.Export, error) {
	graph := make([]opencode.Export, len(record.Sessions))
	index := make(map[string]int, len(record.Sessions))

	for i, raw := range record.Sessions {
		var info opencode.NativeSession
		if err := json.Unmarshal(raw, &info); err != nil {
			return nil, err
		}

		_, duplicate := index[info.ID]
		if !validNativeSessionID(info.ID) || duplicate {
			return nil, errors.New("invalid native export identity")
		}

		if _, parent := index[info.ParentID]; i == 0 && info.ID != record.NativeSessionID || i > 0 && !parent {
			return nil, errors.New("unrelated native export")
		}

		index[info.ID] = i
		graph[i] = opencode.Export{Info: info, InfoRaw: raw}
	}

	if len(graph) == 0 {
		return nil, errors.New("native export missing")
	}

	messages := make([]map[string]bool, len(graph))

	for _, raw := range rows {
		var row storedRow
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}

		at, ok := index[row.SessionID]
		if !ok {
			return nil, errors.New("unrelated native message row")
		}

		message, err := opencode.ParseMessage(row.Message)
		if err != nil {
			return nil, err
		}

		if message.ID == "" || message.Type == "" || messages[at][message.ID] {
			return nil, errors.New("invalid native message identity")
		}

		if messages[at] == nil {
			messages[at] = map[string]bool{}
		}

		messages[at][message.ID] = true
		graph[at].Messages = append(graph[at].Messages, message)
	}

	return graph, nil
}

// hydrate imports absent conversations and adopts a longer native transcript,
// returning the root's verified messages. The native API cannot replace an
// existing shorter transcript, so that case fails closed.
func (s *session) hydrate(ctx context.Context, rt *binding, stored storedSession) ([]opencode.Message, error) {
	s.mu.Lock()
	s.artifacts = cloneArtifacts(stored.record.Artifacts)
	s.mu.Unlock()

	want := stored.graph

	for i := range want {
		saved := &want[i]

		current, readErr := rt.client.Export(ctx, saved.Info.ID)
		switch {
		case opencode.IsMissing(readErr):
			location := saved.Info.Location
			if saved.Info.ID == s.nativeID {
				location.Directory = s.cwd
			}

			if importErr := rt.client.Import(ctx, *saved, location); importErr != nil {
				return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, importErr)
			}

			for _, input := range stored.record.PendingInputs[saved.Info.ID] {
				if enqueueErr := rt.client.Enqueue(ctx, input); enqueueErr != nil {
					return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, enqueueErr)
				}
			}

			if pendingErr := reconcileImportedInputs(ctx, rt, saved.Info.ID, stored.record.PendingInputs[saved.Info.ID]); pendingErr != nil {
				return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, pendingErr)
			}
		case readErr != nil:
			return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, readErr)
		default:
			if !current.ContainsHistory(*saved) {
				return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, errors.New("native history conflicts with mirror"))
			}
		}
	}

	verified, err := s.readGraph(ctx, rt)
	if err != nil {
		return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, err)
	}

	s.mu.Lock()
	pending := clonePendingInputs(s.pendingInputs)
	s.mu.Unlock()

	have := make(map[string]*opencode.Export, len(verified))
	for i := range verified {
		have[verified[i].Info.ID] = &verified[i].Export
	}

	for i := range want {
		saved := &want[i]

		current := have[saved.Info.ID]
		if current == nil {
			return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, errors.New("native import lost a session"))
		}

		if pendingErr := verifyPendingInputs(stored.record.PendingInputs[saved.Info.ID], pending[saved.Info.ID], current.Messages); pendingErr != nil {
			return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, pendingErr)
		}

		if !current.ContainsHistory(*saved) {
			return nil, wire.RestoreRefused(ctx, s.agent.log, vendor, s.id, errors.New("native import changed message content"))
		}
	}

	return verified[0].Messages, nil
}

func sameJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}

	return reflect.DeepEqual(x, y)
}
func (s *session) replay(ctx context.Context, messages []opencode.Message) error {
	c := &cycle{}

	for i := range messages {
		message, err := messages[i].Decode()
		if err != nil {
			return err
		}

		if message.Type != roleUser {
			if err := s.projectMessage(ctx, c, message); err != nil {
				return err
			}

			continue
		}

		if message.Text != "" {
			if err := s.emit(ctx, acp.UpdateUserMessageText(message.Text)); err != nil {
				return err
			}
		}

		for index, file := range message.Files {
			file.ID = attachmentID(message.ID, index)
			for _, block := range s.outputFile(file, nil) {
				if err := s.emit(ctx, acp.UpdateUserMessage(block)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func clonePendingInputs(inputs map[string][]opencode.InboxInput) map[string][]opencode.InboxInput {
	result := make(map[string][]opencode.InboxInput, len(inputs))
	for id, rows := range inputs {
		copied := slices.Clone(rows)
		for i := range copied {
			copied[i].Payload = slices.Clone(copied[i].Payload)
		}

		result[id] = copied
	}

	return result
}

func verifyPendingInputs(saved, current []opencode.InboxInput, messages []opencode.Message) error {
	for _, input := range saved {
		found := false

		for i := range messages {
			if messages[i].ID != input.ID {
				continue
			}

			var payload map[string]json.RawMessage
			if messages[i].Type != "synthetic" || json.Unmarshal(messages[i].Raw, &payload) != nil {
				return errors.New("native pending input changed message type")
			}

			delete(payload, "id")
			delete(payload, "time")
			delete(payload, "type")

			encoded, _ := json.Marshal(payload)
			if !sameJSON(encoded, input.Payload) {
				return errors.New("native pending input changed content")
			}

			found = true

			break
		}

		if found {
			continue
		}

		for _, pending := range current {
			if pending.ID == input.ID && pending.Delivery == input.Delivery && sameJSON(pending.Payload, input.Payload) {
				found = true

				break
			}
		}

		if !found {
			return errors.New("native pending input conflicts with mirror")
		}
	}

	return nil
}

// Import publishes native creation events; plugins may enqueue reminders already
// present in the saved inbox. Retain the saved identities for those reminders.
func reconcileImportedInputs(ctx context.Context, rt *binding, sessionID string, saved []opencode.InboxInput) error {
	if len(saved) == 0 {
		return nil
	}

	current, err := rt.client.Inbox(ctx, sessionID)
	if err != nil {
		return err
	}

	identities := make(map[string]bool, len(saved))
	for _, input := range saved {
		identities[input.ID] = true
	}

	for _, input := range current {
		if identities[input.ID] {
			continue
		}

		for _, original := range saved {
			if input.Delivery == original.Delivery && sameJSON(input.Payload, original.Payload) {
				if err := rt.client.CancelInput(ctx, sessionID, input.ID); err != nil {
					return err
				}

				break
			}
		}
	}

	return nil
}
