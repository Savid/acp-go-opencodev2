package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

// sessionRecord carries the accepted session configuration beside its native export.
type sessionRecord struct {
	SessionID             string                           `json:"sessionId"`
	NativeSessionID       string                           `json:"nativeSessionId"`
	Cwd                   string                           `json:"cwd"`
	AdditionalDirectories []string                         `json:"additionalDirectories,omitempty"`
	Env                   map[string]string                `json:"env,omitempty"`
	ExtraPathDirs         []string                         `json:"extraPathDirs,omitempty"`
	Model                 string                           `json:"model,omitempty"`
	Effort                string                           `json:"effort,omitempty"`
	Mode                  string                           `json:"mode,omitempty"`
	Permission            string                           `json:"permission,omitempty"`
	Artifacts             map[string]imageArtifact         `json:"artifacts,omitempty"`
	PendingInputs         map[string][]opencode.InboxInput `json:"pendingInputs,omitempty"`
	UpdatedAtUnixMilli    int64                            `json:"updatedAtUnixMilli"`
}

func (s *session) record() sessionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return sessionRecord{SessionID: string(s.id), NativeSessionID: s.nativeID, Cwd: s.cwd, AdditionalDirectories: slices.Clone(s.additionalDirectories), Env: maps.Clone(s.options.Env), ExtraPathDirs: slices.Clone(s.options.ExtraPathDirs), Model: s.model, Effort: s.effort, Mode: s.mode, Permission: s.options.Permission, Artifacts: cloneArtifacts(s.artifacts), PendingInputs: clonePendingInputs(s.pendingInputs), UpdatedAtUnixMilli: time.Now().UnixMilli()}
}

func (r sessionRecord) validate(id string) error {
	if !validNativeSessionID(r.NativeSessionID) || r.SessionID != id || !filepath.IsAbs(r.Cwd) || r.UpdatedAtUnixMilli <= 0 {
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

// commitMirror atomically stores a complete native snapshot and its
// configuration. An ephemeral session commits nothing: opencode's own database
// holds it until the host deletes it.
func (s *session) commitMirror(ctx context.Context, rt *binding) error {
	if s.ephemeral {
		return nil
	}

	s.mirrorMu.Lock()
	defer s.mirrorMu.Unlock()

	if rt != nil {
		if err := rt.client.Wait(ctx, s.nativeID); err != nil {
			return err
		}
	}

	rows, err := s.snapshotRows(ctx, rt)
	if err != nil {
		return err
	}

	ctx, finish := s.agent.observe.StartSessionStore(ctx, "replace")
	err = sessionlog.Commit(ctx, s.agent.store, string(s.id), rows, s.record())
	finish(err)

	if err == nil {
		s.mu.Lock()
		s.persisted = true
		s.mu.Unlock()
	}

	return err
}

// snapshotRows reads the whole native history of this session. A commit that
// cannot be attempted is an error: the turn it belongs to is not durable.
func (s *session) snapshotRows(ctx context.Context, rt *binding) ([][]byte, error) {
	if rt == nil {
		return nil, errors.New("session has no native binding")
	}

	rows, err := s.readNativeRows(ctx, rt)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, errors.New("native session history missing")
	}

	s.captureImages(rows)

	return rows, nil
}

type storedSession struct {
	rows   [][]byte
	record sessionRecord
	found  bool
}

func (a *Agent) loadStored(ctx context.Context, id acp.SessionId) (storedSession, error) {
	ctx, cancel := context.WithTimeout(ctx, acpcore.SessionStoreTimeout)
	defer cancel()

	ctx, finish := a.observe.StartSessionStore(ctx, "load")

	var record sessionRecord

	rows, found, err := sessionlog.Load(ctx, a.store, string(id), &record)
	if err == nil && found {
		err = record.validate(string(id))
	}

	if err == nil && found {
		var graph []opencode.Export

		graph, err = decodeExports(rows, record.NativeSessionID)
		if err == nil {
			members := make(map[string]bool, len(graph))
			for i := range graph {
				members[graph[i].Info.ID] = true
			}

			for id := range record.PendingInputs {
				if !members[id] {
					err = errors.New("unrelated native pending input")

					break
				}
			}
		}
	}

	if err == nil && found {
		err = validateStoredImages(rows, record)
	}

	finish(err)

	if err != nil {
		return storedSession{}, a.restoreRefused(ctx, id, err)
	}

	return storedSession{rows: rows, record: record, found: found}, nil
}

// decodeExports validates a parent-first native session graph.
func decodeExports(rows [][]byte, id string) ([]opencode.Export, error) {
	if len(rows) == 0 {
		return nil, errors.New("native export missing")
	}

	exports := make([]opencode.Export, 0, len(rows))
	seen := map[string]bool{}

	for index, row := range rows {
		var item opencode.Export
		if err := json.Unmarshal(row, &item); err != nil {
			return nil, err
		}

		if !validNativeSessionID(item.Info.ID) || seen[item.Info.ID] {
			return nil, errors.New("invalid native export identity")
		}

		if index == 0 && item.Info.ID != id || index > 0 && !seen[item.Info.ParentID] {
			return nil, errors.New("unrelated native export")
		}

		messages := map[string]bool{}

		for i := range item.Messages {
			message := &item.Messages[i]
			if message.ID == "" || message.Type == "" || messages[message.ID] {
				return nil, errors.New("invalid native message identity")
			}

			messages[message.ID] = true
		}

		seen[item.Info.ID] = true
		exports = append(exports, item)
	}

	return exports, nil
}

const snapshotAttempts = 5
const snapshotRetryDelay = 100 * time.Millisecond

// readNativeRows fences the full graph with two matching exports and idle checks.
func (s *session) readNativeRows(ctx context.Context, rt *binding) ([][]byte, error) {
	for range snapshotAttempts {
		first, err := s.exportGraph(ctx, rt)
		if err != nil {
			return nil, err
		}

		if idleErr := s.requireNativeIdle(ctx, rt, first); idleErr != nil {
			return nil, idleErr
		}

		second, err := s.exportGraph(ctx, rt)
		if err != nil {
			return nil, err
		}

		if reflect.DeepEqual(first, second) {
			if err := s.requireNativeIdle(ctx, rt, second); err != nil {
				return nil, err
			}

			inputs := make(map[string][]opencode.InboxInput)

			rows := make([][]byte, 0, len(second))
			for i := range second {
				item := &second[i]

				rows = append(rows, append([]byte(nil), item.Raw...))
				if len(item.Inbox) > 0 {
					inputs[item.Info.ID] = item.Inbox
				}
			}

			s.mu.Lock()
			s.pendingInputs = inputs
			s.mu.Unlock()

			return rows, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(snapshotRetryDelay):
		}
	}

	return nil, errors.New("native history changed while snapshotting")
}

type nativeSnapshot struct {
	opencode.Export
	Inbox []opencode.InboxInput
}

func (s *session) exportGraph(ctx context.Context, rt *binding) ([]nativeSnapshot, error) {
	var exports []nativeSnapshot

	pending := []string{s.nativeID}
	seen := map[string]bool{}

	for len(pending) > 0 {
		id := pending[0]
		pending = pending[1:]

		if seen[id] {
			return nil, errors.New("native session graph cycle")
		}

		seen[id] = true

		item, err := rt.client.Export(ctx, id)
		if err != nil {
			return nil, err
		}

		inbox, err := rt.client.Inbox(ctx, id)
		if err != nil {
			return nil, err
		}

		exports = append(exports, nativeSnapshot{Export: item, Inbox: inbox})
		cursor := ""

		for {
			var children struct {
				Data   []opencode.NativeSession `json:"data"`
				Cursor struct {
					Next string `json:"next"`
				} `json:"cursor"`
			}

			path := "/api/session?parentID=" + url.QueryEscape(id) + "&order=asc&limit=100"
			if cursor != "" {
				path += "&cursor=" + url.QueryEscape(cursor)
			}

			if err := rt.client.Do(ctx, "", http.MethodGet, path, nil, &children); err != nil {
				return nil, err
			}

			for i := range children.Data {
				child := &children.Data[i]
				pending = append(pending, child.ID)
			}

			if children.Cursor.Next == "" {
				break
			}

			if children.Cursor.Next == cursor {
				return nil, errors.New("native pagination did not advance")
			}

			cursor = children.Cursor.Next
		}
	}

	return exports, nil
}

// hydrate imports absent conversations and adopts a longer native transcript.
// The native API cannot replace an existing shorter transcript, so that case fails closed.
func (s *session) hydrate(ctx context.Context, rt *binding, stored storedSession) ([][]byte, error) {
	s.mu.Lock()
	s.artifacts = cloneArtifacts(stored.record.Artifacts)
	s.mu.Unlock()

	want, err := decodeExports(stored.rows, s.nativeID)
	if err != nil {
		return nil, s.agent.restoreRefused(ctx, s.id, err)
	}

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
				return nil, s.agent.restoreRefused(ctx, s.id, importErr)
			}

			for _, input := range stored.record.PendingInputs[saved.Info.ID] {
				if enqueueErr := rt.client.Enqueue(ctx, input); enqueueErr != nil {
					return nil, s.agent.restoreRefused(ctx, s.id, enqueueErr)
				}
			}

			if pendingErr := reconcileImportedInputs(ctx, rt, saved.Info.ID, stored.record.PendingInputs[saved.Info.ID]); pendingErr != nil {
				return nil, s.agent.restoreRefused(ctx, s.id, pendingErr)
			}
		case readErr != nil:
			return nil, s.agent.restoreRefused(ctx, s.id, readErr)
		default:
			if len(current.Messages) < len(saved.Messages) {
				return nil, s.agent.restoreRefused(ctx, s.id, errors.New("native transcript is shorter than its mirror"))
			}

			for index := range saved.Messages {
				message := &saved.Messages[index]
				if !sameJSON(message.Raw, current.Messages[index].Raw) {
					return nil, s.agent.restoreRefused(ctx, s.id, errors.New("native history conflicts with mirror"))
				}
			}
		}
	}

	rows, err := s.readNativeRows(ctx, rt)
	if err != nil {
		return nil, s.agent.restoreRefused(ctx, s.id, err)
	}

	verified, err := decodeExports(rows, s.nativeID)
	if err != nil {
		return nil, s.agent.restoreRefused(ctx, s.id, err)
	}

	s.mu.Lock()
	pending := clonePendingInputs(s.pendingInputs)
	s.mu.Unlock()

	have := map[string]opencode.Export{}

	for i := range verified {
		item := &verified[i]
		have[item.Info.ID] = *item
	}

	for i := range want {
		saved := &want[i]

		current := have[saved.Info.ID]
		if pendingErr := verifyPendingInputs(stored.record.PendingInputs[saved.Info.ID], pending[saved.Info.ID], current.Messages); pendingErr != nil {
			return nil, s.agent.restoreRefused(ctx, s.id, pendingErr)
		}

		if len(current.Messages) < len(saved.Messages) {
			return nil, s.agent.restoreRefused(ctx, s.id, errors.New("native import incomplete"))
		}

		for index := range saved.Messages {
			message := &saved.Messages[index]
			if !sameJSON(message.Raw, current.Messages[index].Raw) {
				return nil, s.agent.restoreRefused(ctx, s.id, errors.New("native import changed message content"))
			}
		}
	}

	return rows, nil
}
func sameJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}

	return reflect.DeepEqual(x, y)
}
func (a *Agent) restoreRefused(ctx context.Context, id acp.SessionId, err error) error {
	a.log.ErrorContext(ctx, "opencode session restore failed", slog.String("session_id", string(id)), slog.String("reason", err.Error()))

	return wire.RestoreFailed(vendor)
}
func storedTitle(id string, rows [][]byte) string {
	for _, row := range rows {
		var item opencode.Export
		if json.Unmarshal(row, &item) == nil && item.Info.ID == id && item.Info.Title != "" {
			return item.Info.Title
		}
	}

	return id
}
func nativeMessages(rows [][]byte, id string) []opencode.NativeMessage {
	for _, row := range rows {
		var item opencode.Export
		if json.Unmarshal(row, &item) == nil && item.Info.ID == id {
			return item.Messages
		}
	}

	return nil
}
func (s *session) replay(ctx context.Context, rows [][]byte) error {
	c := &cycle{}

	messages := nativeMessages(rows, s.nativeID)
	for i := range messages {
		message := &messages[i]
		if message.Type == roleUser {
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
		} else if err := s.projectMessage(ctx, c, *message); err != nil {
			return err
		}
	}

	return nil
}
func (s *session) requireNativeIdle(ctx context.Context, rt *binding, graph []nativeSnapshot) error {
	var active struct {
		Data map[string]opencode.NativeSessionStatus `json:"data"`
	}
	if err := rt.client.Do(ctx, "", http.MethodGet, "/api/session/active", nil, &active); err != nil {
		return err
	}

	for i := range graph {
		item := &graph[i]
		if _, ok := active.Data[item.Info.ID]; ok {
			return errors.New("native session is still running")
		}

		for _, suffix := range []string{"/form", "/permission"} {
			var pending struct {
				Data []json.RawMessage `json:"data"`
			}
			if err := rt.client.Do(ctx, "", http.MethodGet, opencode.SessionPath(item.Info.ID)+suffix, nil, &pending); err != nil {
				return err
			}

			if len(pending.Data) > 0 {
				return errors.New("native session has pending input")
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

func verifyPendingInputs(saved, current []opencode.InboxInput, messages []opencode.NativeMessage) error {
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
