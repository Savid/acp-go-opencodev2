package opencodeacp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

// markerAttempts bounds how long a commit waits for an execution's durable
// idle marker to become visible in the native export after its terminal event.
const markerAttempts = 5
const markerRetryDelay = 100 * time.Millisecond

// errRepublish reports a native history that no longer continues the mirrored
// rows: mirrored messages changed or are gone, or a mirrored session left the
// graph.
var errRepublish = errors.New("native history no longer continues the mirror")

type nativeSnapshot struct {
	opencode.Export
	Inbox []opencode.InboxInput
}

// mirrorMark is where the mirrored graph continues: for each native session,
// how many of its messages the store holds and a digest of their bytes.
type mirrorMark map[string]heldHistory

type heldHistory struct {
	count  int
	digest [sha256.Size]byte
}

// graphMark is the mark of a whole stored graph.
func graphMark(graph []opencode.Export) mirrorMark {
	mark := make(mirrorMark, len(graph))
	for i := range graph {
		digest := sha256.New()
		for _, message := range graph[i].Messages {
			writeMessage(digest, message.Raw)
		}

		mark[graph[i].Info.ID] = heldHistory{count: len(graph[i].Messages), digest: [sha256.Size]byte(digest.Sum(nil))}
	}

	return mark
}

// writeMessage adds one length-prefixed message to a history digest.
func writeMessage(digest hash.Hash, raw []byte) {
	var length [binary.MaxVarintLen64]byte

	digest.Write(length[:binary.PutUvarint(length[:], uint64(len(raw)))])
	digest.Write(raw)
}

// mirrorSource reads the native graph for one commit, leaving its latest read
// in last; the mirror may run it more than once, so the session takes the
// read's state, captured images included, only after the commit succeeds. Continuing from a mark,
// it streams each session's export and keeps only the messages past what the
// store holds, so a held message's bytes are never retained; from the zero
// cursor it returns the whole graph. With a terminal event the root ends at
// that execution's idle marker and each descendant at its latest one; without
// one the whole graph must be idle around the read.
func (s *session) mirrorSource(ctx context.Context, b *binding, terminal *opencode.Event, last *graphTail) sessionlog.Source {
	return func(from sessionlog.Cursor) (sessionlog.Tail, error) {
		mark, continues := from.Position().(mirrorMark)

		var (
			tail graphTail
			err  error
		)

		for attempt := 1; ; attempt++ {
			tail, err = s.readTail(ctx, b, terminal, mark)
			if !errors.Is(err, opencode.ErrMarkerMissing) || attempt == markerAttempts {
				break
			}

			select {
			case <-ctx.Done():
				return sessionlog.Tail{}, ctx.Err()
			case <-time.After(markerRetryDelay):
			}
		}

		if err != nil {
			return sessionlog.Tail{}, err
		}

		tail.artifacts = s.captureImages(tail.root)
		*last = tail

		return sessionlog.Tail{Rows: tail.rows, Next: sessionlog.Position(tail.mark), Full: !continues}, nil
	}
}

// graphTail is one read of the native graph: the rows past the mark, the
// mark after them, and the graph's information and undelivered inputs.
type graphTail struct {
	// artifacts are the images the read's new root messages captured.
	artifacts map[string]imageArtifact
	rows      [][]byte
	mark      mirrorMark
	infos     []json.RawMessage
	inputs    map[string][]opencode.InboxInput
	root      []opencode.Message
}

func (s *session) readTail(ctx context.Context, b *binding, terminal *opencode.Event, mark mirrorMark) (graphTail, error) {
	if terminal == nil {
		if err := requireInactive(ctx, b, []string{s.nativeID}); err != nil {
			return graphTail{}, err
		}
	}

	tail := graphTail{mark: mirrorMark{}, inputs: map[string][]opencode.InboxInput{}}
	pending := []string{s.nativeID}

	var ids []string

	for len(pending) > 0 {
		id := pending[0]
		pending = pending[1:]

		if _, seen := tail.mark[id]; seen {
			return graphTail{}, errors.New("native session graph cycle")
		}

		cut := cutCompleted
		if terminal == nil {
			cut = cutAll
		} else if id == s.nativeID {
			cut = cutMarker
		}

		read, err := readSessionTail(ctx, b, id, mark[id], cut, terminal)
		if err != nil {
			return graphTail{}, err
		}

		if id == s.nativeID {
			tail.root = read.fresh
		}

		for i := range read.fresh {
			tail.rows = append(tail.rows, mirrorRow(id, read.fresh[i].Raw))
		}

		tail.mark[id] = heldHistory{count: mark[id].count + len(read.fresh), digest: read.digest}
		tail.infos = append(tail.infos, read.info)
		ids = append(ids, id)

		if len(read.inputs) > 0 {
			tail.inputs[id] = read.inputs
		}

		children, err := childSessions(ctx, b, id)
		if err != nil {
			return graphTail{}, err
		}

		pending = append(pending, children...)
	}

	for id := range mark {
		if _, ok := tail.mark[id]; !ok {
			return graphTail{}, fmt.Errorf("%w: mirrored session %s left the graph", errRepublish, id)
		}
	}

	if terminal == nil {
		if err := requireNativeIdle(ctx, b, ids); err != nil {
			return graphTail{}, err
		}
	}

	return tail, nil
}

type cutRule int

const (
	// cutAll keeps every settled message of an idle session.
	cutAll cutRule = iota
	// cutMarker ends the root at the terminal execution's idle marker.
	cutMarker
	// cutCompleted ends a descendant at its latest idle marker.
	cutCompleted
)

// sessionTail is one session's read: its information, the settled messages
// past what the store holds, and its undelivered inbox inputs.
type sessionTail struct {
	info   json.RawMessage
	digest [sha256.Size]byte
	fresh  []opencode.Message
	inputs []opencode.InboxInput
}

// readSessionTail streams one session's export past held. A held message is
// read only for its identity and digest; a held history whose bytes changed,
// such as a message captured mid-execution and completed since, no longer
// continues the mirror. An input delivered as a message inside the cut
// leaves the inbox; one delivered past it stays pending.
func readSessionTail(ctx context.Context, b *binding, id string, held heldHistory, cut cutRule, terminal *opencode.Event) (sessionTail, error) {
	// Read the inbox first so delivery cannot disappear between the two APIs.
	inbox, err := b.client.Inbox(ctx, id)
	if err != nil {
		return sessionTail{}, err
	}

	var (
		read      sessionTail
		index     int
		marker    = -1
		digest    = sha256.New()
		delivered = map[string]int{}
	)

	markerID, outcome := "", ""
	if cut == cutMarker {
		if markerID, outcome, err = opencode.ExecutionMarker(*terminal); err != nil {
			return sessionTail{}, err
		}

		if terminal.SessionID != id {
			return sessionTail{}, errors.New("invalid native execution boundary")
		}
	}

	inputs := make(map[string]opencode.InboxInput, len(inbox))
	for _, input := range inbox {
		inputs[input.ID] = input
	}

	err = b.client.StreamExport(ctx, id, func(info json.RawMessage) error {
		var native opencode.NativeSession
		if decodeErr := json.Unmarshal(info, &native); decodeErr != nil {
			return decodeErr
		}

		if native.ID != id {
			return errors.New("native export identity changed")
		}

		read.info = info

		return nil
	}, func(raw []byte) error {
		message, parseErr := opencode.ParseMessage(raw)
		if parseErr != nil {
			return parseErr
		}

		if message.ID == "" {
			return errors.New("native message identity missing")
		}

		if input, ok := inputs[message.ID]; ok {
			if verifyErr := verifyPendingInputs([]opencode.InboxInput{input}, nil, []opencode.Message{message}); verifyErr != nil {
				return verifyErr
			}

			delivered[message.ID] = index
		}

		if message.ID == markerID && message.Type == "idle" && message.Outcome == outcome {
			marker = index
		}

		if index < held.count {
			writeMessage(digest, raw)
		} else {
			message.Raw = bytes.Clone(raw)
			read.fresh = append(read.fresh, message)
		}

		index++

		return nil
	})
	if err != nil {
		return sessionTail{}, err
	}

	if read.info == nil {
		return sessionTail{}, errors.New("native export information missing")
	}

	if index < held.count {
		return sessionTail{}, fmt.Errorf("%w: native history of %s shrank from %d to %d messages", errRepublish, id, held.count, index)
	}

	if held.count > 0 && [sha256.Size]byte(digest.Sum(nil)) != held.digest {
		return sessionTail{}, fmt.Errorf("%w: mirrored messages of %s changed", errRepublish, id)
	}

	if read.fresh, err = cutFresh(read.fresh, cut, marker, held.count); err != nil {
		return sessionTail{}, err
	}

	for i := range read.fresh {
		writeMessage(digest, read.fresh[i].Raw)
	}

	read.digest = [sha256.Size]byte(digest.Sum(nil))
	end := held.count + len(read.fresh)

	for _, input := range inbox {
		if at, ok := delivered[input.ID]; !ok || at >= end {
			read.inputs = append(read.inputs, input)
		}
	}

	return read, nil
}

// cutFresh ends the new messages where the commit's boundary falls. marker is
// the index of the terminal execution's idle marker in the whole history, or
// negative when the export lacks it; held counts the messages before fresh.
func cutFresh(fresh []opencode.Message, cut cutRule, marker, held int) ([]opencode.Message, error) {
	switch cut {
	case cutMarker:
		switch {
		case marker < 0:
			return nil, opencode.ErrMarkerMissing
		case marker < held:
			return fresh[:0], nil
		default:
			return fresh[:marker-held+1], nil
		}
	case cutCompleted:
		for i := range slices.Backward(fresh) {
			if fresh[i].Type == "idle" {
				return fresh[:i+1], nil
			}
		}

		return fresh[:0], nil
	default:
		return fresh, nil
	}
}

// mirrorRow is one stored row: a native message's raw bytes with the session
// it belongs to.
func mirrorRow(sessionID string, message []byte) []byte {
	id, _ := json.Marshal(sessionID)

	row := make([]byte, 0, len(id)+len(message)+26)
	row = append(row, `{"sessionId":`...)
	row = append(row, id...)
	row = append(row, `,"message":`...)
	row = append(row, message...)

	return append(row, '}')
}

// storedRow is the decoded shape of a mirror row.
type storedRow struct {
	SessionID string          `json:"sessionId"`
	Message   json.RawMessage `json:"message"`
}

// readGraph exports the whole idle native session graph, parent first.
func (s *session) readGraph(ctx context.Context, b *binding) ([]nativeSnapshot, error) {
	if err := requireInactive(ctx, b, []string{s.nativeID}); err != nil {
		return nil, err
	}

	graph, err := s.readExports(ctx, b)
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(graph))
	inputs := make(map[string][]opencode.InboxInput)

	for i := range graph {
		item := &graph[i]
		ids[i] = item.Info.ID

		item.Inbox, err = undeliveredInputs(item.Inbox, item.Messages)
		if err != nil {
			return nil, err
		}

		if len(item.Inbox) > 0 {
			inputs[item.Info.ID] = item.Inbox
		}
	}

	if err := requireNativeIdle(ctx, b, ids); err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.pendingInputs = inputs
	s.mu.Unlock()

	return graph, nil
}

// readExports reads the root and every descendant, parent first.
func (s *session) readExports(ctx context.Context, b *binding) ([]nativeSnapshot, error) {
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

		// Read the inbox first so delivery cannot disappear between the two APIs.
		inbox, err := b.client.Inbox(ctx, id)
		if err != nil {
			return nil, err
		}

		item, err := b.client.Export(ctx, id)
		if err != nil {
			return nil, err
		}

		if item.Info.ID != id {
			return nil, errors.New("native export identity changed")
		}

		exports = append(exports, nativeSnapshot{Export: item, Inbox: inbox})

		children, err := childSessions(ctx, b, id)
		if err != nil {
			return nil, err
		}

		pending = append(pending, children...)
	}

	return exports, nil
}

// childSessions lists a session's direct children in creation order.
func childSessions(ctx context.Context, b *binding, id string) ([]string, error) {
	var (
		ids    []string
		cursor string
	)

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

		if err := b.client.Do(ctx, "", http.MethodGet, path, nil, &children); err != nil {
			return nil, err
		}

		for i := range children.Data {
			ids = append(ids, children.Data[i].ID)
		}

		if children.Cursor.Next == "" {
			return ids, nil
		}

		if children.Cursor.Next == cursor {
			return nil, errors.New("native pagination did not advance")
		}

		cursor = children.Cursor.Next
	}
}

func requireInactive(ctx context.Context, b *binding, ids []string) error {
	var active struct {
		Data map[string]opencode.NativeSessionStatus `json:"data"`
	}
	if err := b.client.Do(ctx, "", http.MethodGet, "/api/session/active", nil, &active); err != nil {
		return err
	}

	for _, id := range ids {
		if _, ok := active.Data[id]; ok {
			return errors.New("native session is still running")
		}
	}

	return nil
}

func requireNativeIdle(ctx context.Context, b *binding, ids []string) error {
	if err := requireInactive(ctx, b, ids); err != nil {
		return err
	}

	for _, id := range ids {
		for _, suffix := range []string{"/form", "/permission"} {
			var pending struct {
				Data []json.RawMessage `json:"data"`
			}
			if err := b.client.Do(ctx, "", http.MethodGet, opencode.SessionPath(id)+suffix, nil, &pending); err != nil {
				return err
			}

			if len(pending.Data) > 0 {
				return errors.New("native session has pending input")
			}
		}
	}

	return nil
}

// undeliveredInputs excludes inputs the export already contains as messages.
func undeliveredInputs(inputs []opencode.InboxInput, messages []opencode.Message) ([]opencode.InboxInput, error) {
	var pending []opencode.InboxInput

	for _, input := range inputs {
		index := slices.IndexFunc(messages, func(message opencode.Message) bool { return message.ID == input.ID })
		if index < 0 {
			pending = append(pending, input)

			continue
		}

		if err := verifyPendingInputs([]opencode.InboxInput{input}, nil, messages[index:index+1]); err != nil {
			return nil, err
		}
	}

	return pending, nil
}
