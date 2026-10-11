package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

// markerAttempts bounds how long a commit waits for an execution's durable
// idle marker to become visible in the native export after its terminal event.
const markerAttempts = 5
const markerRetryDelay = 100 * time.Millisecond

type nativeSnapshot struct {
	opencode.Export
	Inbox []opencode.InboxInput
}

// readGraph exports the native session graph once. With a terminal event the
// root ends at that execution's idle marker and each descendant at its latest
// one; without it the whole graph must be idle around the read.
func (s *session) readGraph(ctx context.Context, rt *binding, terminal *opencode.Event) ([]nativeSnapshot, error) {
	for attempt := 1; ; attempt++ {
		graph, err := s.exportGraph(ctx, rt, terminal)
		if !errors.Is(err, opencode.ErrMarkerMissing) || attempt == markerAttempts {
			if err != nil {
				return nil, fmt.Errorf("native history snapshot: %w", err)
			}

			return graph, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(markerRetryDelay):
		}
	}
}

func (s *session) exportGraph(ctx context.Context, rt *binding, terminal *opencode.Event) ([]nativeSnapshot, error) {
	if terminal == nil {
		if err := requireInactive(ctx, rt, []string{s.nativeID}); err != nil {
			return nil, err
		}
	}

	graph, err := s.readExports(ctx, rt)
	if err != nil {
		return nil, err
	}

	if terminal != nil {
		if graph[0].Export, err = graph[0].ExecutionPrefix(*terminal); err != nil {
			return nil, err
		}

		for i := 1; i < len(graph); i++ {
			graph[i].Export = graph[i].CompletedPrefix()
		}
	} else if idleErr := requireNativeIdle(ctx, rt, graph); idleErr != nil {
		return nil, idleErr
	}

	inputs := make(map[string][]opencode.InboxInput)
	infos := make([]json.RawMessage, len(graph))

	for i := range graph {
		item := &graph[i]

		item.Inbox, err = undeliveredInputs(item.Inbox, item.Messages)
		if err != nil {
			return nil, err
		}

		if len(item.Inbox) > 0 {
			inputs[item.Info.ID] = item.Inbox
		}

		infos[i] = item.InfoRaw
	}

	s.mu.Lock()
	s.pendingInputs = inputs
	s.nativeInfos = infos
	s.mu.Unlock()

	return graph, nil
}

// readExports reads the root and every descendant, parent first.
func (s *session) readExports(ctx context.Context, rt *binding) ([]nativeSnapshot, error) {
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
		inbox, err := rt.client.Inbox(ctx, id)
		if err != nil {
			return nil, err
		}

		item, err := rt.client.Export(ctx, id)
		if err != nil {
			return nil, err
		}

		if item.Info.ID != id {
			return nil, errors.New("native export identity changed")
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
				pending = append(pending, children.Data[i].ID)
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

func requireInactive(ctx context.Context, rt *binding, ids []string) error {
	var active struct {
		Data map[string]opencode.NativeSessionStatus `json:"data"`
	}
	if err := rt.client.Do(ctx, "", http.MethodGet, "/api/session/active", nil, &active); err != nil {
		return err
	}

	for _, id := range ids {
		if _, ok := active.Data[id]; ok {
			return errors.New("native session is still running")
		}
	}

	return nil
}

func requireNativeIdle(ctx context.Context, rt *binding, graph []nativeSnapshot) error {
	ids := make([]string, len(graph))
	for i := range graph {
		ids[i] = graph[i].Info.ID
	}

	if err := requireInactive(ctx, rt, ids); err != nil {
		return err
	}

	for _, id := range ids {
		for _, suffix := range []string{"/form", "/permission"} {
			var pending struct {
				Data []json.RawMessage `json:"data"`
			}
			if err := rt.client.Do(ctx, "", http.MethodGet, opencode.SessionPath(id)+suffix, nil, &pending); err != nil {
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

// graphRows lays the graph's messages out as mirror rows: each message's raw
// native bytes, merged across sessions by creation time with each session's
// own order kept, and a session row before every run of one session's
// messages. Later commits add messages created later, so the layout of a
// longer history extends the rows committed before it.
func graphRows(graph []nativeSnapshot) [][]byte {
	total := 0
	for i := range graph {
		total += len(graph[i].Messages)
	}

	rows := make([][]byte, 0, total+len(graph))
	next := make([]int, len(graph))
	current := -1

	for range total {
		pick, created := -1, int64(0)

		for i := range graph {
			messages := graph[i].Messages
			if next[i] < len(messages) && (pick < 0 || messages[next[i]].Created < created) {
				pick, created = i, messages[next[i]].Created
			}
		}

		if pick != current {
			rows = append(rows, sessionRow(graph[pick].Info.ID))
			current = pick
		}

		rows = append(rows, graph[pick].Messages[next[pick]].Raw)
		next[pick]++
	}

	return rows
}

// storedRow is a mirror row: a session row names the session whose messages
// follow; any other row is a native message.
type storedRow struct {
	SessionID string `json:"sessionId"`
	ID        string `json:"id"`
	Type      string `json:"type"`
}

func sessionRow(id string) []byte {
	row, _ := json.Marshal(struct {
		SessionID string `json:"sessionId"`
	}{id})

	return row
}
