package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"time"

	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

const snapshotAttempts = 5
const snapshotRetryDelay = 100 * time.Millisecond

// readNativeRows verifies completed exports against a second read. Without an
// execution boundary, the entire graph must be idle and unchanged.
func (s *session) readNativeRows(ctx context.Context, rt *binding, terminal *opencode.Event) ([][]byte, error) {
	var lastErr error

	for range snapshotAttempts {
		first, err := s.exportGraph(ctx, rt)
		if err != nil {
			return nil, err
		}

		if terminal == nil {
			if idleErr := s.requireNativeIdle(ctx, rt, first); idleErr != nil {
				return nil, idleErr
			}
		}

		second, err := s.exportGraph(ctx, rt)
		if err != nil {
			return nil, err
		}

		if terminal != nil {
			first, err = completedGraph(first, *terminal)
		}

		verified := false

		if err == nil {
			if normalizeErr := normalizeInputs(first); normalizeErr != nil {
				return nil, normalizeErr
			}

			if normalizeErr := normalizeInputs(second); normalizeErr != nil {
				return nil, normalizeErr
			}

			verified = reflect.DeepEqual(first, second)
			if terminal != nil {
				verified, err = snapshotRetained(first, second)
				if err != nil {
					return nil, err
				}
			}
		}

		if verified {
			if terminal == nil {
				if idleErr := s.requireNativeIdle(ctx, rt, second); idleErr != nil {
					return nil, idleErr
				}
			}

			inputs := make(map[string][]opencode.InboxInput)

			rows := make([][]byte, 0, len(first))
			for i := range first {
				item := &first[i]

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

		lastErr = err
		if lastErr == nil {
			lastErr = errors.New("native session graph, retained messages, or configuration changed")
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(snapshotRetryDelay):
		}
	}

	return nil, fmt.Errorf("native history did not yield a verified snapshot after %d attempts: %w", snapshotAttempts, lastErr)
}

// completedGraph fixes the root at its observed terminal event and each child
// at its latest completed execution. A child with no terminal has empty history.
func completedGraph(graph []nativeSnapshot, terminal opencode.Event) ([]nativeSnapshot, error) {
	for i := range graph {
		var err error
		if i == 0 {
			graph[i].Export, err = graph[i].ExecutionPrefix(terminal)
		} else {
			graph[i].Export, err = graph[i].CompletedPrefix()
		}

		if err != nil {
			return nil, err
		}
	}

	return graph, nil
}

// snapshotRetained allows later executions and newly exportable completed rows
// while requiring every captured message and deferred input to remain intact.
func snapshotRetained(first, second []nativeSnapshot) (bool, error) {
	current := make(map[string]*nativeSnapshot, len(second))
	for i := range second {
		current[second[i].Info.ID] = &second[i]
	}

	for i := range first {
		item := &first[i]

		later, ok := current[item.Info.ID]
		if !ok || !later.ContainsHistory(item.Export) || !item.SameConfiguration(later.Export) {
			return false, nil
		}

		if err := verifyPendingInputs(item.Inbox, later.Inbox, later.Messages); err != nil {
			// A new first read could forget an input that disappeared without delivery.
			return false, err
		}
	}

	return true, nil
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

		// Read the inbox first so delivery cannot disappear between the two APIs.
		inbox, err := rt.client.Inbox(ctx, id)
		if err != nil {
			return nil, err
		}

		item, err := rt.client.Export(ctx, id)
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

func normalizeInputs(graph []nativeSnapshot) error {
	for i := range graph {
		inputs, err := undeliveredInputs(graph[i].Inbox, graph[i].Messages)
		if err != nil {
			return err
		}

		graph[i].Inbox = inputs
	}

	return nil
}

// undeliveredInputs excludes inputs the export already contains as messages.
func undeliveredInputs(inputs []opencode.InboxInput, messages []opencode.NativeMessage) ([]opencode.InboxInput, error) {
	var pending []opencode.InboxInput

	for _, input := range inputs {
		index := slices.IndexFunc(messages, func(message opencode.NativeMessage) bool { return message.ID == input.ID })
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
