package opencode

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
)

// ExecutionPrefix cuts the export at the terminal event's durable idle marker.
func (e Export) ExecutionPrefix(event Event) (Export, error) {
	suffix, ok := strings.CutPrefix(event.ID, "evt_")
	if !ok || suffix == "" || event.SessionID() != e.Info.ID {
		return Export{}, errors.New("invalid native execution boundary")
	}

	outcome, ok := strings.CutPrefix(event.Type, "session.execution.")
	if !ok || outcome != "succeeded" && outcome != "failed" && outcome != "interrupted" {
		return Export{}, errors.New("invalid native execution outcome")
	}

	for i := range e.Messages {
		message := &e.Messages[i]
		if message.ID == "msg_"+suffix && message.Type == "idle" && message.Outcome == outcome {
			return e.messagePrefix(i + 1)
		}
	}

	return Export{}, errors.New("native execution marker missing")
}

// CompletedPrefix retains only executions ending in a durable idle marker.
func (e Export) CompletedPrefix() (Export, error) {
	for i := range slices.Backward(e.Messages) {
		if e.Messages[i].Type == "idle" {
			return e.messagePrefix(i + 1)
		}
	}

	return e.messagePrefix(0)
}

// messagePrefix preserves every retained native value's original JSON bytes.
func (e Export) messagePrefix(count int) (Export, error) {
	if count == len(e.Messages) {
		return e, nil
	}

	decoder := json.NewDecoder(bytes.NewReader(e.Raw))
	if _, err := decoder.Token(); err != nil {
		return Export{}, err
	}

	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return Export{}, err
		}

		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return Export{}, err
		}

		if key != "messages" {
			continue
		}

		end := int(decoder.InputOffset())
		raw := append([]byte(nil), e.Raw[:end-len(value)]...)
		raw = append(raw, '[')

		for i := range count {
			if i > 0 {
				raw = append(raw, ',')
			}

			raw = append(raw, e.Messages[i].Raw...)
		}

		raw = append(raw, ']')
		raw = append(raw, e.Raw[end:]...)
		e.Raw = raw
		e.Messages = e.Messages[:count]

		return e, nil
	}

	return Export{}, errors.New("native export messages missing")
}

// ContainsHistory verifies saved identities, order, and content. Export omits
// in-flight assistant, shell, and compaction records; they can appear later at
// their original positions when they finish.
func (e Export) ContainsHistory(saved Export) bool {
	if e.Info.ID != saved.Info.ID || e.Info.ParentID != saved.Info.ParentID {
		return false
	}

	next := 0

	for i := range saved.Messages {
		message := &saved.Messages[i]
		for next < len(e.Messages) && e.Messages[next].ID != message.ID {
			switch e.Messages[next].Type {
			case "assistant", "shell", "compaction":
				next++
			default:
				return false
			}
		}

		if next == len(e.Messages) || !equalJSON(message.Raw, e.Messages[next].Raw) {
			return false
		}

		next++
	}

	return true
}

// SameConfiguration excludes native accounting that advances during execution.
func (e Export) SameConfiguration(later Export) bool {
	a, b := snapshotConfig(e.Raw), snapshotConfig(later.Raw)

	return a != nil && b != nil && reflect.DeepEqual(a, b)
}

func snapshotConfig(raw json.RawMessage) map[string]any {
	var value struct {
		Info map[string]any `json:"info"`
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	if decoder.Decode(&value) != nil {
		return nil
	}

	delete(value.Info, "cost")
	delete(value.Info, "tokens")
	delete(value.Info, "outcome")

	if times, ok := value.Info["time"].(map[string]any); ok {
		delete(times, "updated")
		delete(times, "idle")
	}

	return value.Info
}

func equalJSON(a, b []byte) bool {
	var x, y any

	first, second := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	first.UseNumber()
	second.UseNumber()

	return first.Decode(&x) == nil && second.Decode(&y) == nil && reflect.DeepEqual(x, y)
}
