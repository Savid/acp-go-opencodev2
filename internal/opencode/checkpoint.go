package opencode

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
)

// ErrMarkerMissing reports an export that does not yet hold the execution's
// durable idle marker.
var ErrMarkerMissing = errors.New("native execution marker missing")

// ExecutionMarker names the durable idle marker a terminal execution event
// leaves in its session's history, and the outcome it records.
func ExecutionMarker(event Event) (id, outcome string, err error) {
	id = event.MessageID()
	if id == "" || event.SessionID == "" {
		return "", "", errors.New("invalid native execution boundary")
	}

	outcome, ok := strings.CutPrefix(event.Type, "session.execution.")
	if !ok || outcome != "succeeded" && outcome != "failed" && outcome != "interrupted" {
		return "", "", errors.New("invalid native execution outcome")
	}

	return id, outcome, nil
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

func equalJSON(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}

	var x, y any

	first, second := json.NewDecoder(bytes.NewReader(a)), json.NewDecoder(bytes.NewReader(b))
	first.UseNumber()
	second.UseNumber()

	return first.Decode(&x) == nil && second.Decode(&y) == nil && reflect.DeepEqual(x, y)
}
