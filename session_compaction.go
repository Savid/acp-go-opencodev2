package opencodeacp

import (
	"cmp"
	"context"

	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

func (s *session) projectCompaction(ctx context.Context, event opencode.Event, data eventData) error {
	value := wire.Compaction{}

	switch event.Type {
	case eventCompactionStarted:
		value.Status = wire.CompactionInProgress
	case eventCompactionEnded:
		value.Status = wire.CompactionCompleted
	case eventCompactionFailed:
		value.Status = wire.CompactionFailed

		if data.Error != nil {
			switch data.Error.Type {
			case "aborted", "compaction.interrupted":
				value.Status = wire.CompactionCancelled
			}
		}
	default:
		return nil
	}

	if data.SessionID != "" && data.SessionID != s.nativeID {
		return nil
	}

	if event.ID != "" {
		if s.compactionEvents[event.ID] {
			return nil
		}
	}

	key := data.InputID
	if value.Status == wire.CompactionInProgress {
		key = cmp.Or(key, event.MessageID())
	} else {
		key = cmp.Or(key, s.compactionKey, event.MessageID())
	}

	if key == "" {
		return nil
	}

	if event.ID != "" {
		if s.compactionEvents == nil {
			s.compactionEvents = make(map[string]bool)
		}

		s.compactionEvents[event.ID] = true
	}

	if value.Status == wire.CompactionInProgress {
		s.compactionKey = key
	} else if key == s.compactionKey {
		s.compactionKey = ""
	}

	switch data.Reason {
	case wire.CompactionTriggerAuto, wire.CompactionTriggerManual:
		value.Trigger = data.Reason
	}

	return s.compactions.Publish(ctx, s.agent.connection(), s.id, key, value)
}
