package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/savid/acp-go-opencodev2/internal/opencode"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/stretchr/testify/require"
)

func compactionReports(t *testing.T, notifications []acp.SessionNotification) []wire.Compaction {
	t.Helper()
	var reports []wire.Compaction
	for _, notification := range notifications {
		value, exists := notification.Meta[wire.CompactionKey]
		if !exists {
			continue
		}
		carrier, err := json.Marshal(notification.Update)
		require.NoError(t, err)
		require.JSONEq(t, `{"sessionUpdate":"session_info_update"}`, string(carrier))
		require.Len(t, notification.Meta, 1)
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		var report wire.Compaction
		require.NoError(t, json.Unmarshal(encoded, &report))
		require.NotEmpty(t, report.CompactionID)
		reports = append(reports, report)
	}

	return reports
}

func TestCompactionTransport(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.initialize()
	created := h.newSession()
	for range 2 {
		_, err := h.prompt(created.SessionId, "COMPACT", nil)
		require.NoError(t, err)
	}
	reports := compactionReports(t, h.rec.snapshot())
	require.Len(t, reports, 4)
	require.NotEqual(t, reports[0].CompactionID, reports[2].CompactionID)
	for i := 0; i < len(reports); i += 2 {
		require.Equal(t, wire.CompactionInProgress, reports[i].Status)
		require.Equal(t, wire.CompactionCompleted, reports[i+1].Status)
		require.Equal(t, reports[i].CompactionID, reports[i+1].CompactionID)
		require.Equal(t, "auto", reports[i+1].Trigger)
		require.Nil(t, reports[i+1].ContextAfter)
	}
}

func TestCompactionTerminalEvents(t *testing.T) {
	t.Parallel()
	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	s.nativeID = "root"
	for _, test := range []struct{ id, kind, failure, status string }{
		{"success", "session.compaction.ended", "", wire.CompactionCompleted},
		{"failed", "session.compaction.failed", "compaction.failed", wire.CompactionFailed},
		{"cancelled", "session.compaction.failed", "compaction.interrupted", wire.CompactionCancelled},
	} {
		event := opencode.Event{ID: "evt_" + test.id, Type: test.kind}
		data := eventData{SessionID: "root", Reason: "manual"}
		if test.failure != "" {
			data.Error = &opencode.NativeError{Type: test.failure}
		}
		for range 2 {
			require.NoError(t, s.projectCompaction(t.Context(), event, data))
		}
		reports := compactionReports(t, rec.snapshot())
		require.Equal(t, test.status, reports[len(reports)-1].Status)
		require.Equal(t, "manual", reports[len(reports)-1].Trigger)
	}
	require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "child", Type: "session.compaction.ended"}, eventData{SessionID: "child"}))
	require.Len(t, compactionReports(t, rec.snapshot()), 3)
}

func TestCompactionAttemptOrdering(t *testing.T) {
	t.Parallel()
	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	s.nativeID = "root"
	send := func(id, kind, input, reason, failure string) {
		t.Helper()
		data := eventData{SessionID: "root", InputID: input, Reason: reason}
		if failure != "" {
			data.Error = &opencode.NativeError{Type: failure}
		}
		require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "evt_" + id, Type: kind}, data))
	}
	send("start-auto", eventCompactionStarted, "", "auto", "")
	send("end-auto", eventCompactionEnded, "", "auto", "")
	send("start-manual", eventCompactionStarted, "input-manual", "manual", "")
	send("start-auto", eventCompactionStarted, "", "auto", "")
	send("end-auto", eventCompactionEnded, "", "auto", "")
	send("cancel-manual", eventCompactionFailed, "input-manual", "manual", "aborted")
	send("start-retry", eventCompactionStarted, "input-retry", "manual", "")
	send("cancel-manual-again", eventCompactionFailed, "input-manual", "manual", "aborted")
	send("end-retry", eventCompactionEnded, "", "manual", "")
	send("end-retry", eventCompactionEnded, "", "manual", "")
	reports := compactionReports(t, rec.snapshot())
	require.Len(t, reports, 6)
	for i, status := range []string{wire.CompactionCompleted, wire.CompactionCancelled, wire.CompactionCompleted} {
		start, end := reports[2*i], reports[2*i+1]
		require.Equal(t, wire.CompactionInProgress, start.Status)
		require.Equal(t, status, end.Status)
		require.Equal(t, start.CompactionID, end.CompactionID)
		if i > 0 {
			require.NotEqual(t, reports[2*i-1].CompactionID, start.CompactionID)
		}
	}
	require.Equal(t, "manual", reports[3].Trigger)
}

func TestCompactionRecoveryCancellationIdentity(t *testing.T) {
	t.Parallel()
	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	s.nativeID = "root"
	require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "evt_auto", Type: eventCompactionStarted}, eventData{SessionID: "root", Reason: "auto"}))
	require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "evt_recovered", Type: eventCompactionFailed}, eventData{SessionID: "root", InputID: "msg_auto", Reason: "auto", Error: &opencode.NativeError{Type: "compaction.interrupted"}}))
	require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "evt_next", Type: eventCompactionFailed}, eventData{SessionID: "root", Reason: "auto", Error: &opencode.NativeError{Type: "compaction.failed"}}))
	reports := compactionReports(t, rec.snapshot())
	require.Len(t, reports, 3)
	require.Equal(t, reports[0].CompactionID, reports[1].CompactionID)
	require.Equal(t, wire.CompactionCancelled, reports[1].Status)
	require.NotEqual(t, reports[1].CompactionID, reports[2].CompactionID)
	require.Equal(t, wire.CompactionFailed, reports[2].Status)
}

func TestCompactionMissingIdentityPreservesActiveAttempt(t *testing.T) {
	t.Parallel()
	a := NewAgent()
	rec := newRecorder()
	a.attach(rec, nil)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	s.nativeID = "root"
	for _, kind := range []string{eventCompactionStarted, eventCompactionEnded, eventCompactionFailed} {
		require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "invalid", Type: kind}, eventData{SessionID: "root"}))
	}
	require.Empty(t, rec.snapshot())
	require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "evt_start", Type: eventCompactionStarted}, eventData{SessionID: "root"}))
	require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "invalid", Type: eventCompactionStarted}, eventData{SessionID: "root"}))
	require.NoError(t, s.projectCompaction(t.Context(), opencode.Event{ID: "evt_end", Type: eventCompactionEnded}, eventData{SessionID: "root"}))
	reports := compactionReports(t, rec.snapshot())
	require.Len(t, reports, 2)
	require.Equal(t, wire.CompactionInProgress, reports[0].Status)
	require.Equal(t, wire.CompactionCompleted, reports[1].Status)
	require.Equal(t, reports[0].CompactionID, reports[1].CompactionID)
}

type compactionFailureClient struct {
	*recorder
	failed bool
}

func (c *compactionFailureClient) SessionUpdate(ctx context.Context, notification acp.SessionNotification) error {
	if notification.Meta[wire.CompactionKey] != nil && !c.failed {
		c.failed = true

		return errors.New("compaction delivery unavailable")
	}

	return c.recorder.SessionUpdate(ctx, notification)
}

func TestCompactionSendFailureKeepsRuntime(t *testing.T) {
	t.Parallel()
	a := NewAgent()
	rec := &compactionFailureClient{recorder: newRecorder()}
	a.attach(rec, nil)
	s := a.newSession(sessionStart{cwd: t.TempDir()})
	s.nativeID = "root"
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rt := &binding{cancel: cancel}
	s.runtime = rt
	for _, id := range []string{"evt_failed", "evt_next"} {
		s.handleEvent(ctx, rt, opencode.Event{ID: id, Type: eventCompactionEnded, Data: json.RawMessage(`{"sessionID":"root"}`)})
	}
	require.NoError(t, ctx.Err())
	require.True(t, rec.failed)
	require.Len(t, compactionReports(t, rec.snapshot()), 1)
}
