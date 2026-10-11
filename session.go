package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/sessionlog"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

const (
	sessionAbortTimeout  = 5 * time.Second
	sessionSettleTimeout = 60 * time.Second
)

// session owns one native conversation and one binding to the shared server.
type session struct {
	compactionKey         string
	compactionEvents      map[string]bool
	compactions           wire.Compactions
	agent                 *Agent
	id                    acp.SessionId
	nativeID              string
	cwd                   string
	additionalDirectories []string
	options               OpenCodeOptions
	rawEvents             *wire.RawEvents
	gate                  chan struct{}
	mu                    sync.Mutex
	binding               *binding
	model                 string
	effort                string
	models                []opencode.Model
	artifacts             map[string]imageArtifact
	pendingInputs         map[string][]opencode.InboxInput
	// nativeInfos is the native information of the graph last read, parent first.
	nativeInfos []json.RawMessage
	// mirror publishes the session's store generation. A stored session has
	// one from its establishment; an ephemeral session has none.
	mirror *sessionlog.Mirror
	// nativeMessages holds model identities for streamed response usage.
	nativeMessages map[string]opencode.NativeMessage
	mode           string
	agents         []opencode.NativeAgent
	commands       []opencode.NativeCommand
	title          string
	// cost is the native session's cumulative cost in USD.
	cost      float64
	updatedAt string
	// persisted marks a successfully committed mirror.
	persisted bool
	// ephemeral marks a session the host deletes without needing it back: it
	// is never written to the store and never listed.
	ephemeral bool
	closing   bool
	closeDone chan struct{}
	closeErr  error
	poison    string
	turn      *turn
	cycle     *cycle
	dialogs   map[string]*dialog
	callbacks sync.WaitGroup
	openMu    sync.Mutex
	lcMu      sync.Mutex
	lc        lifecycle.Publisher
}

// binding routes the shared server's events to one held conversation.
type binding struct {
	server   *runtime
	client   *opencode.Client
	cancel   context.CancelFunc
	ending   <-chan struct{}
	bound    chan struct{}
	bindOnce sync.Once
	done     chan struct{}
	events   chan opencode.Event
	results  chan nativePromptResult
	endMu    sync.Mutex
	endErr   error
	// backlog holds, in order, events that arrived while events was full;
	// forwarding marks the goroutine moving them into events.
	backlogMu    sync.Mutex
	backlog      []opencode.Event
	backlogBytes int
	forwarding   bool
	// overflowed marks a binding that outran its backlog; it takes no more
	// events.
	overflowed bool
	// settling counts the pump's waits on a settlement, during which its
	// queue's stall limit is paused.
	settling atomic.Int32
}

// holdSettlement marks the pump as waiting out a settlement, which may take
// as long as the commit it waits on; the returned function ends the wait.
func (b *binding) holdSettlement() func() {
	b.settling.Add(1)

	return func() { b.settling.Add(-1) }
}

// dropBacklog marks b overflowed and releases its backlog. The caller holds
// backlogMu.
func (b *binding) dropBacklog() {
	b.overflowed = true
	b.forwarding = false
	b.backlog, b.backlogBytes = nil, 0
}

// end records why the binding ended, keeping the first cause, and ends it.
func (b *binding) end(cause error) {
	b.endMu.Lock()
	if b.endErr == nil {
		b.endErr = cause
	}
	b.endMu.Unlock()
	b.cancel()
}

// endCause is why the binding ended, when an explicit cause ended it.
func (b *binding) endCause() error {
	b.endMu.Lock()
	defer b.endMu.Unlock()

	return b.endErr
}

// alive reports whether this binding still routes native events: its generation
// has not been ended and its server has not ended. Liveness reads the binding's
// own cancellation rather than its pump: done closes only after runtimeEnded has
// run the generation's lifecycle tail, and a binding handed out during that tail
// takes a prompt no pump is left to settle.
func (b *binding) alive() bool {
	select {
	case <-b.ending:
		return false
	default:
		return b.server.alive()
	}
}

type nativePromptResult struct {
	turn *turn
	err  error
	idle int64
}

type cycle struct {
	terminalEvent *opencode.Event
	lifecycle.Cycle
	cancelled bool
	settling  bool
	terminal  bool
	state     cycleState
	failure   error
	done      chan struct{}
}

type turnEnd int

const (
	turnRunning turnEnd = iota
	turnSettled
	turnTransportEnded
)

type turn struct {
	cycle
	submission lifecycle.Submission
	binding    *binding
	// cancelTurn ends the turn's own context. The session owns it, so a
	// refused peer prompt cancelling this prompt's request context never ends
	// a live turn.
	cancelTurn  context.CancelFunc
	accepted    bool
	ended       turnEnd
	settled     chan struct{}
	settleOnce  sync.Once
	finished    chan struct{}
	messageID   string
	command     bool
	initialIdle int64
	// acceptedBeforeCancel records, under the session lock, that native
	// acceptance was observed before the turn was cancelled, so the cancel's
	// interrupt reached the accepted work.
	acceptedBeforeCancel bool
}

func (t *turn) settle(end turnEnd) {
	t.settleOnce.Do(func() { t.ended = end; close(t.settled) })
}

type dialog struct{ cancel context.CancelCauseFunc }

var errDialogCancelled = errors.New("dialog cancelled by the session")

func (s *session) ensureRuntime(ctx context.Context) (*binding, error) {
	s.mu.Lock()
	b := s.binding
	s.mu.Unlock()

	if b != nil && b.alive() {
		return b, nil
	}

	b, err := s.recoverRuntime(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.openStream(ctx, b); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), b)

		if errors.Is(err, errSessionClosing) {
			return nil, s.closingRefusal()
		}

		return nil, err
	}

	return b, nil
}

func (s *session) recoverRuntime(ctx context.Context) (*binding, error) {
	s.mu.Lock()
	b := s.binding
	selection := OpenCodeOptions{Model: s.model, Mode: s.mode, Effort: s.effort}
	s.mu.Unlock()

	if b != nil {
		s.stopRuntime(context.WithoutCancel(ctx), b)
	}

	var stored storedSession

	if !s.ephemeral {
		var err error

		stored, err = s.agent.loadStored(ctx, s.id)
		if err != nil {
			return nil, err
		}

		if !stored.found {
			return nil, wire.RestoreFailed(vendor)
		}

		selection = OpenCodeOptions{Model: stored.record.Model, Mode: stored.record.Mode, Effort: stored.record.Effort}
	}

	b, err := s.launch(ctx)
	if err != nil {
		return nil, err
	}

	if !s.ephemeral {
		if _, err := s.hydrate(ctx, b, stored); err != nil {
			s.stopRuntime(context.WithoutCancel(ctx), b)

			return nil, err
		}
	}

	if err := s.configureRuntime(ctx, b, selection, s.nativeID); err != nil {
		s.stopRuntime(context.WithoutCancel(ctx), b)

		return nil, err
	}

	return b, nil
}

// pump serializes events and completed HTTP prompts for this binding.
func (s *session) pump(ctx context.Context, b *binding) {
	defer close(b.done)
	defer s.runtimeEnded(ctx, b)

	select {
	case <-b.bound:
	case <-ctx.Done():
		return
	}

	for {
		select {
		case event := <-b.events:
			s.handleEvent(ctx, b, event)
		case result := <-b.results:
			s.mu.Lock()
			current := s.turn == result.turn
			s.mu.Unlock()

			if !current {
				continue
			}

			t := result.turn
			if result.err == nil {
				s.acceptTurn(ctx, t)

				if t.command && t.initialIdle >= result.idle {
					s.finishCommand(ctx, t)
				}

				continue
			}

			s.recordFailure(&t.cycle, s.dispatchFailure(ctx, b, result.err))

			end := turnSettled
			if _, ok := errors.AsType[*opencode.HTTPError](result.err); !ok {
				end = turnTransportEnded
			}

			s.beginSettlement(&t.cycle)
			t.settle(end)

			release := b.holdSettlement()

			select {
			case <-t.finished:
				release()
			case <-ctx.Done():
				release()

				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// runtimeEnded ends this generation: it runs the incarnation's lifecycle tail
// first and drops the binding last, so an operation that relaunches waits on
// this pump and opens an incarnation no earlier fence can reach.
func (s *session) runtimeEnded(ctx context.Context, b *binding) {
	b.server.mu.Lock()
	if b.server.bindings[s.nativeID] == b {
		delete(b.server.bindings, s.nativeID)
	}
	b.server.mu.Unlock()
	s.mu.Lock()
	if s.binding != b {
		s.mu.Unlock()

		return
	}

	t, c, closing := s.turn, s.cycle, s.closing
	if t != nil && t.binding != b {
		t = nil
	}

	s.cycle = nil
	s.mu.Unlock()
	s.cancelDialogs()
	s.callbacks.Wait()

	if t != nil {
		t.cancelTurn()
		t.settle(turnTransportEnded)
		s.clearRuntime(b)

		return
	}

	if c != nil {
		// close commits owed state before it terminalizes an open agent-origin
		// cycle, so a failed commit leaves no terminal idle. The pump only
		// signals here that it has stopped touching the cycle.
		if !closing {
			_ = s.lc.Idle(context.WithoutCancel(ctx), c.Cycle, "", lifecycle.OutcomeFailed)
		}

		close(c.done)
	}

	if !closing {
		s.fenceStream()
	}

	s.clearRuntime(b)
}

// clearRuntime drops the binding once its generation's lifecycle tail has run.
func (s *session) clearRuntime(b *binding) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.binding == b {
		s.binding = nil
	}
}

func (s *session) stopRuntime(_ context.Context, b *binding) { b.cancel(); <-b.done }

// endBinding ends b for a native protocol fault and logs why; a turn it
// interrupts reports the same cause.
func (s *session) endBinding(ctx context.Context, b *binding, cause error) {
	s.agent.log.ErrorContext(ctx, "opencode session binding ended",
		slog.String("session_id", string(s.id)), slog.String("reason", cause.Error()))
	b.end(cause)
}

// rememberMessage stores the latest native info for one message.
func (s *session) rememberMessage(info opencode.NativeMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.nativeMessages == nil {
		s.nativeMessages = map[string]opencode.NativeMessage{}
	}

	s.nativeMessages[info.ID] = info
}

// nativeMessage reads the remembered info for one message.
func (s *session) nativeMessage(id string) opencode.NativeMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.nativeMessages[id]
}

// abort interrupts the native run under a bounded context detached from the
// caller's cancellation.
func (s *session) abort(ctx context.Context, b *binding) {
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionAbortTimeout)
	defer cancel()

	if err := b.client.Interrupt(abortCtx, s.nativeID); err != nil {
		s.agent.log.DebugContext(abortCtx, "opencode abort failed", slog.String("session_id", string(s.id)))
	}
}

// cancel marks the foreground cancelled, ends its dialogs, and interrupts
// native work. The interrupt is joined by the session's shutdown ladder.
func (s *session) cancel(ctx context.Context) {
	s.mu.Lock()
	t, c := s.turn, s.cycle
	b := s.binding

	if t != nil {
		c = &t.cycle
	}

	if c == nil || c.cancelled || c.terminal {
		s.mu.Unlock()

		return
	}

	c.cancelled = true

	interrupt := !c.settling && b != nil && !s.closing && b.alive()
	if interrupt {
		s.callbacks.Add(1)
	}
	s.mu.Unlock()

	if t != nil {
		t.cancelTurn()
	}

	s.cancelDialogs()

	if interrupt {
		go func() {
			defer s.callbacks.Done()

			s.abort(ctx, b)
		}()
	}
}

// repeatInterrupt reports whether a cancelled turn's interrupt must be sent
// again once its prompt request returns, and admits the repeat as a settlement
// callback. An interrupt sent before admission cannot cancel work accepted
// later; one sent after observed acceptance already reached it, and repeating
// it could end native successor work.
func (s *session) repeatInterrupt(t *turn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	repeat := t.cancelled && !t.settling && !t.acceptedBeforeCancel
	if repeat {
		s.callbacks.Add(1)
	}

	return repeat
}

// beginSettlement closes callback admission before joining native interrupts
// and dialogs, so none can reach a later foreground on this runtime.
func (s *session) beginSettlement(c *cycle) {
	s.mu.Lock()
	c.settling = true
	s.mu.Unlock()
	s.cancelDialogs()
	s.callbacks.Wait()
}

// claimCancellation fixes the cancellation verdict before terminal delivery.
func (s *session) claimCancellation(c *cycle) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	c.terminal = true

	return c.cancelled
}

// cycleCancelled reads cancellation under the foreground admission lock.
func (s *session) cycleCancelled(c *cycle) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return c.cancelled
}

func (s *session) registerDialog(id string, cancel context.CancelCauseFunc) (func(), bool) {
	s.mu.Lock()
	if s.dialogs[id] != nil || s.closing || s.binding == nil || ((s.turn != nil && (s.turn.cancelled || s.turn.settling)) || (s.cycle != nil && (s.cycle.cancelled || s.cycle.settling))) {
		s.mu.Unlock()
		cancel(errDialogCancelled)

		return func() {}, false
	}

	if s.dialogs == nil {
		s.dialogs = make(map[string]*dialog)
	}

	s.callbacks.Add(1)

	entry := &dialog{cancel: cancel}
	s.dialogs[id] = entry
	s.mu.Unlock()

	return sync.OnceFunc(func() {
		defer s.callbacks.Done()

		s.mu.Lock()
		if s.dialogs[id] == entry {
			delete(s.dialogs, id)
		}
		s.mu.Unlock()
	}), true
}

func (s *session) cancelDialogs() {
	s.mu.Lock()

	dialogs := make([]*dialog, 0, len(s.dialogs))
	for _, d := range s.dialogs {
		dialogs = append(dialogs, d)
	}
	s.mu.Unlock()

	for _, d := range dialogs {
		d.cancel(errDialogCancelled)
	}
}

// admissionError reports why a session admits no further work: it is closing
// or poisoned.
func (s *session) admissionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.poison != "":
		return wire.SessionPoisoned(vendor, s.poison)
	case s.closing:
		return wire.UnknownSession()
	default:
		return nil
	}
}

// poisonSession fences every operation but close and delete.
func (s *session) poisonSession(ctx context.Context, cause string) {
	s.mu.Lock()

	first := s.poison == ""
	if first {
		s.poison = cause
	}
	s.mu.Unlock()

	if !first {
		return
	}

	_ = s.emit(ctx, acp.SessionUpdate{AvailableCommandsUpdate: &acp.SessionAvailableCommandsUpdate{AvailableCommands: []acp.AvailableCommand{}}})
	s.agent.log.ErrorContext(ctx, "opencode session poisoned",
		slog.String("session_id", string(s.id)), slog.String("cause", cause))
}

// acquireGate admits one foreground operation. limit names the backpressure
// token a refusal carries.
func (s *session) acquireGate(limit string) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closing {
		return nil, wire.UnknownSession()
	}

	return wire.AcquireSessionGate(s.gate, limit)
}

// close interrupts and joins session work, captures native state, and releases its binding.
func (s *session) close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		done := s.closeDone
		s.mu.Unlock()
		<-done

		return s.closeErr
	}

	s.closing = true
	joinEstablishment := !s.persisted
	s.closeDone = make(chan struct{})

	t, b, closingCycle := s.turn, s.binding, s.cycle
	if t != nil {
		t.cancelled = true
	}
	s.mu.Unlock()

	if t != nil {
		t.cancelTurn()
	}

	s.cancelDialogs()
	s.callbacks.Wait()

	if t != nil {
		if b != nil {
			s.abort(ctx, b)
		}

		select {
		case <-t.finished:
		case <-time.After(sessionSettleTimeout):
		}
	}

	if closingCycle != nil && b != nil {
		s.abort(ctx, b)

		select {
		case <-closingCycle.done:
		case <-time.After(sessionAbortTimeout):
			s.stopRuntime(ctx, b)
		}
	}

	// An initial mirror may not have started yet; its establishment owns the gate.
	if joinEstablishment {
		s.gate <- struct{}{}
		defer func() { <-s.gate }()
	}

	s.mu.Lock()
	persisted := s.persisted
	s.mu.Unlock()

	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancel()

	var errs []error

	if b != nil {
		if persisted {
			if err := s.commitMirror(commitCtx, b, nil); err != nil {
				errs = append(errs, err)
			}
		}

		s.stopRuntime(commitCtx, b)
	}

	// An open agent-origin cycle terminalizes as cancelled only after its owed
	// state has committed; a failed commit fences with no terminal idle.
	if closingCycle != nil && len(errs) == 0 {
		_ = s.lc.Idle(context.WithoutCancel(ctx), closingCycle.Cycle, lifecycle.StopReasonCancelled, lifecycle.OutcomeCancelled)
	}

	s.fenceStream()
	s.mu.Lock()
	s.closeErr = errors.Join(errs...)
	close(s.closeDone)
	s.mu.Unlock()

	return s.closeErr
}
