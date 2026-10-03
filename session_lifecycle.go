package opencodeacp

import (
	"context"
	"errors"

	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
)

func (s *session) lifecycleNegotiated() lifecycle.Negotiated { return s.agent.lifecycleNegotiated() }

// errSessionClosing refuses an opening publication because the session began
// closing. It never reaches the wire: a deferred publication ends silently and
// an inline one answers closingRefusal.
var errSessionClosing = errors.New("session closing")

// closingRefusal answers work that reached a closing session: the agent's own
// closure when that is the cause, otherwise the session is gone.
func (s *session) closingRefusal() error {
	if err := s.agent.ensureOpen(); err != nil {
		return err
	}

	return wire.UnknownSession()
}

func (s *session) openStream(ctx context.Context, rt *binding) error {
	s.openMu.Lock()
	defer s.openMu.Unlock()

	s.mu.Lock()

	if s.closing {
		s.mu.Unlock()

		return errSessionClosing
	}

	if rt == nil || s.runtime != rt || !rt.alive() {
		s.mu.Unlock()

		return wire.RuntimeUnavailable(vendor)
	}

	s.mu.Unlock()

	if err := s.emitCommands(ctx); err != nil {
		return err
	}

	if err := s.lc.Open(ctx, lifecycle.NewIncarnation(string(s.id)), s.lifecycleNegotiated(), s.deliverLifecycle); err != nil {
		return err
	}

	rt.bindOnce.Do(func() { close(rt.bound) })

	return nil
}

// fenceStream joins any opening publication before retiring its incarnation.
func (s *session) fenceStream() {
	s.openMu.Lock()
	defer s.openMu.Unlock()

	s.lc.Fence()
}

func (s *session) acceptTurn(ctx context.Context, t *turn) {
	s.mu.Lock()
	if !t.cancelled {
		t.acceptedBeforeCancel = true
	}
	s.mu.Unlock()

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if t.accepted {
		return
	}

	t.accepted = true
	if err := s.lc.Accept(ctx, &t.Cycle, t.submission); err != nil && t.failure == nil {
		t.failure = err
	}
}

// turnAccepted reports whether the turn's prompt has been accepted.
func (s *session) turnAccepted(t *turn) bool {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	return t.accepted
}

// recordFailure keeps the first failure one cycle observed. The pump and the
// prompt both reach it, so every write and read goes through lcMu.
func (s *session) recordFailure(c *cycle, err error) {
	if err == nil {
		return
	}

	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	if c.failure == nil {
		c.failure = err
	}
}

// cycleFailure reads the recorded failure under the same lock.
func (s *session) cycleFailure(c *cycle) error {
	s.lcMu.Lock()
	defer s.lcMu.Unlock()

	return c.failure
}

func (s *session) deliverLifecycle(ctx context.Context, envelope map[string]any) error {
	if conn := s.agent.connection(); conn != nil {
		return conn.SessionUpdate(ctx, wire.LifecycleCarrier(s.id, envelope))
	}

	return nil
}
