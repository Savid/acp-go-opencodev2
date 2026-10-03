package opencodeacp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

const (
	limitSessionPrompt = "session_prompt"

	stopReasonLength    = "length"
	stopReasonMaxTokens = "max_tokens"
	stopReasonError     = "error"
)

type nativePrompt struct {
	message string
	images  []map[string]any
}

// mapPrompt converts ACP prompt content to opencode's prompt shape. Embedded
// context is appended to the message text; images run the core input gates
// and travel as inline base64.
func (s *session) mapPrompt(ctx context.Context, blocks []acp.ContentBlock) (nativePrompt, error) {
	if len(blocks) == 0 {
		return nativePrompt{}, wire.Unsupported("prompt")
	}

	decoded, refusal, err := image.ValidatePrompt(ctx, blocks, image.Options{
		Limits:      s.agent.options.ImageLimits.core(),
		HandoffRoot: s.agent.options.InputHandoffRoot,
		Blobs:       func(string) image.BlobDisposition { return image.BlobGate },
	})
	if err != nil {
		return nativePrompt{}, err
	}

	if refusal != nil {
		return nativePrompt{}, refusal.InvalidParams()
	}

	prompt := nativePrompt{}
	textParts := make([]string, 0, len(blocks))
	imageSeen := false
	orderUnsupported := false
	appendText := func(text string) {
		if strings.TrimSpace(text) == "" {
			return
		}

		if imageSeen {
			orderUnsupported = true
		}

		textParts = append(textParts, text)
	}

	media := make(map[int]image.Decoded, len(decoded))
	for _, item := range decoded {
		media[item.Block] = item
	}

	for index, block := range blocks {
		if item, gated := media[index]; gated {
			if !image.IsImageMIME(item.MIME) {
				if block.Resource != nil && block.Resource.Resource.BlobResourceContents != nil {
					appendText(block.Resource.Resource.BlobResourceContents.Uri)
				}

				continue
			}

			if supported := s.modelImageCapability(); supported != nil && !*supported {
				return nativePrompt{}, image.UnsupportedByModel(item.Field, item.Index).InvalidParams()
			}

			part := map[string]any{fieldType: partFile, "mime": item.MIME, "uri": "data:" + item.MIME + ";base64," + base64.StdEncoding.EncodeToString(item.Data)}
			imageSeen = true

			prompt.images = append(prompt.images, part)

			continue
		}

		switch {
		case block.Text != nil:
			if !wire.AudienceIsUserOnly(block.Text.Annotations) {
				appendText(block.Text.Text)
			}
		case block.ResourceLink != nil:
			appendText(block.ResourceLink.Uri)
		case block.Resource != nil:
			if text := block.Resource.Resource.TextResourceContents; text != nil {
				appendText(wire.ContextResourceText(text.Uri, text.Text))
			}
		default:
			return nativePrompt{}, wire.Unsupported("prompt")
		}
	}

	if orderUnsupported {
		return nativePrompt{}, wire.Unsupported("prompt")
	}

	prompt.message = strings.Join(textParts, "\n")
	if prompt.message == "" && len(prompt.images) == 0 {
		return nativePrompt{}, wire.Unsupported("prompt")
	}

	return prompt, nil
}

// prompt sends one turn to opencode and streams updates until the run settles.
func (s *session) prompt(ctx context.Context, params acp.PromptRequest, raw json.RawMessage) (acp.PromptResponse, error) {
	meta := lifecycle.RetainRequestMetadata(params.Meta, raw)

	submission, paramErr := lifecycle.DecodePromptCorrelation(meta, s.lifecycleNegotiated())
	if paramErr != nil {
		return acp.PromptResponse{}, wire.ParamRefusal(paramErr)
	}

	if err := s.admissionError(); err != nil {
		return acp.PromptResponse{}, err
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	defer release()

	// The turn runs under its own cancellation, not the request's: the SDK
	// cancels a prompt's context when the next prompt for the session arrives,
	// and a refused peer prompt must not end this turn.
	turnCtx, cancelTurn := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelTurn()

	t := &turn{
		Origin:     lifecycle.CauseSubmission,
		state:      cycleState{},
		submission: submission,
		cancelTurn: cancelTurn,
		settled:    make(chan struct{}),
		finished:   make(chan struct{}),
		messageID:  opencode.NewMessageID(),
	}
	defer close(t.finished)

	// The turn is installed before the request-scoped work a cancel has to be
	// able to interrupt, and the busy check shares its critical section so a
	// cycle the pump opens can neither be missed nor wedge the session.
	s.mu.Lock()
	if s.cycle != nil || s.closing {
		s.mu.Unlock()

		return acp.PromptResponse{}, wire.Backpressure(limitSessionPrompt)
	}

	s.turn = t
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.turn == t {
			s.turn = nil
		}
		s.mu.Unlock()
	}()

	mapped, err := s.mapPrompt(turnCtx, params.Prompt)
	if err != nil {
		if turnCtx.Err() != nil {
			return wire.CancelledResponse(params), nil
		}

		return acp.PromptResponse{}, err
	}

	rt, err := s.ensureRuntime(turnCtx)
	if err != nil {
		if turnCtx.Err() != nil {
			return wire.CancelledResponse(params), nil
		}

		return acp.PromptResponse{}, err
	}

	// A cancel that lands before native dispatch answers cancelled with no
	// native work.
	if turnCtx.Err() != nil {
		return wire.CancelledResponse(params), nil
	}

	s.mu.Lock()
	if s.runtime != rt || !rt.alive() {
		s.mu.Unlock()

		return acp.PromptResponse{}, s.transportFailure(context.WithoutCancel(ctx), rt, nil)
	}

	t.runtime = rt
	s.mu.Unlock()

	requestCtx, requestCancel := context.WithCancel(context.WithoutCancel(ctx))
	requestDone := make(chan struct{})

	path, body := s.promptRequest(mapped, t.messageID)
	if path == nativeCommandPath {
		native, err := rt.client.Session(turnCtx, s.nativeID)
		if err != nil {
			requestCancel()

			if turnCtx.Err() != nil {
				return wire.CancelledResponse(params), nil
			}

			return acp.PromptResponse{}, s.dispatchFailure(ctx, rt, err)
		}

		s.mu.Lock()
		t.command = true
		t.initialIdle = native.Time.Idle
		s.mu.Unlock()
	}

	defer func() { requestCancel(); <-requestDone }()

	go func() {
		defer close(requestDone)

		err := rt.client.Do(requestCtx, s.cwd, http.MethodPost, opencode.SessionPath(s.nativeID)+path, body, nil)
		if s.repeatInterrupt(t) {
			s.abort(ctx, rt)
			s.callbacks.Done()
		}

		var idle int64

		if err == nil && path == nativeCommandPath {
			err = rt.client.Wait(requestCtx, s.nativeID)
			if err == nil {
				var native opencode.NativeSession

				native, err = rt.client.Session(requestCtx, s.nativeID)
				idle = native.Time.Idle
			}
		}

		select {
		case rt.results <- nativePromptResult{turn: t, err: err, idle: idle}:
		case <-requestCtx.Done():
		case <-rt.done:
		}
	}()

	select {
	case <-t.settled:
	case <-turnCtx.Done():
		// Whoever ended the turn already interrupted opencode; this bounds how
		// long settlement may take before the binding is dropped.
		select {
		case <-t.settled:
		case <-time.After(sessionAbortTimeout):
			s.stopRuntime(context.WithoutCancel(ctx), rt)
			t.settle(turnTransportEnded)
		}
	}

	return s.settleTurn(ctx, rt, t, params)
}

// finishCommand settles a command that returned without starting execution.
func (s *session) finishCommand(ctx context.Context, t *turn) {
	if t.state.stopReason == "" {
		t.state.stopReason = statusComplete
	}

	s.beginSettlement(&t.cycle)
	t.settle(turnSettled)

	select {
	case <-t.finished:
	case <-ctx.Done():
	}
}

// dispatchFailure classifies a prompt command opencode never accepted: a native
// rejection carries its text as a provider failure, a dead child is a
// process exit, and everything else is transport.
func (s *session) dispatchFailure(ctx context.Context, rt *binding, err error) error {
	if commandErr, ok := errors.AsType[*opencode.HTTPError](err); ok {
		return wire.TurnFailed(vendor, nativeTurnFailure(&commandErr.Native))
	}

	return s.transportFailure(ctx, rt, err)
}

// transportFailure recovers the real cause behind a lost native stream: the
// child's exit status and stderr tail where it died, otherwise the
// transport error.
func (s *session) transportFailure(ctx context.Context, rt *binding, err error) error {
	if nativeErr, ok := errors.AsType[*opencode.HTTPError](err); ok {
		return wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseTransport, Message: nativeErr.Error(), StatusCode: nativeErr.Status})
	}

	return wire.TurnFailed(vendor, wire.TransportFailure(ctx, rt.server.proc, "opencode process", err, rt.server.stream.Err))
}

// cycleVerdict is how one cycle ended, in the terms the lifecycle stream and
// the prompt response need.
type cycleVerdict struct {
	outcome    lifecycle.Outcome
	stopReason string
	failure    error
}

// judgeCycle records how a natively settled cycle finished. The cancel guard
// runs before every failure mapping.
func judgeCycle(c *cycle, failure error, cancelled bool) cycleVerdict {
	switch {
	case cancelled:
		return cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	case failure != nil:
		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: failure}
	case c.state.stopReason == stopReasonError:
		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: wire.TurnFailed(vendor, nativeTurnFailure(c.state.nativeError))}
	}

	stop := acp.StopReasonEndTurn
	outcome := lifecycle.OutcomeSuccess

	switch c.state.stopReason {
	case stopReasonLength, stopReasonMaxTokens:
		stop = acp.StopReasonMaxTokens
		outcome = lifecycle.OutcomeLimit
	case statusInterrupted:
		stop = acp.StopReasonCancelled
		outcome = lifecycle.OutcomeCancelled
	case statusComplete, "stop", "tool-calls":
	default:
		return cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: wire.TurnFailed(vendor, wire.TurnFailure{Cause: wire.CauseProvider, Message: "unknown native finish status: " + c.state.stopReason})}
	}

	return cycleVerdict{outcome: outcome, stopReason: string(stop)}
}

// settleTurn is the one settlement point every accepted prompt reaches:
// session info, the durable mirror commit, the terminal idle, and only then
// the response or error.
func (s *session) settleTurn(ctx context.Context, rt *binding, t *turn, params acp.PromptRequest) (acp.PromptResponse, error) {
	s.beginSettlement(&t.cycle)

	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancel()

	s.mu.Lock()
	cancelled := t.cancelled
	s.mu.Unlock()

	var verdict cycleVerdict

	commitFailed := false

	switch {
	case cancelled:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	case t.ended == turnTransportEnded:
		verdict = cycleVerdict{outcome: lifecycle.OutcomeFailed, failure: s.transportFailure(settleCtx, rt, nil)}
	default:
		verdict = judgeCycle(&t.cycle, s.cycleFailure(&t.cycle), false)
	}

	if t.ended == turnSettled {
		if !cancelled {
			s.emitSessionInfo(settleCtx, params.Prompt)
		}

		if err := s.commitMirror(settleCtx, rt, t.terminalEvent); err != nil {
			s.stopRuntime(settleCtx, rt)
			s.fenceStream()

			commitFailed = true

			mirrorErr := s.mirrorFailure(&t.state, err)

			if verdict.failure == nil {
				verdict.failure = mirrorErr
			}

			verdict.outcome = lifecycle.OutcomeFailed
		}
	}

	if s.claimCancellation(&t.cycle) && !commitFailed {
		verdict = cycleVerdict{outcome: lifecycle.OutcomeCancelled, stopReason: lifecycle.StopReasonCancelled}
	}

	if err := s.lc.Idle(settleCtx, t.Cycle, verdict.stopReason, verdict.outcome); err != nil && verdict.failure == nil {
		verdict.failure = err
	}

	// The incarnation ends with the generation that ran the turn, not with how
	// the turn ended: a native exit after a settled turn must not leave the
	// next process publishing on this stream.
	if t.ended == turnTransportEnded || !s.boundTo(rt) {
		s.fenceStream()
	}

	if verdict.failure != nil {
		return acp.PromptResponse{}, verdict.failure
	}

	return acp.PromptResponse{
		Usage:         t.state.usage,
		StopReason:    acp.StopReason(verdict.stopReason),
		UserMessageId: params.MessageId,
	}, nil
}

// mirrorFailure maps a failed mirror commit onto the turn-failure shape. A
// turn that delivered image bytes lost their durable replay representation.
func (s *session) mirrorFailure(state *cycleState, err error) error {
	s.agent.log.Error("session mirror commit failed", slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))

	failure := wire.TurnFailure{Cause: wire.CauseTransport, Message: "session mirror commit failed"}
	if state.imagesEmitted {
		failure.Message = "image output is no longer available from the artifact store"
		failure.Stage = image.OutputStage
		failure.Reason = image.ReasonStorageFailed
	}

	return wire.TurnFailed(vendor, failure)
}

// boundTo reports whether rt is still this session's live binding.
func (s *session) boundTo(rt *binding) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.runtime == rt && rt.alive()
}

func (s *session) promptRequest(mapped nativePrompt, id string) (string, any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	head, args, _ := strings.Cut(mapped.message, " ")

	files := make([]map[string]any, 0, len(mapped.images))
	for _, item := range mapped.images {
		files = append(files, map[string]any{"uri": item["uri"]})
	}

	for _, command := range s.commands {
		if wire.ValidCommandName(command.Name) && head == "/"+command.Name {
			return nativeCommandPath, map[string]any{"name": command.Name, "text": args, "files": files}
		}
	}

	return "/prompt", map[string]any{"id": id, "text": mapped.message, "files": files}
}

func nativeTurnFailure(native *opencode.NativeError) wire.TurnFailure {
	failure := wire.TurnFailure{Cause: wire.CauseProvider, Message: nativeErrorText(native)}
	if native != nil {
		failure.StatusCode = native.Status
		failure.ProviderCode = native.Type
	}

	return failure
}
