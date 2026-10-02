package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	acp "github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

const (
	eventReasoningDelta       = "session.reasoning.delta"
	eventReasoningEnded       = "session.reasoning.ended"
	eventToolFailed           = "session.tool.failed"
	eventExecutionFailed      = "session.execution.failed"
	eventExecutionStarted     = "session.execution.started"
	eventExecutionInterrupted = "session.execution.interrupted"
	statusCompleted           = "completed"
	statusComplete            = "complete"
	statusInterrupted         = "interrupted"
	fieldCwd                  = "cwd"
	fieldValue                = "value"
	fieldType                 = "type"
	fieldText                 = "text"
	fieldID                   = "id"
	roleUser                  = "user"
	roleAssistant             = "assistant"
	nativeToolShell           = "shell"
	nativeCommandPath         = "/command"
	partFile                  = "file"
	partTool                  = "tool"
	partReasoning             = "reasoning"
	eventPermissionAsked      = "permission.asked"
	eventFormCreated          = "form.created"
	approvalReject            = "reject"
	approvalOnce              = "once"
	approvalAlways            = "always"
	statusIdle                = "idle"
)

type cycleState struct {
	text          map[string]string
	tools         map[string]bool
	terminalTools map[string]bool
	toolNames     map[string]string
	toolInputs    map[string]any
	files         map[string]bool
	reported      map[string]bool
	usage         *acp.Usage
	stopReason    string
	nativeError   *opencode.NativeError
	imagesEmitted bool
}

func (state *cycleState) init() {
	if state.text != nil {
		return
	}

	state.text = map[string]string{}
	state.tools = map[string]bool{}
	state.terminalTools = map[string]bool{}
	state.toolNames = map[string]string{}
	state.toolInputs = map[string]any{}
	state.files = map[string]bool{}
	state.reported = map[string]bool{}
}

//nolint:tagliatelle // Native event identities use uppercase ID suffixes.
type eventData struct {
	SessionID          string                `json:"sessionID"`
	AssistantMessageID string                `json:"assistantMessageID"`
	InboxID            string                `json:"inboxID"`
	ID                 string                `json:"id"`
	Name               string                `json:"name"`
	Ordinal            int                   `json:"ordinal"`
	Text               string                `json:"text"`
	Delta              string                `json:"delta"`
	Model              opencode.ModelRef     `json:"model"`
	Input              any                   `json:"input"`
	Content            []opencode.Content    `json:"content"`
	Error              *opencode.NativeError `json:"error"`
	Tokens             opencode.NativeTokens `json:"tokens"`
	Cost               float64               `json:"cost"`
	Finish             string                `json:"finish"`
}

func isTurnAcceptance(t *turn, eventType, inboxID string) bool {
	switch eventType {
	case "session.inbox.enqueued":
		return inboxID == t.messageID
	case eventExecutionStarted:
		return true
	case eventPermissionAsked, eventFormCreated:
		return t.command
	default:
		return false
	}
}

func (s *session) handleEvent(ctx context.Context, rt *binding, event opencode.Event) {
	s.mu.Lock()
	t, c, closing, current := s.turn, s.cycle, s.closing, s.runtime == rt
	s.mu.Unlock()

	if !current || closing && t == nil && c == nil {
		return
	}

	s.emitRawEvent(ctx, event)

	switch event.Type {
	case "session.deleted", "todo.updated", "session.inbox.enqueued", eventExecutionStarted, "session.execution.succeeded", eventExecutionFailed, eventExecutionInterrupted, "session.step.started", "session.step.ended", "session.step.failed", "session.text.delta", eventReasoningDelta, "session.text.ended", eventReasoningEnded, "session.tool.input.started", "session.tool.called", "session.tool.success", eventToolFailed, "session.compaction.ended", "session.compaction.failed", eventPermissionAsked, eventFormCreated:
	default:
		return
	}

	var data eventData
	if json.Unmarshal(event.Data, &data) != nil {
		rt.cancel()

		return
	}

	if event.Type == "session.deleted" {
		s.poisonSession(ctx, "native_session_identity_drift")
		rt.cancel()

		return
	}

	if event.Type == "todo.updated" {
		_ = s.emitPlan(ctx, event.Data)

		return
	}

	control := event.Type == eventPermissionAsked || event.Type == eventFormCreated

	if t != nil {
		if isTurnAcceptance(t, event.Type, data.InboxID) {
			s.acceptTurn(ctx, t)
		}

		if !s.turnAccepted(t) {
			return
		}

		c = &t.cycle
	} else if c == nil && event.Type == eventExecutionStarted {
		c = s.openAgentCycle(ctx, rt)
	}

	if c == nil {
		return
	}

	c.state.init()

	if control {
		s.handleControl(ctx, rt, c, event)

		return
	}

	terminal := event.Type == "session.execution.succeeded" || event.Type == eventExecutionFailed || event.Type == eventExecutionInterrupted
	if !s.cycleCancelled(c) {
		s.recordFailure(c, s.projectEvent(ctx, c, event, data))
	}

	if !terminal {
		return
	}

	if event.Type == eventExecutionFailed {
		c.state.stopReason = stopReasonError
		c.state.nativeError = data.Error
	}

	if event.Type == eventExecutionInterrupted {
		c.state.stopReason = statusInterrupted
	}

	if c.state.stopReason == "" {
		c.state.stopReason = statusComplete
	}

	if t != nil {
		if t.command {
			t.seenIdle = max(t.seenIdle+1, event.Created)
			s.finishCommand(ctx, t)

			return
		}
		// This event is ordered after all model and tool events in this execution.
		s.beginSettlement(c)
		t.settle(turnSettled)

		select {
		case <-t.finished:
		case <-ctx.Done():
		}
	} else {
		s.settleAgentCycle(ctx, rt, c)
	}
}
func (s *session) projectEvent(ctx context.Context, c *cycle, event opencode.Event, data eventData) error {
	state := &c.state

	kind := fieldText
	if strings.HasPrefix(event.Type, "session.reasoning.") {
		kind = partReasoning
	}

	key := data.AssistantMessageID + "/" + kind + "/" + strconv.Itoa(data.Ordinal)

	switch event.Type {
	case "session.step.started":
		s.rememberMessage(opencode.NativeMessage{ID: data.AssistantMessageID, Type: roleAssistant, Model: data.Model})
	case "session.text.delta", eventReasoningDelta:
		state.text[key] += data.Delta
		if event.Type == eventReasoningDelta {
			return s.emit(ctx, acp.UpdateAgentThoughtText(data.Delta))
		}

		return s.emit(ctx, acp.UpdateAgentMessageText(data.Delta))
	case "session.text.ended", eventReasoningEnded:
		return s.emitText(ctx, state, key, data.Text, event.Type == eventReasoningEnded)
	case "session.tool.input.started":
		state.toolNames[data.ID] = data.Name

		return s.emitTool(ctx, state, data.ID, data.Name, opencode.ToolState{Status: "pending"})
	case "session.tool.called":
		state.toolInputs[data.ID] = data.Input

		return s.emitTool(ctx, state, data.ID, state.toolNames[data.ID], opencode.ToolState{Status: "running", Input: data.Input})
	case "session.tool.success", eventToolFailed:
		status := statusCompleted
		if event.Type == eventToolFailed {
			status = stopReasonError
		}

		return s.emitTool(ctx, state, data.ID, state.toolNames[data.ID], opencode.ToolState{Status: status, Input: state.toolInputs[data.ID], Content: data.Content, Error: data.Error})
	case "session.step.ended", "session.step.failed":
		message := s.nativeMessage(data.AssistantMessageID)
		message.Tokens = data.Tokens
		message.Cost = data.Cost
		message.Finish = data.Finish

		message.Error = data.Error
		if data.Finish != "" {
			state.stopReason = data.Finish
		}

		return s.emitResponseUsage(ctx, c, message)
	case "session.compaction.ended", "session.compaction.failed":
		// The compaction response consumes tokens but does not describe the new context.
		if callUsage(data.Tokens).Known() {
			state.usage = addUsage(state.usage, data.Tokens)
		}
	}

	return nil
}
func (s *session) emitText(ctx context.Context, state *cycleState, key, text string, thought bool) error {
	state.init()

	suffix, ok := strings.CutPrefix(text, state.text[key])
	if !ok {
		return errors.New("native text changed after publication")
	}

	state.text[key] = text

	if suffix == "" {
		return nil
	}

	if thought {
		return s.emit(ctx, acp.UpdateAgentThoughtText(suffix))
	}

	return s.emit(ctx, acp.UpdateAgentMessageText(suffix))
}
func contextTokens(tokens opencode.NativeTokens) int {
	return int(tokens.Input + tokens.Output + tokens.Reasoning + tokens.Cache.Read + tokens.Cache.Write)
}
func callUsage(tokens opencode.NativeTokens) wire.CallUsage {
	return wire.CallUsage{InputTokens: new(int(tokens.Input)), CachedReadTokens: new(int(tokens.Cache.Read)), CachedWriteTokens: new(int(tokens.Cache.Write)), OutputTokens: new(int(tokens.Output + tokens.Reasoning))}
}
func (s *session) emitResponseUsage(ctx context.Context, c *cycle, message opencode.NativeMessage) error {
	if s.cycleCancelled(c) {
		return nil
	}

	c.state.init()

	if c.state.reported[message.ID] {
		return nil
	}

	c.state.reported[message.ID] = true

	call := callUsage(message.Tokens)
	if !call.Known() {
		return nil
	}

	c.state.usage = addUsage(c.state.usage, message.Tokens)
	update := acp.SessionUsageUpdate{Used: contextTokens(message.Tokens), Size: s.knownContextWindow(message.Model.ProviderID, message.Model.ID)}
	s.mu.Lock()
	rt := s.runtime
	s.mu.Unlock()

	if rt != nil {
		readCtx, cancel := context.WithTimeout(ctx, serverHealthTimeout)
		defer cancel()

		native, err := rt.client.Session(readCtx, s.nativeID)
		if err != nil {
			return err
		}

		update.Cost = &acp.Cost{Amount: native.Cost, Currency: "USD"}
	}

	update.Meta = call.Apply(nil)

	if s.cycleCancelled(c) {
		return nil
	}

	return s.emit(ctx, acp.SessionUpdate{UsageUpdate: &update})
}
func addUsage(total *acp.Usage, tokens opencode.NativeTokens) *acp.Usage {
	if total == nil {
		total = &acp.Usage{CachedReadTokens: new(0), CachedWriteTokens: new(0), ThoughtTokens: new(0)}
	}

	total.InputTokens += int(tokens.Input)
	total.OutputTokens += int(tokens.Output)
	*total.CachedReadTokens += int(tokens.Cache.Read)
	*total.CachedWriteTokens += int(tokens.Cache.Write)
	*total.ThoughtTokens += int(tokens.Reasoning)
	total.TotalTokens = total.InputTokens + total.OutputTokens + *total.CachedReadTokens + *total.CachedWriteTokens + *total.ThoughtTokens

	return total
}
func (s *session) knownContextWindow(providerID, modelID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.models {
		m := &s.models[i]
		if m.ProviderID == providerID && m.ID == modelID {
			return int(m.Limit["context"])
		}
	}

	return 0
}
func (s *session) projectMessage(ctx context.Context, c *cycle, message opencode.NativeMessage) error {
	c.state.init()

	if message.Type != roleAssistant {
		return nil
	}

	for index, content := range message.Content {
		key := attachmentID(message.ID, index)

		switch content.Type {
		case fieldText, partReasoning:
			if err := s.emitText(ctx, &c.state, key, content.Text, content.Type == partReasoning); err != nil {
				return err
			}
		case partTool:
			var state opencode.ToolState
			if err := json.Unmarshal(content.State, &state); err != nil {
				return err
			}

			if err := s.emitTool(ctx, &c.state, content.ID, content.Name, state); err != nil {
				return err
			}
		case partFile:
			for _, block := range s.outputFile(opencode.NativeAttachment{ID: key, MIME: content.MIME, URL: content.URI, Filename: content.Name}, nil) {
				if err := s.emit(ctx, acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{Content: block}}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
func (s *session) emitTool(ctx context.Context, state *cycleState, id, name string, native opencode.ToolState) error {
	state.init()

	if id == "" {
		return errors.New("native tool identity missing")
	}

	if state.terminalTools[id] {
		return nil
	}

	toolID := acp.ToolCallId(id)
	if !state.tools[id] {
		if err := s.emit(ctx, acp.StartToolCall(toolID, name, acp.WithStartKind(toolKind(name)), acp.WithStartStatus(acp.ToolCallStatusInProgress), acp.WithStartRawInput(native.Input))); err != nil {
			return err
		}

		state.tools[id] = true
	}

	if native.Status != statusCompleted && native.Status != stopReasonError {
		if native.Input != nil {
			return s.emit(ctx, acp.UpdateToolCall(toolID, acp.WithUpdateRawInput(native.Input)))
		}

		return nil
	}

	state.terminalTools[id] = true
	status := acp.ToolCallStatusCompleted
	content := []acp.ToolCallContent{}
	used := int64(0)

	for index, item := range native.Content {
		switch item.Type {
		case fieldText:
			content = append(content, acp.ToolContent(acp.TextBlock(item.Text)))
		case partFile:
			for _, block := range s.outputFile(opencode.NativeAttachment{ID: attachmentID(id, index), MIME: item.MIME, URL: item.URI, Filename: item.Name}, &used) {
				if block.Text != nil {
					status = acp.ToolCallStatusFailed
				}

				if block.Image != nil {
					state.imagesEmitted = true
				}

				content = append(content, acp.ToolContent(block))
			}
		}
	}

	if native.Status == stopReasonError {
		status = acp.ToolCallStatusFailed

		content = append(content, acp.ToolContent(acp.TextBlock(nativeErrorText(native.Error))))
	}

	return s.emit(ctx, acp.UpdateToolCall(toolID, acp.WithUpdateStatus(status), acp.WithUpdateRawInput(native.Input), acp.WithUpdateContent(content)))
}

func (s *session) openAgentCycle(ctx context.Context, rt *binding) *cycle {
	c := &cycle{Cycle: s.lc.NewAgentCycle(), done: make(chan struct{})}
	s.mu.Lock()
	if s.turn != nil || s.cycle != nil || s.closing || s.runtime != rt {
		s.mu.Unlock()

		return nil
	}

	s.cycle = c
	s.mu.Unlock()
	s.recordFailure(c, s.lc.OpenAgentCycle(ctx, c.Cycle))
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.runtime != rt || s.cycle != c || s.closing {
		return nil
	}

	return c
}

func nativeErrorText(native *opencode.NativeError) string {
	if native == nil {
		return "native request failed"
	}

	if native.Message != "" {
		return native.Message
	}

	return native.Type
}

func (s *session) emit(ctx context.Context, updates ...acp.SessionUpdate) error {
	conn := s.agent.connection()
	if conn == nil {
		return nil
	}

	for _, update := range updates {
		if err := conn.SessionUpdate(context.WithoutCancel(ctx), acp.SessionNotification{SessionId: s.id, Update: update}); err != nil {
			return err
		}
	}

	return nil
}

func (s *session) emitRawEvent(ctx context.Context, event opencode.Event) {
	if !s.rawEvents.Enabled() {
		return
	}

	conn := s.agent.connection()
	if conn == nil {
		return
	}

	var payload map[string]any
	if json.Unmarshal(event.Raw, &payload) != nil {
		return
	}

	redactImageURLs(payload)

	if err := s.rawEvents.Emit(ctx, func(ctx context.Context, method string, params map[string]any) error {
		return conn.NotifyExtension(ctx, method, params)
	}, payload); err != nil {
		s.agent.observe.RecordRawMessageEmitFailure(ctx, err)
	}
}

func (s *session) emitSessionInfo(ctx context.Context, prompt []acp.ContentBlock) {
	updatedAt := time.Now().UTC().Format(time.RFC3339)
	update := acp.SessionSessionInfoUpdate{UpdatedAt: &updatedAt}

	s.mu.Lock()
	s.updatedAt = updatedAt

	if s.title == "" {
		if title := wire.PromptTitle(prompt); title != "" {
			s.title = title
			update.Title = &title
		}
	}
	s.mu.Unlock()

	_ = s.emit(ctx, acp.SessionUpdate{SessionInfoUpdate: &update})
}

func (s *session) sessionInfo() acp.SessionInfo {
	s.mu.Lock()
	title := s.title
	updatedAt := s.updatedAt
	s.mu.Unlock()

	if title == "" {
		title = string(s.id)
	}

	info := acp.SessionInfo{
		Meta:                  wire.NativeSessionMeta(vendor, s.nativeID),
		SessionId:             s.id,
		Title:                 &title,
		Cwd:                   s.cwd,
		AdditionalDirectories: append([]string(nil), s.additionalDirectories...),
	}
	if updatedAt != "" {
		info.UpdatedAt = &updatedAt
	}

	return info
}

func (s *session) settleAgentCycle(ctx context.Context, rt *binding, c *cycle) {
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sessionSettleTimeout)
	defer cancel()

	s.beginSettlement(c)

	if c.state.stopReason == "" {
		c.state.stopReason = statusComplete
	}

	if err := s.commitMirror(settleCtx, rt); err != nil {
		s.recordFailure(c, s.mirrorFailure(&c.state, err))
		s.fenceStream()
		// A fenced incarnation is terminal, so the binding ends with it and the
		// next operation relaunches and opens a new one. This runs on the
		// binding's own pump, which joins itself, so the cancel is the drop.
		rt.cancel()
	}

	verdict := judgeCycle(c, s.cycleFailure(c), s.claimCancellation(c))
	_ = s.lc.Idle(settleCtx, c.Cycle, verdict.stopReason, verdict.outcome)
	close(c.done)

	s.mu.Lock()
	if s.cycle == c {
		s.cycle = nil
	}
	s.mu.Unlock()
}

func redactImageURLs(value any) {
	switch object := value.(type) {
	case map[string]any:
		if encoded, ok := object["data"].(string); ok && object["mime"] != nil {
			object["encodedBytes"] = len(encoded)
			object["data"] = ""
		}

		if raw, ok := object["uri"].(string); ok && strings.HasPrefix(raw, "data:") {
			object["uri"] = ""
			if _, data, ok := strings.Cut(raw, ","); ok {
				object["encodedBytes"] = len(data)
			}
		}

		for _, item := range object {
			redactImageURLs(item)
		}
	case []any:
		for _, item := range object {
			redactImageURLs(item)
		}
	}
}

func toolKind(name string) acp.ToolKind {
	switch name {
	case nativeToolShell:
		return acp.ToolKindExecute
	case "read":
		return acp.ToolKindRead
	case "edit", "write", "apply_patch":
		return acp.ToolKindEdit
	case "glob", "grep", "websearch":
		return acp.ToolKindSearch
	case "webfetch":
		return acp.ToolKindFetch
	default:
		return acp.ToolKindOther
	}
}

func (s *session) emitPlan(ctx context.Context, payload json.RawMessage) error {
	var event struct {
		Todos []opencode.NativeTodo `json:"todos"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}

	entries := make([]acp.PlanEntry, 0, len(event.Todos))
	for _, todo := range event.Todos {
		status := acp.PlanEntryStatusPending

		switch todo.Status {
		case "in_progress":
			status = acp.PlanEntryStatusInProgress
		case statusCompleted:
			status = acp.PlanEntryStatusCompleted
		case lifecycle.StopReasonCancelled:
			continue
		}

		priority := acp.PlanEntryPriorityMedium

		switch todo.Priority {
		case "high":
			priority = acp.PlanEntryPriorityHigh
		case "low":
			priority = acp.PlanEntryPriorityLow
		}

		entries = append(entries, acp.PlanEntry{Content: todo.Content, Priority: priority, Status: status})
	}

	return s.emit(ctx, acp.UpdatePlan(entries...))
}
