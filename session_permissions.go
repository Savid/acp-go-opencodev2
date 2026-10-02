package opencodeacp

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"

	acp "github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

func (s *session) handleControl(ctx context.Context, rt *binding, c *cycle, event opencode.Event) {
	var (
		permission opencode.PermissionRequest
		formEvent  struct {
			Form opencode.Form `json:"form"`
		}
	)

	id := ""

	if event.Type == eventPermissionAsked {
		if json.Unmarshal(event.Data, &permission) != nil {
			rt.cancel()

			return
		}

		id = permission.ID
	} else {
		if json.Unmarshal(event.Data, &formEvent) != nil {
			rt.cancel()

			return
		}

		id = formEvent.Form.ID
	}

	if id == "" {
		rt.cancel()

		return
	}

	owned := event.Type != eventPermissionAsked || permission.Source.Type == "tool" && c.state.tools[permission.Source.ID]
	callbackCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))

	release, admitted := s.registerDialog(id, cancel)
	if !admitted {
		return
	}

	go func() {
		defer release()
		defer cancel(nil)

		path := opencode.SessionPath(s.nativeID)
		method := http.MethodPost

		var body any

		if event.Type == eventPermissionAsked {
			choice := approvalReject
			if owned {
				choice = s.requestPermission(callbackCtx, c, permission)
			}

			path += "/permission/" + url.PathEscape(id) + "/reply"
			body = map[string]string{"decision": choice}
		} else {
			var answer map[string]any
			if owned {
				answer = s.elicit(callbackCtx, c, formEvent.Form)
			}

			path += "/form/" + url.PathEscape(id)

			if answer == nil {
				method = http.MethodDelete
			} else {
				path += "/reply"
				body = map[string]any{"answer": answer}
			}
		}

		replyCtx, replyCancel := context.WithTimeout(context.WithoutCancel(ctx), sessionAbortTimeout)
		defer replyCancel()

		if err := rt.client.Do(replyCtx, "", method, path, body, nil); err != nil && !opencode.IsMissing(err) {
			rt.cancel()
		}
	}()
}

func (s *session) requestPermission(ctx context.Context, c *cycle, request opencode.PermissionRequest) string {
	conn := s.agent.connection()
	if conn == nil || ctx.Err() != nil {
		return approvalReject
	}

	options := []acp.PermissionOption{
		{OptionId: approvalOnce, Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: approvalAlways, Name: "Allow for session", Kind: acp.PermissionOptionKindAllowAlways},
		{OptionId: approvalReject, Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
	}

	title := request.Action
	if len(request.Resources) > 0 {
		title += " " + strings.Join(request.Resources, ", ")
	}

	ctx, finish := s.agent.observe.StartPermission(ctx, title, "native")
	response, err := announcedRequest(ctx, s, c, lifecycle.ActionPermission, func(ctx context.Context, meta map[string]any) (acp.RequestPermissionResponse, error) {
		return conn.RequestPermission(ctx, acp.RequestPermissionRequest{Meta: meta, SessionId: s.id, ToolCall: acp.ToolCallUpdate{ToolCallId: acp.ToolCallId(request.Source.ID), Title: &title}, Options: options})
	}, func(response acp.RequestPermissionResponse, err error) lifecycle.ActionState {
		if err != nil {
			return lifecycle.ActionFailed
		}

		if response.Outcome.Selected == nil {
			return lifecycle.ActionCancelled
		}

		if response.Outcome.Selected.OptionId == approvalOnce || response.Outcome.Selected.OptionId == approvalAlways {
			return lifecycle.ActionAccepted
		}

		return lifecycle.ActionDeclined
	})

	choice := approvalReject
	if err == nil && ctx.Err() == nil && response.Outcome.Selected != nil && slices.Contains([]string{approvalOnce, approvalAlways}, string(response.Outcome.Selected.OptionId)) {
		choice = string(response.Outcome.Selected.OptionId)
	}

	finish(observer.PermissionResult{Behavior: choice, Mode: "native", ToolName: request.Action})

	return choice
}

func (s *session) elicit(ctx context.Context, c *cycle, form opencode.Form) map[string]any {
	conn := s.agent.connection()
	if conn == nil || !s.agent.clientSupportsFormElicitation() || ctx.Err() != nil {
		return nil
	}

	schema, ok := formSchema(form)
	if !ok {
		return nil
	}

	ctx, finish := s.agent.observe.StartElicitation(ctx)
	response, err := announcedRequest(ctx, s, c, lifecycle.ActionElicitation, func(ctx context.Context, meta map[string]any) (acp.UnstableCreateElicitationResponse, error) {
		return conn.UnstableCreateElicitation(ctx, acp.UnstableCreateElicitationRequest{Form: &acp.UnstableCreateElicitationForm{Meta: meta, Mode: "form", Message: form.Title, RequestedSchema: schema}})
	}, func(response acp.UnstableCreateElicitationResponse, err error) lifecycle.ActionState {
		if err != nil {
			return lifecycle.ActionFailed
		}

		if response.Accept != nil {
			return lifecycle.ActionAccepted
		}

		if response.Decline != nil {
			return lifecycle.ActionDeclined
		}

		return lifecycle.ActionCancelled
	})
	finish(observer.ElicitationResult{Accepted: err == nil && response.Accept != nil, Err: err})

	if err != nil || ctx.Err() != nil || response.Accept == nil {
		return nil
	}

	return response.Accept.Content
}

// formSchema refuses conditional or external forms the negotiated ACP form cannot express.
func formSchema(form opencode.Form) (acp.UnstableElicitationSchema, bool) {
	schema := acp.UnstableElicitationSchema{Type: acp.UnstableElicitationSchemaTypeObject, Properties: map[string]any{}}

	for _, field := range form.Fields {
		var native map[string]any
		if json.Unmarshal(field.Raw, &native) != nil || field.Key == "" {
			return schema, false
		}

		if field.Type == "external" || native["when"] != nil || native["hidden"] == true {
			return schema, false
		}

		value := map[string]any{fieldType: field.Type}

		for _, key := range []string{"title", "description", "format", "minLength", "maxLength", "pattern", "default", "minimum", "maximum"} {
			if v, ok := native[key]; ok {
				value[key] = v
			}
		}

		choices := make([]string, 0, len(field.Options))
		for _, option := range field.Options {
			choices = append(choices, option.Value)
		}

		if len(choices) > 0 && native["custom"] != true {
			value["enum"] = choices
		}

		if field.Type == "multiselect" {
			item := map[string]any{fieldType: "string"}
			if len(choices) > 0 && native["custom"] != true {
				item["enum"] = choices
			}

			value[fieldType] = "array"
			value["items"] = item
			value["uniqueItems"] = true
			delete(value, "enum")

			for _, key := range []string{"minItems", "maxItems"} {
				if v, ok := native[key]; ok {
					value[key] = v
				}
			}
		}

		if field.Required {
			schema.Required = append(schema.Required, field.Key)
		}

		schema.Properties[field.Key] = value
	}

	return schema, len(schema.Properties) > 0
}

// announcedRequest sends one client request that holds native work, announces
// the action it answers once the request is on the wire, and resolves that
// action exactly once.
func announcedRequest[T any](
	ctx context.Context,
	s *session,
	c *cycle,
	kind lifecycle.ActionKind,
	send func(context.Context, map[string]any) (T, error),
	resolved func(T, error) lifecycle.ActionState,
) (T, error) {
	var zero T

	releaseCall, err := s.agent.acquireClientCall()
	if err != nil {
		return zero, err
	}
	defer releaseCall()

	actionID := s.reserveAction(c)
	if actionID == "" {
		return send(ctx, nil)
	}

	value, callErr := wire.CallAndAnnounce(ctx, s.agent.transportRef(), s.lc.Correlation(c.Cycle, actionID), send, func() {
		if err := s.lc.ActionPending(ctx, c.Cycle, actionID, kind); err != nil {
			s.agent.log.ErrorContext(ctx, "announce lifecycle action failed",
				slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))
		}
	})

	state := resolved(value, callErr)

	if callErr != nil && errors.Is(context.Cause(ctx), errDialogCancelled) {
		state = lifecycle.ActionCancelled
	}

	if err := s.lc.ActionResolved(context.WithoutCancel(ctx), c.Cycle, actionID, state); err != nil {
		s.agent.log.ErrorContext(ctx, "resolve lifecycle action failed",
			slog.String("session_id", string(s.id)), slog.String("reason", err.Error()))
	}

	return value, callErr
}

// reserveAction mints the action id one announcement correlates on, or an
// empty id when no incarnation owns the cycle.
func (s *session) reserveAction(c *cycle) string {
	if !s.lc.Active() || c.TurnID == "" {
		return ""
	}

	return s.lc.NextID("action")
}
