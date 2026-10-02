package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

func (input InboxInput) Validate(sessionID string) error {
	var payload struct {
		Text *string `json:"text"`
	}
	if input.SessionID != sessionID || !strings.HasPrefix(input.ID, "msg_") || input.Type != "synthetic" || (input.Delivery != "steer" && input.Delivery != "queue") || json.Unmarshal(input.Payload, &payload) != nil || payload.Text == nil {
		return errors.New("native inbox cannot be mirrored")
	}

	return nil
}

func (c *Client) CancelInput(ctx context.Context, sessionID, inputID string) error {
	return c.Do(ctx, "", http.MethodDelete, SessionPath(sessionID)+"/inbox/"+url.PathEscape(inputID), nil, nil)
}

func (c *Client) Inbox(ctx context.Context, sessionID string) ([]InboxInput, error) {
	var out struct {
		Data []InboxInput `json:"data"`
	}
	if err := c.Do(ctx, "", http.MethodGet, SessionPath(sessionID)+"/inbox", nil, &out); err != nil {
		return nil, err
	}

	for _, input := range out.Data {
		if err := input.Validate(sessionID); err != nil {
			return nil, err
		}
	}

	return out.Data, nil
}

func (c *Client) Enqueue(ctx context.Context, input InboxInput) error {
	if err := input.Validate(input.SessionID); err != nil {
		return err
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(input.Payload, &body); err != nil {
		return err
	}

	body["id"], _ = json.Marshal(input.ID)
	body["delivery"], _ = json.Marshal(input.Delivery)
	body["resume"] = json.RawMessage("false")

	return c.Do(ctx, "", http.MethodPost, SessionPath(input.SessionID)+"/synthetic", body, nil)
}
