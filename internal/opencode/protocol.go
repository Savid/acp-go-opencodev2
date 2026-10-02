//nolint:tagliatelle // The native API uses uppercase ID suffixes.
package opencode

import "encoding/json"

type ModelRef struct {
	ID         string `json:"id"`
	ProviderID string `json:"providerID"`
	Variant    string `json:"variant,omitempty"`
}
type Location struct {
	Directory string `json:"directory"`
}
type NativeSession struct {
	ID       string         `json:"id"`
	ParentID string         `json:"parentID,omitempty"`
	Title    string         `json:"title,omitempty"`
	Agent    string         `json:"agent,omitempty"`
	Model    ModelRef       `json:"model"`
	Location Location       `json:"location"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Cost     float64        `json:"cost"`
	Outcome  string         `json:"outcome,omitempty"`
	Time     struct {
		Created int64 `json:"created"`
		Updated int64 `json:"updated"`
		Idle    int64 `json:"idle"`
	} `json:"time"`
}

// NativeMessage is a projected message returned by the native transcript API.
// Raw retains fields needed by import even when ACP does not display them.
type NativeMessage struct {
	ID      string             `json:"id"`
	Type    string             `json:"type"`
	Text    string             `json:"text,omitempty"`
	Model   ModelRef           `json:"model"`
	Agent   string             `json:"agent,omitempty"`
	Content []Content          `json:"content,omitempty"`
	Files   []NativeAttachment `json:"files,omitempty"`
	Finish  string             `json:"finish,omitempty"`
	Outcome string             `json:"outcome,omitempty"`
	Error   *NativeError       `json:"error,omitempty"`
	Tokens  NativeTokens       `json:"tokens"`
	Cost    float64            `json:"cost"`
	Time    struct {
		Created   int64 `json:"created"`
		Completed int64 `json:"completed"`
	} `json:"time"`
	Raw json.RawMessage `json:"-"`
}

func (m *NativeMessage) UnmarshalJSON(data []byte) error {
	type plain NativeMessage

	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*m = NativeMessage(value)

	m.Raw = append(json.RawMessage(nil), data...)

	return nil
}

// Content is one native assistant or tool content block.
type Content struct {
	Type  string          `json:"type"`
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Text  string          `json:"text,omitempty"`
	State json.RawMessage `json:"state,omitempty"`
	URI   string          `json:"uri,omitempty"`
	MIME  string          `json:"mime,omitempty"`
}
type ToolState struct {
	Status   string         `json:"status"`
	Input    any            `json:"input"`
	Content  []Content      `json:"content,omitempty"`
	Error    *NativeError   `json:"error,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}
type NativeAttachment struct {
	Data     string `json:"data,omitempty"`
	ID       string `json:"id,omitempty"`
	Type     string `json:"type,omitempty"`
	MIME     string `json:"mime,omitempty"`
	Filename string `json:"name,omitempty"`
	URL      string `json:"uri"`
}
type NativeError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Status  int    `json:"status,omitempty"`
}

type NativeTokens struct {
	Input     float64 `json:"input"`
	Output    float64 `json:"output"`
	Reasoning float64 `json:"reasoning"`
	Cache     struct {
		Read  float64 `json:"read"`
		Write float64 `json:"write"`
	} `json:"cache"`
}
type NativeAgent struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Mode   string `json:"mode"`
	Hidden bool   `json:"hidden"`
}
type NativeCommand struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}
type NativeSessionStatus struct {
	Type string `json:"type"`
}
type NativeTodo struct {
	Content  string `json:"content"`
	Status   string `json:"status"`
	Priority string `json:"priority"`
}
type Model struct {
	Package      string `json:"package"`
	ID           string `json:"id"`
	ProviderID   string `json:"providerID"`
	Name         string `json:"name"`
	Enabled      bool   `json:"enabled"`
	Capabilities struct {
		Input []string `json:"input"`
	} `json:"capabilities"`
	Variants []struct {
		ID string `json:"id"`
	} `json:"variants"`
	Limit map[string]float64 `json:"limit"`
}
type Event struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Created int64           `json:"created"`
	Data    json.RawMessage `json:"data"`
	Durable *struct {
		AggregateID string `json:"aggregateID"`
		Seq         int64  `json:"seq"`
		Version     int    `json:"version"`
	} `json:"durable,omitempty"`
	Raw json.RawMessage `json:"-"`
}

func (e *Event) UnmarshalJSON(data []byte) error {
	type plain Event

	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*e = Event(value)

	e.Raw = append(json.RawMessage(nil), data...)

	return nil
}
func (e Event) SessionID() string {
	var data struct {
		SessionID string `json:"sessionID"`
		Form      struct {
			SessionID string `json:"sessionID"`
		} `json:"form"`
	}
	if json.Unmarshal(e.Data, &data) != nil {
		return ""
	}

	if data.SessionID == "" {
		return data.Form.SessionID
	}

	return data.SessionID
}

type PermissionRequest struct {
	ID        string   `json:"id"`
	SessionID string   `json:"sessionID"`
	Action    string   `json:"action"`
	Resources []string `json:"resources"`
	Source    struct {
		Type      string `json:"type"`
		MessageID string `json:"messageID"`
		ID        string `json:"id"`
	} `json:"source"`
}
type Form struct {
	ID        string         `json:"id"`
	SessionID string         `json:"sessionID"`
	Title     string         `json:"title"`
	Fields    []FormField    `json:"fields"`
	Metadata  map[string]any `json:"metadata"`
}
type FormField struct {
	Key         string `json:"key"`
	Type        string `json:"type"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
	Options     []struct {
		Value string `json:"value"`
		Label string `json:"label"`
	} `json:"options"`
	Raw json.RawMessage `json:"-"`
}

func (f *FormField) UnmarshalJSON(data []byte) error {
	type plain FormField

	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*f = FormField(value)

	f.Raw = append(json.RawMessage(nil), data...)

	return nil
}

// Export is the native, lossless session transfer format.
type Export struct {
	Info     NativeSession   `json:"info"`
	Messages []NativeMessage `json:"messages"`
	Raw      json.RawMessage `json:"-"`
}

// InboxInput is native input waiting for the next execution.
//
//nolint:tagliatelle // Native event identities use uppercase ID suffixes.
type InboxInput struct {
	ID        string          `json:"id"`
	SessionID string          `json:"sessionID"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Delivery  string          `json:"delivery"`
	Time      struct {
		Created int64 `json:"created"`
	} `json:"time"`
}

func (e *Export) UnmarshalJSON(data []byte) error {
	type plain Export

	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}

	*e = Export(value)

	e.Raw = append(json.RawMessage(nil), data...)

	return nil
}
