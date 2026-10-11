package opencode

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"

	"github.com/savid/acp-go-core/wire"
)

const MaxBodyBytes = 64 << 20

// EventCapacity bounds the decoded frames waiting for the runtime pump.
const EventCapacity = 256

// ServerUsername is the basic-auth user the adapter configures the native
// server with and sends on every request.
const ServerUsername = "opencode"

// Client is an authenticated native HTTP endpoint shared by every session.
type Client struct {
	URL      string
	Password string
	http     *http.Client
}

type HTTPError struct {
	Status int
	Native NativeError
}

// ProtocolError reports a response that cannot belong to the native JSON API.
type ProtocolError struct{ Message string }

func (e *ProtocolError) Error() string { return e.Message }

func (e *HTTPError) Error() string { return fmt.Sprintf("opencode HTTP status %d", e.Status) }
func IsMissing(err error) bool {
	var e *HTTPError

	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

func NewClient() *Client {
	return &Client{Password: NewID(""), http: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func NewID(prefix string) string {
	var value [24]byte

	_, _ = rand.Read(value[:])

	return prefix + hex.EncodeToString(value[:])
}

func (c *Client) Args() []string {
	return []string{"serve", "--hostname", "127.0.0.1", "--port", "0"}
}

// ReadAddress reads the owned server's bound loopback address before any HTTP request.
func (c *Client) ReadAddress(stdout io.Reader) error {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		address, announced := strings.CutPrefix(scanner.Text(), "server listening on ")
		if !announced {
			continue
		}

		host, httpAddress := strings.CutPrefix(address, "http://")

		endpoint, err := netip.ParseAddrPort(host)
		if !httpAddress || err != nil || endpoint.Addr().String() != "127.0.0.1" || endpoint.Port() == 0 {
			return errors.New("opencode announced an invalid server address")
		}

		c.URL = address

		return nil
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("opencode server address: %w", err)
	}

	return errors.New("opencode server exited without announcing its address")
}

func (c *Client) request(ctx context.Context, directory, method, path string, body any) (*http.Response, error) {
	var reader io.Reader

	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}

		reader = bytes.NewReader(data)
	}

	target := c.URL + path
	if directory != "" {
		target += "?location[directory]=" + url.QueryEscape(directory)
	}

	request, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, err
	}

	request.SetBasicAuth(ServerUsername, c.Password)
	request.Header.Set("Content-Type", "application/json")

	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("opencode request failed: %w", err)
	}

	return response, nil
}

func (c *Client) Do(ctx context.Context, directory, method, path string, body, out any) error {
	response, err := c.request(ctx, directory, method, path, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	data, err := io.ReadAll(io.LimitReader(response.Body, MaxBodyBytes+1))
	if err != nil {
		return err
	}

	if len(data) > MaxBodyBytes {
		return errors.New("opencode response exceeds size limit")
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		e := &HTTPError{Status: response.StatusCode}

		_ = json.Unmarshal(data, &e.Native)
		if e.Native.Status == 0 {
			e.Native.Status = response.StatusCode
		}

		return e
	}

	if len(data) > 0 && !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
		return &ProtocolError{Message: "OpenCode v2 JSON API required"}
	}

	if out == nil || len(data) == 0 {
		return nil
	}

	return json.Unmarshal(data, out)
}

func SessionPath(id string) string { return "/api/session/" + url.PathEscape(id) }

// WaitForPlugins uses the integration endpoint's activation barrier before
// reading catalogs whose endpoints return the current, possibly partial snapshot.
func (c *Client) WaitForPlugins(ctx context.Context, directory string) error {
	return c.Do(ctx, directory, http.MethodGet, "/api/integration", nil, nil)
}

func (c *Client) Session(ctx context.Context, id string) (NativeSession, error) {
	var out struct {
		Data NativeSession `json:"data"`
	}

	err := c.Do(ctx, "", http.MethodGet, SessionPath(id), nil, &out)

	return out.Data, err
}
func (c *Client) Export(ctx context.Context, id string) (Export, error) {
	var out struct {
		Data Export `json:"data"`
	}

	err := c.Do(ctx, "", http.MethodGet, "/api/experimental/session/"+url.PathEscape(id)+"/export", nil, &out)

	return out.Data, err
}

// StreamExport reads one session export as it arrives: info receives the
// session information, then message receives each settled message's raw bytes
// in native order. A message's bytes are valid only during its call, so the
// export is never held whole.
func (c *Client) StreamExport(ctx context.Context, id string, info func(json.RawMessage) error, message func(raw []byte) error) error {
	response, err := c.request(ctx, "", http.MethodGet, "/api/experimental/session/"+url.PathEscape(id)+"/export", nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, MaxBodyBytes))
		e := &HTTPError{Status: response.StatusCode}

		_ = json.Unmarshal(data, &e.Native)
		if e.Native.Status == 0 {
			e.Native.Status = response.StatusCode
		}

		return e
	}

	if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
		return &ProtocolError{Message: "OpenCode v2 JSON API required"}
	}

	decoder := jsontext.NewDecoder(response.Body)

	return readObject(decoder, func(key string) error {
		if key != fieldData {
			return decoder.SkipValue()
		}

		return readObject(decoder, func(key string) error {
			switch key {
			case "info":
				value, err := decoder.ReadValue()
				if err != nil {
					return err
				}

				return info(append(json.RawMessage(nil), value...))
			case "messages":
				return readArray(decoder, func() error {
					value, err := decoder.ReadValue()
					if err != nil {
						return err
					}

					if len(value) > MaxBodyBytes {
						return errors.New("opencode message exceeds size limit")
					}

					return message(value)
				})
			default:
				return decoder.SkipValue()
			}
		})
	})
}

// readObject calls member for each key of the next JSON object; member reads
// or skips that key's value.
func readObject(decoder *jsontext.Decoder, member func(key string) error) error {
	token, err := decoder.ReadToken()
	if err != nil {
		return err
	}

	if token.Kind() != '{' {
		return &ProtocolError{Message: "opencode export is not an object"}
	}

	for decoder.PeekKind() != '}' {
		key, keyErr := decoder.ReadToken()
		if keyErr != nil {
			return keyErr
		}

		if memberErr := member(key.String()); memberErr != nil {
			return memberErr
		}
	}

	_, err = decoder.ReadToken()

	return err
}

// readArray calls element for each element of the next JSON array.
func readArray(decoder *jsontext.Decoder, element func() error) error {
	token, err := decoder.ReadToken()
	if err != nil {
		return err
	}

	if token.Kind() != '[' {
		return &ProtocolError{Message: "opencode export messages are not an array"}
	}

	for decoder.PeekKind() != ']' {
		if elementErr := element(); elementErr != nil {
			return elementErr
		}
	}

	_, err = decoder.ReadToken()

	return err
}

func (c *Client) Import(ctx context.Context, snapshot Export, location Location) error {
	messages := snapshot.Messages
	if messages == nil {
		messages = []Message{}
	}

	body := struct {
		Info     json.RawMessage `json:"info"`
		Messages []Message       `json:"messages"`
		Location Location        `json:"location"`
	}{snapshot.InfoRaw, messages, location}

	return c.Do(ctx, "", http.MethodPost, "/api/experimental/session/import", body, nil)
}

func (c *Client) Move(ctx context.Context, id, directory string) error {
	if err := c.Do(ctx, "", http.MethodPost, SessionPath(id)+"/move", Location{Directory: directory}, nil); err != nil {
		return err
	}

	return c.Wait(ctx, id)
}
func (c *Client) Interrupt(ctx context.Context, id string) error {
	return c.Do(ctx, "", http.MethodPost, SessionPath(id)+"/interrupt", map[string]any{}, nil)
}
func (c *Client) Wait(ctx context.Context, id string) error {
	return c.Do(ctx, "", http.MethodPost, "/api/experimental/session/"+url.PathEscape(id)+"/wait", map[string]any{}, nil)
}

// Stream owns one ordered global SSE connection. Closing it joins its reader.
type Stream struct {
	Events chan Event
	done   chan struct{}
	cancel context.CancelFunc
	body   io.ReadCloser
	mu     sync.Mutex
	err    error
}

func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.err
}

func (s *Stream) Close() { s.cancel(); _ = s.body.Close(); <-s.done }

//nolint:bodyclose // The returned stream owns the response body and closes it in its joined reader.
func (c *Client) Subscribe(ctx context.Context) (*Stream, error) {
	streamCtx, cancel := context.WithCancel(ctx)

	response, err := c.request(streamCtx, "", http.MethodGet, "/api/event", nil)
	if err != nil {
		cancel()

		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()

		cancel()

		return nil, errors.New("opencode SSE subscription refused")
	}

	s := &Stream{Events: make(chan Event, EventCapacity), done: make(chan struct{}), cancel: cancel, body: response.Body}
	go func() {
		defer close(s.done)
		defer close(s.Events)
		defer response.Body.Close()

		err := s.read(streamCtx, response.Body)
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
	}()

	return s, nil
}

// read decodes each SSE frame once and delivers it in order. A full queue
// holds the reader, so the native server's stream waits instead of an event
// being lost.
func (s *Stream) read(ctx context.Context, reader io.Reader) error {
	lines := wire.NewLineReader(reader, MaxBodyBytes)

	var data []byte

	dispatch := func() error {
		if len(data) == 0 {
			return nil
		}

		event, err := DecodeEvent(data)
		if err != nil {
			return err
		}

		if event.Type == "" {
			return errors.New("native event type missing")
		}

		select {
		case s.Events <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		line, err := lines.Next()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}

			if dispatchErr := dispatch(); dispatchErr != nil {
				return dispatchErr
			}

			return io.EOF
		}

		if len(line) == 0 {
			if err := dispatch(); err != nil {
				return err
			}

			data = nil

			continue
		}

		value, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok {
			continue
		}

		value = bytes.TrimPrefix(value, []byte(" "))
		if data == nil {
			data = value

			continue
		}

		data = append(append(data, '\n'), value...)
		if len(data) > MaxBodyBytes {
			return errors.New("opencode event exceeds size limit")
		}
	}
}
