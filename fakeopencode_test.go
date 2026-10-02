package opencodeacp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

const fakeOpenCodeEnv = "ACP_GO_OPENCODEV2_TEST_FAKE"
const fakeOpenCodeEnvHistoryAdvance = "ACP_GO_OPENCODEV2_TEST_HISTORY_ADVANCE"
const fakeOpenCodeEnvResumeHold = "ACP_GO_OPENCODEV2_TEST_RESUME_HOLD"
const fakeOpenCodeEnvStartHold = "ACP_GO_OPENCODEV2_TEST_START_HOLD"
const fakeOpenCodeEnvHealthHold = "ACP_GO_OPENCODEV2_TEST_HEALTH_HOLD"

type fakeExport struct {
	Info     opencode.NativeSession   `json:"info"`
	Messages []opencode.NativeMessage `json:"messages"`
}
type fakeOpenCode struct {
	mu          sync.Mutex
	healthCalls atomic.Int32
	sessions    map[string]*fakeExport
	subscribers map[chan []byte]bool
	pending     map[string]chan struct{}
	answers     map[string]chan json.RawMessage
	environment map[string]map[string]string
	path        string
}

func runFakeOpenCode(args []string) int {
	port := ""
	for i, arg := range args {
		if arg == "--port" && i+1 < len(args) {
			port = args[i+1]
		}
	}
	if port == "" {
		return 2
	}
	f := &fakeOpenCode{sessions: map[string]*fakeExport{}, subscribers: map[chan []byte]bool{}, pending: map[string]chan struct{}{}, answers: map[string]chan json.RawMessage{}, environment: map[string]map[string]string{}, path: filepath.Join(os.Getenv("XDG_DATA_HOME"), "opencode", "fake.json")}
	if data, err := os.ReadFile(f.path); err == nil {
		if json.Unmarshal(data, &f.sessions) != nil {
			return 3
		}
	}
	if hold := os.Getenv(fakeOpenCodeEnvStartHold); hold != "" {
		if _, err := os.Stat(hold + ".armed"); err == nil {
			_ = os.WriteFile(hold, []byte("held\n"), 0o600)
			for {
				if _, err := os.Stat(hold); err != nil {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	server := &http.Server{Addr: "127.0.0.1:" + port, Handler: f, ReadHeaderTimeout: time.Second}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return 4
	}
	_, _ = fmt.Fprintln(os.Stdout, "server listening on http://"+listener.Addr().String())
	if server.Serve(listener) != nil {
		return 4
	}

	return 0
}
func (f *fakeOpenCode) save() {
	data, _ := json.Marshal(f.sessions)
	_ = os.MkdirAll(filepath.Dir(f.path), 0o700)
	_ = os.WriteFile(f.path, data, 0o600)
}
func (f *fakeOpenCode) publish(id, typ string, data map[string]any) {
	data["sessionID"] = id
	created := time.Now().UnixMilli()
	if typ == "session.execution.succeeded" || typ == eventExecutionFailed || typ == eventExecutionInterrupted {
		if session := f.sessions[id]; session != nil {
			session.Info.Time.Idle = max(created, session.Info.Time.Idle+1)
			f.save()
		}
	}
	raw, _ := json.Marshal(map[string]any{"id": opencode.NewID("evt_"), "type": typ, "created": created, "data": data})
	for ch := range f.subscribers {
		select {
		case ch <- raw:
		default:
			panic("fake event overflow")
		}
	}
}
func fakeWrite(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
func fakeData(w http.ResponseWriter, value any) { fakeWrite(w, map[string]any{"data": value}) }
func fakeTokens(input, cacheRead, output float64) opencode.NativeTokens {
	tokens := opencode.NativeTokens{Input: input, Output: output}
	tokens.Cache.Read = cacheRead

	return tokens
}
func (f *fakeOpenCode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	username, password, ok := r.BasicAuth()
	if !ok || username != opencode.ServerUsername || password != os.Getenv("OPENCODE_SERVER_PASSWORD") {
		w.WriteHeader(http.StatusUnauthorized)

		return
	}
	path := r.URL.Path
	if path == "/api/event" {
		f.events(w, r)

		return
	}
	if hold := os.Getenv(fakeOpenCodeEnvResumeHold); hold != "" && r.Method == http.MethodGet && strings.HasPrefix(path, "/api/session/ses") && strings.Count(path, "/") == 3 {
		_ = os.WriteFile(hold, []byte("held\n"), 0o600)
		<-r.Context().Done()

		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.catalogHTTP(w, r) || f.globalHTTP(w, r) {
		return
	}
	pieces := strings.Split(strings.Trim(path, "/"), "/")
	if len(pieces) >= 5 && pieces[1] == "experimental" && pieces[2] == "session" {
		f.persistenceHTTP(w, r, pieces)

		return
	}
	if len(pieces) < 3 || pieces[1] != "session" {
		w.WriteHeader(404)

		return
	}
	id := pieces[2]
	session := f.sessions[id]
	if session == nil {
		w.WriteHeader(404)

		return
	}
	f.sessionHTTP(w, r, id, session, pieces)
}

func (f *fakeOpenCode) sessionHTTP(w http.ResponseWriter, r *http.Request, id string, session *fakeExport, pieces []string) {
	if len(pieces) == 3 {
		if r.Method == http.MethodPatch {
			var body struct {
				Metadata map[string]any `json:"metadata"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			session.Info.Metadata = body.Metadata
			f.save()
			w.WriteHeader(204)

			return
		}
		fakeData(w, session.Info)

		return
	}
	switch pieces[3] {
	case "move":
		var location opencode.Location
		if json.NewDecoder(r.Body).Decode(&location) != nil {
			w.WriteHeader(400)

			return
		}
		session.Info.Location = location
		f.save()
		w.WriteHeader(204)
	case "message":
		fakeData(w, session.Messages)
	case "inbox":
		fakeData(w, []any{})
	case "permission", "form":
		if len(pieces) == 4 {
			fakeData(w, []any{})

			return
		}
		if answer := f.answers[pieces[4]]; answer != nil {
			var body json.RawMessage
			_ = json.NewDecoder(r.Body).Decode(&body)
			answer <- body
			delete(f.answers, pieces[4])
		}
		w.WriteHeader(204)
	case "environment":
		var body struct {
			Variables map[string]string `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.environment[id] = body.Variables
		w.WriteHeader(204)
	case "model":
		var body struct {
			Model opencode.ModelRef `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		session.Info.Model = body.Model
		f.save()
		w.WriteHeader(204)
	case "agent":
		var body struct {
			Agent string `json:"agent"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		session.Info.Agent = body.Agent
		f.save()
		w.WriteHeader(204)
	case "interrupt":
		if pending := f.pending[id]; pending != nil {
			select {
			case <-pending:
			default:
				close(pending)
			}
		}
		fakeWrite(w, map[string]bool{"interrupted": true})
	case "prompt", "command":
		f.promptHTTP(w, r, id, session)
	default:
		w.WriteHeader(404)
	}
}

func (f *fakeOpenCode) advanceHistory(id string) {
	marker := os.Getenv(fakeOpenCodeEnvHistoryAdvance)
	data, err := os.ReadFile(marker)
	if err != nil {
		return
	}
	mode := string(data)
	if mode != "always" {
		_ = os.Remove(marker)
	}
	session := f.sessions[id]
	if mode == "child" {
		child := session.Info
		child.ID = opencode.NewID("ses_")
		child.ParentID = id
		child.Title = "late history child"
		f.sessions[child.ID] = &fakeExport{Info: child, Messages: []opencode.NativeMessage{}}
	} else {
		session.Info.Title = "late history row"
		if mode == "always" {
			session.Info.Title += opencode.NewID("")
		}
	}
	f.save()
}
func (f *fakeOpenCode) agents(w http.ResponseWriter, r *http.Request) {
	var config struct {
		Plugins []string `json:"plugins"`
	}
	_ = json.Unmarshal([]byte(os.Getenv("OPENCODE_CONFIG_CONTENT")), &config)
	for _, plugin := range config.Plugins {
		u, _ := url.Parse(plugin)
		dir, _ := filepath.EvalSymlinks(r.URL.Query().Get("location[directory]"))
		hash := sha256.Sum256([]byte(dir))
		root := filepath.Join(u.Path, "ready")
		_ = os.MkdirAll(root, 0o700)
		_ = os.WriteFile(filepath.Join(root, hex.EncodeToString(hash[:])), []byte(dir), 0o600)
	}
	fakeData(w, []map[string]string{{"id": "build", "name": "build", "mode": "primary"}, {"id": "plan", "name": "plan", "mode": "primary"}})
}
func (f *fakeOpenCode) events(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}
	ch := make(chan []byte, 256)
	f.mu.Lock()
	f.subscribers[ch] = true
	f.mu.Unlock()
	defer func() { f.mu.Lock(); delete(f.subscribers, ch); f.mu.Unlock() }()
	_, _ = fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()
	for {
		select {
		case data := <-ch:
			_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
func (f *fakeOpenCode) call(id, text string, tokens opencode.NativeTokens) {
	session := f.sessions[id]
	message := opencode.NativeMessage{ID: opencode.NewMessageID(), Type: roleAssistant, Model: session.Info.Model, Tokens: tokens, Finish: "stop"}
	f.publish(id, "session.step.started", map[string]any{"assistantMessageID": message.ID, "model": message.Model})
	message.Content = []opencode.Content{{Type: fieldText, Text: text}}
	f.publish(id, "session.text.ended", map[string]any{"assistantMessageID": message.ID, "ordinal": 0, "text": text})
	f.publish(id, "session.step.ended", map[string]any{"assistantMessageID": message.ID, "finish": "stop", "tokens": tokens, "cost": 0})
	session.Messages = append(session.Messages, message)
}
func (f *fakeOpenCode) runPrompt(id, text, command string, pending chan struct{}) {
	f.mu.Lock()
	session := f.sessions[id]
	f.publish(id, "session.execution.started", map[string]any{})
	tokens := fakeTokens(10, 0, 3)
	switch text {
	case "MULTI", "STEER", "STEERSLOW":
		f.call(id, "first", fakeTokens(100, 1000, 20))
		f.call(id, "second", fakeTokens(50, 1120, 30))
		tokens = fakeTokens(40, 1200, 10)
	case "STEPSLOW":
		f.call(id, "first", fakeTokens(100, 1000, 20))
		tokens = fakeTokens(50, 1120, 30)
	case "COMPACT":
		f.call(id, "first", fakeTokens(100, 1000, 20))
		f.publish(id, "session.compaction.ended", map[string]any{"tokens": fakeTokens(1120, 0, 200)})
		tokens = fakeTokens(300, 0, 20)
	case "COMPACTFAIL":
		f.publish(id, "session.compaction.failed", map[string]any{"tokens": fakeTokens(1120, 0, 200), "error": map[string]string{"type": "compaction.failed", "message": "Summary format invalid"}})
		tokens = opencode.NativeTokens{}
	case "REPLAY":
		first := fakeTokens(100, 1000, 20)
		first.Cache.Write = 50
		first.Reasoning = 5
		f.call(id, "first", first)
		tokens = opencode.NativeTokens{}
	case "FLAKY":
		tokens = fakeTokens(100, 1000, 20)
	}
	message := opencode.NativeMessage{ID: opencode.NewMessageID(), Type: roleAssistant, Model: session.Info.Model, Tokens: tokens, Finish: "stop"}
	f.publish(id, "session.step.started", map[string]any{"assistantMessageID": message.ID, "model": message.Model})
	if slices.Contains([]string{"SLOW", "STEPSLOW", "STEERSLOW"}, text) {
		f.mu.Unlock()
		<-pending
		f.mu.Lock()
	}
	if text == "PERMISSION" || text == "QUESTION" {
		callID := opencode.NewID("call_")
		requestID := opencode.NewID("per_")
		answer := make(chan json.RawMessage, 1)
		f.answers[requestID] = answer
		f.publish(id, "session.tool.input.started", map[string]any{"assistantMessageID": message.ID, "id": callID, "name": "shell"})
		f.publish(id, "session.tool.called", map[string]any{"assistantMessageID": message.ID, "id": callID, "input": map[string]any{"command": "echo test"}})
		if text == "PERMISSION" {
			f.publish(id, "permission.asked", map[string]any{"id": requestID, "action": "shell", "resources": []string{"echo test"}, "source": map[string]string{"type": "tool", "id": callID, "messageID": message.ID}})
		} else {
			f.publish(id, "form.created", map[string]any{"form": map[string]any{"id": requestID, "sessionID": id, "title": "Pick a color", "fields": []any{map[string]any{"key": "0", "type": "string", "required": true, "options": []any{map[string]string{"value": "blue", "label": "blue"}, map[string]string{"value": "red", "label": "red"}}}}}})
		}
		f.mu.Unlock()
		select {
		case <-answer:
		case <-pending:
		}
		f.mu.Lock()
		state := opencode.ToolState{Status: "completed", Input: map[string]string{"command": "echo test"}, Content: []opencode.Content{{Type: fieldText, Text: "done"}}}
		raw, _ := json.Marshal(state)
		message.Content = append(message.Content, opencode.Content{ID: callID, Type: partTool, Name: "shell", State: raw})
		f.publish(id, "session.tool.success", map[string]any{"assistantMessageID": message.ID, "id": callID, "content": state.Content})
	}
	output := "hello " + text
	if command != "" {
		output = "command:" + command + " args:" + text
	}
	if text == "AGENT" {
		output = "agent:" + session.Info.Agent + " variant:" + session.Info.Model.Variant
	}
	if text == "ENV" {
		carrier, _ := session.Info.Metadata[opencode.CarrierKey].(map[string]any)
		raw, _ := json.Marshal(carrier)
		output = string(raw)
	}
	if text == "NOISE" {
		fmt.Println("not json")
		fmt.Fprintln(os.Stderr, "native stderr noise")
	}
	if text == "STREAM" {
		f.publish(id, "session.text.delta", map[string]any{"assistantMessageID": message.ID, "ordinal": 0, "delta": "abc"})
		output = "abcdef"
	}
	message.Content = append(message.Content, opencode.Content{Type: fieldText, Text: output})
	f.publish(id, "session.text.ended", map[string]any{"assistantMessageID": message.ID, "ordinal": 0, "text": output})
	if text == "IMAGE" {
		path := filepath.Join(session.Info.Location.Directory, "output.png")
		file, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		if err := png.Encode(file, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
			panic(err)
		}
		_ = file.Close()
		callID := opencode.NewID("call_")
		content := []opencode.Content{{Type: partFile, MIME: "image/png", URI: path}}
		raw, _ := json.Marshal(opencode.ToolState{Status: "completed", Content: content})
		message.Content = append(message.Content, opencode.Content{ID: callID, Type: partTool, Name: "read", State: raw})
		f.publish(id, "session.tool.input.started", map[string]any{"assistantMessageID": message.ID, "id": callID, "name": "read"})
		f.publish(id, "session.tool.success", map[string]any{"assistantMessageID": message.ID, "id": callID, "content": content})
	}
	cancelled := false
	select {
	case <-pending:
		cancelled = true
	default:
		close(pending)
	}
	delete(f.pending, id)
	outcome := "succeeded"
	if cancelled {
		outcome = "interrupted"
		message.Tokens = opencode.NativeTokens{}
	} else {
		f.publish(id, "session.step.ended", map[string]any{"assistantMessageID": message.ID, "tokens": tokens, "finish": "stop", "cost": 0})
	}
	session.Messages = append(session.Messages, message, opencode.NativeMessage{ID: opencode.NewMessageID(), Type: "idle", Outcome: outcome})
	session.Info.Outcome = outcome
	terminal := map[string]any{}
	if text == "ERROR" {
		outcome = "failed"
		session.Info.Outcome = outcome
		session.Messages[len(session.Messages)-1].Outcome = outcome
		terminal["error"] = map[string]any{"type": "rate_limit", "message": "account rate limit", "status": 429}
	}
	if text == "FORGET" {
		delete(f.sessions, id)
	}
	f.save()
	f.publish(id, "session.execution."+outcome, terminal)
	f.mu.Unlock()
}

func (f *fakeOpenCode) globalHTTP(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case "/api/info":
		if hold := os.Getenv(fakeOpenCodeEnvHealthHold); hold != "" && f.healthCalls.Add(1) == 1 {
			_ = os.WriteFile(hold, []byte("held\n"), 0o600)
			<-r.Context().Done()

			return true
		}
		fakeWrite(w, map[string]any{"version": "2.0.21"})

		return true
	case "/openapi.json":
		fakeWrite(w, map[string]any{"components": map[string]any{"schemas": map[string]any{"Session.Message.Info": map[string]any{}, "Form.Info": map[string]any{}}}})

		return true
	case "/api/integration", "/api/credential":
		fakeData(w, []any{})

		return true
	case "/api/agent":
		f.agents(w, r)

		return true
	case "/api/command":
		fakeData(w, []map[string]string{{"name": "inspect", "description": "Inspect workspace"}, {"name": ""}, {"name": "group/nested"}, {"name": "nb\u00a0sp"}, {"name": "zero\u200bwidth"}, {"name": "bell\a"}})

		return true
	case "/api/session/active":
		active := map[string]any{}
		for id := range f.pending {
			active[id] = map[string]string{"type": "running"}
		}
		fakeData(w, active)

		return true
	case "/api/experimental/session/import":
		var body struct {
			fakeExport
			Location opencode.Location `json:"location"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(400)

			return true
		}
		value := body.fakeExport
		value.Info.Location = body.Location
		if value.Info.Location.Directory == "" {
			value.Info.Location.Directory, _ = os.Getwd()
		}
		if f.sessions[value.Info.ID] != nil {
			w.WriteHeader(409)

			return true
		}
		f.sessions[value.Info.ID] = &value
		f.save()
		fakeData(w, value.Info)

		return true
	case "/api/session":
		if r.Method == http.MethodGet {
			rows := []opencode.NativeSession{}
			for _, s := range f.sessions {
				if s.Info.ParentID == r.URL.Query().Get("parentID") {
					rows = append(rows, s.Info)
				}
			}
			sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
			fakeData(w, rows)

			return true
		}
		var session opencode.NativeSession
		if json.NewDecoder(r.Body).Decode(&session) != nil {
			w.WriteHeader(400)

			return true
		}
		session.ID = opencode.NewID("ses_")
		session.Time.Created = time.Now().UnixMilli()
		session.Time.Updated = session.Time.Created
		f.sessions[session.ID] = &fakeExport{Info: session, Messages: []opencode.NativeMessage{}}
		f.save()
		fakeData(w, session)

		return true
	}

	return false
}
func (f *fakeOpenCode) catalogHTTP(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case "/api/config":
		providers := map[string]any{}
		if catalogPath := os.Getenv("ACP_GO_OPENCODEV2_TEST_PROVIDERS"); catalogPath != "" {
			data, err := os.ReadFile(catalogPath)
			if err != nil {
				w.WriteHeader(500)

				return true
			}
			var catalog struct {
				Data []map[string]any `json:"data"`
			}
			if json.Unmarshal(data, &catalog) != nil {
				w.WriteHeader(500)

				return true
			}
			for _, p := range catalog.Data {
				id, _ := p["id"].(string)
				providers[id] = p
			}
		}
		fakeWrite(w, []any{map[string]any{"type": "document", "info": map[string]any{"providers": providers}}})

		return true
	case "/api/model/default":
		fakeData(w, map[string]string{"id": "vision", "providerID": "fake", "package": "@opencode/ai/providers/openai-compatible"})

		return true
	case "/api/model":
		if catalogPath := os.Getenv("ACP_GO_OPENCODEV2_TEST_PROVIDERS"); catalogPath != "" {
			data, err := os.ReadFile(catalogPath)
			if err != nil {
				w.WriteHeader(500)

				return true
			}
			var config struct {
				Models []json.RawMessage `json:"models"`
			}
			if json.Unmarshal(data, &config) != nil {
				w.WriteHeader(500)

				return true
			}
			fakeData(w, config.Models)

			return true
		}
		fakeData(w, []any{
			map[string]any{"id": "vision", "providerID": "fake", "name": "Vision", "enabled": true, "limit": map[string]any{"context": 32000}, "capabilities": map[string]any{"input": []string{"text", "image"}}, "variants": []any{map[string]string{"id": "low"}, map[string]string{"id": "high"}}},
			map[string]any{"id": "text", "providerID": "fake", "name": "Text", "enabled": true, "capabilities": map[string]any{"input": []string{"text"}}},
		})

		return true
	case "/api/provider":
		if catalogPath := os.Getenv("ACP_GO_OPENCODEV2_TEST_PROVIDERS"); catalogPath != "" {
			data, err := os.ReadFile(catalogPath)
			if err != nil {
				w.WriteHeader(500)

				return true
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(data)

			return true
		}
		fakeData(w, []any{})

		return true
	}

	return false
}

func (f *fakeOpenCode) promptHTTP(w http.ResponseWriter, r *http.Request, id string, session *fakeExport) {
	var body struct {
		ID    string                      `json:"id"`
		Text  string                      `json:"text"`
		Name  string                      `json:"name"`
		Files []opencode.NativeAttachment `json:"files"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		w.WriteHeader(400)

		return
	}
	if body.Text == "CRASH" {
		os.Exit(17)
	}
	if body.Text == "REFUSE" {
		w.WriteHeader(400)
		fakeWrite(w, map[string]string{"message": "provider refused prompt"})

		return
	}
	if f.pending[id] != nil {
		w.WriteHeader(409)

		return
	}
	if body.ID == "" {
		body.ID = opencode.NewMessageID()
	}
	for i := range body.Files {
		file := &body.Files[i]
		if header, data, ok := strings.Cut(file.URL, ","); ok && strings.HasPrefix(header, "data:") {
			file.MIME = strings.TrimSuffix(strings.TrimPrefix(header, "data:"), ";base64")
			file.Data = data
			file.URL = ""
		}
	}
	user := opencode.NativeMessage{ID: body.ID, Type: roleUser, Text: body.Text, Files: body.Files}
	session.Messages = append(session.Messages, user)
	f.save()
	pending := make(chan struct{})
	f.pending[id] = pending
	f.publish(id, "session.inbox.enqueued", map[string]any{"inboxID": body.ID})
	fakeData(w, map[string]string{"id": body.ID})
	go f.runPrompt(id, body.Text, body.Name, pending)
}

func (f *fakeOpenCode) persistenceHTTP(w http.ResponseWriter, r *http.Request, pieces []string) {
	id := pieces[3]
	session := f.sessions[id]
	if session == nil {
		w.WriteHeader(404)

		return
	}
	if pieces[4] == "wait" {
		pending := f.pending[id]
		if pending != nil {
			f.mu.Unlock()
			select {
			case <-pending:
			case <-r.Context().Done():
			}
			f.mu.Lock()
		}
		w.WriteHeader(204)

		return
	}
	if pieces[4] == "export" {
		f.advanceHistory(id)
		fakeData(w, session)

		return
	}
	w.WriteHeader(404)
}
