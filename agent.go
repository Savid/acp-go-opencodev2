package opencodeacp

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/coder/acp-go-sdk"

	acpcore "github.com/savid/acp-go-core"
	"github.com/savid/acp-go-core/image"
	"github.com/savid/acp-go-core/lifecycle"
	"github.com/savid/acp-go-core/observer"
	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/usage/anthropic"
	"github.com/savid/acp-go-core/usage/openaicodex"
	"github.com/savid/acp-go-core/usage/opencodego"
	"github.com/savid/acp-go-core/usage/openrouter"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

const (
	// RawEventMethod is the notification carrying one raw opencode event when a
	// session opted in through _meta.opencode.rawEvent.enabled.
	RawEventMethod = "_opencode/rawEvent"
	// AccountUsageMethod reads the addressed native provider's account usage.
	AccountUsageMethod = "_opencode/accountUsage"
	// SessionStoreFormat identifies the store layout this package writes: the
	// native export graph under main plus the adapter's session record
	// under the config subpath.
	SessionStoreFormat = "opencode-session-export-v1"

	vendor = "opencode"

	capabilityMethodKey      = "method"
	capabilityElicitationKey = "elicitation"
)

// client is the host side of the connection, as the sessions use it.
type client interface {
	SessionUpdate(ctx context.Context, params acp.SessionNotification) error
	RequestPermission(ctx context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error)
	UnstableCreateElicitation(ctx context.Context, params acp.UnstableCreateElicitationRequest) (acp.UnstableCreateElicitationResponse, error)
	NotifyExtension(ctx context.Context, method string, params any) error
}

// Agent exposes the opencode coding agent through ACP.
type Agent struct {
	options        Options
	log            *slog.Logger
	observe        *observer.Observer
	optionErr      *acp.RequestError
	usageTransport http.RoundTripper
	// processEnv is the adapter's own environment, read once at construction.
	processEnv []string
	store      acpcore.SessionStore

	mu                 sync.Mutex
	conn               client
	transport          *wire.Transport
	closed             bool
	clientCapabilities acp.ClientCapabilities
	// lifecycle is the answer this connection gave at initialize. An absent
	// answer leaves the extension dormant for every session on it.
	lifecycle lifecycle.Negotiated
	restores  wire.SessionRequests
	sessions  map[acp.SessionId]*session
	deleted   map[acp.SessionId]bool
	// ephemeral holds the ids the host opened as ephemeral, kept past close so
	// their delete never touches the store.
	ephemeral   map[acp.SessionId]bool
	clientCalls chan struct{}

	runtimeMu sync.Mutex
	runtime   *runtime
}

var (
	_ acp.Agent                  = (*Agent)(nil)
	_ acp.AgentLoader            = (*Agent)(nil)
	_ acp.ExtensionMethodHandler = (*Agent)(nil)
)

// NewAgent creates an ACP agent for the opencode coding agent CLI. Construction
// never fails; a refused option is reported by Initialize and every
// session-establishing method as opencode_invalid_options.
func NewAgent(opts ...Option) *Agent {
	options := applyOptions(opts)

	log := options.Logger
	if log == nil {
		log = slog.Default()
	}

	store := options.SessionStore
	if store == nil {
		store = acpcore.NewInMemorySessionStore()
	}

	agent := &Agent{
		options: options,
		log:     log,
		observe: observer.New(observer.Config{
			Vendor: vendor, NativeClient: "opencode-coding-agent",
			MeterProvider:  options.MeterProvider,
			Propagator:     options.TextMapPropagator,
			TracerProvider: options.TracerProvider,
			Version:        options.AgentVersion,
		}),
		processEnv:  os.Environ(),
		store:       store,
		sessions:    make(map[acp.SessionId]*session),
		deleted:     make(map[acp.SessionId]bool),
		ephemeral:   make(map[acp.SessionId]bool),
		clientCalls: make(chan struct{}, max(0, options.ConcurrencyLimits.MaxConcurrentClientCalls)),
	}
	agent.optionErr = agent.validateOptions()

	return agent
}

// validateOptions reports the first refused option. The reason goes to the
// log; the wire answer names only the option.
func (a *Agent) validateOptions() *acp.RequestError {
	options := a.options

	checks := []struct {
		field string
		err   error
	}{
		{"home", process.ValidateOptionalAbsolutePath(options.Home)},
		{"scratchDir", process.ValidateOptionalAbsolutePath(options.ScratchDir)},
		{"inputHandoffRoot", image.ValidateHandoffRoot(options.InputHandoffRoot)},
		{"defaultModel", validateOptionalModel(options.DefaultModel)},
		{"configuredModels", validateConfiguredModels(options.ConfiguredModels)},
		{metaEnvKey, process.ValidateNames(options.Env)},
		{"concurrencyLimits", wire.ValidateConcurrencyLimits(options.ConcurrencyLimits.MaxActiveSessions, options.ConcurrencyLimits.MaxConcurrentClientCalls)},
		{"imageLimits", options.ImageLimits.core().Validate()},
	}

	for _, check := range checks {
		if check.err == nil {
			continue
		}

		a.log.Error("opencode agent option rejected", slog.String("field", check.field), slog.String("reason", check.err.Error()))

		return wire.InvalidOptions(vendor, check.field)
	}

	return nil
}

func validateOptionalModel(model string) error {
	if model == "" {
		return nil
	}

	return opencode.ModelSelectionShapeError(model)
}

func validateConfiguredModels(ids []string) error {
	seen := make(map[string]struct{}, len(ids))

	for index, id := range ids {
		if err := opencode.ModelSelectionShapeError(id); err != nil || id != strings.TrimSpace(id) {
			return fmt.Errorf("configured model %d %q is not a model id", index, id)
		}

		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("configured model %q is listed twice", id)
		}

		seen[id] = struct{}{}
	}

	return nil
}

// Serve runs an ACP agent over the provided streams. It blocks until the
// context is cancelled or the peer closes the connection, then closes the
// agent.
func Serve(ctx context.Context, input io.Reader, output io.Writer, opts ...Option) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	agent := NewAgent(opts...)
	defer func() {
		if closeErr := agent.Close(); closeErr != nil {
			returnErr = closeErr
		}
	}()

	transport := wire.NewTransport(input, output)
	defer transport.Close()

	conn := acp.NewAgentSideConnection(agent, transport.Writer(), transport.Reader())
	conn.SetLogger(agent.log)
	agent.attach(conn, transport)
	transport.Start()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		return nil
	}
}

// attach binds the host connection the sessions emit through.
func (a *Agent) attach(conn client, transport *wire.Transport) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.conn = conn
	a.transport = transport
}

func (a *Agent) connection() client {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.conn
}

// Close runs the shutdown ladder for every session and refuses every later
// request.
func (a *Agent) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return nil
	}

	a.closed = true
	sessions := slices.Collect(maps.Values(a.sessions))
	a.mu.Unlock()

	var errs []error

	// The ladder's terminal events still need the connection, so it is cleared
	// only once every session has run its own shutdown.
	for _, s := range sessions {
		if err := s.close(context.Background()); err != nil {
			errs = append(errs, err)
		}

		a.detach(context.Background(), s)
	}

	a.stopRuntime()

	a.mu.Lock()
	a.conn = nil
	a.mu.Unlock()

	return errors.Join(errs...)
}

func (a *Agent) ensureOpen() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return wire.AgentClosed()
	}

	return nil
}

// Initialize implements ACP initialize.
func (a *Agent) Initialize(ctx context.Context, params acp.InitializeRequest) (resp acp.InitializeResponse, err error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.InitializeResponse{}, openErr
	}

	_, finish := a.observe.StartACP(ctx, params.Meta, acp.AgentMethodInitialize)
	defer func() { finish(err) }()

	if a.optionErr != nil {
		return acp.InitializeResponse{}, a.optionErr
	}

	meta := params.Meta
	if t := a.transportRef(); t != nil {
		meta = lifecycle.RetainRequestMetadata(meta, t.TakeRaw(acp.AgentMethodInitialize))
	}

	present, paramErr := lifecycle.DecodeOffer(meta)
	if paramErr != nil {
		return acp.InitializeResponse{}, wire.ParamRefusal(paramErr)
	}

	var negotiated lifecycle.Negotiated
	if present {
		negotiated = lifecycle.Answer(lifecycle.Negotiated{UpdatesOutsidePrompt: true, ActivityKinds: []lifecycle.ActivityKind{}})
	}

	encoding := wire.SelectPositionEncoding(params.ClientCapabilities.PositionEncodings)

	a.mu.Lock()
	a.clientCapabilities = params.ClientCapabilities
	a.lifecycle = negotiated
	a.mu.Unlock()

	title := a.options.AgentTitle

	capabilityMeta := map[string]any{
		vendor: map[string]any{
			wire.AccountUsageCapabilityKey: wire.AccountUsageAdvertisement(AccountUsageMethod, wire.AccountUsageScopeSession, opencodego.ProviderID, openrouter.ProviderID, anthropic.ProviderID, openaicodex.ProviderID),
			capabilityElicitationKey:       map[string]any{"unstable": true, "scope": "session", "tracks": "ACP v1 elicitation"},
			metaRawEventKey: map[string]any{
				capabilityMethodKey: RawEventMethod, "enabledBy": "_meta.opencode.rawEvent.enabled",
				"maxBytes": wire.RawEventMaxBytes, "defaultEnabled": false,
			},
			"sessionStore": map[string]any{"format": SessionStoreFormat, "key": []string{"sessionId", "subpath"}},
		},
		wire.MediaEnvelopeKey: image.MediaEnvelope(a.options.ImageLimits.core(), image.Envelope{DocumentFormats: []string{}}),
	}
	if a.options.InputHandoffRoot != "" {
		capabilityMeta[wire.HandoffKey] = image.HandoffAdvertisement()
	}

	var responseMeta map[string]any
	if negotiated.Present() {
		responseMeta = map[string]any{wire.LifecycleKey: negotiated.Advertisement()}
	}

	return acp.InitializeResponse{
		Meta:            responseMeta,
		ProtocolVersion: acp.ProtocolVersionNumber,
		AgentInfo: &acp.Implementation{
			Name:    a.options.AgentName,
			Title:   &title,
			Version: a.options.AgentVersion,
		},
		AuthMethods: []acp.AuthMethod{},
		AgentCapabilities: acp.AgentCapabilities{
			Meta:             capabilityMeta,
			LoadSession:      true,
			PositionEncoding: &encoding,
			PromptCapabilities: acp.PromptCapabilities{
				EmbeddedContext: true,
				Image:           true,
			},
			SessionCapabilities: acp.SessionCapabilities{
				Close:                 &acp.SessionCloseCapabilities{},
				Delete:                &acp.SessionDeleteCapabilities{},
				List:                  &acp.SessionListCapabilities{},
				Resume:                &acp.SessionResumeCapabilities{},
				AdditionalDirectories: &acp.SessionAdditionalDirectoriesCapabilities{},
			},
		},
	}, nil
}

// Authenticate exists because the SDK interface requires it. The harness
// authenticates itself in its own home, outside ACP.
func (a *Agent) Authenticate(_ context.Context, params acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.AuthenticateResponse{}, openErr
	}

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.AuthenticateResponse{}, wire.ParamRefusal(refusal)
	}

	return acp.AuthenticateResponse{}, acp.NewInvalidParams(map[string]any{"methodId": params.MethodId})
}

// Logout exists because the SDK interface requires it.
func (a *Agent) Logout(_ context.Context, params acp.LogoutRequest) (acp.LogoutResponse, error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.LogoutResponse{}, openErr
	}

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.LogoutResponse{}, wire.ParamRefusal(refusal)
	}

	return acp.LogoutResponse{}, acp.NewMethodNotFound(acp.AgentMethodLogout)
}

// SetSessionMode exists because the SDK interface requires it. Native modes
// are config options, never ACP session modes.
func (a *Agent) SetSessionMode(_ context.Context, params acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return acp.SetSessionModeResponse{}, openErr
	}

	if refusal := lifecycle.RejectKey(params.Meta); refusal != nil {
		return acp.SetSessionModeResponse{}, wire.ParamRefusal(refusal)
	}

	return acp.SetSessionModeResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionSetMode)
}

// HandleExtensionMethod serves the account-usage read; every other extension
// method is method-not-found.
func (a *Agent) HandleExtensionMethod(ctx context.Context, method string, params json.RawMessage) (any, error) {
	if openErr := a.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	if method == AccountUsageMethod {
		response, err := a.accountUsage(ctx, params)
		if err != nil {
			return nil, err
		}

		return response, nil
	}

	var envelope struct {
		Meta map[string]any `json:"_meta"` //nolint:tagliatelle // ACP reserves this wire spelling.
	}

	if err := json.Unmarshal(params, &envelope); err == nil {
		if refusal := lifecycle.RejectKey(envelope.Meta); refusal != nil {
			return nil, wire.ParamRefusal(refusal)
		}
	}

	return nil, acp.NewMethodNotFound(method)
}

func (a *Agent) transportRef() *wire.Transport {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.transport
}

func (a *Agent) lifecycleNegotiated() lifecycle.Negotiated {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.lifecycle
}

func (a *Agent) clientSupportsFormElicitation() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.clientCapabilities.Elicitation != nil && a.clientCapabilities.Elicitation.Form != nil
}

// acquireClientCall takes one slot of the server-to-client call budget
// without waiting.
func (a *Agent) acquireClientCall() (func(), error) {
	select {
	case a.clientCalls <- struct{}{}:
		return func() { <-a.clientCalls }, nil
	default:
		return nil, wire.Backpressure("client_calls")
	}
}

// ensureExecutable resolves the opencode executable against the base
// environment, so a session directory can never shadow it.
func (a *Agent) ensureExecutable(ctx context.Context) (string, error) {
	base, err := a.environment(nil, nil).Base()
	if err == nil {
		var executable string
		if executable, err = process.ResolveExecutable(cmp.Or(a.options.ExecutablePath, vendor), base); err == nil {
			return executable, nil
		}
	}

	a.log.ErrorContext(ctx, "opencode executable resolution failed", slog.String("reason", err.Error()))

	return "", wire.InternalFailure(vendor, internalClassNativeStart)
}

// environment builds the merge for one launch: the inherited process
// environment, the agent overlay, the session env, then the keys this
// adapter owns because of how it launches opencode.
func (a *Agent) environment(sessionEnv map[string]string, owned map[string]string) process.Environment {
	merged := opencode.HomeEnvironment(a.options.Home)

	maps.Copy(merged, owned)

	return process.Environment{
		Process:        a.processEnv,
		Agent:          a.options.Env,
		Session:        sessionEnv,
		Owned:          merged,
		InternalPrefix: opencode.InternalEnvPrefix,
	}
}

// internalClassNativeStart is the opencode_internal_failure class of a native
// opencode process that could not be started or configured for a session.
const internalClassNativeStart = "native_start"
