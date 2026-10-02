package opencodeacp

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/savid/acp-go-core/process"
	"github.com/savid/acp-go-core/usage"
	"github.com/savid/acp-go-core/usage/anthropic"
	"github.com/savid/acp-go-core/usage/gateway"
	"github.com/savid/acp-go-core/usage/openaicodex"
	"github.com/savid/acp-go-core/usage/opencodego"
	"github.com/savid/acp-go-core/usage/openrouter"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

func (a *Agent) accountUsage(ctx context.Context, params json.RawMessage) (response wire.AccountUsageResponse, err error) {
	request, refusal := wire.DecodeAccountUsageRequest(params, wire.AccountUsageScopeSession)

	ctx, finish := a.observe.StartACP(ctx, request.Meta, AccountUsageMethod)
	defer func() { finish(err) }()

	if refusal != nil {
		return wire.AccountUsageResponse{}, refusal
	}

	if request.ProviderID == "" {
		return wire.AccountUsageResponse{}, wire.Missing("providerId")
	}

	// A provider without a native reader is read only through the gateways
	// the catalog routes to.
	var reader usage.Reader

	switch request.ProviderID {
	case opencodego.ProviderID:
		reader = opencodego.Reader{Transport: a.usageTransport}
	case openrouter.ProviderID:
		reader = openrouter.Reader{Transport: a.usageTransport}
	case anthropic.ProviderID, openaicodex.ProviderID:
	default:
		return wire.AccountUsageResponse{}, wire.Unsupported("providerId")
	}

	s, err := a.session(ctx, request.SessionID)
	if err != nil {
		return wire.AccountUsageResponse{}, err
	}

	if admission := s.admissionError(); admission != nil {
		return wire.AccountUsageResponse{}, admission
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return wire.AccountUsageResponse{}, err
	}
	defer release()

	readCtx, cancel := context.WithTimeout(ctx, wire.AccountUsageReadTimeout)
	defer cancel()

	rt, err := s.ensureRuntime(readCtx)
	if err != nil {
		return wire.AccountUsageResponse{}, err
	}

	response, err = s.readProviderUsage(readCtx, rt, request.ProviderID, reader)

	if ctx.Err() != nil {
		return wire.AccountUsageResponse{}, ctx.Err()
	}

	if admission := s.admissionError(); admission != nil {
		return wire.AccountUsageResponse{}, admission
	}

	if err != nil || !rt.alive() {
		if err != nil {
			a.log.ErrorContext(ctx, "opencode account usage read failed", slog.String("reason", err.Error()))
		}

		return wire.AccountUsageResponse{}, usage.RequestError(vendor, err)
	}

	return response, nil
}

func (s *session) readProviderUsage(ctx context.Context, rt *binding, providerID string, reader usage.Reader) (wire.AccountUsageResponse, error) {
	s.mu.Lock()
	provider, model, _ := strings.Cut(s.model, "/")
	selected := opencode.ModelRef{ProviderID: provider, ID: model, Variant: s.effort}
	s.mu.Unlock()

	response := wire.AccountUsageUnavailable(wire.AccountUsageNotAuthenticated)

	if reader != nil {
		var err error

		response, err = usage.ReadVerified(ctx, func(ctx context.Context) (usage.Access, error) {
			return rt.client.UsageAccess(ctx, s.cwd, providerID, selected, func(key string) (string, bool) { return process.Lookup(rt.server.environment, key) })
		}, reader)
		if err != nil || response.Available {
			return response, err
		}
	}

	// A provider opencode holds no native account for may be brokered by a
	// gateway the catalog routes to.
	return gateway.ReadRoutes(ctx, s.agent.usageTransport, func(ctx context.Context) ([]gateway.Route, error) {
		return rt.client.UsageGateways(ctx, s.cwd, selected, func(key string) (string, bool) { return process.Lookup(rt.server.environment, key) })
	}, providerID, response)
}
