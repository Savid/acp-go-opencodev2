package opencode

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"maps"
	"net/http"
	"strings"

	"github.com/savid/acp-go-core/usage"
	"github.com/savid/acp-go-core/usage/gateway"
	"github.com/savid/acp-go-core/usage/opencodego"
	"github.com/savid/acp-go-core/usage/openrouter"
	"github.com/savid/acp-go-core/wire"
)

//nolint:tagliatelle // Native catalog fields use uppercase ID suffixes.
type usageProvider struct {
	ID            string                     `json:"id"`
	ProviderID    string                     `json:"providerID"`
	IntegrationID string                     `json:"integrationID"`
	Package       string                     `json:"package"`
	Settings      map[string]json.RawMessage `json:"settings"`
	Headers       map[string]string          `json:"headers"`
	Variants      []usageVariant             `json:"variants"`
}
type usageVariant struct {
	ID       string                     `json:"id"`
	Settings map[string]json.RawMessage `json:"settings"`
	Headers  map[string]string          `json:"headers"`
}

type usageCredential struct {
	Type          string                     `json:"type"`
	Key           string                     `json:"key"`
	Metadata      map[string]json.RawMessage `json:"metadata"`
	Configuration map[string]json.RawMessage `json:"configuration"`
}

type usageConnection struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Name   string `json:"name"`
	Method string `json:"method"`
}

func (c *Client) usageCatalog(ctx context.Context, directory string) ([]usageProvider, []usageProvider, error) {
	var providers, models struct {
		Data []usageProvider `json:"data"`
	}
	if err := c.Do(ctx, directory, http.MethodGet, "/api/provider", nil, &providers); err != nil {
		return nil, nil, err
	}

	if err := c.Do(ctx, directory, http.MethodGet, "/api/model", nil, &models); err != nil {
		return nil, nil, err
	}

	return providers.Data, models.Data, nil
}

type usageCredentials struct {
	client      *Client
	directory   string
	lookup      func(string) (string, bool)
	connections map[string]usageConnection
	keys        map[string]usageCredential
}

// key follows the native integration's first, active connection. Catalogs are
// read once per resolution, then read afresh when the account binding is checked.
func (c *usageCredentials) key(ctx context.Context, p usageProvider) (string, string, error) {
	if c.connections == nil {
		var integrations struct {
			Data []struct {
				ID          string            `json:"id"`
				Connections []usageConnection `json:"connections"`
			} `json:"data"`
		}
		if err := c.client.Do(ctx, c.directory, http.MethodGet, "/api/integration", nil, &integrations); err != nil {
			return "", "", err
		}

		c.connections = map[string]usageConnection{}

		for _, integration := range integrations.Data {
			if len(integration.Connections) > 0 {
				c.connections[integration.ID] = integration.Connections[0]
			}
		}
	}

	integrationID := p.IntegrationID
	if integrationID == "" {
		integrationID = p.ID
	}

	connection, ok := c.connections[integrationID]
	if !ok {
		var key string
		if raw, present := p.Settings["apiKey"]; present {
			if json.Unmarshal(raw, &key) != nil || string(raw) == "null" {
				return "", wire.AccountUsageNotReported, nil //nolint:nilerr // Unrecognized credentials cannot identify an account.
			}
		}

		return key, "", nil
	}

	if connection.Type == "env" {
		key, _ := c.lookup(connection.Name)

		return key, "", nil
	}

	if connection.Type != "credential" || connection.Method != "key" {
		return "", wire.AccountUsageNotReported, nil
	}

	if c.keys == nil {
		var credentials struct {
			Data []struct {
				ID    string          `json:"id"`
				Value usageCredential `json:"value"`
			} `json:"data"`
		}
		if err := c.client.Do(ctx, "", http.MethodGet, "/api/credential", nil, &credentials); err != nil {
			return "", "", err
		}

		c.keys = map[string]usageCredential{}

		for _, credential := range credentials.Data {
			if credential.Value.Type == "key" {
				c.keys[credential.ID] = credential.Value
			}
		}
	}

	if credential, ok := c.keys[connection.ID]; ok {
		if len(credential.Metadata) > 0 || len(credential.Configuration) > 0 {
			return "", wire.AccountUsageNotReported, nil
		}

		return credential.Key, "", nil
	}

	return "", wire.AccountUsageNotAuthenticated, nil
}

func usageSettings(p, m usageProvider, variant string) (usageProvider, bool) {
	if variant != "" && variant != "default" {
		found := false

		for _, v := range m.Variants {
			if v.ID == variant {
				m.Settings = mergeUsageSettings(m.Settings, v.Settings)
				m.Headers = mergeUsageHeaders(m.Headers, v.Headers)
				found = true

				break
			}
		}

		if !found {
			return usageProvider{}, false
		}
	}

	merged := p
	merged.Settings = mergeUsageSettings(p.Settings, m.Settings)

	merged.Headers = mergeUsageHeaders(p.Headers, m.Headers)
	if m.Package != "" {
		merged.Package = m.Package
	}

	return merged, true
}

func mergeUsageSettings(base, overlay map[string]json.RawMessage) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(base)+len(overlay))
	maps.Copy(result, base)

	for key, value := range overlay {
		var left, right map[string]json.RawMessage
		if json.Unmarshal(result[key], &left) == nil && left != nil && json.Unmarshal(value, &right) == nil && right != nil {
			result[key], _ = json.Marshal(mergeUsageSettings(left, right))
		} else {
			result[key] = value
		}
	}

	return result
}

func mergeUsageHeaders(base, overlay map[string]string) map[string]string {
	result := make(map[string]string, len(base)+len(overlay))
	for key, value := range base {
		result[strings.ToLower(key)] = value
	}

	for key, value := range overlay {
		result[strings.ToLower(key)] = value
	}

	return result
}

func usageBase(p usageProvider) string {
	var base string

	_ = json.Unmarshal(p.Settings["baseURL"], &base)

	return strings.TrimSuffix(base, "/")
}
func usageHeaders(headers map[string]string) bool {
	for key := range headers {
		switch strings.ToLower(key) {
		case "http-referer", "x-title", "x-source":
		default:
			return false
		}
	}

	return true
}
func usageAuthentication(p usageProvider) bool {
	if !usageHeaders(p.Headers) {
		return false
	}

	for _, key := range []string{"fetch", "auth", "token", "accessToken"} {
		if _, present := p.Settings[key]; present {
			return false
		}
	}

	if raw, ok := p.Settings["headers"]; ok {
		var headers map[string]string
		if json.Unmarshal(raw, &headers) != nil || !usageHeaders(headers) {
			return false
		}
	}

	return true
}
func usageRoute(providerID, endpoint, pkg string) bool {
	switch providerID {
	case opencodego.ProviderID:
		return endpoint == strings.TrimSuffix(opencodego.Endpoint, "/usage") && (pkg == "@opencode/ai/providers/openai-compatible" || pkg == "@opencode/ai/providers/anthropic" || pkg == "@opencode/ai/providers/openai")
	case openrouter.ProviderID:
		return endpoint == strings.TrimSuffix(openrouter.Endpoint, "/key") && (pkg == "@opencode/ai/providers/openrouter" || pkg == "@opencode/ai/providers/openai-compatible")
	default:
		return false
	}
}

// UsageAccess verifies the effective model route and the native active credential.
func (c *Client) UsageAccess(ctx context.Context, directory, providerID string, selected ModelRef, lookup func(string) (string, bool)) (usage.Access, error) {
	providers, models, err := c.usageCatalog(ctx, directory)
	if err != nil {
		return usage.Access{}, err
	}

	credentials := usageCredentials{client: c, directory: directory, lookup: lookup}

	for _, p := range providers {
		if p.ID != providerID {
			continue
		}

		var key string

		found := false
		fingerprint := []byte{}

		for _, m := range models {
			if m.ProviderID != providerID || selected.ProviderID == providerID && selected.ID != "" && m.ID != selected.ID {
				continue
			}

			variant := ""
			if m.ProviderID == selected.ProviderID && m.ID == selected.ID {
				variant = selected.Variant
			}

			merged, valid := usageSettings(p, m, variant)
			if !valid || !usageAuthentication(merged) || !usageRoute(providerID, usageBase(merged), merged.Package) {
				return usage.Access{Reason: wire.AccountUsageNotReported}, nil
			}

			current, reason, err := credentials.key(ctx, merged)
			if err != nil {
				return usage.Access{}, err
			}

			if reason != "" {
				return usage.Access{Reason: reason}, nil
			}

			if found && current != key {
				return usage.Access{Reason: wire.AccountUsageNotReported}, nil
			}

			key = current
			found = true
			row, _ := json.Marshal(merged)
			fingerprint = append(fingerprint, row...)
		}

		if !found {
			return usage.Access{Reason: wire.AccountUsageNotReported}, nil
		}

		if strings.TrimSpace(key) == "" {
			return usage.Access{Reason: wire.AccountUsageNotAuthenticated}, nil
		}

		return usage.Access{APIKey: key, Fingerprint: sha256.Sum256(append(fingerprint, []byte(key)...))}, nil
	}

	return usage.Access{Reason: wire.AccountUsageNotAuthenticated}, nil
}
func (c *Client) UsageGateways(ctx context.Context, directory string, selected ModelRef, lookup func(string) (string, bool)) ([]gateway.Route, error) {
	providers, models, err := c.usageCatalog(ctx, directory)
	if err != nil {
		return nil, err
	}

	configured, err := c.configuredUsageRoutes(ctx, directory, selected)
	if err != nil {
		return nil, err
	}

	routes := []gateway.Route{}
	seen := map[string]bool{}
	credentials := usageCredentials{client: c, directory: directory, lookup: lookup}

	for _, p := range providers {
		for _, m := range models {
			if m.ProviderID != p.ID || p.ID == selected.ProviderID && selected.ID != "" && m.ID != selected.ID {
				continue
			}

			if !configured[p.ID][""] && !configured[p.ID][m.ID] {
				continue
			}

			variant := ""
			if m.ProviderID == selected.ProviderID && m.ID == selected.ID {
				variant = selected.Variant
			}

			merged, valid := usageSettings(p, m, variant)
			if !valid {
				continue
			}

			base := usageBase(merged)
			if base == "" || !usageAuthentication(merged) {
				continue
			}

			key, reason, err := credentials.key(ctx, merged)
			if err != nil {
				return nil, err
			}

			if reason != "" || key == "" {
				continue
			}
			// The anonymous free-model route has no account credential.
			if p.ID == "opencode" && key == "public" {
				continue
			}

			id := p.ID + "\x00" + base + "\x00" + key
			if seen[id] {
				continue
			}

			seen[id] = true

			routes = append(routes, gateway.Route{Provider: p.ID, BaseURL: base, Token: key})
		}
	}

	return routes, nil
}

// configuredUsageRoutes limits gateway discovery to explicit native endpoint
// configuration; built-in public and local model routes are not account gateways.
func (c *Client) configuredUsageRoutes(ctx context.Context, directory string, selected ModelRef) (map[string]map[string]bool, error) {
	var entries []struct {
		Info struct {
			Providers map[string]struct {
				Settings map[string]json.RawMessage `json:"settings"`
				Models   map[string]struct {
					Settings map[string]json.RawMessage `json:"settings"`
					Variants []usageVariant             `json:"variants"`
				} `json:"models"`
			} `json:"providers"`
		} `json:"info"`
	}
	if err := c.Do(ctx, directory, http.MethodGet, "/api/config", nil, &entries); err != nil {
		return nil, err
	}

	result := map[string]map[string]bool{}

	for _, entry := range entries {
		for id, p := range entry.Info.Providers {
			if result[id] == nil {
				result[id] = map[string]bool{}
			}

			if raw, ok := p.Settings["baseURL"]; ok {
				var base string

				_ = json.Unmarshal(raw, &base)
				result[id][""] = base != ""
			}

			for mid, m := range p.Models {
				if id == selected.ProviderID && mid == selected.ID && selected.Variant != "" && selected.Variant != "default" {
					for _, variant := range m.Variants {
						if variant.ID == selected.Variant {
							m.Settings = mergeUsageSettings(m.Settings, variant.Settings)
						}
					}
				}

				if raw, ok := m.Settings["baseURL"]; ok {
					var base string

					_ = json.Unmarshal(raw, &base)
					result[id][mid] = base != ""
				}
			}
		}
	}

	return result, nil
}
