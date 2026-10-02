package opencodeacp

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	acp "github.com/coder/acp-go-sdk"
	"github.com/savid/acp-go-core/wire"
	"github.com/savid/acp-go-opencodev2/internal/opencode"
)

const configModel acp.SessionConfigId = "model"
const configMode acp.SessionConfigId = "mode"
const configEffort acp.SessionConfigId = "effort"

func (s *session) resolveSelectionDefaults(ctx context.Context, rt *binding) error {
	s.mu.Lock()
	model := s.model
	s.mu.Unlock()

	var selected struct {
		Data opencode.Model `json:"data"`
	}
	if model == "" {
		if err := rt.client.Do(ctx, s.cwd, http.MethodGet, "/api/model/default", nil, &selected); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.model == "" {
		if selected.Data.Package == "" {
			for i := range s.models {
				if s.models[i].Package != "" {
					selected.Data = s.models[i]

					break
				}
			}
		}

		if selected.Data.ID != "" && selected.Data.ProviderID != "" && selected.Data.Package != "" {
			s.model = selected.Data.ProviderID + "/" + selected.Data.ID
		}
	}

	if s.model == "" && s.effort != "" {
		return errors.New("no native model available for selected effort")
	}

	if s.mode == "" {
		for _, candidate := range s.agents {
			if candidate.Mode != "subagent" && !candidate.Hidden {
				s.mode = candidate.ID

				break
			}
		}
	}

	return nil
}

// refreshCatalogs fetches the model, agent, and command catalogs this session
// advertises.
func (s *session) refreshCatalogs(ctx context.Context, rt *binding) error {
	var models struct {
		Data []opencode.Model `json:"data"`
	}
	if err := rt.client.Do(ctx, s.cwd, http.MethodGet, "/api/model", nil, &models); err != nil {
		return err
	}

	var agents struct {
		Data []opencode.NativeAgent `json:"data"`
	}
	if err := rt.client.Do(ctx, s.cwd, http.MethodGet, "/api/agent", nil, &agents); err != nil {
		return err
	}

	var commands struct {
		Data []opencode.NativeCommand `json:"data"`
	}
	if err := rt.client.Do(ctx, s.cwd, http.MethodGet, "/api/command", nil, &commands); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.models = models.Data
	s.agents = agents.Data
	s.commands = commands.Data

	return nil
}

func (s *session) modelImageCapability() *bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.models {
		m := &s.models[i]
		if m.ProviderID+"/"+m.ID == s.model && len(m.Capabilities.Input) > 0 {
			return new(slices.Contains(m.Capabilities.Input, "image"))
		}
	}

	return nil
}

func (s *session) configOptions() []acp.SessionConfigOption {
	s.mu.Lock()
	defer s.mu.Unlock()

	rows := make([]wire.ModelRow, 0, len(s.models))

	var efforts []string

	for i := range s.models {
		m := &s.models[i]
		id := m.ProviderID + "/" + m.ID

		variants := make([]string, 0, len(m.Variants))
		for _, v := range m.Variants {
			variants = append(variants, v.ID)
		}

		meta := map[string]any{}
		if len(variants) > 0 {
			meta["supportedEffortLevels"] = variants
		}

		if window, ok := m.Limit["context"]; ok {
			meta["contextWindow"] = window
		}

		if limit, ok := m.Limit["output"]; ok {
			meta["maxOutputTokens"] = limit
		}

		rows = append(rows, wire.ModelRow{ID: id, Name: m.ProviderID + " / " + m.Name, Meta: meta})
		if id == s.model {
			efforts = variants
		}
	}

	models := wire.ModelSelectOptions(vendor, s.model, rows, s.agent.options.ConfiguredModels)

	options := []acp.SessionConfigOption{}

	selectOption := func(id acp.SessionConfigId, name, current string, category acp.SessionConfigOptionCategory, values acp.SessionConfigSelectOptionsUngrouped) {
		options = append(options, acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{Id: id, Name: name, Type: "select", Category: &category, CurrentValue: acp.SessionConfigValueId(current), Options: acp.SessionConfigSelectOptions{Ungrouped: &values}}})
	}
	if len(models) > 0 {
		selectOption(configModel, "Model", s.model, acp.SessionConfigOptionCategoryModel, models)
	}

	modes := acp.SessionConfigSelectOptionsUngrouped{}
	found := false

	for _, a := range s.agents {
		if a.Mode == "subagent" || a.Hidden {
			continue
		}

		modes = append(modes, acp.SessionConfigSelectOption{Value: acp.SessionConfigValueId(a.ID), Name: a.Name})
		found = found || a.ID == s.mode
	}

	if !found && s.mode != "" {
		modes = append(modes, acp.SessionConfigSelectOption{Value: acp.SessionConfigValueId(s.mode), Name: s.mode})
	}

	if len(modes) > 0 {
		selectOption(configMode, "Agent", s.mode, acp.SessionConfigOptionCategoryMode, modes)
	}

	if s.effort != "" && !slices.Contains(efforts, s.effort) {
		efforts = append(efforts, s.effort)
	}

	if s.effort != "" && len(efforts) > 0 {
		values := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(efforts))
		for _, v := range efforts {
			values = append(values, acp.SessionConfigSelectOption{Value: acp.SessionConfigValueId(v), Name: v})
		}

		selectOption(configEffort, "Effort", s.effort, acp.SessionConfigOptionCategoryThoughtLevel, values)
	}

	return options
}

func (s *session) setConfigOption(ctx context.Context, id acp.SessionConfigId, value string) ([]acp.SessionConfigOption, error) {
	if id != configModel && id != configMode && id != configEffort {
		return nil, wire.Unsupported("configId")
	}

	if value == "" || (id == configModel && validateOptionalModel(value) != nil) {
		return nil, wire.Unsupported(fieldValue)
	}

	if err := s.admissionError(); err != nil {
		return nil, err
	}

	release, err := s.acquireGate(limitSessionPrompt)
	if err != nil {
		return nil, err
	}
	defer release()

	s.mu.Lock()
	busy := s.cycle != nil
	s.mu.Unlock()

	if busy {
		return nil, wire.Backpressure(limitSessionPrompt)
	}

	rt, err := s.ensureRuntime(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()

	oldModel, oldMode, oldEffort := s.model, s.mode, s.effort
	if id == configEffort && oldModel == "" {
		s.mu.Unlock()

		return nil, wire.Unsupported(fieldValue)
	}

	switch id {
	case configModel:
		s.model = value
	case configMode:
		s.mode = value
	case configEffort:
		s.effort = value
	}
	s.mu.Unlock()

	if err := s.applyConfigOption(ctx, rt, id); err != nil {
		s.mu.Lock()
		s.model, s.mode, s.effort = oldModel, oldMode, oldEffort
		s.mu.Unlock()
		s.stopRuntime(context.WithoutCancel(ctx), rt)
		s.fenceStream()

		return nil, wire.InternalFailure(vendor, "")
	}

	if err := s.commitMirror(ctx, rt, nil); err != nil {
		s.mu.Lock()
		s.model, s.mode, s.effort = oldModel, oldMode, oldEffort
		s.mu.Unlock()
		s.stopRuntime(context.WithoutCancel(ctx), rt)
		s.fenceStream()

		return nil, wire.InternalFailure(vendor, "")
	}

	options := s.configOptions()
	_ = s.emit(ctx, acp.SessionUpdate{ConfigOptionUpdate: &acp.SessionConfigOptionUpdate{ConfigOptions: options}})

	return options, nil
}

// emitCommands publishes a snapshot of the current catalog.
func (s *session) emitCommands(ctx context.Context) error {
	s.mu.Lock()
	native := append([]opencode.NativeCommand(nil), s.commands...)
	s.mu.Unlock()

	commands := []acp.AvailableCommand{}

	seen := map[string]bool{}
	for _, c := range native {
		if !wire.ValidCommandName(c.Name) || seen[c.Name] {
			continue
		}

		seen[c.Name] = true

		command := acp.AvailableCommand{Name: c.Name, Description: c.Description}

		commands = append(commands, command)
	}

	return s.emit(ctx, acp.SessionUpdate{AvailableCommandsUpdate: &acp.SessionAvailableCommandsUpdate{AvailableCommands: commands}})
}

func (s *session) applySelection(ctx context.Context, rt *binding) error {
	if err := s.applyConfigOption(ctx, rt, configModel); err != nil {
		return err
	}

	return s.applyConfigOption(ctx, rt, configMode)
}

func (s *session) applyConfigOption(ctx context.Context, rt *binding, configID acp.SessionConfigId) error {
	s.mu.Lock()
	model, mode, effort := s.model, s.mode, s.effort
	s.mu.Unlock()

	if configID != configMode && model != "" {
		provider, id, _ := strings.Cut(model, "/")
		if err := rt.client.Do(ctx, "", http.MethodPost, opencode.SessionPath(s.nativeID)+"/model", map[string]any{"model": opencode.ModelRef{ID: id, ProviderID: provider, Variant: effort}}, nil); err != nil {
			return err
		}
	}

	if configID == configMode && mode != "" {
		return rt.client.Do(ctx, "", http.MethodPost, opencode.SessionPath(s.nativeID)+"/agent", map[string]any{"agent": mode}, nil)
	}

	return nil
}
