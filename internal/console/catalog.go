package console

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
)

func (facade *Facade) GetCatalog(ctx context.Context) (Envelope, error) {
	data, _, status, err := facade.backend.request(ctx, "dispatcher", http.MethodGet, "/api/catalog", "", nil)
	if err != nil {
		return Envelope{}, err
	}
	if status == http.StatusNotFound || status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented {
		return Envelope{}, failure("unsupported", "Dispatcher does not support logical catalog inspection")
	}
	if status != http.StatusOK {
		return Envelope{}, failure("unavailable", "Dispatcher logical catalog is unavailable")
	}
	var raw CatalogView
	if json.Unmarshal(data, &raw) != nil || raw.Scenarios == nil || raw.KnowledgeBases == nil || raw.Tools == nil || raw.Skills == nil {
		return Envelope{}, failure("unavailable", "Dispatcher logical catalog returned an invalid shape")
	}
	view, ok := sanitizeCatalog(raw)
	if !ok {
		return Envelope{}, failure("unavailable", "Dispatcher logical catalog contains invalid identifiers")
	}
	return envelope(view, "", "dispatcher_logical_catalog", "partial"), nil
}

func sanitizeCatalog(raw CatalogView) (CatalogView, bool) {
	seenScenario := map[string]bool{}
	scenarios := make([]CatalogScenario, 0, len(raw.Scenarios))
	for _, s := range raw.Scenarios {
		if !mountIDPattern.MatchString(s.ID) || seenScenario[s.ID] {
			return CatalogView{}, false
		}
		seenScenario[s.ID] = true
		scenarios = append(scenarios, s)
	}
	sort.Slice(scenarios, func(i, j int) bool { return scenarios[i].ID < scenarios[j].ID })

	seenKB := map[string]bool{}
	kbs := make([]CatalogKnowledgeBase, 0, len(raw.KnowledgeBases))
	for _, kb := range raw.KnowledgeBases {
		if !mountIDPattern.MatchString(kb.ID) || seenKB[kb.ID] {
			return CatalogView{}, false
		}
		seenKB[kb.ID] = true
		kbs = append(kbs, kb)
	}
	sort.Slice(kbs, func(i, j int) bool { return kbs[i].ID < kbs[j].ID })

	seenTool := map[string]bool{}
	tools := make([]CatalogTool, 0, len(raw.Tools))
	for _, tool := range raw.Tools {
		if !mountIDPattern.MatchString(tool.ID) || seenTool[tool.ID] {
			return CatalogView{}, false
		}
		seenTool[tool.ID] = true
		tools = append(tools, tool)
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i].ID < tools[j].ID })

	seenSkill := map[string]bool{}
	skills := make([]CatalogSkill, 0, len(raw.Skills))
	for _, sk := range raw.Skills {
		if !mountIDPattern.MatchString(sk.ID) || seenSkill[sk.ID] {
			return CatalogView{}, false
		}
		seenSkill[sk.ID] = true
		skills = append(skills, sk)
	}
	sort.Slice(skills, func(i, j int) bool { return skills[i].ID < skills[j].ID })

	return CatalogView{
		Scenarios:      scenarios,
		KnowledgeBases: kbs,
		Tools:          tools,
		Skills:         skills,
	}, true
}
