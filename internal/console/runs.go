package console

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"jinx/internal/observability"
)

const listingTTL = 5 * time.Minute
const listingBudget = 32 << 20

type RunFilter struct {
	From         *time.Time `json:"from"`
	To           *time.Time `json:"to"`
	GroupID      string     `json:"groupId"`
	Phases       []string   `json:"phases"`
	Outcomes     []string   `json:"outcomes"`
	Deliveries   []string   `json:"deliveries"`
	BackendID    string     `json:"backendId"`
	Fallback     *bool      `json:"fallback"`
	Version      string     `json:"version"`
	Completeness string     `json:"completeness"`
	Query        string     `json:"query"`
	Limit        int        `json:"limit"`
}

type runListing struct {
	items         []RunView
	filterHash    string
	watermark     string
	streamID      string
	lastSeq       uint64
	coverageSince time.Time
	createdAt     time.Time
	secret        string
	size          int
}

type runCursor struct {
	ID      string `json:"id"`
	Offset  int    `json:"offset"`
	Binding string `json:"binding"`
}

func parseRunFilter(query url.Values) (RunFilter, error) {
	filter := RunFilter{Limit: 50}
	allowed := map[string]bool{}
	for _, name := range []string{"from", "to", "groupId", "phases", "outcomes", "deliveries", "backendId", "fallback", "version", "completeness", "query", "cursor", "limit"} {
		allowed[name] = true
	}
	for name, values := range query {
		if !allowed[name] || (len(values) != 1 && name != "phases" && name != "outcomes" && name != "deliveries") {
			return filter, failure("validation", "Unknown or repeated run query field")
		}
	}
	for name, target := range map[string]**time.Time{"from": &filter.From, "to": &filter.To} {
		if raw := query.Get(name); raw != "" {
			parsed, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return filter, failure("validation", "Run time filters require RFC3339 timestamps")
			}
			parsed = parsed.UTC()
			*target = &parsed
		}
	}
	if filter.From != nil && filter.To != nil && !filter.From.Before(*filter.To) {
		return filter, failure("validation", "Run from must be before to")
	}
	for name, target := range map[string]*string{"groupId": &filter.GroupID, "backendId": &filter.BackendID, "version": &filter.Version} {
		*target = query.Get(name)
		if len(*target) > 256 {
			return filter, failure("validation", "Run filter value is too long")
		}
	}
	if raw := query.Get("fallback"); raw != "" {
		if raw != "true" && raw != "false" {
			return filter, failure("validation", "Run fallback must be true or false")
		}
		value := raw == "true"
		filter.Fallback = &value
	}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			return filter, failure("validation", "Run limit must be 1..100")
		}
		filter.Limit = limit
	}
	filter.Completeness = query.Get("completeness")
	if filter.Completeness != "" && !strings.Contains("|complete|partial|legacy_partial|", "|"+filter.Completeness+"|") {
		return filter, failure("validation", "Unknown run completeness")
	}
	filter.Query = strings.ToLower(strings.TrimSpace(query.Get("query")))
	if len(filter.Query) > 256 {
		return filter, failure("validation", "Run query is too long")
	}
	for _, field := range []struct {
		name    string
		target  *[]string
		allowed string
	}{
		{"phases", &filter.Phases, "accepted aggregating gating queued executing checking submitting confirming terminal unknown"},
		{"outcomes", &filter.Outcomes, "pending replied silent blocked suppressed expired canceled failed unknown"},
		{"deliveries", &filter.Deliveries, "not_started blocked submitting submitted confirmed failed unknown"},
	} {
		seen := map[string]bool{}
		for _, raw := range query[field.name] {
			for _, value := range strings.Split(raw, ",") {
				if value == "" || !strings.Contains(" "+field.allowed+" ", " "+value+" ") {
					return filter, failure("validation", "Unknown run state filter")
				}
				seen[value] = true
			}
		}
		for value := range seen {
			*field.target = append(*field.target, value)
		}
		sort.Strings(*field.target)
	}
	return filter, nil
}

func matchingRun(run RunView, filter RunFilter) bool {
	if run.AcceptedAt == nil || (filter.From != nil && run.AcceptedAt.Before(*filter.From)) || (filter.To != nil && !run.AcceptedAt.Before(*filter.To)) {
		return false
	}
	if filter.GroupID != "" && run.Source.ID != filter.GroupID && run.Target.ID != filter.GroupID {
		return false
	}
	for _, state := range []struct {
		values []string
		actual string
	}{
		{filter.Phases, string(run.Phase)}, {filter.Outcomes, string(run.Outcome)}, {filter.Deliveries, string(run.Delivery)},
	} {
		if len(state.values) > 0 && !containsString(state.values, state.actual) {
			return false
		}
	}
	if filter.Completeness != "" && filter.Completeness != string(run.Completeness) {
		return false
	}
	fallbackObserved := false
	backendMatches := run.PlannedBackendID != nil && *run.PlannedBackendID == filter.BackendID
	for _, attempt := range run.Attempts {
		backendMatches = backendMatches || attempt.BackendID == filter.BackendID
		fallbackObserved = fallbackObserved || attempt.FallbackReason != nil
	}
	if filter.Fallback != nil && *filter.Fallback != fallbackObserved {
		return false
	}
	if filter.BackendID != "" && !backendMatches {
		return false
	}
	if filter.Version != "" && filter.Version != run.Versions.Dispatcher && filter.Version != run.Versions.Fingerprint && (run.Versions.Routing == nil || *run.Versions.Routing != filter.Version) && (run.Versions.Policy == nil || *run.Versions.Policy != filter.Version) {
		return false
	}
	search := []string{run.ID, run.Source.ID, run.Target.ID}
	search = append(search, run.MessageIDs...)
	return filter.Query == "" || strings.Contains(strings.ToLower(strings.Join(search, " ")), filter.Query)
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func sortedRuns(snapshot observability.Snapshot) []RunView {
	items := make([]RunView, 0, len(snapshot.Runs))
	for _, run := range snapshot.Runs {
		items = append(items, runView(run))
	}
	sort.Slice(items, func(first, second int) bool {
		if items[first].AcceptedAt.Equal(*items[second].AcceptedAt) {
			return items[first].ID < items[second].ID
		}
		return items[first].AcceptedAt.After(*items[second].AcceptedAt)
	})
	return items
}

func cursorBinding(listing *runListing, offset int) string {
	return digest([]any{listing.secret, listing.filterHash, listing.watermark, offset})
}

func (facade *Facade) ListRuns(ctx context.Context, query url.Values) (Envelope, error) {
	filter, err := parseRunFilter(query)
	if err != nil {
		return Envelope{}, err
	}
	filterHash := digest(filter)
	var position runCursor
	if cursor := query.Get("cursor"); cursor != "" {
		data, err := base64.RawURLEncoding.DecodeString(cursor)
		if len(cursor) > 2048 || err != nil || json.Unmarshal(data, &position) != nil || position.Offset < 1 || position.ID == "" {
			return Envelope{}, failure("reset_required", "Run page cursor is invalid; start a new query")
		}
	} else {
		snapshot, err := facade.backend.traceSnapshot(ctx)
		if err != nil {
			return Envelope{}, err
		}
		listing := &runListing{items: []RunView{}, filterHash: filterHash, watermark: snapshot.Cursor, streamID: snapshot.StreamID, lastSeq: snapshot.LastSeq, coverageSince: snapshot.CoverageSince, createdAt: time.Now().UTC(), secret: newID()}
		for _, run := range sortedRuns(snapshot) {
			if matchingRun(run, filter) {
				listing.items = append(listing.items, run)
			}
		}
		encoded, _ := json.Marshal(listing.items)
		listing.size = len(encoded)
		if listing.size > listingBudget {
			return Envelope{}, failure("too_large", "Run query snapshot exceeds the pagination budget")
		}
		position.ID = newID()
		facade.traceMu.Lock()
		facade.pruneListings(listing.size)
		facade.listings[position.ID] = listing
		facade.traceMu.Unlock()
	}
	facade.traceMu.Lock()
	defer facade.traceMu.Unlock()
	listing := facade.listings[position.ID]
	if listing == nil || time.Since(listing.createdAt) >= listingTTL {
		delete(facade.listings, position.ID)
		return Envelope{}, failure("reset_required", "Run page cursor expired or was evicted; start a new query")
	}
	if listing.filterHash != filterHash || (position.Offset > 0 && position.Binding != cursorBinding(listing, position.Offset)) || position.Offset > len(listing.items) {
		return Envelope{}, failure("reset_required", "Run cursor does not match filters or watermark; start a new query")
	}
	end := position.Offset + filter.Limit
	if end > len(listing.items) {
		end = len(listing.items)
	}
	page := RunPage{Items: []RunView{}, CoverageSince: listing.coverageSince, Watermark: listing.watermark}
	data, _ := json.Marshal(listing.items[position.Offset:end])
	_ = json.Unmarshal(data, &page.Items)
	if end < len(listing.items) {
		next, _ := json.Marshal(runCursor{ID: position.ID, Offset: end, Binding: cursorBinding(listing, end)})
		page.NextCursor = nullable(base64.RawURLEncoding.EncodeToString(next))
	}
	result := envelope(page, listing.watermark, "dispatcher_trace_snapshot", "partial")
	result.ObservedAt, result.StreamID = listing.createdAt, listing.streamID
	seq := listing.lastSeq
	result.EntitySeq = &seq
	return result, nil
}

func (facade *Facade) pruneListings(incoming int) {
	for id, listing := range facade.listings {
		if time.Since(listing.createdAt) >= listingTTL {
			delete(facade.listings, id)
		}
	}
	for {
		total := incoming
		oldest := ""
		for id, listing := range facade.listings {
			total += listing.size
			if oldest == "" || listing.createdAt.Before(facade.listings[oldest].createdAt) {
				oldest = id
			}
		}
		if oldest == "" || (total <= listingBudget && len(facade.listings) < 16) {
			return
		}
		delete(facade.listings, oldest)
	}
}
