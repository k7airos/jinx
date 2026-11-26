package console

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/observability"
)

const maxTraceBody = 16 << 20

type tracePosition struct {
	StreamID string
	Seq      uint64
}

func parseTraceCursor(cursor string) (tracePosition, error) {
	var position tracePosition
	if len(cursor) > 2048 {
		return position, failure("reset_required", "Trace cursor is invalid; bootstrap again")
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || base64.RawURLEncoding.EncodeToString(data) != cursor {
		return position, failure("reset_required", "Trace cursor is invalid; bootstrap again")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&position) != nil || decoder.Decode(new(any)) != io.EOF || safeToken(position.StreamID) == "" {
		return position, failure("reset_required", "Trace cursor is invalid; bootstrap again")
	}
	return position, nil
}

func traceStatus(status int, err error) error {
	if err != nil {
		return err
	}
	switch status {
	case http.StatusOK:
		return nil
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return failure("unsupported", "Dispatcher trace API is not available")
	case http.StatusConflict:
		return failure("reset_required", "Trace replay is no longer available; bootstrap again")
	default:
		return failure("unavailable", "Dispatcher trace API did not return a usable response")
	}
}

func (backend *upstream) traceSnapshot(ctx context.Context) (observability.Snapshot, error) {
	var snapshot observability.Snapshot
	data, _, status, err := backend.boundedRequest(ctx, backend.client, maxTraceBody, "dispatcher", http.MethodGet, "/api/trace/snapshot", "", nil)
	if err := traceStatus(status, err); err != nil {
		return snapshot, err
	}
	if json.Unmarshal(data, &snapshot) != nil || snapshot.CoverageSince.IsZero() || snapshot.Runs == nil {
		return snapshot, failure("unavailable", "Dispatcher trace snapshot is invalid")
	}
	position, err := parseTraceCursor(snapshot.Cursor)
	if err != nil || position.StreamID != snapshot.StreamID || position.Seq != snapshot.LastSeq {
		return snapshot, failure("unavailable", "Dispatcher trace snapshot position is invalid")
	}
	seen := map[string]bool{}
	for _, run := range snapshot.Runs {
		if contracts.ValidateRun(run) != nil || safeToken(run.ID) == "" || safeToken(run.Revision) == "" || run.EntitySeq == 0 || run.EntitySeq > snapshot.LastSeq || seen[run.ID] {
			return snapshot, failure("unavailable", "Dispatcher trace run is invalid")
		}
		seen[run.ID] = true
	}
	return snapshot, nil
}

func (backend *upstream) traceEvents(ctx context.Context, cursor string, wait bool) (observability.EventPage, error) {
	var page observability.EventPage
	position, err := parseTraceCursor(cursor)
	if err != nil {
		return page, err
	}
	query := url.Values{"cursor": {cursor}, "limit": {"100"}}
	client := backend.client
	if wait {
		query.Set("wait", "1")
		client = backend.traceClient
	}
	data, _, status, err := backend.boundedRequest(ctx, client, maxTraceBody, "dispatcher", http.MethodGet, "/api/trace/events?"+query.Encode(), "", nil)
	if err := traceStatus(status, err); err != nil {
		return page, err
	}
	if json.Unmarshal(data, &page) != nil || page.Events == nil || len(page.Events) > 100 || (page.HasMore && len(page.Events) == 0) {
		return page, failure("unavailable", "Dispatcher trace event page is invalid")
	}
	for _, event := range page.Events {
		next, err := parseTraceCursor(event.Cursor)
		if err != nil || event.StreamID != position.StreamID || event.Seq != position.Seq+1 || next.StreamID != event.StreamID || next.Seq != event.Seq || event.Entity != "run" || safeToken(event.EntityID) == "" || safeToken(event.Revision) == "" || event.OccurredAt.IsZero() {
			return page, failure("reset_required", "Trace event gap or stream change; bootstrap again")
		}
		position = next
	}
	if len(page.Events) == 0 && page.Cursor != cursor {
		return page, failure("reset_required", "Empty trace page changed cursor; bootstrap again")
	}
	end, err := parseTraceCursor(page.Cursor)
	if err != nil || end != position {
		return page, failure("reset_required", "Trace page position does not match its events; bootstrap again")
	}
	return page, nil
}

func (facade *Facade) GetRun(ctx context.Context, id string) (Envelope, error) {
	if safeToken(id) == "" {
		return Envelope{}, failure("validation", "Run ID is invalid")
	}
	data, _, status, err := facade.backend.boundedRequest(ctx, facade.backend.client, maxTraceBody, "dispatcher", http.MethodGet, "/api/trace/runs/"+url.PathEscape(id), "", nil)
	if status == http.StatusNotFound && err == nil {
		if _, probeErr := facade.backend.traceSnapshot(ctx); probeErr != nil {
			return Envelope{}, probeErr
		}
		return Envelope{}, failure("not_found", "Run was not found in trace")
	}
	if err := traceStatus(status, err); err != nil {
		return Envelope{}, err
	}
	var detail struct {
		Run    contracts.Run     `json:"run"`
		Events []contracts.Event `json:"events"`
	}
	if json.Unmarshal(data, &detail) != nil || detail.Run.ID != id || contracts.ValidateRun(detail.Run) != nil || safeToken(detail.Run.Revision) == "" || detail.Events == nil {
		return Envelope{}, failure("unavailable", "Dispatcher run detail is invalid")
	}
	timeline := []TimelineItem{}
	var previous uint64
	var streamID string
	for _, event := range detail.Events {
		position, err := parseTraceCursor(event.Cursor)
		if err != nil || event.Entity != "run" || event.EntityID != id || event.Seq <= previous || position.StreamID != event.StreamID || position.Seq != event.Seq || (streamID != "" && streamID != event.StreamID) || event.OccurredAt.IsZero() || safeToken(event.Revision) == "" {
			return Envelope{}, failure("unavailable", "Dispatcher run timeline is invalid")
		}
		streamID, previous = event.StreamID, event.Seq
		timeline = append(timeline, TimelineItem{
			ID: event.StreamID + ":" + strconv.FormatUint(event.Seq, 10), RunID: id, OccurredAt: factTime(event.OccurredAt), Source: "dispatcher_trace",
			Kind: safeToken(event.Kind), ReasonCode: nullable(safeToken(event.ReasonCode)), Summary: runSummary(event.Phase, event.Outcome, event.Delivery), Certainty: "observed",
		})
	}
	if len(detail.Events) == 0 {
		timeline = append(timeline, TimelineItem{ID: id + ":missing", RunID: id, Source: "dispatcher_trace", Kind: "history_unavailable", Summary: "Run event history was not retained", Certainty: "missing"})
	} else {
		last := detail.Events[len(detail.Events)-1]
		if last.Seq != detail.Run.EntitySeq || last.Revision != detail.Run.Revision || last.Phase != detail.Run.Phase || last.Outcome != detail.Run.Outcome || last.Delivery != detail.Run.Delivery || last.ReasonCode != detail.Run.ReasonCode || !last.OccurredAt.Equal(detail.Run.UpdatedAt) {
			return Envelope{}, failure("unavailable", "Dispatcher run and timeline revisions disagree")
		}
	}
	view := runView(detail.Run)
	if view.ConfigSnapshotID != nil {
		if historical, err := facade.GetConfigSnapshot(ctx, *view.ConfigSnapshotID); err == nil && historical.Data != nil {
			view.MissingSnapshotReason = nil
		}
	}
	result := envelope(RunDetail{Run: view, Timeline: timeline}, detail.Run.Revision, "dispatcher_trace", "partial")
	result.StreamID, result.EntitySeq = streamID, &detail.Run.EntitySeq
	return result, nil
}

type TraceState struct {
	Availability
	Domain        string     `json:"domain"`
	StreamID      *string    `json:"streamId"`
	Cursor        *string    `json:"cursor"`
	LastSeq       *uint64    `json:"lastSeq"`
	CoverageSince *time.Time `json:"coverageSince"`
}

type Bootstrap struct {
	StreamID         *string                 `json:"streamId"`
	Cursor           *string                 `json:"cursor"`
	LastSeq          *uint64                 `json:"lastSeq"`
	Overview         Envelope                `json:"overview"`
	ActiveOperations []Operation             `json:"activeOperations"`
	Capabilities     map[string]Availability `json:"capabilities"`
	Schemas          map[string]any          `json:"schemas"`
	EventStream      TraceState              `json:"eventStream"`
}

func traceState(snapshot observability.Snapshot, err error) TraceState {
	state := TraceState{Domain: "run", Availability: Availability{Reason: "Dispatcher trace is unavailable"}}
	if err == nil {
		state.Availability = Availability{Supported: true, Enabled: true}
		state.StreamID, state.Cursor, state.LastSeq, state.CoverageSince = &snapshot.StreamID, &snapshot.Cursor, &snapshot.LastSeq, &snapshot.CoverageSince
	}
	return state
}

func (facade *Facade) Bootstrap(ctx context.Context) Bootstrap {
	snapshot, traceErr := facade.backend.traceSnapshot(ctx)
	streamErr := traceErr
	if traceErr == nil {
		_, streamErr = facade.backend.traceEvents(ctx, snapshot.Cursor, false)
	}
	capabilities := Capabilities()
	if len(facade.backend.connectorPlatforms()) > 0 {
		capabilities["connectors"] = Availability{Supported: true, Enabled: true}
	}
	if facade.backend.bases["relay"] != "" {
		if _, err := facade.backend.relayConfig(ctx); err == nil {
			capabilities["relay.rules"] = Availability{Supported: true, Enabled: true}
		}
	}
	if traceErr == nil {
		capabilities["traceRuns"] = Availability{Supported: true, Enabled: true}
		capabilities["runs"] = capabilities["traceRuns"]
		for _, run := range snapshot.Runs {
			if safeToken(run.ConfigSnapshotID) == "" {
				continue
			}
			if historical, err := facade.GetConfigSnapshot(ctx, run.ConfigSnapshotID); err == nil && historical.Data != nil {
				capabilities["configSnapshots"] = Availability{Supported: true, Enabled: true}
			}
			break
		}
	}
	stream := traceState(snapshot, streamErr)
	capabilities["resumableRunEvents"], capabilities["events.resume"] = stream.Availability, stream.Availability
	position := traceState(snapshot, traceErr)
	return Bootstrap{
		StreamID: position.StreamID, Cursor: position.Cursor, LastSeq: position.LastSeq,
		Overview: facade.overview(ctx, snapshot, traceErr), ActiveOperations: facade.ActiveOperations(), Capabilities: capabilities,
		Schemas: map[string]any{"schemaVersion": SchemaVersion, "group": groupSchema(), "global": globalSchema()}, EventStream: stream,
	}
}
