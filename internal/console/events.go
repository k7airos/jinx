package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/observability"
)

const keepaliveInterval = 15 * time.Second

type pollResult struct {
	page observability.EventPage
	err  error
}

func publicEvent(event contracts.Event) contracts.Event {
	event.Kind = safeToken(event.Kind)
	event.ReasonCode = safeToken(event.ReasonCode)
	event.Phase = contracts.Phase(safeToken(string(event.Phase)))
	event.Outcome = contracts.Outcome(safeToken(string(event.Outcome)))
	event.Delivery = contracts.Delivery(safeToken(string(event.Delivery)))
	metadata, err := architectureEvent(event)
	event.EnvelopeID, event.Source, event.Action = "", nil, nil
	if err == nil {
		event.EnvelopeID, event.Source, event.Action = metadata.EnvelopeID, metadata.Source, metadata.Action
	}
	return event
}

func (facade *Facade) events(writer http.ResponseWriter, request *http.Request) {
	facade.streamEvents(writer, request, false)
}

func (facade *Facade) architectureEvents(writer http.ResponseWriter, request *http.Request) {
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		writeResult(writer, nil, failure("validation", "Invalid architecture stream query"))
		return
	}
	if err := architectureQuery(query); err != nil {
		writeResult(writer, nil, err)
		return
	}
	facade.streamEvents(writer, request, true)
}

func architectureEvent(event contracts.Event) (contracts.ArchitectureEvent, error) {
	value := contracts.ArchitectureEvent{
		EventID: digest(event.Cursor), Domain: contracts.DomainProcessing, Kind: contracts.KindRunChanged,
		OccurredAt: event.OccurredAt, RunID: event.EntityID, Phase: event.Phase, Outcome: event.Outcome,
		Certainty: contracts.CertaintyObserved, ReasonCode: architectureReason(event.ReasonCode),
		EnvelopeID: event.EnvelopeID, Source: publicSource(event.Source), Action: event.Action,
	}
	if value.Phase == contracts.PhaseUnknown || value.Outcome == contracts.OutcomeUnknown {
		value.Certainty = contracts.CertaintyUnknown
	}
	if err := contracts.ValidateArchitectureEvent(value); err != nil {
		return value, failure("reset_required", "Trace event cannot be represented; read architecture again")
	}
	return value, nil
}

func (facade *Facade) streamEvents(writer http.ResponseWriter, request *http.Request, architecture bool) {
	cursor := request.URL.Query().Get("cursor")
	if resumed := request.Header.Get("Last-Event-ID"); resumed != "" {
		cursor = resumed
	}
	if cursor == "" {
		writeResult(writer, nil, failure("validation", "A bootstrap trace cursor is required"))
		return
	}
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	page, err := facade.backend.traceEvents(ctx, cursor, false)
	if err != nil {
		writeResult(writer, nil, err)
		return
	}
	if architecture {
		for _, event := range page.Events {
			if _, err := architectureEvent(event); err != nil {
				writeResult(writer, nil, err)
				return
			}
		}
	}
	if _, ok := writer.(http.Flusher); !ok {
		writeResult(writer, nil, failure("unsupported", "Streaming response is not supported by this transport"))
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(writer)
	flush := func() error {
		if err := controller.Flush(); err != nil {
			return err
		}
		_ = controller.SetWriteDeadline(time.Time{})
		return nil
	}
	writeFrame := func(kind, id string, value any) error {
		_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if id != "" {
			if _, err := fmt.Fprintf(writer, "id: %s\n", id); err != nil {
				return err
			}
		}
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", kind, data); err != nil {
			return err
		}
		return flush()
	}
	writePage := func(page observability.EventPage) error {
		if architecture {
			for _, event := range page.Events {
				if _, err := architectureEvent(event); err != nil {
					return err
				}
			}
		}
		for _, event := range page.Events {
			var value any = publicEvent(event)
			kind := "run"
			if architecture {
				value, _ = architectureEvent(event)
				kind = "architecture"
			}
			if err := writeFrame(kind, event.Cursor, value); err != nil {
				return err
			}
		}
		return nil
	}
	if err := writePage(page); err != nil {
		return
	}
	if err := flush(); err != nil {
		return
	}
	cursor = page.Cursor
	results := make(chan pollResult, 1)
	poll := func(cursor string, wait bool, delay time.Duration) {
		go func() {
			if delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
			}
			page, err := facade.backend.traceEvents(ctx, cursor, wait)
			select {
			case results <- pollResult{page: page, err: err}:
			case <-ctx.Done():
			}
		}()
	}
	poll(cursor, !page.HasMore, 0)
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := fmt.Fprint(writer, ": keepalive\n\n"); err != nil {
				return
			}
			if err := flush(); err != nil {
				return
			}
		case result := <-results:
			if result.err != nil {
				if ctx.Err() == nil {
					kind := "unavailable"
					if problem, ok := result.err.(*Problem); ok && problem.Code == "reset_required" {
						kind = "reset_required"
					}
					_ = writeFrame(kind, "", failure(kind, "Run stream interrupted; read bootstrap before reconnecting"))
				}
				return
			}
			if err := writePage(result.page); err != nil {
				if problem, ok := err.(*Problem); ok && ctx.Err() == nil {
					_ = writeFrame(problem.Code, "", problem)
				}
				return
			}
			cursor = result.page.Cursor
			delay := time.Duration(0)
			if len(result.page.Events) == 0 {
				delay = 250 * time.Millisecond
			}
			poll(cursor, !result.page.HasMore, delay)
		}
	}
}
