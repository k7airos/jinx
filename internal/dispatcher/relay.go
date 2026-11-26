package dispatcher

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func readSSE(reader io.Reader, consume func(event, id, data string) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var event, id string
	var data []string
	size := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(data) > 0 {
				if event == "" {
					event = "message"
				}
				if err := consume(event, id, strings.Join(data, "\n")); err != nil {
					return err
				}
			}
			event = ""
			id = ""
			data = nil
			size = 0
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			event = value
		case "id":
			if !strings.ContainsRune(value, '\x00') {
				id = value
			}
		case "data":
			size += len(value)
			if size > 2<<20 {
				return fmt.Errorf("SSE event too large")
			}
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF // An incomplete event must not be acknowledged.
}

type heartbeatReader struct {
	io.Reader
	timer *time.Timer
}

func (r heartbeatReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.timer.Reset(45 * time.Second)
	}
	return n, err
}

func (d *Dispatcher) consumeEvent(event, id, data string) error {
	switch event {
	case "message":
		var msg RelayMessage
		if err := json.Unmarshal([]byte(data), &msg); err != nil {
			return fmt.Errorf("malformed relay message: %w", err)
		}
		return d.handleIncomingMessage(msg, id)
	case "gap":
		d.mu.Lock()
		d.state.Stats.ReplayGaps++
		err := d.saveStateLocked()
		d.mu.Unlock()
		d.Log("WARN", "Relay replay gap; only retained fresh messages can be recovered")
		return err
	case "connected":
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.state.LastEventID == "" && id != "" {
			d.state.LastEventID = id
			return d.saveStateLocked()
		}
	}
	return nil
}

func (d *Dispatcher) relayOnce() error {
	cfg := d.configSnapshot()
	ctx, cancel := context.WithCancel(d.ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.Relay.StreamURL, nil)
	if err != nil {
		return err
	}
	d.mu.Lock()
	lastID := d.state.LastEventID
	d.mu.Unlock()
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return fmt.Errorf("relay HTTP %d or invalid content type", resp.StatusCode)
	}
	d.mu.Lock()
	d.state.Stats.RelayConnected = true
	d.mu.Unlock()
	defer func() { d.mu.Lock(); d.state.Stats.RelayConnected = false; d.mu.Unlock() }()
	d.Log("INFO", "Relay connected")
	timer := time.AfterFunc(45*time.Second, cancel)
	defer timer.Stop()
	return readSSE(heartbeatReader{Reader: resp.Body, timer: timer}, d.consumeEvent)
}

func (d *Dispatcher) listenRelay() {
	for d.ctx.Err() == nil {
		if err := d.relayOnce(); err != nil && d.ctx.Err() == nil {
			d.Log("WARN", "Relay disconnected: %v", err)
		}
		timer := time.NewTimer(3 * time.Second)
		select {
		case <-timer.C:
		case <-d.ctx.Done():
			timer.Stop()
			return
		}
	}
}
