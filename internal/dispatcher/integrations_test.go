package dispatcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestPostBridgeDeliveryFocusRetry: bridge first answers 409 INPUT_FOCUS_FAILED,
// then 200 OK — the helper must retry once and succeed, with exactly two requests sent.
func TestPostBridgeDeliveryFocusRetry(t *testing.T) {
	origDelay := focusRetryDelay
	focusRetryDelay = 10 * time.Millisecond
	defer func() { focusRetryDelay = origDelay }()

	var requests atomic.Int32
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		buf := make([]byte, 4096)
		read, _ := r.Body.Read(buf)
		bodies = append(bodies, string(buf[:read]))
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if n == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte("FAIL: INPUT_FOCUS_FAILED"))
			return
		}
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	payload := []byte(`{"group_id":"100@chatroom","target":"demo-group","text":"probe"}`)

	status, text, err := postBridgeDelivery(context.Background(), client, srv.URL, payload)
	if err != nil {
		t.Fatalf("expected no error after retry, got: %v", err)
	}
	if status != http.StatusOK || text != "OK" {
		t.Fatalf("expected 200 OK after retry, got %d %q", status, text)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("expected exactly 2 requests (original + retry), got %d", got)
	}
	if len(bodies) == 2 && bodies[0] != bodies[1] {
		t.Fatalf("retry must resend the identical body, got %q then %q", bodies[0], bodies[1])
	}
}

// TestPostBridgeDeliveryFocusRetryExhausted: INPUT_FOCUS_FAILED on both attempts
// must not retry a third time and must surface the 409 to the caller.
func TestPostBridgeDeliveryFocusRetryExhausted(t *testing.T) {
	origDelay := focusRetryDelay
	focusRetryDelay = 10 * time.Millisecond
	defer func() { focusRetryDelay = origDelay }()

	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("FAIL: INPUT_FOCUS_FAILED"))
	}))
	defer srv.Close()

	status, text, err := postBridgeDelivery(context.Background(), srv.Client(), srv.URL, []byte(`{}`))
	if err != nil {
		t.Fatalf("HTTP-level rejection must be returned as status, not error: %v", err)
	}
	if status != http.StatusConflict || text != "FAIL: INPUT_FOCUS_FAILED" {
		t.Fatalf("expected 409 FAIL: INPUT_FOCUS_FAILED, got %d %q", status, text)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("expected exactly 2 requests (no third retry), got %d", got)
	}
}

// TestPostBridgeDeliveryOther409NoRetry: a 409 with a different reason must NOT be retried.
func TestPostBridgeDeliveryOther409NoRetry(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("FAIL: SENDING_DISABLED"))
	}))
	defer srv.Close()

	status, text, err := postBridgeDelivery(context.Background(), srv.Client(), srv.URL, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != http.StatusConflict || text != "FAIL: SENDING_DISABLED" {
		t.Fatalf("expected 409 FAIL: SENDING_DISABLED, got %d %q", status, text)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("non-focus 409 must not be retried, got %d requests", got)
	}
}

// TestPostBridgeDelivery500NoRetry: 5xx must NOT be retried.
func TestPostBridgeDelivery500NoRetry(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("FAIL: INTERNAL"))
	}))
	defer srv.Close()

	status, _, err := postBridgeDelivery(context.Background(), srv.Client(), srv.URL, []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", status)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("500 must not be retried, got %d requests", got)
	}
}
