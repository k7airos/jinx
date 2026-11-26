package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
)

type stubExecutor struct {
	ready   bool
	reason  string
	calls   atomic.Int32
	execute func(ctx context.Context, req ActionRequest) (ActionResponse, error)
}

func (s *stubExecutor) Platform() contracts.Platform { return contracts.PlatformBilibili }
func (s *stubExecutor) AccountID() string            { return "bilibili-ego" }
func (s *stubExecutor) ConnectorName() string        { return "stub-connector" }
func (s *stubExecutor) Capabilities() []contracts.ActionKind {
	return []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionThreadReply}
}
func (s *stubExecutor) Health(context.Context) (bool, string) { return s.ready, s.reason }
func (s *stubExecutor) Execute(ctx context.Context, req ActionRequest) (ActionResponse, error) {
	s.calls.Add(1)
	return s.execute(ctx, req)
}

func TestIdempotencyAndCursorStoresPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	idemPath := filepath.Join(dir, "idempotency.json")
	store, err := OpenIdempotencyStore(idemPath)
	if err != nil {
		t.Fatal(err)
	}
	req := ActionRequest{
		ProtocolVersion: 1,
		ActionID:        "act-1",
		RunID:           "run-1",
		IdempotencyKey:  "idem-1",
		Platform:        "bilibili",
		Kind:            "thread_reply",
		Text:            "hello",
		ExpiresAt:       time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
		Target: ActionTarget{
			Platform:    "bilibili",
			AccountID:   "bilibili-ego",
			ContainerID: "bilibili:BV1example0001",
			ContentID:   "rpid-1",
		},
	}
	digest := RequestDigest(req)
	if _, state, err := store.Claim("idem-1", digest); err != nil || state != "claimed" {
		t.Fatalf("expected claimed, got state=%q err=%v", state, err)
	}
	if _, state, err := store.Claim("idem-1", digest); err != nil || state != "in_flight" {
		t.Fatalf("expected in_flight, got state=%q err=%v", state, err)
	}
	if _, state, err := store.Claim("idem-1", "different-digest"); err != nil || state != "conflict" {
		t.Fatalf("expected conflict, got state=%q err=%v", state, err)
	}
	if err := store.Complete("idem-1", digest, http.StatusOK, ActionResponse{
		ActionID:   "act-1",
		Status:     "confirmed",
		ExternalID: "rpid-999",
	}); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenIdempotencyStore(idemPath)
	if err != nil {
		t.Fatal(err)
	}
	rec, state, err := reopened.Claim("idem-1", digest)
	if err != nil || state != "replay" || rec == nil || rec.Response == nil || rec.Response.ExternalID != "rpid-999" {
		t.Fatalf("expected replay from reopened store, got rec=%+v state=%q err=%v", rec, state, err)
	}

	cursorPath := filepath.Join(dir, "cursor.json")
	cursors, err := OpenCursorStore(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	if cursors.IsSeeded("bilibili:BV1example0001") || cursors.HasSeen("bilibili:BV1example0001", "c-1") {
		t.Fatal("fresh cursor store should not be seeded or have seen IDs")
	}
	if err := cursors.MarkSeen("bilibili:BV1example0001", "c-1", 1700000000000); err != nil {
		t.Fatal(err)
	}
	if err := cursors.MarkSeeded("bilibili:BV1example0001"); err != nil {
		t.Fatal(err)
	}
	cursors2, err := OpenCursorStore(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	if !cursors2.IsSeeded("bilibili:BV1example0001") || !cursors2.HasSeen("bilibili:BV1example0001", "c-1") {
		t.Fatal("reopened cursor store lost seeded or seen state")
	}
}

func TestRelayClientPush(t *testing.T) {
	for _, bad := range []string{
		"https://example.com",
		"http://127.0.0.1",
		"http://127.0.0.1:8080/path",
		"http://127.0.0.1:8080?x=1",
		"http://127.0.0.1:8080?",
		"http://127.0.0.1:8080#frag",
	} {
		if _, err := NewRelayClient(bad, time.Second); err == nil {
			t.Fatalf("expected invalid loopback URL rejection for %q", bad)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/messages" || r.Method != http.MethodPost {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var payload InboundPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, RelayPushResponse{
			Status:  "ok",
			ID:      payload.ID,
			Allowed: true,
			Action:  "forward",
		})
	}))
	defer srv.Close()

	client, err := NewRelayClient(srv.URL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Push(context.Background(), InboundPayload{
		ID:        "rpid-1",
		Source:    "bilibili:ego",
		Platform:  "bilibili",
		GroupID:   "bilibili:BV1example0001",
		SenderID:  "9001",
		Type:      1,
		Content:   "test comment",
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil || !res.Allowed || res.ID != "rpid-1" {
		t.Fatalf("unexpected relay push result: %+v %v", res, err)
	}
}

func TestServerSafetyGatesIdempotencyAndLoopback(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	dir := t.TempDir()
	store, err := OpenIdempotencyStore(filepath.Join(dir, "idempotency.json"))
	if err != nil {
		t.Fatal(err)
	}
	exec := &stubExecutor{
		ready: true,
		execute: func(_ context.Context, req ActionRequest) (ActionResponse, error) {
			return ActionResponse{
				ActionID:   req.ActionID,
				Status:     "confirmed",
				ExternalID: "rpid-created-1",
			}, nil
		},
	}

	// Dry-run gate prevents execution.
	drySrv := NewServer(ServerOptions{
		Executor: exec,
		Store:    store,
		DryRun:   true,
		Now:      func() time.Time { return now },
	})
	validReq := ActionRequest{
		ProtocolVersion: 1,
		ActionID:        "act-1",
		RunID:           "run-1",
		IdempotencyKey:  "idem-1",
		Platform:        "bilibili",
		Kind:            "thread_reply",
		Text:            "reply text",
		ExpiresAt:       now.Add(time.Minute).Format(time.RFC3339Nano),
		ExpiresAtUnixMS: now.Add(time.Minute).UnixMilli(),
		Target: ActionTarget{
			Platform:    "bilibili",
			AccountID:   "bilibili-ego",
			ContainerID: "bilibili:BV1example0001",
			ContentID:   "rpid-orig",
		},
	}
	raw, _ := json.Marshal(validReq)
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	drySrv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict || exec.calls.Load() != 0 {
		t.Fatalf("expected dry_run 409 with 0 calls, got %d %s (calls=%d)", rec.Code, rec.Body.String(), exec.calls.Load())
	}

	// Live server tests: loopback guard, expired, replay, conflict, unknown downgrade.
	liveSrv := NewServer(ServerOptions{
		Executor: exec,
		Store:    store,
		DryRun:   false,
		Now:      func() time.Time { return now },
	})

	// Non-loopback Host header blocked.
	badHost := httptest.NewRequest(http.MethodGet, "http://evil.example/health", nil)
	badHostRec := httptest.NewRecorder()
	liveSrv.Handler().ServeHTTP(badHostRec, badHost)
	if badHostRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback host, got %d", badHostRec.Code)
	}

	// Expired action rejected before Claim/Execute.
	expiredReq := validReq
	expiredReq.ExpiresAt = now.Add(-time.Second).Format(time.RFC3339Nano)
	expiredReq.ExpiresAtUnixMS = now.Add(-time.Second).UnixMilli()
	expiredRaw, _ := json.Marshal(expiredReq)
	rExp := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(expiredRaw)))
	rExp.Header.Set("Content-Type", "application/json")
	recExp := httptest.NewRecorder()
	liveSrv.Handler().ServeHTTP(recExp, rExp)
	if recExp.Code != http.StatusConflict || exec.calls.Load() != 0 {
		t.Fatalf("expected expired 409 with 0 calls, got %d %s", recExp.Code, recExp.Body.String())
	}

	// Platform mismatch rejected with 400.
	wrongPlatform := validReq
	wrongPlatform.Platform = "wechat"
	wrongPlatform.Target.Platform = "wechat"
	wpRaw, _ := json.Marshal(wrongPlatform)
	rWP := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(wpRaw)))
	rWP.Header.Set("Content-Type", "application/json")
	recWP := httptest.NewRecorder()
	liveSrv.Handler().ServeHTTP(recWP, rWP)
	if recWP.Code != http.StatusBadRequest || exec.calls.Load() != 0 {
		t.Fatalf("expected platform mismatch 400 with 0 calls, got %d %s", recWP.Code, recWP.Body.String())
	}

	// First live call executes once; second identical call replays without re-executing.
	for i := 0; i < 2; i++ {
		rLive := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(raw)))
		rLive.Header.Set("Content-Type", "application/json")
		recLive := httptest.NewRecorder()
		liveSrv.Handler().ServeHTTP(recLive, rLive)
		if recLive.Code != http.StatusOK {
			t.Fatalf("call %d expected 200, got %d %s", i, recLive.Code, recLive.Body.String())
		}
	}
	if exec.calls.Load() != 1 {
		t.Fatalf("expected exactly 1 executor call across replay, got %d", exec.calls.Load())
	}

	// Same idempotency key with different text returns 409 conflict without re-executing.
	conflictReq := validReq
	conflictReq.Text = fmt.Sprintf("%s edited", validReq.Text)
	cRaw, _ := json.Marshal(conflictReq)
	rConf := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(cRaw)))
	rConf.Header.Set("Content-Type", "application/json")
	recConf := httptest.NewRecorder()
	liveSrv.Handler().ServeHTTP(recConf, rConf)
	if recConf.Code != http.StatusConflict || exec.calls.Load() != 1 {
		t.Fatalf("expected 409 conflict with 1 total call, got %d %s", recConf.Code, recConf.Body.String())
	}

	// Confirmed outcome without a valid external_id downgrades to HTTP 502 unknown.
	badExtExec := &stubExecutor{
		ready: true,
		execute: func(_ context.Context, req ActionRequest) (ActionResponse, error) {
			return ActionResponse{
				ActionID:   req.ActionID,
				Status:     "confirmed",
				ExternalID: "",
			}, nil
		},
	}
	badExtSrv := NewServer(ServerOptions{
		Executor: badExtExec,
		Store:    store,
		DryRun:   false,
		Now:      func() time.Time { return now },
	})
	badExtReq := validReq
	badExtReq.ActionID = "act-bad-ext"
	badExtReq.IdempotencyKey = "idem-bad-ext"
	badExtRaw, _ := json.Marshal(badExtReq)
	rBadExt := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(badExtRaw)))
	rBadExt.Header.Set("Content-Type", "application/json")
	recBadExt := httptest.NewRecorder()
	badExtSrv.Handler().ServeHTTP(recBadExt, rBadExt)
	var badExtResp ActionResponse
	_ = json.NewDecoder(recBadExt.Body).Decode(&badExtResp)
	if recBadExt.Code != http.StatusBadGateway || badExtResp.Status != "unknown" || badExtResp.ReasonCode != "invalid_external_id" {
		t.Fatalf("expected 502 unknown for missing external_id, got %d %+v", recBadExt.Code, badExtResp)
	}

	// Failed outcome with proven submitted=false returns HTTP 422 failed.
	failExec := &stubExecutor{
		ready: true,
		execute: func(_ context.Context, req ActionRequest) (ActionResponse, error) {
			return ActionResponse{
				ActionID:   req.ActionID,
				Status:     "failed",
				Submitted:  BoolPtr(false),
				ReasonCode: "comment_deleted",
			}, nil
		},
	}
	failSrv := NewServer(ServerOptions{
		Executor: failExec,
		Store:    store,
		DryRun:   false,
		Now:      func() time.Time { return now },
	})
	failReq := validReq
	failReq.ActionID = "act-fail"
	failReq.IdempotencyKey = "idem-fail"
	failRaw, _ := json.Marshal(failReq)
	rFail := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(failRaw)))
	rFail.Header.Set("Content-Type", "application/json")
	recFail := httptest.NewRecorder()
	failSrv.Handler().ServeHTTP(recFail, rFail)
	var failResp ActionResponse
	_ = json.NewDecoder(recFail.Body).Decode(&failResp)
	if recFail.Code != http.StatusUnprocessableEntity || failResp.Status != "failed" || failResp.Submitted == nil || *failResp.Submitted || failResp.ReasonCode != "comment_deleted" {
		t.Fatalf("expected 422 failed with submitted=false, got %d %+v", recFail.Code, failResp)
	}
}

type statusStubExecutor struct {
	*stubExecutor
	status StatusSnapshot
}

func (s *statusStubExecutor) Status() StatusSnapshot {
	return s.status
}

func TestServerHealthAndDryRunControl(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	dir := t.TempDir()
	store, err := OpenIdempotencyStore(filepath.Join(dir, "idempotency.json"))
	if err != nil {
		t.Fatal(err)
	}
	polledAt := now.Add(-15 * time.Second)
	exec := &statusStubExecutor{
		stubExecutor: &stubExecutor{
			ready: true,
			execute: func(_ context.Context, req ActionRequest) (ActionResponse, error) {
				return ActionResponse{
					ActionID:   req.ActionID,
					Status:     "confirmed",
					ExternalID: "rpid-created-2",
				}, nil
			},
		},
		status: StatusSnapshot{
			Targets: []HealthTarget{
				{ContainerID: "bilibili:BV1example0001", Label: "Demo", Enabled: true},
			},
			LastPollAt:        &polledAt,
			LastPollErrorCode: "fetch_failed",
		},
	}

	var callbackCalls atomic.Int32
	var failCallback atomic.Bool
	srv := NewServer(ServerOptions{
		Executor: exec,
		Store:    store,
		DryRun:   true,
		Now:      func() time.Time { return now },
		OnDryRunChange: func(dryRun bool) (bool, error) {
			callbackCalls.Add(1)
			if failCallback.Load() {
				return false, fmt.Errorf("disk full")
			}
			return true, nil
		},
	})
	handler := srv.Handler()

	// Health includes controls and status reporter snapshot.
	hRec := httptest.NewRecorder()
	handler.ServeHTTP(hRec, httptest.NewRequest(http.MethodGet, "http://127.0.0.1/health", nil))
	if hRec.Code != http.StatusOK {
		t.Fatalf("GET /health status=%d body=%s", hRec.Code, hRec.Body.String())
	}
	var h HealthResponse
	if err := json.NewDecoder(hRec.Body).Decode(&h); err != nil {
		t.Fatal(err)
	}
	if !h.DryRun || len(h.Controls) != 1 || h.Controls[0] != "dry_run" ||
		len(h.Targets) != 1 || h.Targets[0].ContainerID != "bilibili:BV1example0001" ||
		h.LastPollAt == nil || !h.LastPollAt.Equal(polledAt) || h.LastPollErrorCode != "fetch_failed" {
		t.Fatalf("unexpected health response: %+v", h)
	}

	// Method & security & input guards on POST /v1/control/dry-run.
	recGet := httptest.NewRecorder()
	handler.ServeHTTP(recGet, httptest.NewRequest(http.MethodGet, "http://127.0.0.1/v1/control/dry-run", nil))
	if recGet.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for GET dry-run control, got %d", recGet.Code)
	}

	recCT := httptest.NewRecorder()
	reqCT := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/control/dry-run", strings.NewReader(`{"dry_run":false,"request_id":"req-1"}`))
	handler.ServeHTTP(recCT, reqCT)
	if recCT.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415 without JSON content-type, got %d", recCT.Code)
	}

	recBadHost := httptest.NewRecorder()
	reqBadHost := httptest.NewRequest(http.MethodPost, "http://evil.example/v1/control/dry-run", strings.NewReader(`{"dry_run":false,"request_id":"req-1"}`))
	reqBadHost.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(recBadHost, reqBadHost)
	if recBadHost.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback host, got %d", recBadHost.Code)
	}

	recCross := httptest.NewRecorder()
	reqCross := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/control/dry-run", strings.NewReader(`{"dry_run":false,"request_id":"req-1"}`))
	reqCross.Header.Set("Content-Type", "application/json")
	reqCross.Header.Set("Sec-Fetch-Site", "cross-site")
	handler.ServeHTTP(recCross, reqCross)
	if recCross.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-site request, got %d", recCross.Code)
	}

	recOrigin := httptest.NewRecorder()
	reqOrigin := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18092/v1/control/dry-run", strings.NewReader(`{"dry_run":false,"request_id":"req-1"}`))
	reqOrigin.Header.Set("Content-Type", "application/json")
	reqOrigin.Header.Set("Origin", "http://127.0.0.1:9999")
	handler.ServeHTTP(recOrigin, reqOrigin)
	if recOrigin.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for mismatched origin, got %d", recOrigin.Code)
	}

	for _, badBody := range []string{
		`{"dry_run":false,"request_id":"req-1","extra":1}`,
		`{"dry_run":false,"request_id":"req-1"}{"dry_run":true,"request_id":"req-2"}`,
		`{"dry_run":false,"request_id":""}`,
		`{"dry_run":false,"request_id":"bad id with spaces"}`,
	} {
		recBad := httptest.NewRecorder()
		reqBad := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/control/dry-run", strings.NewReader(badBody))
		reqBad.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(recBad, reqBad)
		if recBad.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for body %s, got %d (%s)", badBody, recBad.Code, recBad.Body.String())
		}
	}

	// Callback failure keeps dry_run unchanged.
	failCallback.Store(true)
	recPersistErr := httptest.NewRecorder()
	reqPersistErr := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/control/dry-run", strings.NewReader(`{"dry_run":false,"request_id":"req-fail"}`))
	reqPersistErr.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(recPersistErr, reqPersistErr)
	if recPersistErr.Code != http.StatusInternalServerError || !srv.isDryRun() {
		t.Fatalf("expected 500 persist_failed leaving dry_run=true, got %d dryRun=%t", recPersistErr.Code, srv.isDryRun())
	}
	failCallback.Store(false)

	// Valid toggle off + idempotent replay + conflicting replay.
	postDryRun := func(body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/control/dry-run", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(rec, req)
		return rec
	}
	beforeCalls := callbackCalls.Load()
	recOff1 := postDryRun(`{"dry_run":false,"request_id":"req-off-1"}`)
	if recOff1.Code != http.StatusOK {
		t.Fatalf("toggle dry_run=false failed: %d %s", recOff1.Code, recOff1.Body.String())
	}
	var ctrlResp DryRunControlResponse
	if err := json.NewDecoder(recOff1.Body).Decode(&ctrlResp); err != nil {
		t.Fatal(err)
	}
	if !ctrlResp.OK || ctrlResp.DryRun || !ctrlResp.Persisted || ctrlResp.Platform != "bilibili" || ctrlResp.RequestID != "req-off-1" {
		t.Fatalf("unexpected dry-run control response: %+v", ctrlResp)
	}
	if callbackCalls.Load() != beforeCalls+1 {
		t.Fatalf("expected 1 new callback call, got delta=%d", callbackCalls.Load()-beforeCalls)
	}

	// Replay with identical request_id + dry_run should NOT invoke OnDryRunChange again.
	recOffReplay := postDryRun(`{"dry_run":false,"request_id":"req-off-1"}`)
	if recOffReplay.Code != http.StatusOK || callbackCalls.Load() != beforeCalls+1 {
		t.Fatalf("expected idempotent 200 without callback re-run, got %d calls=%d", recOffReplay.Code, callbackCalls.Load())
	}

	// Replay with same request_id + different dry_run -> 409 idempotency_conflict.
	recConflict := postDryRun(`{"dry_run":true,"request_id":"req-off-1"}`)
	if recConflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 idempotency_conflict, got %d %s", recConflict.Code, recConflict.Body.String())
	}

	// Action now executes live when dry_run=false.
	actionPayload, _ := json.Marshal(ActionRequest{
		ProtocolVersion: 1,
		ActionID:        "act-live-after-toggle",
		RunID:           "run-live-1",
		IdempotencyKey:  "idem-live-1",
		Platform:        "bilibili",
		Kind:            "thread_reply",
		Text:            "live reply",
		ExpiresAt:       now.Add(time.Minute).Format(time.RFC3339Nano),
		Target: ActionTarget{
			Platform:    "bilibili",
			AccountID:   "bilibili-ego",
			ContainerID: "bilibili:BV1example0001",
			ContentID:   "rpid-orig",
		},
	})
	recActLive := httptest.NewRecorder()
	reqActLive := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(actionPayload)))
	reqActLive.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(recActLive, reqActLive)
	if recActLive.Code != http.StatusOK || exec.calls.Load() != 1 {
		t.Fatalf("expected live action 200 after dry_run=false, got %d calls=%d body=%s", recActLive.Code, exec.calls.Load(), recActLive.Body.String())
	}

	// Toggle back to dry_run=true and verify concurrent /health reads & action gating are race-free.
	recOn := postDryRun(`{"dry_run":true,"request_id":"req-on-1"}`)
	if recOn.Code != http.StatusOK || !srv.isDryRun() {
		t.Fatalf("expected toggle back to dry_run=true, got %d", recOn.Code)
	}
	done := make(chan struct{})
	for g := 0; g < 4; g++ {
		go func() {
			for i := 0; i < 20; i++ {
				rr := httptest.NewRecorder()
				handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "http://127.0.0.1/health", nil))
			}
			done <- struct{}{}
		}()
	}
	for g := 0; g < 4; g++ {
		<-done
	}

	// Trailing JSON and same-site Sec-Fetch-Site must be rejected on /v1/actions.
	trailingReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(actionPayload)+`{}`))
	trailingReq.Header.Set("Content-Type", "application/json")
	trailingRec := httptest.NewRecorder()
	handler.ServeHTTP(trailingRec, trailingReq)
	if trailingRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for trailing JSON on /v1/actions, got %d", trailingRec.Code)
	}
	sameSiteReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/control/dry-run", strings.NewReader(`{"dry_run":false,"request_id":"req-ss"}`))
	sameSiteReq.Header.Set("Content-Type", "application/json")
	sameSiteReq.Header.Set("Sec-Fetch-Site", "same-site")
	sameSiteRec := httptest.NewRecorder()
	handler.ServeHTTP(sameSiteRec, sameSiteReq)
	if sameSiteRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for Sec-Fetch-Site=same-site, got %d", sameSiteRec.Code)
	}
}
