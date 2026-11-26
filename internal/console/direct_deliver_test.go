package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func TestConsoleDirectDeliverProxyAndErrorMapping(t *testing.T) {
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/direct-deliver" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req DirectDeliverInput
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		switch req.TargetContainerID {
		case "forbidden@chatroom":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"container_not_allowlisted","error":"not allowlisted"}`))
		case "conflict@chatroom":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"idempotency_key_conflict","error":"mismatch"}`))
		default:
			_ = json.NewEncoder(w).Encode(DirectDeliverResult{
				RunID:             "run-direct-demo-001",
				Replayed:          false,
				DryRun:            req.DryRun,
				TargetContainerID: req.TargetContainerID,
				Trigger:           "direct:" + req.Source,
				Phase:             contracts.PhaseTerminal,
				Outcome:           contracts.OutcomeReplied,
				Delivery:          contracts.DeliveryConfirmed,
			})
		}
	}))
	defer upstreamSrv.Close()

	facade, err := New(Options{DispatcherURL: upstreamSrv.URL, StateDir: t.TempDir(), Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = facade.Close() }()

	// 1. Proxies valid request to Dispatcher and wraps response cleanly.
	rec := consolePost(facade, "direct-deliver", DirectDeliverInput{
		TargetContainerID: "100001@chatroom",
		Text:              "推特快讯推送测试",
		IdempotencyKey:    "tw-proxy-1",
		Source:            "twitter",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /console/v1/direct-deliver status=%d body=%s", rec.Code, rec.Body.String())
	}
	var env struct {
		OK    bool                `json:"ok"`
		Value DirectDeliverResult `json:"value"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil || !env.OK || env.Value.RunID != "run-direct-demo-001" || env.Value.Trigger != "direct:twitter" {
		t.Fatalf("unexpected console direct-deliver response: %s %v", rec.Body.String(), err)
	}

	// 2. Rejects unknown fields cleanly via decodeInput.
	badReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/console/v1/direct-deliver", strings.NewReader(`{"target_container_id":"100001@chatroom","text":"hi","idempotency_key":"k1","unexpected":true}`))
	badReq.Header.Set("Content-Type", "application/json")
	badRec := httptest.NewRecorder()
	facade.Handler().ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on unknown field, got %d %s", badRec.Code, badRec.Body.String())
	}

	// 3. Maps upstream 403 and 409 into standard console problem statuses.
	forbRec := consolePost(facade, "direct-deliver", DirectDeliverInput{
		TargetContainerID: "forbidden@chatroom",
		Text:              "hi",
		IdempotencyKey:    "k2",
	})
	if forbRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 mapped, got %d %s", forbRec.Code, forbRec.Body.String())
	}

	confRec := consolePost(facade, "direct-deliver", DirectDeliverInput{
		TargetContainerID: "conflict@chatroom",
		Text:              "hi",
		IdempotencyKey:    "k3",
	})
	if confRec.Code != http.StatusConflict {
		t.Fatalf("expected 409 mapped, got %d %s", confRec.Code, confRec.Body.String())
	}
}
