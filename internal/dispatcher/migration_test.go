package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func configRequest(d *Dispatcher, method, body, version string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "http://localhost/api/config", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("If-Match", version)
	response := httptest.NewRecorder()
	d.Handler().ServeHTTP(response, request)
	return response
}

func TestConfigRoundTripPreservesNilAndEmptyArrays(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%t", empty), func(t *testing.T) {
			cfg := testConfig()
			cfg.Rules.SelfSenderIDs = []string{"global-self"}
			rule := cfg.Rules.GroupRules[groupA]
			if empty {
				rule.SelfSenderIDs, rule.AllowedTypes = []string{}, []int{}
			}
			cfg.Rules.GroupRules[groupA] = rule
			d := newTestDispatcher(t, cfg)
			get := configRequest(d, http.MethodGet, "", "")
			if get.Code != http.StatusOK {
				t.Fatal(get.Code, get.Body.String())
			}
			put := configRequest(d, http.MethodPut, get.Body.String(), get.Header().Get("ETag"))
			if put.Code != http.StatusOK {
				t.Fatal(put.Code, put.Body.String())
			}
			patch := configRequest(d, http.MethodPost, `{"idle_timeout_sec":2}`, put.Header().Get("ETag"))
			if patch.Code != http.StatusOK {
				t.Fatal(patch.Code, patch.Body.String())
			}
			data, err := os.ReadFile(d.cfgFile)
			if err != nil {
				t.Fatal(err)
			}
			saved, err := decodeConfig(data)
			if err != nil {
				t.Fatal(err)
			}
			for _, current := range []Config{d.configSnapshot(), saved} {
				got := current.Rules.GroupRules[groupA]
				if (got.SelfSenderIDs != nil) != empty || (got.AllowedTypes != nil) != empty {
					t.Fatalf("nil/empty changed: %+v", got)
				}
				message := testMessage(groupA, "identity")
				message.SenderID = "global-self"
				if isSelf(message, current) == empty {
					t.Fatal("self identity inheritance changed")
				}
				resolved, _ := current.route(groupA)
				if (len(resolved.AllowedTypes) == 0) != empty {
					t.Fatal("allowed types inheritance changed")
				}
				if _, exists := current.Rules.GroupRules[groupB]; !exists {
					t.Fatal("unrelated group lost")
				}
			}
			stale := configRequest(d, http.MethodPost, `{"dry_run":true}`, get.Header().Get("ETag"))
			if stale.Code != http.StatusConflict {
				t.Fatal("stale version accepted", stale.Code)
			}
		})
	}
}

func TestConfigWriterDoesNotBlockDeliveryEcho(t *testing.T) {
	var dispatcher *Dispatcher
	submitted := make(chan struct{})
	release := make(chan struct{})
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var payload struct{ Text string }
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		close(submitted)
		<-release
		echo := testMessage(groupA, "echo-with-writer")
		echo.IsEcho, echo.Content = true, payload.Text
		if err := dispatcher.handleIncomingMessage(echo, "echo:1"); err != nil {
			t.Error(err)
		}
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	dispatcher = newTestDispatcher(t, cfg)
	dispatcher.echoTimeout = time.Second
	snapshot := dispatcher.configSnapshot()
	rule, _ := snapshot.route(groupA)
	work := batch{GroupID: groupA, Config: snapshot, Rule: rule, Policy: policyID(snapshot), Messages: []RelayMessage{testMessage(groupA, "source")}}
	delivered := make(chan error, 1)
	go func() { delivered <- dispatcher.deliver(context.Background(), work, "reply") }()
	select {
	case <-submitted:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("delivery not submitted")
	}
	writerStarted := make(chan struct{})
	saved := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		close(writerStarted)
		saved <- configRequest(dispatcher, http.MethodPost, `{"dry_run":true}`, work.Policy)
	}()
	<-writerStarted
	time.Sleep(30 * time.Millisecond)
	close(release)
	select {
	case err := <-delivered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("configuration writer blocked the echo")
	}
	select {
	case response := <-saved:
		if response.Code != http.StatusOK {
			t.Fatal(response.Code, response.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("configuration writer did not complete")
	}
	if err := dispatcher.deliver(context.Background(), work, "stale"); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatal("stale delivery accepted", err)
	}
}

func TestHealthReadsOnlyMemory(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	d.client = &http.Client{Transport: rejectingTransport{t}}
	if err := os.Remove(d.cfgFile); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://localhost/health", nil)
	response := httptest.NewRecorder()
	d.Handler().ServeHTTP(response, request)
	var health Health
	if err := json.Unmarshal(response.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || health.Service != "dispatcher" || health.RelayConnected || health.LastProgressAt != nil {
		t.Fatal(response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "bin_path") || strings.Contains(response.Body.String(), "group_rules") {
		t.Fatal("health leaked configuration")
	}
	d.Close()
	response = httptest.NewRecorder()
	d.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatal("closed dispatcher reported ready")
	}
}

type rejectingTransport struct{ testing *testing.T }

func (transport rejectingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	transport.testing.Error("health made a network request")
	return nil, fmt.Errorf("unexpected network request")
}
