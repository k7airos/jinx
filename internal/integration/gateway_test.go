package integration_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func gatewayJSON(t *testing.T, server *httptest.Server, method, path string, body, target any) {
	t.Helper()
	var response struct {
		OK    bool            `json:"ok"`
		Value json.RawMessage `json:"value"`
	}
	requestJSON(t, server.Client(), method, server.URL+path, body, &response)
	if !response.OK || len(response.Value) == 0 || json.Unmarshal(response.Value, target) != nil {
		t.Fatalf("invalid Gateway response: ok=%t value=%s", response.OK, response.Value)
	}
}

func gatewayGet(t *testing.T, server *httptest.Server, path string, target any) {
	t.Helper()
	gatewayJSON(t, server, http.MethodGet, path, nil, target)
}
