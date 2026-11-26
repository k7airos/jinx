package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestArchitectureBridgeAvailabilityMatchesSendBoundary(t *testing.T) {
	for _, test := range []struct {
		name     string
		protocol any
		dryRun   any
		trusted  bool
		health   string
		enabled  bool
	}{
		{"ready", 2, false, true, "ready", true},
		{"dry_run", 2, true, true, "not_ready", false},
		{"old_protocol", 1, false, true, "not_ready", false},
		{"fractional_protocol", 2.5, false, true, "not_ready", false},
		{"missing_dry_run", 2, nil, true, "unknown", false},
		{"string_protocol", "2", false, true, "unknown", false},
		{"missing_protocol", nil, false, true, "unknown", false},
		{"permission_denied", 2, false, false, "not_ready", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			facade, _ := traceSetup(t)
			bridge := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet || request.URL.Path != "/health" {
					t.Errorf("architecture performed an action: %s %s", request.Method, request.URL.Path)
					http.NotFound(writer, request)
					return
				}
				health := map[string]any{"status": "ready", "group_routing": true, "serial_ui": true, "accessibility_trusted": test.trusted, "screen_capture_authorized": true}
				if test.protocol != nil {
					health["protocol_version"] = test.protocol
				}
				if test.dryRun != nil {
					health["dry_run"] = test.dryRun
				}
				_ = json.NewEncoder(writer).Encode(health)
			}))
			defer bridge.Close()
			facade.backend.bases["bridge"] = bridge.URL
			view := facade.Architecture(context.Background()).Actions[0]
			if view.State != test.health || !view.Availability.Supported || view.Availability.Enabled != test.enabled {
				t.Fatalf("send boundary differs: state=%s availability=%+v", view.State, view.Availability)
			}
			if !test.enabled && view.Availability.Reason == "" {
				t.Fatal("blocked send lacks a reason")
			}
		})
	}
}
