package dispatcher

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReminderAppTransportAndExplicitAuthorization(t *testing.T) {
	for _, operation := range []string{"read", "authorize", "status", "upsert"} {
		t.Run(operation, func(t *testing.T) {
			var path string
			response, err := launchReminderAppWith(context.Background(), demoReminderPolicy(), reminderRequest{Operation: operation}, func(_ context.Context, p string) error {
				path = p
				info, err := os.Stat(filepath.Dir(p))
				if err != nil || info.Mode().Perm() != 0700 {
					t.Fatal("insecure directory")
				}
				data, _ := os.ReadFile(p)
				if !strings.Contains(string(data), `"operation":"`+operation+`"`) {
					t.Fatal("operation changed")
				}
				return os.WriteFile(filepath.Join(filepath.Dir(p), "response.json"), []byte(`{"application":"DemoDesk Reminders","bundle_id":"org.example.demodesk-reminders","full_access":true}`), 0600)
			})
			if err != nil || !response.FullAccess {
				t.Fatal(response, err)
			}
			if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
				t.Fatal("request data retained")
			}
		})
	}
}
func TestReminderAppDeniedMalformedAndWrongIdentityFailClosed(t *testing.T) {
	for _, output := range []string{`{"error":"reminders_full_access_required"}`, `{broken`, `{"application":"Other","bundle_id":"other"}`} {
		_, err := launchReminderAppWith(context.Background(), demoReminderPolicy(), reminderRequest{Operation: "read"}, func(_ context.Context, p string) error {
			return os.WriteFile(filepath.Join(filepath.Dir(p), "response.json"), []byte(output), 0600)
		})
		if err == nil {
			t.Fatal("unsafe response accepted")
		}
	}
}
