package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// LaunchServices gives EventKit an independent, stable app identity instead of
// inheriting the terminal host or the dispatcher's TCC responsibility.
func launchReminderApp(parent context.Context, policy reminderPolicy, req reminderRequest) (reminderResponse, error) {
	return launchReminderAppWith(parent, policy, req, func(ctx context.Context, requestPath string) error {
		return exec.CommandContext(ctx, "/usr/bin/open", "-n", "-g", "-W", "-a", policy.AppPath, "--args", "--request-file", requestPath).Run()
	})
}

func launchReminderAppWith(parent context.Context, policy reminderPolicy, req reminderRequest, launch func(context.Context, string) error) (reminderResponse, error) {
	var response reminderResponse
	if !policy.Enabled || policy.Application == "" || policy.BundleID == "" || policy.AppPath == "" {
		return response, fmt.Errorf("reminders app disabled by local policy")
	}
	timeout := 25 * time.Second
	if req.Operation == "authorize" {
		timeout = 130 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	dir, err := os.MkdirTemp("", "support-reminders-")
	if err != nil {
		return response, err
	}
	defer os.RemoveAll(dir)
	data, err := json.Marshal(req)
	if err != nil {
		return response, err
	}
	requestPath := filepath.Join(dir, "request.json")
	if err = os.WriteFile(requestPath, data, 0600); err != nil {
		return response, err
	}
	if err = launch(ctx, requestPath); err != nil {
		return response, fmt.Errorf("reminders app launch failed")
	}
	output, err := os.ReadFile(filepath.Join(dir, "response.json"))
	if err != nil {
		return response, fmt.Errorf("reminders app response unavailable")
	}
	var failure struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(output, &failure) != nil {
		return response, fmt.Errorf("invalid reminders response")
	}
	if failure.Error != "" {
		switch failure.Error {
		case "authorization_pending_user_action", "reminders_full_access_required":
			return response, fmt.Errorf("%s", failure.Error)
		default:
			return response, fmt.Errorf("reminders app operation failed")
		}
	}
	if json.Unmarshal(output, &response) != nil || response.BundleID != policy.BundleID || response.Application != policy.Application {
		return response, fmt.Errorf("invalid reminders app identity")
	}
	return response, nil
}
