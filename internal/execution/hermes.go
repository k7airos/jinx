package execution

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type limitedOutput struct {
	bytes.Buffer
	limit int
}

func (output *limitedOutput) Write(data []byte) (int, error) {
	if output.Len()+len(data) > output.limit {
		return 0, failure("too_large", "command_output")
	}
	return output.Buffer.Write(data)
}

func runHermes(ctx context.Context, options HermesOptions, model string, request Request, boundary func(func() error) error) (executionOutput, error) {
	args := []string{"-p", options.Profile, "chat", "--query-file", "-", "-Q", "--format", "stream-json", "--max-turns", strconv.Itoa(options.MaxTurns), "-m", model, "-t", strings.Join(options.Toolsets, ",")}
	command := exec.CommandContext(ctx, options.Command, args...)
	command.Dir = options.Dir
	command.Env = append(os.Environ(), options.Env...)
	command.Stdin = strings.NewReader(requestText(request))
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }
	command.WaitDelay = 2 * time.Second
	stdout := &limitedOutput{limit: 4 << 20}
	stderr := &limitedOutput{limit: 128 << 10}
	command.Stdout, command.Stderr = stdout, stderr
	output := executionOutput{remote: "not_applicable"}
	if err := boundary(func() error {
		if err := command.Start(); err != nil {
			return failure("backend_failed", "hermes_start")
		}
		output.remote = "unknown"
		return nil
	}); err != nil {
		return output, err
	}
	if err := command.Wait(); err != nil {
		if ctx.Err() != nil {
			return output, ctx.Err()
		}
		return output, failure("backend_failed", "hermes_command")
	}
	scanner := bufio.NewScanner(bytes.NewReader(stdout.Bytes()))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	count := 0
	for scanner.Scan() {
		var event struct {
			Type      string `json:"type"`
			Text      string `json:"text"`
			SessionID string `json:"session_id"`
			ExitCode  *int   `json:"exit_code"`
			Error     string `json:"error"`
		}
		if json.Unmarshal(scanner.Bytes(), &event) != nil || event.Type != "result" {
			continue
		}
		count++
		if event.ExitCode == nil || *event.ExitCode != 0 || event.Error != "" || !tokenPattern.MatchString(event.SessionID) || strings.TrimSpace(event.Text) == "" {
			return output, failure("invalid_output", "hermes_result")
		}
		output.text, output.external = event.Text, event.SessionID
	}
	if scanner.Err() != nil || count != 1 {
		return output, failure("invalid_output", "hermes_result_count")
	}
	return output, nil
}
