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

type agyPromptEnvelope struct {
	Event   string `json:"event"`
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
}

func buildAgyArgs(options AgyOptions, model string, timeoutSec int) []string {
	args := []string{
		"--input-format", "stream-json",
		"--output-format", "stream-json",
		"--model", model,
	}
	if timeoutSec > 0 {
		args = append(args, "--print-timeout", strconv.Itoa(timeoutSec)+"s")
	}
	if options.Effort != "" {
		args = append(args, "--effort", options.Effort)
	}
	if options.Agent != "" {
		args = append(args, "--agent", options.Agent)
	}
	if options.Sandbox {
		args = append(args, "--sandbox")
	}
	if options.DisableSlashCommands {
		args = append(args, "--disable-slash-commands")
	}
	if options.SettingsFile != "" {
		args = append(args, "--settings", options.SettingsFile)
	}
	for _, dir := range options.AddDirs {
		args = append(args, "--add-dir", dir)
	}
	return args
}

func runAgy(ctx context.Context, options AgyOptions, model string, timeoutSec int, request Request, boundary func(func() error) error) (executionOutput, error) {
	var envelope agyPromptEnvelope
	envelope.Event = "user"
	envelope.Message.Content = requestText(request)
	stdinLine, err := json.Marshal(envelope)
	if err != nil {
		return executionOutput{}, failure("validation", "agy_prompt")
	}
	stdinLine = append(stdinLine, '\n')

	args := buildAgyArgs(options, model, timeoutSec)
	command := exec.CommandContext(ctx, options.Command, args...)
	command.Dir = options.Dir
	command.Env = append(os.Environ(), options.Env...)
	command.Stdin = bytes.NewReader(stdinLine)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGKILL) }
	command.WaitDelay = 2 * time.Second
	stdout := &limitedOutput{limit: 4 << 20}
	stderr := &limitedOutput{limit: 128 << 10}
	command.Stdout, command.Stderr = stdout, stderr
	output := executionOutput{remote: "not_applicable"}
	if err := boundary(func() error {
		if err := command.Start(); err != nil {
			return failure("backend_failed", "agy_start")
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
		return output, failure("backend_failed", "agy_command")
	}
	scanner := bufio.NewScanner(bytes.NewReader(stdout.Bytes()))
	scanner.Buffer(make([]byte, 4096), 4<<20)
	count := 0
	for scanner.Scan() {
		var event struct {
			Event  string `json:"event"`
			Result *struct {
				ConversationID string `json:"conversation_id"`
				Status         string `json:"status"`
				Response       string `json:"response"`
				Error          string `json:"error"`
			} `json:"result"`
		}
		if json.Unmarshal(scanner.Bytes(), &event) != nil || event.Event != "result" || event.Result == nil {
			continue
		}
		count++
		res := event.Result
		if res.Status != "SUCCESS" || res.Error != "" || !tokenPattern.MatchString(res.ConversationID) || strings.TrimSpace(res.Response) == "" {
			return output, failure("invalid_output", "agy_result")
		}
		output.text, output.external = res.Response, res.ConversationID
	}
	if scanner.Err() != nil || count != 1 {
		return output, failure("invalid_output", "agy_result_count")
	}
	return output, nil
}
