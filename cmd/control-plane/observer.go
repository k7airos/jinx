package main

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strconv"

	"jinx/internal/supervisor"
)

type launchdObserver struct {
	uid    uint32
	labels []string
}

var _ supervisor.Commander = launchdObserver{}

type boundedOutput struct {
	bytes.Buffer
	limit int
}

func (output *boundedOutput) Write(data []byte) (int, error) {
	if output.Len()+len(data) > output.limit {
		return 0, errors.New("observer output exceeds limit")
	}
	return output.Buffer.Write(data)
}

func (observer launchdObserver) Run(ctx context.Context, executable string, arguments ...string) (string, string, int, error) {
	if executable != "/bin/launchctl" || len(arguments) != 2 || arguments[0] != "print" {
		return "", "", -1, errors.New("observer only allows fixed service readback")
	}
	domain := "gui/" + strconv.FormatUint(uint64(observer.uid), 10) + "/"
	allowedLabels := observer.labels
	if len(allowedLabels) == 0 {
		defaults := supervisor.DefaultLabels()
		for _, id := range []supervisor.ServiceID{supervisor.Relay, supervisor.Dispatcher, supervisor.Decision, supervisor.Bridge} {
			allowedLabels = append(allowedLabels, defaults[id])
		}
	}
	allowed := false
	for _, label := range allowedLabels {
		allowed = allowed || arguments[1] == domain+label
	}
	if !allowed {
		return "", "", -1, errors.New("observer target is not allowed")
	}
	command := exec.CommandContext(ctx, executable, arguments...)
	stdout := &boundedOutput{limit: 4 << 20}
	stderr := &boundedOutput{limit: 16 << 10}
	command.Stdout, command.Stderr = stdout, stderr
	err := command.Run()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return stdout.String(), stderr.String(), exitError.ExitCode(), nil
	}
	if err != nil {
		return "", "", -1, errors.New("observer readback failed")
	}
	return stdout.String(), stderr.String(), 0, nil
}
