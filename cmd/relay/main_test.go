package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"jinx/internal/relay/config"
)

func TestListenerFailureExitsWithoutWaitingForSignal(t *testing.T) {
	if os.Getenv("RELAY_SYNTHETIC_START_FAILURE") == "1" {
		os.Args = append(os.Args[:1], flag.Args()...)
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		main()
		return
	}
	path := filepath.Join(t.TempDir(), "config.json")
	cfg := config.DefaultConfig()
	cfg.Server.ListenAddr = "synthetic-invalid-address"
	cfg.Upstream.Enabled = false
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestListenerFailureExitsWithoutWaitingForSignal$", "--", "-config", path)
	command.Env = append(os.Environ(), "RELAY_SYNTHETIC_START_FAILURE=1")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err = command.Run()
	if ctx.Err() != nil || err == nil {
		t.Fatalf("failed listener must promptly exit nonzero: %v %s", err, output.String())
	}
	if !strings.Contains(output.String(), "HTTP listener stopped before startup") || strings.Contains(output.String(), "HTTP 监听已启动") || strings.Contains(output.String(), "服务启动完成") || strings.Contains(output.String(), "exited cleanly") {
		t.Fatal("listener failure reported success", output.String())
	}
}
