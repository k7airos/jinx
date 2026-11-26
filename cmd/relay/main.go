package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"jinx/internal/relay/config"
	"jinx/pkg/relay"
)

func main() {
	if err := run(); err != nil {
		log.Printf("[Main] %v", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config.json", "Path to config.json")
	port := flag.String("port", "", "Server listen port (e.g. :8080)")
	upstream := flag.String("upstream", "", "Upstream WebSocket URL (e.g. ws://127.0.0.1:5032)")
	flag.Parse()

	log.Println("==================================================")
	log.Println(" WeChat Group Message Relay & AI Gateway (Go)   ")
	log.Println("==================================================")

	// 1. Load configuration
	cfgMgr, err := config.NewManager(*configPath)
	if err != nil {
		return fmt.Errorf("initialize config: %w", err)
	}

	cfg := cfgMgr.Get()
	if *port != "" {
		addr := strings.TrimSpace(*port)
		if strings.HasPrefix(addr, ":") {
			addr = "127.0.0.1" + addr
		} else if !strings.Contains(addr, ":") {
			addr = "127.0.0.1:" + addr
		} else {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return fmt.Errorf("invalid -port address %q: %w", *port, err)
			}
			if host != "localhost" {
				ip := net.ParseIP(strings.Trim(host, "[]"))
				if ip == nil || !ip.IsLoopback() {
					return fmt.Errorf("listen address %q must bind to loopback", addr)
				}
			}
		}
		cfg.Server.ListenAddr = addr
	}
	if *upstream != "" {
		if err := config.ValidateLoopbackWSURL(*upstream); err != nil {
			return err
		}
		cfg.Upstream.WSURL = *upstream
		cfg.Upstream.Enabled = true
	}
	if *port != "" || *upstream != "" {
		if err := cfgMgr.Update(cfg); err != nil {
			return fmt.Errorf("save config overrides: %w", err)
		}
	}

	log.Printf("[Main] Config loaded from %s (Listen: %s)", *configPath, cfg.Server.ListenAddr)

	// 2. Initialize filter engine & event hub
	app := relay.NewApp(cfgMgr, nil)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)
	serverErrors := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { serverErrors <- app.Run(ctx) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Stop(ctx); err != nil {
			log.Printf("[Main] Server shutdown error: %v", err)
		}
	}()
	select {
	case err := <-serverErrors:
		return fmt.Errorf("HTTP listener stopped before startup: %v", err)
	case <-app.Ready():
	case <-quit:
		return nil
	}

	fmt.Printf("\nHTTP 监听已启动：%s\n", cfg.Server.ListenAddr)
	fmt.Println("👉 AI 实时 SSE 流地址:        http://localhost" + cfg.Server.ListenAddr + "/api/v1/stream")
	fmt.Println("👉 AI 实时 WebSocket 流地址:  ws://localhost" + cfg.Server.ListenAddr + "/api/v1/ws")
	fmt.Println("👉 上游 Webhook 接收地址:     http://localhost" + cfg.Server.ListenAddr + "/api/webhook")
	fmt.Print("\n按 Ctrl+C 停止服务...\n\n")

	// Wait for termination signal
	select {
	case err := <-serverErrors:
		return fmt.Errorf("HTTP listener stopped unexpectedly: %v", err)
	case <-quit:
	}

	log.Println("\n[Main] Shutting down WeChat Group Relay...")
	shutdown, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()

	if err := app.Stop(shutdown); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	log.Println("[Main] Server exited cleanly. Bye!")
	return nil
}
