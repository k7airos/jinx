// Command v2ex-connector is JINX's self-hosted Reference Connector for
// monitoring authorized V2EX topic replies via Action Connector HTTP v1. It
// defaults to dry_run=true, fails closed on missing login, captcha challenges,
// rate limits, or active user takeover, and does not bypass any platform controls.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"jinx/internal/connector"
	"jinx/internal/connector/v2ex"
)

func main() {
	cfgPath := flag.String("config", "config.json", "v2ex connector config path")
	once := flag.Bool("once", false, "run one comment poll pass and exit")
	flag.Parse()

	absCfg, err := filepath.Abs(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := v2ex.LoadConfig(absCfg)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := os.MkdirAll(cfg.StateDir, 0700); err != nil {
		log.Fatalf("create state_dir: %v", err)
	}
	conn, err := v2ex.NewConnector(cfg, nil)
	if err != nil {
		log.Fatalf("init connector: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *once {
		report, err := conn.PollOnce(ctx)
		log.Printf("v2ex-connector once report=%+v err=%v", report, err)
		if err != nil {
			os.Exit(1)
		}
		return
	}

	lockPath := filepath.Join(cfg.StateDir, ".v2ex-connector.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		log.Fatalf("open lock: %v", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		log.Fatal("another v2ex-connector instance is already running")
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.ListenAddr, err)
	}
	defer ln.Close()

	srv := &http.Server{
		Handler:           conn.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      time.Duration(cfg.Ego.TimeoutSec+15) * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	var regClient *connector.RegistryClient
	regDone := make(chan struct{})
	if cfg.Registry.URL != "" {
		regClient, err = connector.NewRegistryClient(cfg.Registry, ln.Addr().String(), conn, conn.IsDryRun)
		if err != nil {
			log.Fatalf("init registry client: %v", err)
		}
		go func() {
			defer close(regDone)
			regClient.Run(ctx)
		}()
	} else {
		close(regDone)
	}

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		runPollLoop(ctx, conn, time.Duration(cfg.PollIntervalSec)*time.Second)
	}()

	go func() {
		<-ctx.Done()
		if regClient != nil {
			unregCtx, unregCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = regClient.Unregister(unregCtx)
			unregCancel()
		}
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	log.Printf("v2ex-connector listening on http://%s (dry_run=%t, targets=%d)", ln.Addr(), cfg.IsDryRun(), len(cfg.Targets))
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("http server error: %v", err)
		stop()
	}
	<-pollDone
	<-regDone
	fmt.Fprintln(os.Stderr, "v2ex-connector stopped")
}

func runPollLoop(ctx context.Context, conn *v2ex.Connector, interval time.Duration) {
	run := func() {
		report, err := conn.PollOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("[poll] forwarded=%d seen=%d hist=%d errors=%d err=%v", report.Forwarded, report.SkippedSeen, report.SkippedHistorical, report.PushErrors, err)
		} else if report.Forwarded > 0 {
			log.Printf("[poll] forwarded=%d fetched=%d", report.Forwarded, report.CommentsFetched)
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
