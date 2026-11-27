// Command bilibili-connector is JINX's self-hosted Reference Connector for
// managing a creator's own authorized Bilibili video comments via Action
// Connector HTTP v1. It defaults to dry_run=true, fails closed on missing login,
// captcha challenges, rate limits, or active user takeover, and does not
// bypass any platform controls.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"jinx/internal/connector/bilibili"
)

func main() {
	cfgPath := flag.String("config", "", "bilibili connector config path")
	once := flag.Bool("once", false, "run one comment poll pass and exit")
	searchKw := flag.String("search", "", "search Bilibili videos by keyword and exit")
	searchOrder := flag.String("search-order", "totalrank", "search sort order: totalrank|pubdate|click|dm|stow")
	searchLimit := flag.Int("search-limit", 5, "number of videos to return when -search is set")
	showComments := flag.Bool("show-comments", false, "print collected comments when -once is set")
	skipRelay := flag.Bool("skip-relay", false, "with -once, inspect comments without pushing to Relay or writing cursor")
	jsonOut := flag.Bool("json", false, "print JSON output for -search or -once")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if strings.TrimSpace(*searchKw) != "" {
		egoCfg := bilibili.EgoConfig{
			TaskSpace:  "jinx-bilibili-comments",
			TimeoutSec: 45,
		}
		if *cfgPath != "" {
			absCfg, err := filepath.Abs(*cfgPath)
			if err != nil {
				log.Fatal(err)
			}
			cfg, err := bilibili.LoadConfig(absCfg)
			if err != nil {
				log.Fatalf("load config: %v", err)
			}
			egoCfg = cfg.Ego
		}
		drv := bilibili.NewEgoDriver(egoCfg)
		searchCtx, cancel := context.WithTimeout(ctx, time.Duration(egoCfg.TimeoutSec)*time.Second)
		videos, err := drv.SearchVideos(searchCtx, bilibili.SearchQuery{
			Keyword: *searchKw,
			Order:   *searchOrder,
			Limit:   *searchLimit,
		})
		cancel()
		if err != nil {
			log.Fatalf("search videos: %v", err)
		}
		if *jsonOut {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(videos); err != nil {
				log.Fatal(err)
			}
			return
		}
		for i, v := range videos {
			fmt.Printf("[%d] %s | oid=%s | comments=%d | plays=%d | author=%s | %s\n",
				i+1, v.BVID, v.OID, v.CommentCount, v.PlayCount, v.Author, v.Title)
		}
		return
	}

	resolvedCfgPath := *cfgPath
	if resolvedCfgPath == "" {
		resolvedCfgPath = "config.json"
	}
	absCfg, err := filepath.Abs(resolvedCfgPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := bilibili.LoadConfig(absCfg)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := os.MkdirAll(cfg.StateDir, 0700); err != nil {
		log.Fatalf("create state_dir: %v", err)
	}
	conn, err := bilibili.NewConnector(cfg, nil)
	if err != nil {
		log.Fatalf("init connector: %v", err)
	}

	if *once {
		report, err := conn.PollOnceWithOptions(ctx, bilibili.PollOptions{SkipRelay: *skipRelay})
		if *jsonOut {
			out := struct {
				Report   bilibili.PollReport `json:"report"`
				Comments []bilibili.Comment  `json:"comments,omitempty"`
				Error    string              `json:"error,omitempty"`
			}{
				Report: report,
			}
			if *showComments {
				out.Comments = report.FetchedComments
			}
			if err != nil {
				out.Error = err.Error()
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(out)
		} else {
			log.Printf("bilibili-connector once report={discovered:%d polled:%d fetched:%d forwarded:%d relay_blocked:%d seen:%d hist:%d push_errors:%d} err=%v",
				report.VideosDiscovered, report.TargetsPolled, report.CommentsFetched, report.Forwarded, report.RelayBlocked, report.SkippedSeen, report.SkippedHistorical, report.PushErrors, err)
			if *showComments {
				printComments(report.FetchedComments)
			}
		}
		if err != nil {
			os.Exit(1)
		}
		return
	}

	lockPath := filepath.Join(cfg.StateDir, ".bilibili-connector.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		log.Fatalf("open lock: %v", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		log.Fatal("another bilibili-connector instance is already running")
	}

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           conn.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      time.Duration(cfg.Ego.TimeoutSec+15) * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		runPollLoop(ctx, conn, time.Duration(cfg.PollIntervalSec)*time.Second)
	}()

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	log.Printf("bilibili-connector listening on http://%s (dry_run=%t, targets=%d, search_targets=%d)", cfg.ListenAddr, cfg.IsDryRun(), len(cfg.Targets), len(cfg.SearchTargets))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("http server error: %v", err)
		stop()
	}
	<-pollDone
	fmt.Fprintln(os.Stderr, "bilibili-connector stopped")
}

func printComments(comments []bilibili.Comment) {
	for i, c := range comments {
		kind := "comment"
		if c.RootID != "" {
			kind = "reply->" + c.RootID
		}
		ts := ""
		if c.TimestampMS > 0 {
			ts = time.UnixMilli(c.TimestampMS).Format("2006-01-02 15:04:05")
		}
		msg := strings.Join(strings.Fields(c.Content), " ")
		fmt.Printf("[%d] %s (%s) %s | %s (uid:%s) | %s\n",
			i+1, c.BVID, kind, ts, c.SenderName, c.SenderID, msg)
	}
}

func runPollLoop(ctx context.Context, conn *bilibili.Connector, interval time.Duration) {
	run := func() {
		report, err := conn.PollOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("[poll] discovered=%d forwarded=%d relay_blocked=%d seen=%d hist=%d errors=%d err=%v",
				report.VideosDiscovered, report.Forwarded, report.RelayBlocked, report.SkippedSeen, report.SkippedHistorical, report.PushErrors, err)
		} else if report.Forwarded > 0 || report.VideosDiscovered > 0 || report.RelayBlocked > 0 {
			log.Printf("[poll] discovered=%d fetched=%d forwarded=%d relay_blocked=%d",
				report.VideosDiscovered, report.CommentsFetched, report.Forwarded, report.RelayBlocked)
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
