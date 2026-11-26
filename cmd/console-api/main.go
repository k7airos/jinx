package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jinx/internal/console"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8090", "loopback console address")
	dispatcher := flag.String("dispatcher", "", "required Dispatcher HTTP loopback origin (trace API discovered read-only)")
	relay := flag.String("relay", "", "optional Relay HTTP loopback origin")
	decision := flag.String("decision", "", "optional Decision HTTP loopback origin")
	bridge := flag.String("bridge", "", "optional Bridge HTTP loopback origin")
	stateDir := flag.String("state-dir", "", "required isolated console journal directory")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || (host != "localhost" && !net.ParseIP(host).IsLoopback()) {
		log.Fatal("console must listen on loopback")
	}
	facade, err := console.New(console.Options{DispatcherURL: *dispatcher, RelayURL: *relay, DecisionURL: *decision, BridgeURL: *bridge, StateDir: *stateDir})
	if err != nil {
		log.Fatal(err)
	}
	defer facade.Close()
	server := &http.Server{Addr: *listen, Handler: facade.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("console API listening on %s", *listen)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
