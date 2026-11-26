package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"jinx/internal/console"
	"jinx/internal/execution"
	"jinx/internal/gateway"
	"jinx/internal/supervisor"
)

func main() {
	config, err := parseSettings(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, config); err != nil {
		log.Fatal(err)
	}
}

func assemble(config settings) (http.Handler, func(), error) {
	coordinator, err := gateway.NewDispatcherDrainCoordinator(config.dispatcher, nil)
	if err != nil {
		return nil, nil, errors.New("invalid Dispatcher origin")
	}
	backends, err := loadBackends(config.backendDefinitions)
	if err != nil {
		return nil, nil, err
	}
	engine, err := execution.Open(config.executionConfig, execution.Options{Backends: backends})
	if err != nil {
		return nil, nil, errors.New("cannot initialize execution state or backend definitions")
	}
	facade, err := console.New(console.Options{
		DispatcherURL:        config.dispatcher,
		RelayURL:             config.relay,
		DecisionURL:          config.decision,
		BridgeURL:            config.bridge,
		BilibiliConnectorURL: config.bilibiliConnector,
		WebConnectorURL:      config.webConnector,
		Connectors:           config.connectors,
		StateDir:             filepath.Join(config.stateDir, "console"),
		ExecutionCatalog:     engine,
	})
	if err != nil {
		return nil, nil, errors.New("cannot initialize console facade")
	}
	options := supervisor.Options{EnableControl: config.enableControl, PlistDir: config.plistDir, ServiceLabels: config.serviceLabels, DrainCoordinator: coordinator}
	if config.serviceUID >= 0 {
		uid := uint32(config.serviceUID)
		resolved, _ := supervisor.ResolveLabels(config.serviceLabels)
		observerLabels := make([]string, 0, len(resolved))
		for _, id := range []supervisor.ServiceID{supervisor.Relay, supervisor.Dispatcher, supervisor.Decision, supervisor.Bridge} {
			observerLabels = append(observerLabels, resolved[id])
		}
		options.UID, options.Observer = &uid, launchdObserver{uid: uid, labels: observerLabels}
		options.DataPath = filepath.Join(config.stateDir, "supervisor.json")
	}
	manager, err := supervisor.New(options)
	if err != nil {
		facade.Close()
		return nil, nil, errors.New("cannot initialize supervisor journal")
	}
	cleanup := func() {
		manager.Close()
		facade.Close()
	}
	control, err := gateway.New(gateway.Options{Console: facade, Execution: engine, Supervisor: manager, DispatcherURL: config.dispatcher})
	if err != nil {
		cleanup()
		return nil, nil, errors.New("cannot initialize control gateway")
	}
	return control.Handler(), cleanup, nil
}

func serve(ctx context.Context, config settings) error {
	handler, cleanup, err := assemble(config)
	if err != nil {
		return err
	}
	defer cleanup()
	server := &http.Server{Addr: config.listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		case <-finished:
		}
	}()
	log.Printf("control-plane listening on %s; service control enabled=%t", config.listen, config.enableControl)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return errors.New("control-plane HTTP server failed")
	}
	return nil
}
