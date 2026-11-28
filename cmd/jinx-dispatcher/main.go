package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"jinx/internal/dispatcher"
	"jinx/internal/execution"
	"jinx/internal/runtimebridge"
)

type arguments struct {
	config             string
	executionConfig    string
	backendsJSON       string
	localPolicy        string
	connectorsDir      string
	connectorTokenFile string
	checkConfig        bool
}

func parseArgs(argv []string, output io.Writer) (arguments, error) {
	args := arguments{}
	flags := flag.NewFlagSet("jinx-dispatcher", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&args.config, "config", "", "dispatcher configuration (required)")
	flags.StringVar(&args.executionConfig, "execution-config", "", "existing execution engine store (required)")
	flags.StringVar(&args.backendsJSON, "backends-json", "", "private operator backend definitions (required)")
	flags.StringVar(&args.localPolicy, "local-policy", "", "optional operator-owned local policy file (0600); defaults to JINX_LOCAL_POLICY")
	flags.StringVar(&args.connectorsDir, "connectors-dir", "", "optional drop-in directory (*.json) for connector auto-discovery")
	flags.StringVar(&args.connectorTokenFile, "connector-token-file", "", "optional 0600 token file enabling authenticated connector self-registration")
	flags.BoolVar(&args.checkConfig, "check-config", false, "validate dispatcher configuration without starting services")
	if err := flags.Parse(argv); err != nil {
		return args, err
	}
	if args.checkConfig {
		if flags.NArg() != 0 || args.config == "" {
			return args, errors.New("explicit -config is required for -check-config")
		}
	} else if flags.NArg() != 0 || args.config == "" || args.executionConfig == "" || args.backendsJSON == "" {
		return args, errors.New("explicit -config, -execution-config and -backends-json are required")
	}
	if args.localPolicy == "" {
		args.localPolicy = os.Getenv("JINX_LOCAL_POLICY")
	}
	for _, path := range []*string{&args.config, &args.executionConfig, &args.backendsJSON, &args.localPolicy, &args.connectorsDir, &args.connectorTokenFile} {
		if *path == "" {
			continue
		}
		absolute, err := filepath.Abs(*path)
		if err != nil {
			return args, err
		}
		*path = absolute
	}
	return args, nil
}

func acquireLock(configPath string) (*os.File, error) {
	lock, err := os.OpenFile(filepath.Join(filepath.Dir(configPath), ".dispatcher.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("another dispatcher is already running")
	}
	return lock, nil
}

type dispatcherService interface {
	Start()
	Close()
	Handler() http.Handler
}

type dispatcherFactory func(string, dispatcher.Options) (dispatcherService, error)

func loadDispatcher(args arguments, create dispatcherFactory) (dispatcherService, string, error) {
	options, err := runtimebridge.LoadBackendOptions(args.backendsJSON)
	if err != nil {
		return nil, "", fmt.Errorf("operator backends rejected: %w", err)
	}
	if _, err := os.Stat(args.executionConfig); err != nil {
		return nil, "", fmt.Errorf("existing execution store required: %w", err)
	}
	engine, err := execution.Open(args.executionConfig, options)
	if err != nil {
		return nil, "", err
	}
	runner, err := runtimebridge.NewRunner(engine)
	if err != nil {
		return nil, "", err
	}
	file, err := os.Open(args.config)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil || len(data) > 4<<20 {
		return nil, "", errors.New("dispatcher config unreadable or too large")
	}
	var config dispatcher.Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, "", err
	}
	host, _, err := net.SplitHostPort(config.Server.ListenAddr)
	if err != nil || (host != "127.0.0.1" && host != "::1" && host != "localhost") {
		return nil, "", errors.New("dispatcher listen address must bind to loopback")
	}
	service, err := create(args.config, dispatcher.Options{
		Runner:             runner,
		LocalPolicyPath:    args.localPolicy,
		ConnectorsDir:      args.connectorsDir,
		ConnectorTokenFile: args.connectorTokenFile,
	})
	return service, config.Server.ListenAddr, err
}

func run(ctx context.Context, argv []string, output io.Writer) error {
	args, err := parseArgs(argv, output)
	if err != nil {
		return err
	}
	if args.checkConfig {
		if err := dispatcher.ValidateConfigFileWithOptions(args.config, dispatcher.Options{
			LocalPolicyPath:    args.localPolicy,
			ConnectorsDir:      args.connectorsDir,
			ConnectorTokenFile: args.connectorTokenFile,
		}); err != nil {
			return err
		}
		_, err := fmt.Fprintln(output, "configuration valid")
		return err
	}
	lock, err := acquireLock(args.config)
	if err != nil {
		return err
	}
	defer lock.Close()
	service, address, err := loadDispatcher(args, func(path string, options dispatcher.Options) (dispatcherService, error) {
		return dispatcher.NewDispatcherWithOptions(path, options)
	})
	if err != nil {
		return err
	}
	defer service.Close()
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: service.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	service.Start()
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			service.Close()
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		case <-done:
		}
	}()
	log.Printf("jinx-dispatcher listening on %s; execution runner connected", listener.Addr())
	err = server.Serve(listener)
	close(done)
	<-stopped
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stderr)
	cancel()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		log.Print(err)
		os.Exit(1)
	}
}
