package relay

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingExtension struct {
	started  chan struct{}
	stopping chan struct{}
	release  chan struct{}
	once     sync.Once
	stops    atomic.Int32
}

func (*blockingExtension) Groups(context.Context) []GroupInfo   { return nil }
func (*blockingExtension) RegisterRoutes(RouteRegistrar) error  { return nil }
func (extension *blockingExtension) Start(func(*Message)) error { close(extension.started); return nil }
func (extension *blockingExtension) Stop() {
	extension.stops.Add(1)
	extension.once.Do(func() { close(extension.stopping) })
	<-extension.release
}

func TestStopJoinsSourceBeforeClosingHTTP(t *testing.T) {
	manager, err := NewConfigManager(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	cfg := manager.Get()
	cfg.Server.ListenAddr = address
	if err := manager.Update(cfg); err != nil {
		t.Fatal(err)
	}
	extension := &blockingExtension{started: make(chan struct{}), stopping: make(chan struct{}), release: make(chan struct{})}
	app := NewApp(manager, extension)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	select {
	case <-extension.started:
	case <-ctx.Done():
		t.Fatal("source never started")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- app.Stop(ctx) }()
	<-extension.stopping
	request, _ := http.NewRequestWithContext(ctx, "GET", "http://"+address+"/health", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		close(extension.release)
		t.Fatal("HTTP closed before source finished", err)
	}
	response.Body.Close()
	select {
	case <-stopDone:
		close(extension.release)
		t.Fatal("Stop returned before source finished")
	default:
	}
	close(extension.release)
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	var callers sync.WaitGroup
	for caller := 0; caller < 8; caller++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			if err := app.Stop(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	callers.Wait()
	if stops := extension.stops.Load(); stops != 1 {
		t.Fatalf("source Stop called %d times", stops)
	}
}
