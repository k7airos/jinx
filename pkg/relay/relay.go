package relay

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"jinx/internal/relay/config"
	"jinx/internal/relay/filter"
	"jinx/internal/relay/hub"
	"jinx/internal/relay/ingest"
	"jinx/internal/relay/model"
	"jinx/internal/relay/web"
)

type Message = model.Message
type MessageDetails = model.MessageDetails
type GroupInfo = model.GroupInfo
type FilterResult = model.FilterResult
type Config = config.Config
type ConfigManager = config.Manager
type GroupControl = config.GroupControl
type FilterConfig = config.FilterConfig
type ServerConfig = config.ServerConfig
type UpstreamConfig = config.UpstreamConfig
type PublicConfig = config.PublicConfig
type PublicSnapshot = config.PublicSnapshot
type Extension = web.Extension
type GroupProvider = web.GroupProvider
type PageProvider = web.PageProvider
type RouteRegistrar = web.RouteRegistrar
type ServerStats = model.ServerStats

const (
	TypeText   = model.TypeText
	TypeImage  = model.TypeImage
	TypeVoice  = model.TypeVoice
	TypeVideo  = model.TypeVideo
	TypeEmoji  = model.TypeEmoji
	TypeSystem = model.TypeSystem
)

var NewConfigManager = config.NewManager
var DefaultConfig = config.DefaultConfig
var MatchesGroup = config.MatchesGroup
var WritePrivateJSON = config.WritePrivateJSON
var ExpandPath = config.ExpandPath
var GetTypeName = ingest.GetTypeName
var SummarizeContent = ingest.SummarizeContent
var ParseMessageDetails = ingest.ParseMessageDetails
var SanitizeMessage = ingest.SanitizeMessage
var ErrRevisionConflict = config.ErrRevisionConflict

type App struct {
	server    *web.Server
	upstream  *ingest.UpstreamListener
	extension Extension
	mu        sync.Mutex
	started   bool
	stopped   bool
	stopOnce  sync.Once
}

func NewApp(manager *ConfigManager, extension Extension) *App {
	events := hub.NewHub(200)
	server := web.NewServer(manager, events, filter.NewEngine(), extension)
	return &App{server: server, extension: extension, upstream: ingest.NewUpstreamListener(manager, events, func(message *Message) { server.ProcessIncomingMessage(message) })}
}

func (a *App) Handler() (http.Handler, error) { return a.server.Handler() }
func (a *App) Ready() <-chan struct{}         { return a.server.Ready() }
func (a *App) ProcessIncomingMessage(message *Message) FilterResult {
	return a.server.ProcessIncomingMessage(message)
}

func (a *App) Run(ctx context.Context) error {
	a.mu.Lock()
	if a.started || a.stopped {
		a.mu.Unlock()
		return fmt.Errorf("app already started or stopped")
	}
	a.started = true
	a.mu.Unlock()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- a.server.Start() }()
	defer func() {
		a.stopSources()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.server.Stop(shutdown)
	}()
	select {
	case err := <-serverErrors:
		return fmt.Errorf("HTTP listener stopped before startup: %v", err)
	case <-ctx.Done():
		return nil
	case <-a.server.Ready():
	}
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return nil
	}
	if a.extension != nil {
		if err := a.extension.Start(func(message *Message) { a.server.ProcessIncomingMessage(message) }); err != nil {
			a.mu.Unlock()
			return fmt.Errorf("start extension: %w", err)
		}
	}
	a.upstream.Start()
	a.mu.Unlock()
	select {
	case err := <-serverErrors:
		a.mu.Lock()
		stopped := a.stopped
		a.mu.Unlock()
		if stopped {
			return nil
		}
		return fmt.Errorf("HTTP listener stopped unexpectedly: %v", err)
	case <-ctx.Done():
		return nil
	}
}

func (a *App) stopSources() {
	a.stopOnce.Do(func() {
		a.mu.Lock()
		a.stopped = true
		a.mu.Unlock()
		if a.extension != nil {
			a.extension.Stop()
		}
		a.upstream.Stop()
	})
}

func (a *App) Stop(ctx context.Context) error {
	a.stopSources()
	return a.server.Stop(ctx)
}
