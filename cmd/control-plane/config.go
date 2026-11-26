package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"jinx/internal/execution"
	"jinx/internal/runtimebridge"
	"jinx/internal/supervisor"
)

type settings struct {
	listen             string
	dispatcher         string
	relay              string
	decision           string
	bridge             string
	bilibiliConnector  string
	webConnector       string
	stateDir           string
	executionConfig    string
	backendDefinitions string
	serviceUID         int
	plistDir           string
	serviceLabelsRaw   string
	serviceLabels      map[supervisor.ServiceID]string
	enableControl      bool
}

func parseSettings(arguments []string, output io.Writer) (settings, error) {
	var config settings
	flags := flag.NewFlagSet("control-plane", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&config.listen, "listen", "127.0.0.1:8090", "HTTP loopback listen address")
	flags.StringVar(&config.dispatcher, "dispatcher", "", "required Dispatcher HTTP loopback origin")
	flags.StringVar(&config.relay, "relay", "", "optional Relay HTTP loopback origin")
	flags.StringVar(&config.decision, "decision", "", "optional Decision HTTP loopback origin")
	flags.StringVar(&config.bridge, "bridge", "", "optional Bridge HTTP loopback origin")
	flags.StringVar(&config.bilibiliConnector, "bilibili-connector", "", "optional Bilibili connector HTTP loopback origin")
	flags.StringVar(&config.webConnector, "web-connector", "", "optional Web connector HTTP loopback origin")
	flags.StringVar(&config.stateDir, "state-dir", "", "required absolute isolated control-plane data directory")
	flags.StringVar(&config.executionConfig, "execution-config", "", "required execution state JSON path inside state-dir; initialized empty when absent")
	flags.StringVar(&config.backendDefinitions, "backend-definitions", "", "required trusted schemaVersion 1 backend JSON shared with jinx-dispatcher -backends-json; unversioned definitions are deprecated")
	flags.IntVar(&config.serviceUID, "service-uid", -1, "trusted launchd GUI UID; required for runtime observation and service control")
	flags.StringVar(&config.plistDir, "service-plist-dir", "", "trusted absolute launchd plist directory; required to enable service control")
	flags.StringVar(&config.serviceLabelsRaw, "service-labels", "", "optional comma-separated service launchd labels (relay=...,dispatcher=...,decision=...,bridge=...)")
	flags.BoolVar(&config.enableControl, "enable-service-control", false, "explicitly allow planned service actions; startup never executes an action")
	if err := flags.Parse(arguments); err != nil {
		return config, err
	}
	if flags.NArg() != 0 {
		return config, errors.New("unexpected positional arguments")
	}
	if config.serviceLabelsRaw != "" {
		overrides := make(map[supervisor.ServiceID]string)
		for _, pair := range strings.Split(config.serviceLabelsRaw, ",") {
			key, value, ok := strings.Cut(strings.TrimSpace(pair), "=")
			id := supervisor.ServiceID(strings.TrimSpace(key))
			value = strings.TrimSpace(value)
			if !ok || id == "" || value == "" {
				return config, errors.New("service-labels entries must be non-empty service=label pairs")
			}
			if _, dup := overrides[id]; dup {
				return config, errors.New("duplicate service ID in service-labels")
			}
			overrides[id] = value
		}
		resolved, err := supervisor.ResolveLabels(overrides)
		if err != nil {
			return config, errors.New("invalid service-labels value")
		}
		config.serviceLabels = resolved
	}
	host, port, err := net.SplitHostPort(config.listen)
	number, portError := strconv.Atoi(port)
	if err != nil || portError != nil || number < 1 || number > 65535 || host != "localhost" && !net.ParseIP(host).IsLoopback() {
		return config, errors.New("listen must be a loopback address with an explicit port")
	}
	if config.dispatcher == "" || !filepath.IsAbs(config.stateDir) || !filepath.IsAbs(config.executionConfig) || !filepath.IsAbs(config.backendDefinitions) {
		return config, errors.New("dispatcher and absolute state-dir, execution-config, backend-definitions are required")
	}
	config.stateDir = filepath.Clean(config.stateDir)
	config.executionConfig = filepath.Clean(config.executionConfig)
	relative, err := filepath.Rel(config.stateDir, config.executionConfig)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || relative == "supervisor.json" || relative == "console" || strings.HasPrefix(relative, "console"+string(filepath.Separator)) {
		return config, errors.New("execution-config must be an independent file inside state-dir, outside console and supervisor data")
	}
	if config.serviceUID < -1 || int64(config.serviceUID) > int64(^uint32(0)) || config.plistDir != "" && !filepath.IsAbs(config.plistDir) {
		return config, errors.New("invalid service UID or plist directory")
	}
	if config.enableControl && (config.serviceUID < 0 || config.plistDir == "") {
		return config, errors.New("service control requires explicit service-uid and service-plist-dir")
	}
	return config, nil
}

type legacyBackendFile struct {
	Backends []legacyBackendDefinition `json:"backends"`
}

type legacyBackendDefinition struct {
	ID          string            `json:"id"`
	Kind        execution.Kind    `json:"kind"`
	Models      []execution.Model `json:"models"`
	AlmaBaseURL string            `json:"almaBaseURL,omitempty"`
	TextBaseURL string            `json:"textBaseURL,omitempty"`
	Hermes      *hermesDefinition `json:"hermes,omitempty"`
}

type hermesDefinition struct {
	Command  string   `json:"command"`
	Dir      string   `json:"dir"`
	Profile  string   `json:"profile"`
	Toolsets []string `json:"toolsets"`
	MaxTurns int      `json:"maxTurns"`
}

func loadBackends(path string) ([]execution.BackendOptions, error) {
	options, err := runtimebridge.LoadBackendOptions(path)
	if err == nil {
		return options.Backends, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot read backend definitions")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (256<<10)+1))
	if err != nil || len(data) > 256<<10 {
		return nil, errors.New("backend definitions exceed the size limit or cannot be read")
	}
	var definitions legacyBackendFile
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&definitions) != nil || decoder.Decode(new(any)) != io.EOF || definitions.Backends == nil {
		return nil, errors.New("invalid backend definitions JSON")
	}
	backends := make([]execution.BackendOptions, 0, len(definitions.Backends))
	for _, definition := range definitions.Backends {
		option := execution.BackendOptions{ID: definition.ID, Kind: definition.Kind, Models: definition.Models}
		switch definition.Kind {
		case execution.AlmaPersonal:
			if definition.AlmaBaseURL == "" || definition.TextBaseURL != "" || definition.Hermes != nil {
				return nil, errors.New("invalid personal backend definition")
			}
			option.AlmaBaseURL = definition.AlmaBaseURL
		case execution.TextOnly:
			if definition.AlmaBaseURL != "" || definition.Hermes != nil {
				return nil, errors.New("invalid text backend definition")
			}
			if definition.TextBaseURL != "" {
				return nil, errors.New("deprecated textBaseURL requires migration to schemaVersion 1 with almaURL and providerID")
			}
		case execution.LegacyHermes:
			if definition.Hermes == nil || definition.TextBaseURL != "" || definition.AlmaBaseURL != "" {
				return nil, errors.New("invalid Hermes backend definition")
			}
			trusted := definition.Hermes
			option.Hermes = execution.HermesOptions{Command: trusted.Command, Dir: trusted.Dir, Profile: trusted.Profile, Toolsets: trusted.Toolsets, MaxTurns: trusted.MaxTurns}
		default:
			return nil, errors.New("unsupported backend kind")
		}
		backends = append(backends, option)
	}
	log.Print("deprecated backend definitions without schemaVersion; migrate to the shared schemaVersion 1 format")
	return backends, nil
}
