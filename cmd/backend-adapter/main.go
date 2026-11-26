package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"jinx/internal/execution"
)

type arguments struct {
	profile  string
	model    string
	routing  string
	operator string
	noTools  bool
}

type operatorFile struct {
	SchemaVersion string            `json:"schemaVersion"`
	Backends      []operatorBackend `json:"backends"`
}

type operatorBackend struct {
	ID      string            `json:"id"`
	Kind    execution.Kind    `json:"kind"`
	Models  []execution.Model `json:"models"`
	BaseURL string            `json:"baseURL"`
	TextAPI string            `json:"textAPI"`
	Hermes  struct {
		Command  string   `json:"command"`
		Dir      string   `json:"dir"`
		Profile  string   `json:"profile"`
		Toolsets []string `json:"toolsets"`
		MaxTurns int      `json:"maxTurns"`
	} `json:"hermes"`
}

func parseArgs(argv []string) (arguments, error) {
	args := arguments{routing: os.Getenv("JINX_ROUTING_PATH"), operator: os.Getenv("JINX_OPERATOR_PATH")}
	seen := map[string]bool{}
	chat, query, format := false, false, false
	for index := 0; index < len(argv); index++ {
		flag := argv[index]
		if flag == "chat" || flag == "-Q" || flag == "--accept-hooks" {
			if seen[flag] {
				return args, fmt.Errorf("duplicate argument")
			}
			seen[flag] = true
			chat = chat || flag == "chat"
			continue
		}
		name, value, inline := strings.Cut(flag, "=")
		switch name {
		case "--profile":
			name = "-p"
		case "--toolsets":
			name = "-t"
		case "--model":
			name = "-m"
		}
		switch name {
		case "-p", "--query-file", "--format", "--resume", "--max-turns", "-t", "-m", "--routing", "--operator":
		default:
			return args, fmt.Errorf("unsupported argument")
		}
		if seen[name] {
			return args, fmt.Errorf("duplicate argument")
		}
		seen[name] = true
		if !inline {
			index++
			if index >= len(argv) {
				return args, fmt.Errorf("missing argument value")
			}
			value = argv[index]
		}
		if value == "" {
			return args, fmt.Errorf("empty argument value")
		}
		switch name {
		case "-p":
			args.profile = value
		case "--query-file":
			query = value == "-"
		case "--format":
			format = value == "stream-json"
		case "-m":
			args.model = value
		case "--routing":
			args.routing = value
		case "--operator":
			args.operator = value
		case "--max-turns":
			turns, err := strconv.Atoi(value)
			if err != nil || turns < 1 || turns > 1000 {
				return args, fmt.Errorf("invalid turns")
			}
		case "-t":
			for _, toolset := range strings.Split(value, ",") {
				if toolset != "memory" && toolset != "skills" && toolset != "web" && toolset != "none" {
					return args, fmt.Errorf("unknown toolset")
				}
			}
			if strings.Contains(value, "none") {
				if value != "none" {
					return args, fmt.Errorf("none cannot combine with toolsets")
				}
				args.noTools = true
			}
		case "--resume":
			if strings.ContainsAny(value, "\x00\r\n") || len(value) > 256 {
				return args, fmt.Errorf("invalid resume marker")
			}
		}
	}
	if !chat || !query || !format || args.profile == "" || args.routing == "" || args.operator == "" {
		return args, fmt.Errorf("profile, chat, stdin, stream-json and operator/routing paths required")
	}
	return args, nil
}

func readOptions(path string) (execution.Options, error) {
	file, err := os.Open(path)
	if err != nil {
		return execution.Options{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return execution.Options{}, fmt.Errorf("operator file too large or unreadable")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var config operatorFile
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF || config.SchemaVersion != execution.SchemaVersion {
		return execution.Options{}, fmt.Errorf("operator schema rejected")
	}
	options := execution.Options{}
	for _, backend := range config.Backends {
		option := execution.BackendOptions{ID: backend.ID, Kind: backend.Kind, Models: backend.Models, AlmaBaseURL: backend.BaseURL,
			Hermes: execution.HermesOptions{Command: backend.Hermes.Command, Dir: backend.Hermes.Dir, Profile: backend.Hermes.Profile,
				Toolsets: backend.Hermes.Toolsets, MaxTurns: backend.Hermes.MaxTurns}}
		if backend.Kind == execution.TextOnly && backend.TextAPI != "" {
			option.Provider, err = execution.NewHTTPTextProvider(execution.HTTPTextOptions{BaseURL: backend.BaseURL, Path: backend.TextAPI})
			if err != nil {
				return execution.Options{}, err
			}
		}
		options.Backends = append(options.Backends, option)
	}
	return options, nil
}

type resultEvent struct {
	Type           string            `json:"type"`
	Text           string            `json:"text"`
	SessionID      string            `json:"session_id"`
	ExitCode       int               `json:"exit_code"`
	Error          string            `json:"error,omitempty"`
	Backend        string            `json:"backend,omitempty"`
	BackendID      string            `json:"backend_id,omitempty"`
	RunID          string            `json:"run_id,omitempty"`
	Fallback       bool              `json:"fallback"`
	FallbackReason string            `json:"fallback_reason,omitempty"`
	Execution      *execution.Result `json:"execution,omitempty"`
}

func run(ctx context.Context, argv []string, input io.Reader, output io.Writer) int {
	reject := func(code string) int {
		_ = json.NewEncoder(output).Encode(resultEvent{Type: "result", ExitCode: 1, Error: code})
		return 1
	}
	args, err := parseArgs(argv)
	if err != nil {
		return reject("invalid_arguments")
	}
	options, err := readOptions(args.operator)
	if err != nil {
		return reject("operator_configuration_rejected")
	}
	engine, err := execution.Open(args.routing, options)
	if err != nil {
		return reject(execution.Code(err))
	}
	groupID, err := engine.GroupForProfile(ctx, args.profile)
	if err != nil {
		return reject(execution.Code(err))
	}
	group, err := engine.GetGroup(groupID)
	if err != nil || (args.model != "" && args.model != group.Routing.Primary.ModelID) {
		return reject("model_override_rejected")
	}
	if args.noTools && !group.Policy.NoTools {
		return reject("no_tools_requires_hard_routing_policy")
	}
	prompt, err := io.ReadAll(io.LimitReader(input, (1<<20)+1))
	if err != nil || len(prompt) > 1<<20 {
		return reject("invalid_stdin")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return reject("run_id_unavailable")
	}
	result, err := engine.Execute(ctx, execution.Request{TraceID: hex.EncodeToString(random[:]), GroupID: groupID, Prompt: string(prompt), ExpectedRevision: group.Revision})
	if err != nil {
		_ = json.NewEncoder(output).Encode(resultEvent{Type: "result", ExitCode: 1, Error: execution.Code(err), RunID: result.RunID, Execution: &result})
		return 1
	}
	if err := engine.CheckDelivery(ctx, result, groupID); err != nil && !result.Silent {
		return reject(execution.Code(err))
	}
	backend, sessionID, remoteID := "TextOnly", "execution:"+result.RunID, result.RunID
	if len(result.Attempts) > 0 {
		if external := result.Attempts[len(result.Attempts)-1].ExternalID; external != "" {
			remoteID = external
		}
	}
	if result.Kind == execution.AlmaPersonal {
		backend, sessionID = "Alma", "alma:"+remoteID
	} else if result.Kind == execution.LegacyHermes {
		backend, sessionID = "Hermes", remoteID
	}
	if err := json.NewEncoder(output).Encode(resultEvent{Type: "result", Text: result.Text, SessionID: sessionID,
		Backend: backend, BackendID: result.Backend, RunID: remoteID, Fallback: result.Fallback, FallbackReason: result.FallbackReason, Execution: &result}); err != nil {
		return 1
	}
	return 0
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout))
}
