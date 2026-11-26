package supervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const launchctl = "/bin/launchctl"

var serviceOrder = []ServiceID{Relay, Dispatcher, Decision, Bridge}

var labels = map[ServiceID]string{
	Relay:      "io.jinx.relay",
	Dispatcher: "io.jinx.dispatcher",
	Decision:   "io.jinx.decision",
	Bridge:     "io.jinx.bridge",
}

var serviceLabelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$`)

func DefaultLabels() map[ServiceID]string {
	out := make(map[ServiceID]string, len(labels))
	for _, id := range serviceOrder {
		out[id] = labels[id]
	}
	return out
}

func ValidServiceLabel(label string) bool {
	return serviceLabelPattern.MatchString(label) && strings.Contains(label, ".") && !strings.Contains(label, "..")
}

func ResolveLabels(overrides map[ServiceID]string) (map[ServiceID]string, error) {
	resolved := DefaultLabels()
	for id, label := range overrides {
		if _, known := resolved[id]; !known {
			return nil, failure("invalid_service")
		}
		if !ValidServiceLabel(label) {
			return nil, failure("invalid_options")
		}
		resolved[id] = label
	}
	seen := make(map[string]bool, len(resolved))
	for _, id := range serviceOrder {
		label := resolved[id]
		if seen[label] {
			return nil, failure("invalid_options")
		}
		seen[label] = true
	}
	return resolved, nil
}

type executionPermission struct{}

type executionGrant struct {
	Domain        string
	Args          []string
	AllowedLabels []string
}

type boundedOutput struct {
	bytes.Buffer
	limit int
}

func (output *boundedOutput) Write(data []byte) (int, error) {
	if output.Len()+len(data) > output.limit {
		return 0, errors.New("command output exceeds limit")
	}
	return output.Buffer.Write(data)
}

type ExecCommander struct{}

func (ExecCommander) Run(ctx context.Context, executable string, args ...string) (string, string, int, error) {
	grant, permitted := ctx.Value(executionPermission{}).(executionGrant)
	allowed := permitted && fingerprint(args) == fingerprint(grant.Args)
	if permitted && len(args) == 2 && args[0] == "print" {
		allowedSet := grant.AllowedLabels
		if len(allowedSet) == 0 {
			for _, label := range labels {
				allowedSet = append(allowedSet, label)
			}
		}
		for _, label := range allowedSet {
			allowed = allowed || args[1] == grant.Domain+"/"+label
		}
	}
	if !allowed || executable != launchctl || ctx.Err() != nil {
		return "", "", -1, failure("unsupported")
	}
	command := exec.CommandContext(ctx, executable, args...)
	stdout := &boundedOutput{limit: 4 << 20}
	stderr := &boundedOutput{limit: 64 << 10}
	command.Stdout, command.Stderr = stdout, stderr
	err := command.Run()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return stdout.String(), stderr.String(), exitError.ExitCode(), nil
	}
	if err != nil {
		return "", "", -1, failure("command_unknown")
	}
	return stdout.String(), stderr.String(), 0, nil
}

func fingerprint(value any) string {
	data, _ := json.Marshal(value)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func serviceVersion(service Service) Service {
	service.Version = ""
	service.Problem = nil
	service.Version = fingerprint(service)
	return service
}

func unknownService(id ServiceID) Service {
	return unknownServiceWithLabel(id, labels[id])
}

func unknownServiceWithLabel(id ServiceID, label string) Service {
	service := serviceVersion(Service{ID: id, Label: label, Registration: "unknown", State: "unknown"})
	service.Problem = failure("observation_unknown")
	return service
}

func (manager *Manager) label(id ServiceID) string {
	if manager.labels != nil {
		if label := manager.labels[id]; label != "" {
			return label
		}
	}
	return labels[id]
}

func (manager *Manager) allowedLabels() []string {
	out := make([]string, 0, len(serviceOrder))
	for _, id := range serviceOrder {
		out = append(out, manager.label(id))
	}
	return out
}

func (manager *Manager) domain() string {
	return "gui/" + strconv.FormatUint(uint64(*manager.options.UID), 10)
}

func (manager *Manager) target(id ServiceID) string {
	return manager.domain() + "/" + manager.label(id)
}

func (manager *Manager) observe(ctx context.Context, id ServiceID) Service {
	label := manager.label(id)
	if label == "" || manager.observer == nil || manager.options.UID == nil {
		return unknownServiceWithLabel(id, label)
	}
	ctx, cancel := context.WithTimeout(ctx, manager.options.CommandTimeout)
	defer cancel()
	stdout, stderr, exit, err := manager.observer.Run(ctx, launchctl, "print", manager.target(id))
	if err != nil || ctx.Err() != nil {
		return unknownServiceWithLabel(id, label)
	}
	if exit == 113 && strings.Contains(stderr, `Could not find service "`+label+`"`) {
		return serviceVersion(Service{ID: id, Label: label, Registration: "unregistered", State: "stopped"})
	}
	if exit != 0 {
		return unknownServiceWithLabel(id, label)
	}
	return parsePrintWithLabel(id, label, manager.target(id), stdout)
}

func parsePrint(id ServiceID, target, output string) Service {
	return parsePrintWithLabel(id, labels[id], target, output)
}

func parsePrintWithLabel(id ServiceID, label, target, output string) Service {
	if len(output) > 4<<20 {
		return unknownServiceWithLabel(id, label)
	}
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[0]) != target+" = {" {
		return unknownServiceWithLabel(id, label)
	}
	fields := make(map[string]string)
	depth := 1
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line == "}" {
			depth--
			if depth == 0 {
				continue
			}
		}
		if depth == 1 {
			name, value, found := strings.Cut(line, " = ")
			if found {
				switch name {
				case "state", "pid", "runs", "instance", "instance id", "unique id", "last exit code":
					if _, duplicate := fields[name]; duplicate {
						return unknownServiceWithLabel(id, label)
					}
					fields[name] = value
				}
			}
		}
		if strings.HasSuffix(line, "{") {
			depth++
		}
		if depth < 0 {
			return unknownServiceWithLabel(id, label)
		}
	}
	if depth != 0 {
		return unknownServiceWithLabel(id, label)
	}
	service := Service{ID: id, Label: label, Registration: "registered", State: fields["state"]}
	switch service.State {
	case "running", "waiting", "exited", "not running", "spawn scheduled", "spawn failed", "waiting for demand":
	default:
		return unknownServiceWithLabel(id, label)
	}
	if raw, present := fields["pid"]; present {
		pid, err := strconv.Atoi(raw)
		if err != nil || pid <= 0 {
			return unknownServiceWithLabel(id, label)
		}
		service.PID = pid
	}
	if service.State == "running" && service.PID == 0 || service.State != "running" && service.PID != 0 {
		return unknownServiceWithLabel(id, label)
	}
	identity := []string{strconv.Itoa(service.PID)}
	for _, name := range []string{"runs", "instance", "instance id", "unique id"} {
		if value := fields[name]; value != "" {
			if !safeID(value) {
				return unknownServiceWithLabel(id, label)
			}
			identity = append(identity, name, value)
		}
	}
	if service.PID != 0 {
		service.InstanceID = fingerprint(identity)
	}
	if raw, present := fields["last exit code"]; present {
		if raw == "(never exited)" {
			return serviceVersion(service)
		}
		code, err := strconv.Atoi(raw)
		if err != nil {
			return unknownServiceWithLabel(id, label)
		}
		service.LastExit = &code
	}
	return serviceVersion(service)
}

func safeID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}

func (manager *Manager) command(plan ActionPlan) []string {
	target := manager.target(plan.ServiceID)
	switch plan.Kind {
	case Start:
		if findService(plan.Base, plan.ServiceID).Registration == "unregistered" {
			return []string{"bootstrap", manager.domain(), filepath.Join(manager.options.PlistDir, manager.label(plan.ServiceID)+".plist")}
		}
		return []string{"kickstart", target}
	case Stop:
		return []string{"bootout", target}
	case Restart:
		return []string{"kickstart", "-k", target}
	}
	return nil
}

func findService(list ServiceList, id ServiceID) Service {
	for _, service := range list.Services {
		if service.ID == id {
			return service
		}
	}
	return unknownService(id)
}
