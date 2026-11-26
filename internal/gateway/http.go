package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"jinx/internal/execution"
	"jinx/internal/supervisor"
)

const maxBody = 256 << 10

type Problem struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	CurrentRevision string `json:"currentRevision,omitempty"`
	Status          int    `json:"-"`
}

func (problem *Problem) Error() string { return problem.Code }

func failure(status int, code string) *Problem {
	return &Problem{Status: status, Code: code, Message: "Control request could not be completed"}
}

func origin(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" || (parsed.Path != "" && parsed.Path != "/") || !loopbackAuthority(parsed.Host) {
		return "", failure(http.StatusBadRequest, "invalid_upstream")
	}
	return strings.TrimSuffix(parsed.String(), "/"), nil
}

func loopbackAuthority(authority string) bool {
	host, port, err := net.SplitHostPort(authority)
	if err != nil {
		return false
	}
	number, err := strconv.Atoi(port)
	return err == nil && number > 0 && number <= 65535 && (host == "localhost" || net.ParseIP(host).IsLoopback())
}

func guard(request *http.Request) error {
	if !loopbackAuthority(request.Host) {
		return failure(http.StatusForbidden, "forbidden")
	}
	if site := request.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return failure(http.StatusForbidden, "forbidden")
	}
	if raw := request.Header.Get("Origin"); raw != "" {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "http" || parsed.Host != request.Host || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
			return failure(http.StatusForbidden, "forbidden")
		}
	}
	return nil
}

func decodeInput(writer http.ResponseWriter, request *http.Request, target any) error {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return failure(http.StatusUnsupportedMediaType, "json_required")
	}
	data, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, maxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return failure(http.StatusRequestEntityTooLarge, "too_large")
		}
		return failure(http.StatusBadRequest, "validation")
	}
	if err := strictObject(data, target); err != nil {
		return failure(http.StatusBadRequest, "validation")
	}
	return nil
}

func strictObject(data []byte, target any) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return failure(http.StatusBadRequest, "validation")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 64 {
			return failure(http.StatusBadRequest, "validation")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if delimiter, ok := token.(json.Delim); ok {
			keys := map[string]bool{}
			for decoder.More() {
				if delimiter == '{' {
					key, err := decoder.Token()
					if err != nil {
						return err
					}
					name, ok := key.(string)
					if !ok || keys[strings.ToLower(name)] {
						return failure(http.StatusBadRequest, "validation")
					}
					keys[strings.ToLower(name)] = true
				}
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		}
		return nil
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return failure(http.StatusBadRequest, "validation")
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeResult(writer http.ResponseWriter, value any, err error) {
	if err == nil {
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "value": value})
		return
	}
	problem := failure(http.StatusInternalServerError, "internal")
	var gatewayProblem *Problem
	var executionProblem *execution.Error
	var serviceProblem *supervisor.Problem
	switch {
	case errors.As(err, &gatewayProblem):
		problem = gatewayProblem
	case errors.As(err, &executionProblem):
		problem = failure(executionStatus(executionProblem.Code), executionProblem.Code)
		problem.CurrentRevision = executionProblem.CurrentRevision
	case errors.As(err, &serviceProblem):
		problem = failure(serviceStatus(serviceProblem.Code), serviceProblem.Code)
	}
	writer.WriteHeader(problem.Status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"ok": false, "problem": problem})
}

func executionStatus(code string) int {
	switch code {
	case "validation":
		return http.StatusBadRequest
	case "conflict", "idempotency_conflict", "policy_mismatch", "fallback_policy_mismatch", "policy_blocked":
		return http.StatusConflict
	case "not_found":
		return http.StatusNotFound
	case "unsupported":
		return http.StatusNotImplemented
	case "too_large":
		return http.StatusRequestEntityTooLarge
	case "unavailable":
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func serviceStatus(code string) int {
	switch code {
	case "invalid_service", "invalid_kind", "invalid_request", "invalid_options":
		return http.StatusBadRequest
	case "not_found":
		return http.StatusNotFound
	case "readonly", "invalid_token":
		return http.StatusForbidden
	case "unsupported":
		return http.StatusNotImplemented
	case "closed", "journal_unavailable", "observation_unknown", "control_unknown":
		return http.StatusServiceUnavailable
	default:
		return http.StatusConflict
	}
}

func safeID(id string) bool {
	if len(id) == 0 || len(id) > 200 || id == "." || id == ".." {
		return false
	}
	for _, character := range id {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}
