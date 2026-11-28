package connector

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"jinx/internal/contracts"
)

type StatusSnapshot struct {
	Targets           []HealthTarget
	LastPollAt        *time.Time
	LastPollErrorCode string
}

type StatusReporter interface {
	Status() StatusSnapshot
}

type ActionExecutor interface {
	Platform() contracts.Platform
	AccountID() string
	ConnectorName() string
	Capabilities() []contracts.ActionKind
	Health(ctx context.Context) (ready bool, reason string)
	Execute(ctx context.Context, req ActionRequest) (ActionResponse, error)
}

type ServerOptions struct {
	Executor       ActionExecutor
	Store          *IdempotencyStore
	DryRun         bool
	Now            func() time.Time
	OnDryRunChange func(dryRun bool) (persisted bool, err error)
}

type Server struct {
	executor       ActionExecutor
	store          *IdempotencyStore
	mu             sync.RWMutex
	dryRun         bool
	onDryRunChange func(dryRun bool) (persisted bool, err error)
	dryRunReqs     map[string]DryRunControlResponse
	dryRunOrder    []string
	now            func() time.Time
}

func NewServer(opts ServerOptions) *Server {
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Server{
		executor:       opts.Executor,
		store:          opts.Store,
		dryRun:         opts.DryRun,
		onDryRunChange: opts.OnDryRunChange,
		dryRunReqs:     map[string]DryRunControlResponse{},
		now:            nowFn,
	}
}

func (s *Server) isDryRun() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dryRun
}

// IsDryRun reports the current runtime dry-run state.
func (s *Server) IsDryRun() bool {
	return s.isDryRun()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/v1/actions", s.handleActions)
	mux.HandleFunc("/v1/control/dry-run", s.handleDryRunControl)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		host := r.Host
		if parsed, _, err := net.SplitHostPort(host); err == nil {
			host = parsed
		}
		if host != "localhost" && !net.ParseIP(strings.Trim(host, "[]")).IsLoopback() {
			http.Error(w, "local host required", http.StatusForbidden)
			return
		}
		if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
			http.Error(w, "cross-site access denied", http.StatusForbidden)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			if err != nil || u.Host != r.Host || u.Scheme != scheme || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
				http.Error(w, "origin denied", http.StatusForbidden)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ready, reason := s.executor.Health(r.Context())
	status := "not_ready"
	if ready {
		status = "ready"
	}
	caps := make([]string, 0, len(s.executor.Capabilities()))
	for _, cap := range s.executor.Capabilities() {
		caps = append(caps, string(cap))
	}
	resp := HealthResponse{
		Status:          status,
		ProtocolVersion: ProtocolVersion,
		Platform:        string(s.executor.Platform()),
		Ready:           ready,
		DryRun:          s.isDryRun(),
		Capabilities:    caps,
		Controls:        []string{"dry_run"},
		AccountID:       s.executor.AccountID(),
		ConnectorName:   s.executor.ConnectorName(),
		Reason:          reason,
	}
	if reporter, ok := s.executor.(StatusReporter); ok {
		snap := reporter.Status()
		resp.Targets = snap.Targets
		resp.LastPollAt = snap.LastPollAt
		resp.LastPollErrorCode = snap.LastPollErrorCode
	}
	writeJSON(w, http.StatusOK, resp)
}

func isJSONMediaType(raw string) bool {
	mediaType, _, err := mime.ParseMediaType(raw)
	return err == nil && mediaType == "application/json"
}

func (s *Server) handleDryRunControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !isJSONMediaType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"code": "unsupported_media_type"})
		return
	}
	var req DryRunControlRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_json"})
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_json"})
		return
	}
	if !contracts.ValidID(req.RequestID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_request_id"})
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, seen := s.dryRunReqs[req.RequestID]; seen {
		if prev.DryRun != req.DryRun {
			writeJSON(w, http.StatusConflict, map[string]string{"code": "idempotency_conflict"})
			return
		}
		writeJSON(w, http.StatusOK, prev)
		return
	}
	persisted := false
	if s.onDryRunChange != nil && req.DryRun != s.dryRun {
		p, err := s.onDryRunChange(req.DryRun)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"code": "persist_failed"})
			return
		}
		persisted = p
	}
	s.dryRun = req.DryRun
	resp := DryRunControlResponse{
		OK:        true,
		Platform:  string(s.executor.Platform()),
		DryRun:    s.dryRun,
		Persisted: persisted,
		RequestID: req.RequestID,
	}
	if _, exists := s.dryRunReqs[req.RequestID]; !exists {
		if len(s.dryRunOrder) >= 256 {
			oldest := s.dryRunOrder[0]
			s.dryRunOrder = s.dryRunOrder[1:]
			delete(s.dryRunReqs, oldest)
		}
		s.dryRunOrder = append(s.dryRunOrder, req.RequestID)
	}
	s.dryRunReqs[req.RequestID] = resp
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleActions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !isJSONMediaType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, ActionResponse{
			Status:     "failed",
			Submitted:  BoolPtr(false),
			ReasonCode: "unsupported_media_type",
		})
		return
	}
	var req ActionRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ActionResponse{
			Status:     "failed",
			Submitted:  BoolPtr(false),
			ReasonCode: "invalid_json",
		})
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, ActionResponse{
			Status:     "failed",
			Submitted:  BoolPtr(false),
			ReasonCode: "invalid_json",
		})
		return
	}
	if err := ValidateActionRequest(req, s.executor.Platform(), s.executor.Capabilities()); err != nil {
		writeJSON(w, http.StatusBadRequest, ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  BoolPtr(false),
			ReasonCode: "invalid_request",
		})
		return
	}
	expiry, _ := RequestExpiry(req)
	if !s.now().Before(expiry) {
		writeJSON(w, http.StatusConflict, ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  BoolPtr(false),
			ReasonCode: "expired",
		})
		return
	}
	if s.isDryRun() {
		writeJSON(w, http.StatusConflict, ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  BoolPtr(false),
			ReasonCode: "dry_run",
		})
		return
	}
	digest := RequestDigest(req)
	existing, state, err := s.store.Claim(req.IdempotencyKey, digest)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  BoolPtr(false),
			ReasonCode: "idempotency_store_error",
		})
		return
	}
	switch state {
	case "replay":
		status := existing.HTTPStatus
		if status == 0 {
			status = http.StatusOK
		}
		writeJSON(w, status, existing.Response)
		return
	case "conflict":
		writeJSON(w, http.StatusConflict, ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  BoolPtr(false),
			ReasonCode: "idempotency_conflict",
		})
		return
	case "in_flight":
		writeJSON(w, http.StatusConflict, ActionResponse{
			ActionID:   req.ActionID,
			Status:     "unknown",
			ReasonCode: "action_in_flight",
		})
		return
	}

	resp, execErr := s.executor.Execute(r.Context(), req)
	httpStatus, normalized := normalizeExecOutcome(req.ActionID, resp, execErr)
	if completeErr := s.store.Complete(req.IdempotencyKey, digest, httpStatus, normalized); completeErr != nil {
		writeJSON(w, http.StatusBadGateway, ActionResponse{
			ActionID:   req.ActionID,
			Status:     "unknown",
			ReasonCode: "idempotency_persist_error",
		})
		return
	}
	writeJSON(w, httpStatus, normalized)
}

func normalizeExecOutcome(actionID string, resp ActionResponse, execErr error) (int, ActionResponse) {
	resp.ActionID = actionID
	if execErr != nil {
		if resp.Status == "failed" && resp.Submitted != nil && !*resp.Submitted {
			if resp.ReasonCode == "" {
				resp.ReasonCode = "executor_rejected"
			}
			return http.StatusUnprocessableEntity, resp
		}
		reason := resp.ReasonCode
		if reason == "" {
			reason = "executor_error"
		}
		return http.StatusBadGateway, ActionResponse{
			ActionID:   actionID,
			Status:     "unknown",
			ReasonCode: reason,
		}
	}
	switch resp.Status {
	case "confirmed":
		if !contracts.ValidID(resp.ExternalID) {
			return http.StatusBadGateway, ActionResponse{
				ActionID:   actionID,
				Status:     "unknown",
				ReasonCode: "invalid_external_id",
			}
		}
		return http.StatusOK, resp
	case "submitted":
		return http.StatusOK, resp
	case "failed":
		if resp.Submitted != nil && !*resp.Submitted {
			if resp.ReasonCode == "" {
				resp.ReasonCode = "executor_rejected"
			}
			return http.StatusUnprocessableEntity, resp
		}
		return http.StatusBadGateway, ActionResponse{
			ActionID:   actionID,
			Status:     "unknown",
			ReasonCode: "ambiguous_failure",
		}
	default:
		if resp.ReasonCode == "" {
			resp.ReasonCode = "executor_unknown"
		}
		resp.Status = "unknown"
		return http.StatusBadGateway, resp
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
