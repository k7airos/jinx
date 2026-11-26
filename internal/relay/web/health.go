package web

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	health := s.hub.HealthSnapshot()
	s.lifecycleMu.Lock()
	if s.failed {
		health.Status = "failed"
	} else if s.stopped {
		health.Status = "stopped"
	}
	s.lifecycleMu.Unlock()
	if health.Status != "ready" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(health)
}
