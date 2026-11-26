package web

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	"jinx/internal/relay/config"
)

func (s *Server) handleConsoleConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.sendPublicConfig(w, s.cfgMgr.GetPublic())
	case http.MethodPut:
		match := strings.TrimSpace(r.Header.Get("If-Match"))
		if match == "" {
			http.Error(w, "If-Match is required", http.StatusPreconditionRequired)
			return
		}
		if !strongETag(match) || len(r.Header.Values("If-Match")) != 1 {
			http.Error(w, "If-Match must be the single strong ETag returned by GET", http.StatusBadRequest)
			return
		}
		contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || contentType != "application/json" {
			http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, "request body exceeds 256 KiB", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "cannot read request body", http.StatusBadRequest)
			}
			return
		}
		var public config.PublicConfig
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if !utf8.Valid(data) || decoder.Decode(&public) != nil || decoder.Decode(new(interface{})) != io.EOF || !completePublicJSON(data) {
			http.Error(w, "expected complete group_control and filters objects with known fields and JSON types", http.StatusBadRequest)
			return
		}
		snapshot, err := s.cfgMgr.CompareAndSwap(match[1:len(match)-1], public)
		switch {
		case errors.Is(err, config.ErrRevisionConflict):
			http.Error(w, "configuration revision conflict; GET and retry", http.StatusConflict)
		case errors.Is(err, config.ErrInvalidPublic):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case err != nil:
			http.Error(w, "failed to persist public configuration", http.StatusInternalServerError)
		default:
			s.sendPublicConfig(w, snapshot)
		}
	default:
		w.Header().Set("Allow", "GET, PUT")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func strongETag(value string) bool {
	if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
		return false
	}
	for _, character := range []byte(value[1 : len(value)-1]) {
		if character == '"' || character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

func completePublicJSON(data []byte) bool {
	var root map[string]json.RawMessage
	if json.Unmarshal(data, &root) != nil || len(root) != 2 {
		return false
	}
	for _, object := range []struct {
		name   string
		fields map[string]bool
	}{
		{"group_control", map[string]bool{"monitor_all": false, "monitored_groups": true, "exposed_groups": true, "group_aliases": true, "drop_unexposed_completely": false}},
		{"filters", map[string]bool{"keyword_blocklist": true, "regex_blocklist": true, "sender_blocklist": true, "allowed_types": true, "clean_content": false, "ignore_echo": false}},
	} {
		var members map[string]json.RawMessage
		if json.Unmarshal(root[object.name], &members) != nil || len(members) != len(object.fields) {
			return false
		}
		for name, nullable := range object.fields {
			value, ok := members[name]
			if !ok || (!nullable && bytes.Equal(bytes.TrimSpace(value), []byte("null"))) {
				return false
			}
			if nullable && name != "group_aliases" {
				var entries []json.RawMessage
				if json.Unmarshal(value, &entries) != nil {
					return false
				}
				for _, entry := range entries {
					if bytes.Equal(bytes.TrimSpace(entry), []byte("null")) {
						return false
					}
				}
			}
			if name == "group_aliases" {
				var aliases map[string]json.RawMessage
				if json.Unmarshal(value, &aliases) != nil {
					return false
				}
				for _, alias := range aliases {
					if bytes.Equal(bytes.TrimSpace(alias), []byte("null")) {
						return false
					}
				}
			}
		}
	}
	return true
}

func (s *Server) sendPublicConfig(w http.ResponseWriter, snapshot config.PublicSnapshot) {
	w.Header().Set("ETag", `"`+snapshot.Revision+`"`)
	s.sendJSON(w, http.StatusOK, snapshot)
}
