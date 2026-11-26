package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type testPageExtension struct {
	testExtension
	handler http.Handler
}

func (extension testPageExtension) PageHandler() http.Handler { return extension.handler }

func TestPublicPageWithoutPageProvider(t *testing.T) {
	for _, extension := range []Extension{nil, testExtension{}} {
		server := newTestServer(t)
		server.extension = extension
		handler, err := server.Handler()
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"/", "/index.html"} {
			response := localRequest(t, handler, http.MethodGet, path, "")
			if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/html; charset=utf-8" {
				t.Fatal(path, response.Code, response.Header())
			}
			page := response.Body.String()
			for _, marker := range []string{`id="tab-stream"`, `id="tab-groups"`, `id="tab-filters"`, `id="tab-integration"`, "/api/v1/monitor/stream"} {
				if !strings.Contains(page, marker) {
					t.Fatal("public page missing UI", path, marker)
				}
			}
			for _, marker := range []string{"tab-history", "tab-sources", "history-open", "local-status-badge", "localCapability", "fetchSources", "saveLocalSource", "/api/v1/history", "/api/v1/sources", "/api/v1/attachments"} {
				if strings.Contains(page, marker) {
					t.Fatal("public page contains extension UI", path, marker)
				}
			}
		}
	}
}

func TestPageProviderUsesAdminGuard(t *testing.T) {
	server := newTestServer(t)
	cfg := server.cfgMgr.Get()
	cfg.Server.ListenAddr = ":8080"
	cfg.Server.EnableCORS = true
	if err := server.cfgMgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	server.extension = testPageExtension{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("synthetic extension page"))
	})}
	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/index.html"} {
		response := localRequest(t, handler, http.MethodGet, path, "")
		if response.Code != http.StatusOK || response.Body.String() != "synthetic extension page" {
			t.Fatal("extension page not served", path, response.Code, response.Body.String())
		}
		for _, method := range []string{http.MethodGet, http.MethodOptions} {
			for _, remote := range []string{"127.0.0.1:1234", "192.0.2.1:1234"} {
				request := httptest.NewRequest(method, "http://localhost"+path, nil)
				request.RemoteAddr = remote
				if strings.HasPrefix(remote, "127.") {
					request.Header.Set("Origin", "https://attacker.invalid")
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusForbidden || response.Header().Get("Access-Control-Allow-Origin") != "" {
					t.Fatal("extension page bypassed guard", method, path, response.Code)
				}
			}
		}
	}
	if response := localRequest(t, handler, http.MethodGet, "/missing.html", ""); response.Code != http.StatusNotFound {
		t.Fatal("page provider intercepted unrelated path", response.Code)
	}
}
