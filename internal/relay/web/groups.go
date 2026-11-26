package web

import (
	"context"
	"encoding/json"
	"mime"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"jinx/internal/contracts"
	"jinx/internal/relay/config"
	"jinx/internal/relay/model"
)

func isLoopbackHostHeader(hostHeader string) bool {
	if hostHeader == "" {
		return false
	}
	host := hostHeader
	if h, _, err := net.SplitHostPort(hostHeader); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}

func sameOriginRequest(r *http.Request) bool {
	if !isLoopbackHostHeader(r.Host) {
		return false
	}
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		parsed, err := url.Parse(origin)
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if err != nil || parsed.Host != r.Host || parsed.Scheme != scheme || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return false
		}
	}
	return true
}

func rejectFormContentType(w http.ResponseWriter, r *http.Request) bool {
	ct := strings.TrimSpace(r.Header.Get("Content-Type"))
	if ct == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil || mediaType != "application/json" {
		http.Error(w, "JSON content-type required", http.StatusUnsupportedMediaType)
		return true
	}
	return false
}

func (s *Server) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Del("Access-Control-Allow-Origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			http.Error(w, "管理接口仅允许本机访问", http.StatusForbidden)
			return
		}
		if !isLoopbackHostHeader(r.Host) {
			http.Error(w, "请通过 localhost 或回环地址访问管理接口", http.StatusForbidden)
			return
		}
		if !sameOriginRequest(r) {
			http.Error(w, "不允许跨站访问管理接口", http.StatusForbidden)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		next(w, r)
	}
}

func (s *Server) allGroups(ctx context.Context, cfg config.Config) []*model.GroupInfo {
	var extensionGroups []model.GroupInfo
	if s.extension != nil {
		extensionGroups = s.extension.Groups(ctx)
	}
	knownNames := map[string]bool{}
	for _, group := range extensionGroups {
		knownNames[group.GroupName] = true
	}
	groups := []*model.GroupInfo{}
	byID := map[string]*model.GroupInfo{}
	for _, group := range s.hub.GetGroups(cfg) {
		if group.MessageCount == 0 && knownNames[group.GroupID] && !contracts.ValidRelayContainerID(group.GroupID) {
			continue
		}
		groups = append(groups, group)
		byID[group.GroupID] = group
	}
	for _, group := range extensionGroups {
		if current := byID[group.GroupID]; current != nil {
			current.GroupName = group.GroupName
		} else {
			item := group
			groups = append(groups, &item)
			byID[group.GroupID] = &item
		}
	}
	for _, group := range groups {
		group.IsMonitored = cfg.GroupControl.MonitorAll || config.MatchesGroup(cfg.GroupControl.MonitoredGroups, group.GroupID, group.GroupName)
		group.IsExposed = config.MatchesGroup(cfg.GroupControl.ExposedGroups, group.GroupID, group.GroupName)
	}
	sort.Slice(groups, func(first, second int) bool { return groups[first].GroupName < groups[second].GroupName })
	return groups
}

func (s *Server) handleGroupMonitor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", 405)
		return
	}
	if rejectFormContentType(w, r) {
		return
	}
	var request struct {
		GroupID   string `json:"group_id"`
		Monitored bool   `json:"monitored"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&request) != nil {
		http.Error(w, "群 ID 无效", 400)
		return
	}
	request.GroupID = strings.TrimSpace(request.GroupID)
	if !contracts.ValidRelayContainerID(request.GroupID) {
		http.Error(w, "群 ID 无效", 400)
		return
	}
	cfg := s.cfgMgr.Get()
	if cfg.GroupControl.MonitorAll {
		http.Error(w, "请先关闭监控全部群聊，再设置单群监控", 409)
		return
	}
	if !request.Monitored && config.MatchesGroup(cfg.GroupControl.MonitoredGroups, "", "") {
		http.Error(w, "监控名单包含 *，请先从配置中移除通配符", 409)
		return
	}
	name := request.GroupID
	for _, group := range s.allGroups(r.Context(), cfg) {
		if group.GroupID == request.GroupID {
			name = group.GroupName
			break
		}
	}
	list := []string{}
	for _, group := range cfg.GroupControl.MonitoredGroups {
		if strings.TrimSpace(group) != request.GroupID && strings.TrimSpace(group) != name {
			list = append(list, group)
		}
	}
	if request.Monitored {
		list = append(list, request.GroupID)
	}
	cfg.GroupControl.MonitoredGroups = list
	if err := s.cfgMgr.Update(cfg); err != nil {
		http.Error(w, "failed to save configuration", 500)
		return
	}
	s.sendJSON(w, 200, map[string]string{"status": "ok"})
}
