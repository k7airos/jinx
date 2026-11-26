package dispatcher

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

type HermesResult struct {
	WebEvidence    []WebEvidence `json:"web_evidence,omitempty"`
	Backend        string        `json:"backend,omitempty"`
	Fallback       bool          `json:"fallback,omitempty"`
	FallbackReason string        `json:"fallback_reason,omitempty"`
	RunID          string        `json:"run_id,omitempty"`
	Text           string        `json:"text"`
	SessionID      string        `json:"session_id"`
}

type echoWaiter struct {
	runID           string
	done            chan struct{}
	responsePending bool
}

type cappedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
	max int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.buf.Len()+len(p) > b.max {
		return 0, fmt.Errorf("process output exceeded limit")
	}
	return b.buf.Write(p)
}

func (b *cappedBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *cappedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// modelFingerprint reads the profile's configured default model so a session that was
// created under a different model is never resumed: --resume keeps the session's
// original launch policy, so an old session would keep answering with the old model.
func modelFingerprint(profileDir string) string {
	return modelConfigValue(profileDir, "default")
}

// modelProvider reads the inference provider configured for the profile.
func modelProvider(profileDir string) string {
	return modelConfigValue(profileDir, "provider")
}

// modelConfigValue reads a scalar key from the profile's top-level `model:` block.
func modelConfigValue(profileDir, key string) string {
	data, err := os.ReadFile(filepath.Join(profileDir, "config.yaml"))
	if err != nil {
		return ""
	}
	inModelBlock := false
	for _, line := range strings.Split(string(data), "\n") {
		if line == "model:" {
			inModelBlock = true
			continue
		}
		if inModelBlock {
			// A new top-level key ends the model block.
			if line != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "	") {
				break
			}
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, key+":") {
				return strings.TrimSpace(strings.TrimPrefix(trimmed, key+":"))
			}
		}
	}
	return ""
}

func runHermes(ctx context.Context, b batch) (HermesResult, error) {
	if b.Rule.SupportMode {
		b.SessionID = ""
	} // One fresh Alma attempt; never retry a stale support session.
	profileDir := filepath.Join(b.Config.Hermes.ProfilesDir, b.Rule.HermesProfile)
	if b.SessionID != "" {
		// A session created under a different model must not be resumed, otherwise it keeps
		// answering with that old model even though the profile now points elsewhere.
		if current := modelFingerprint(profileDir); current != "" && b.SessionModel != current {
			log.Printf("hermes session model changed (%s -> %s), starting a new session", b.SessionModel, current)
			b.SessionID = ""
		}
	}
	if b.SessionID != "" {
		result, err := runHermesOnce(ctx, b, profileDir, b.SessionID)
		if err == nil {
			return result, nil
		}
		// A stale or policy-incompatible session must not mute the group: retry with a fresh session.
		log.Printf("hermes resume failed (%s), retrying with a new session: %v", b.GroupID, err)
		b.SessionID = ""
		return runHermesOnce(ctx, b, profileDir, "")
	}
	return runHermesOnce(ctx, b, profileDir, "")
}

func runHermesOnce(ctx context.Context, b batch, profileDir, sessionID string) (HermesResult, error) {
	if b.Rule.SupportMode {
		// Never resume a session created with broader tools or an older support policy.
		sessionID = ""
	}
	data, err := os.ReadFile(filepath.Join(profileDir, "mem0.json"))
	if err != nil {
		return HermesResult{}, fmt.Errorf("isolated profile not prepared: %w", err)
	}
	var memory struct {
		UserID string `json:"user_id"`
		Mode   string `json:"mode"`
		OSS    struct {
			VectorStore struct {
				Provider string `json:"provider"`
				Config   struct {
					Collection string `json:"collection_name"`
				} `json:"config"`
			} `json:"vector_store"`
		} `json:"oss"`
	}
	identity := b.Config.identity(b.GroupID)
	if identity.Profile != "" && identity.Profile != b.Rule.HermesProfile {
		return HermesResult{}, fmt.Errorf("profile identity does not match group")
	}
	if err := b.Config.validateSupportRule(b.GroupID, b.Rule); err != nil {
		return HermesResult{}, err
	}
	if json.Unmarshal(data, &memory) != nil || memory.UserID != identity.MemoryUser || memory.Mode != "oss" || memory.OSS.VectorStore.Provider != "qdrant" || memory.OSS.VectorStore.Config.Collection != identity.VectorCollection {
		return HermesResult{}, fmt.Errorf("profile memory identity does not match group")
	}
	args := []string{"-p", b.Rule.HermesProfile, "chat", "--query-file", "-", "-Q", "--format", "stream-json"}
	if sessionID != "" {
		// Resume keeps the session's original launch policy: toolsets/max-turns overrides are
		// rejected on resume, so only pass them when starting a fresh session.
		args = append(args, "--resume", sessionID)
	} else {
		// Pin the model explicitly so a group never answers with a stale model even if the
		// profile's config.yaml drifts. Resume cannot carry these overrides, which is why
		// they only appear on the fresh-session branch.
		args = append(args, "--max-turns", "8", "-t", "memory,skills,web")
		if b.Rule.SupportMode {
			// Product-only web toolset; profile pre_tool_call hook denies all other actions.
			args[len(args)-1] = "web"
			args = append(args, "--accept-hooks")
		}
		if model := modelFingerprint(profileDir); model != "" {
			args = append(args, "-m", model)
		}
		if provider := modelProvider(profileDir); provider != "" {
			args = append(args, "--provider", provider)
		}
	}
	binPath := b.Config.Hermes.BinPath
	if b.Rule.SupportMode {
		// Only the isolated product route opts into Alma's support protocol.
		args = append(args, "--support-mode")
	}

	cmd := exec.CommandContext(ctx, binPath, args...)
	cmd.Dir = filepath.Join(profileDir, "workspace")
	for _, key := range []string{"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "LC_CTYPE", "SSL_CERT_FILE", "SSL_CERT_DIR", "HERMES_ALMA_TIMEOUT", "HERMES_ALMA_MAX_TEXT"} {
		if val, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+val)
		}
	}
	cmd.Env = append(cmd.Env, "HERMES_HOME="+profileDir, "POSTHOG_DISABLED=1", "DO_NOT_TRACK=1")
	cmd.Stdin = strings.NewReader(buildPrompt(b))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return HermesResult{}, err
	}
	stderr := &cappedBuffer{max: 128 << 10}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return HermesResult{}, fmt.Errorf("Hermes failed to start: %w", err)
	}

	scanner := bufio.NewScanner(stdoutPipe)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var result HermesResult
	hasResult := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event struct {
			WebEvidence    []WebEvidence `json:"web_evidence"`
			Backend        string        `json:"backend"`
			Fallback       bool          `json:"fallback"`
			FallbackReason string        `json:"fallback_reason"`
			RunID          string        `json:"run_id"`
			Type           string        `json:"type"`
			Text           string        `json:"text"`
			SessionID      string        `json:"session_id"`
			ExitCode       *int          `json:"exit_code"`
			Error          string        `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}
		if event.Type == "result" {
			if event.ExitCode == nil || *event.ExitCode != 0 || event.Error != "" || event.SessionID == "" {
				if cmd.Process != nil {
					_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				}
				_ = cmd.Wait()
				detail := strings.TrimSpace(event.Error)
				if detail == "" {
					detail = strings.TrimSpace(stderr.String())
				}
				if detail == "" {
					detail = fmt.Sprintf("exit_code=%v", event.ExitCode)
				}
				return result, fmt.Errorf("Hermes reported unsuccessful result: %s", detail)
			}
			result.Text = event.Text
			result.SessionID = event.SessionID
			result.Backend = event.Backend
			result.Fallback = event.Fallback
			result.FallbackReason = event.FallbackReason
			result.RunID = event.RunID
			result.WebEvidence = event.WebEvidence
			if b.Rule.SupportMode && !validSupportBackend(result) {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				_ = cmd.Wait()
				return HermesResult{}, fmt.Errorf("invalid support backend metadata")
			}
			hasResult = true
			break
		}
	}

	waitChild := func() {
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			<-done
		}
	}

	if !hasResult {
		waitChild()
		errOutput := strings.TrimSpace(stderr.String())
		if errOutput != "" {
			return HermesResult{}, fmt.Errorf("Hermes failed to produce result: %s", errOutput)
		}
		return HermesResult{}, fmt.Errorf("Hermes stream ended without result")
	}
	go waitChild()
	return result, nil
}

func parseHermesOutput(data []byte) (HermesResult, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	var result HermesResult
	results := 0
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var event struct {
			WebEvidence    []WebEvidence `json:"web_evidence"`
			Backend        string        `json:"backend"`
			Fallback       bool          `json:"fallback"`
			FallbackReason string        `json:"fallback_reason"`
			RunID          string        `json:"run_id"`
			Type           string        `json:"type"`
			Text           string        `json:"text"`
			SessionID      string        `json:"session_id"`
			ExitCode       *int          `json:"exit_code"`
			Error          string        `json:"error"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return result, fmt.Errorf("invalid Hermes JSON event")
		}
		if results != 0 {
			return result, fmt.Errorf("Hermes emitted data after terminal result")
		}
		switch event.Type {
		case "result":
			if event.ExitCode == nil || *event.ExitCode != 0 || event.Error != "" || event.SessionID == "" {
				detail := strings.TrimSpace(event.Error)
				if detail == "" {
					detail = fmt.Sprintf("exit_code=%v", event.ExitCode)
				}
				return result, fmt.Errorf("Hermes reported unsuccessful result: %s", detail)
			}
			result = HermesResult{Text: event.Text, SessionID: event.SessionID, Backend: event.Backend, Fallback: event.Fallback, FallbackReason: event.FallbackReason, RunID: event.RunID, WebEvidence: event.WebEvidence}
			results++
		case "system", "text", "tool_use", "tool_result":
		default:
			return result, fmt.Errorf("unknown Hermes event type %q", event.Type)
		}
	}
	if scanner.Err() != nil || results != 1 {
		return result, fmt.Errorf("Hermes result missing or truncated")
	}
	return result, nil
}

func (d *Dispatcher) checkDecision(ctx context.Context, b batch) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var chat strings.Builder

	// 拼接最近滑动窗口历史上下文（最多取最近 15 条），让前置模型看全聊天语境
	historySlice := b.History
	if len(historySlice) > 15 {
		historySlice = historySlice[len(historySlice)-15:]
	}
	replacer := strings.NewReplacer("\n", " ", "\r", " ", "<", "＜", ">", "＞")
	for _, msg := range historySlice {
		name := replacer.Replace(msg.SenderName)
		fmt.Fprintf(&chat, "%s: %s\n", name, msg.Content)
	}

	// 拼接本批次待处理的新消息
	for _, msg := range b.Messages {
		name := replacer.Replace(msg.SenderName)
		fmt.Fprintf(&chat, "%s: %s\n", name, msg.Content)
	}
	background := b.Config.Rules.GroupProfiles[b.GroupID]
	if background == "" {
		background = b.Config.Rules.DefaultProfile
	}
	if b.Rule.DecisionContext != "" {
		background = b.Rule.DecisionContext
	}
	if b.Warm {
		background += "；机器人刚被@参与对话，针对机器人上一回复的纠错、调侃、差评、追问均属于可接话内容，不应仅因闲聊判为无价值；敏感限制仍适用。"
	}
	payload := map[string]interface{}{"chat_text": chat.String(), "group_type": background, "threshold": b.Rule.DecisionThreshold}
	if b.Rule.SensitiveCriteria != "" {
		payload["sensitive_criteria"] = b.Rule.SensitiveCriteria
	}
	if b.Rule.NoiseCriteria != "" {
		payload["noise_criteria"] = b.Rule.NoiseCriteria
	}
	if b.Rule.SafeCriteria != "" {
		payload["safe_criteria"] = b.Rule.SafeCriteria
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(b.Config.Rules.LayaURL, "/")+"/filter", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("content gate HTTP %d", resp.StatusCode)
	}
	var result struct {
		ShouldReply bool    `json:"should_reply"`
		Probability float64 `json:"probability"`
		Intent      string  `json:"intent"`
		Decision    string  `json:"decision"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&result); err != nil {
		return fmt.Errorf("invalid gate response: %w", err)
	}
	// Business topics: "is this relevant to the owner's products" is not the small model's call, so
	// a noise (or low-confidence safe) verdict does not block a promo batch. Sensitive and unknown
	// verdicts still do, and Hermes plus the skill decide whether to speak at all.
	if (b.Promo || b.Warm) && (result.Intent == "noise" || result.Intent == "safe") && result.Probability >= 0 && result.Probability <= 1 {
		return nil
	}
	if !result.ShouldReply || result.Intent != "safe" || result.Decision != "allow" || result.Probability < b.Rule.DecisionThreshold || result.Probability > 1 {
		return fmt.Errorf("content gate rejected: %s", result.Intent)
	}
	return nil
}

func (d *Dispatcher) deliver(ctx context.Context, b batch, reply string) (deliveryErr error) {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	stopOnShutdown := context.AfterFunc(d.ctx, stop)
	defer stopOnShutdown()
	if policyID(d.configSnapshot()) != b.Policy {
		if b.TraceID != "" {
			d.traceTerminal(b, contracts.OutcomeSuppressed, "config_changed")
		}
		return fmt.Errorf("configuration changed; stale reply discarded")
	}
	direct := b.TraceID == ""
	if err := d.ensureTrace(&b); err != nil {
		return err
	}
	if direct {
		if err := d.tracePhase(b, contracts.PhaseChecking, "direct_delivery"); err != nil {
			return err
		}
	}
	run, err := d.trace.GetRun(b.TraceID)
	if err != nil {
		return d.traceFailure()
	}
	if run.Phase != contracts.PhaseChecking || run.Outcome != contracts.OutcomePending || run.Delivery != contracts.DeliveryNotStarted || run.Action != nil || run.DeliveryEvidence != (contracts.DeliveryEvidence{}) {
		return fmt.Errorf("run is not eligible for a new submission")
	}
	crossed := false
	outcome, reason := contracts.OutcomeBlocked, "bridge_unavailable"
	defer func() {
		if deliveryErr != nil && !crossed && !d.traceFailed.Load() {
			if d.ctx.Err() != nil {
				_ = d.traceUnknown(b, "submission_interrupted")
			} else if !d.traceTerminal(b, outcome, reason) {
				deliveryErr = errTraceUnavailable
			}
		}
	}()
	// The bridge also serializes all callers. Keep this lock through the cooldown, including errors.
	d.sendMu.Lock()
	defer d.sendMu.Unlock()
	if ctx.Err() != nil {
		outcome, reason = contracts.OutcomeExpired, "submission_context_expired"
		return ctx.Err()
	}
	cfg := d.configSnapshot()
	if policyID(cfg) != b.Policy {
		outcome, reason = contracts.OutcomeSuppressed, "config_changed"
		return fmt.Errorf("configuration changed; stale reply discarded")
	}
	rule, ok := cfg.route(b.GroupID)
	if !ok || cfg.Bridge.DryRun || rule.DryRun || cfg.endpointDryRun(b.Rule.TargetGroupID) {
		outcome, reason = contracts.OutcomeSuppressed, "delivery_disabled"
		return fmt.Errorf("delivery disabled")
	}
	if time.Since(time.UnixMilli(b.Messages[len(b.Messages)-1].Timestamp)) > time.Duration(cfg.Rules.MaxMessageAgeSec)*time.Second {
		outcome, reason = contracts.OutcomeExpired, "submission_expired"
		return fmt.Errorf("reply expired while waiting for GUI")
	}
	d.mu.Lock()
	g := d.groupLocked(b.GroupID)
	// A direct @ is an explicit summon the group can see; it must still go out even if the
	// send-time anti-flood window is full (ownReplyOvertookBatch is a staleness check, not
	// anti-flood, and always applies).
	if !b.Direct && !rule.SupportMode && ((!b.Mentioned && replyLimited(g, rule, time.Now(), b.Warm && warmLevel(g, time.Now()) > 0)) || ownReplyOvertookBatch(g, b)) {
		d.mu.Unlock()
		outcome, reason = contracts.OutcomeSuppressed, "rate_limited"
		return fmt.Errorf("reply rate limit")
	}
	d.mu.Unlock()
	platform, _, platformOK := contracts.ParseContainerID(b.Rule.TargetGroupID)
	if !platformOK {
		return fmt.Errorf("invalid target container %q", b.Rule.TargetGroupID)
	}
	timeoutSec := cfg.Bridge.TimeoutSec
	var endpoint ActionEndpointConfig
	if platform != contracts.PlatformWeChat {
		var hasEndpoint bool
		_, endpoint, hasEndpoint = cfg.actionEndpoint(b.Rule.TargetGroupID)
		if !hasEndpoint {
			outcome, reason = contracts.OutcomeBlocked, "action_endpoint_unconfigured"
			return fmt.Errorf("action endpoint not configured for platform %s", platform)
		}
		timeoutSec = endpoint.TimeoutSec
		outcome, reason = contracts.OutcomeBlocked, "connector_unavailable"
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()
	var (
		action      contracts.ActionRequest
		target      contracts.SourceRef
		actionKind  contracts.ActionKind
		postURL     string
		body        []byte
		isConnector bool
	)
	expires, _ := ctx.Deadline()
	messageExpiry := time.UnixMilli(b.Messages[len(b.Messages)-1].Timestamp).Add(time.Duration(cfg.Rules.MaxMessageAgeSec) * time.Second)
	if messageExpiry.Before(expires) {
		expires = messageExpiry
	}
	if platform == contracts.PlatformWeChat {
		// Refuse older bridge versions that ignore group_id, request_id, or explicit text delivery.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(cfg.Bridge.URL, "/")+"/health", nil)
		if err != nil {
			return err
		}
		resp, err := d.client.Do(req)
		if err != nil {
			return err
		}
		var health struct {
			Protocol      int  `json:"protocol_version"`
			GroupRouting  bool `json:"group_routing"`
			SerialUI      bool `json:"serial_ui"`
			Accessibility bool `json:"accessibility_trusted"`
			ScreenCapture bool `json:"screen_capture_authorized"`
			DryRun        bool `json:"dry_run"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&health)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || health.Protocol < 2 || !health.GroupRouting || !health.SerialUI {
			return fmt.Errorf("bridge does not support verified group routing")
		}
		if !health.Accessibility || !health.ScreenCapture || health.DryRun {
			return fmt.Errorf("bridge sending unavailable: accessibility=%t screen_capture=%t dry_run=%t", health.Accessibility, health.ScreenCapture, health.DryRun)
		}
		requestID := messageDigest(b.TraceID + ":bridge")
		run, err = d.trace.GetRun(b.TraceID)
		if err != nil {
			return d.traceFailure()
		}
		action, err = WeChatActionRequest(run, reply, requestID, expires)
		if err != nil {
			return err
		}
		if err := ValidateWeChatActionRequest(run, action, expires); err != nil {
			return err
		}
		body, _ = json.Marshal(map[string]interface{}{"group_id": action.Target.Source.ContainerID, "target": rule.Target, "text": action.Text, "request_id": action.ActionID, "idempotency_key": action.IdempotencyKey, "expires_at_unix_ms": action.ExpiresAt.UnixMilli(), "delivery": map[string]string{"type": "text"}})
		postURL = cfg.Bridge.URL
	} else {
		isConnector = true
		var replyCtx json.RawMessage
		target, actionKind, replyCtx, err = ActionTargetForBatch(b)
		if err != nil {
			return err
		}
		baseURL := strings.TrimRight(endpoint.URL, "/")
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
		if err != nil {
			return err
		}
		resp, err := d.client.Do(req)
		if err != nil {
			return err
		}
		var health struct {
			Status       string   `json:"status"`
			Protocol     int      `json:"protocol_version"`
			Platform     string   `json:"platform"`
			Ready        bool     `json:"ready"`
			DryRun       bool     `json:"dry_run"`
			Capabilities []string `json:"capabilities"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&health)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || health.Protocol < 1 || health.Platform != string(platform) || !health.Ready || (health.Status != "ok" && health.Status != "ready" && health.Status != "healthy") {
			return fmt.Errorf("connector health check failed")
		}
		if health.DryRun {
			reason = "connector_dry_run"
			return fmt.Errorf("connector dry_run enabled")
		}
		if len(health.Capabilities) > 0 && !contains(health.Capabilities, string(actionKind)) {
			return fmt.Errorf("connector does not support %s", actionKind)
		}
		requestID := messageDigest(b.TraceID + ":action:" + string(platform))
		run, err = d.trace.GetRun(b.TraceID)
		if err != nil {
			return d.traceFailure()
		}
		action, err = PlatformActionRequest(run, target, actionKind, reply, requestID, expires)
		if err != nil {
			return err
		}
		if err := ValidatePlatformActionRequest(run, target, actionKind, action, expires); err != nil {
			return err
		}
		payload := map[string]interface{}{
			"protocol_version":   1,
			"action_id":          action.ActionID,
			"run_id":             action.RunID,
			"idempotency_key":    action.IdempotencyKey,
			"platform":           string(action.Target.Source.Platform),
			"kind":               string(action.Kind),
			"text":               action.Text,
			"expires_at":         action.ExpiresAt.UTC().Format(time.RFC3339Nano),
			"expires_at_unix_ms": action.ExpiresAt.UnixMilli(),
			"target": map[string]interface{}{
				"platform":        string(action.Target.Source.Platform),
				"account_id":      action.Target.Source.AccountID,
				"container_id":    action.Target.Source.ContainerID,
				"container_label": rule.Target,
				"content_id":      action.Target.Source.ContentID,
				"parent_id":       action.Target.Source.ParentID,
				"root_id":         action.Target.Source.RootID,
				"target_label":    rule.Target,
			},
		}
		if len(replyCtx) > 0 {
			payload["reply_context"] = replyCtx
		}
		body, _ = json.Marshal(payload)
		postURL = baseURL + "/v1/actions"
	}
	d.submissionMu.Lock()
	locked := true
	defer func() {
		if locked {
			d.submissionMu.Unlock()
		}
	}()
	var confirmation *awaitingConfirmation
	var waiter *echoWaiter
	admit := func(admissionCtx context.Context) error {
		if crossed {
			return fmt.Errorf("submission already admitted")
		}
		if policyID(d.configSnapshot()) != b.Policy {
			outcome, reason = contracts.OutcomeSuppressed, "config_changed"
			return fmt.Errorf("configuration changed; stale reply discarded")
		}
		if err := admissionCtx.Err(); err != nil {
			outcome, reason = contracts.OutcomeExpired, "submission_context_expired"
			return err
		}
		if err := ctx.Err(); err != nil {
			outcome, reason = contracts.OutcomeExpired, "submission_context_expired"
			return err
		}
		if !time.Now().Before(action.ExpiresAt) {
			outcome, reason = contracts.OutcomeExpired, "submission_expired"
			return fmt.Errorf("action expired before submission")
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.closed || d.ctx.Err() != nil || d.traceFailed.Load() {
			return errTraceUnavailable
		}
		if blocked := d.batchBlockLocked(b); blocked != "" {
			outcome, reason = contracts.OutcomeSuppressed, blocked
			if blocked == "run_canceled" {
				outcome = contracts.OutcomeCanceled
			}
			return fmt.Errorf("%s", blocked)
		}
		if time.Since(time.UnixMilli(b.Messages[len(b.Messages)-1].Timestamp)) > time.Duration(cfg.Rules.MaxMessageAgeSec)*time.Second {
			outcome, reason = contracts.OutcomeExpired, "submission_expired"
			return fmt.Errorf("reply expired before submission")
		}
		if !b.Direct && !rule.SupportMode && ((!b.Mentioned && replyLimited(g, rule, time.Now(), b.Warm && warmLevel(g, time.Now()) > 0)) || ownReplyOvertookBatch(g, b)) {
			outcome, reason = contracts.OutcomeSuppressed, "own_reply_overtook"
			return fmt.Errorf("own reply arrived before GUI submission")
		}
		current, err := d.trace.GetRun(b.TraceID)
		if err != nil {
			return d.traceFailure()
		}
		if isConnector {
			if err := ValidatePlatformActionRequest(current, target, actionKind, action, expires); err != nil {
				return err
			}
		} else {
			if err := ValidateWeChatActionRequest(current, action, expires); err != nil {
				return err
			}
		}
		confirmation = d.newConfirmationLocked(b, action.Text, action.ActionID)
		g.SendStarted = true
		if err := d.persistConfirmationsLocked(); err != nil {
			outcome, reason = contracts.OutcomeFailed, "state_persistence_failed"
			return err
		}
		if err := d.updateTrace(b, "submission_started", func(run *contracts.Run) {
			run.Phase, run.Delivery, run.ReasonCode = contracts.PhaseSubmitting, contracts.DeliverySubmitting, ""
			run.DeliveryEvidence.SubmissionStartedAt = &confirmation.SubmissionStartedAt
			run.DeliveryEvidence.BridgeRequestID = action.ActionID
			run.Action = &contracts.ActionRef{ActionID: action.ActionID, Kind: action.Kind, Platform: action.Target.Source.Platform, IdempotencyKey: action.IdempotencyKey, AdmissionID: "admission-" + action.ActionID, AdmittedAt: &confirmation.SubmissionStartedAt}
		}); err != nil {
			return err
		}
		crossed = true
		waiter = &echoWaiter{runID: b.TraceID, done: make(chan struct{}), responsePending: true}
		d.awaitingEcho = waiter
		return nil
	}
	if runner, ok := d.runner.(runnerDeliveryGate); ok && !b.Direct {
		outcome, reason = contracts.OutcomeSuppressed, "permission_changed"
		err = runner.DeliveryGate(ctx, b.TraceID, b.GroupID, admit)
	} else {
		err = admit(ctx)
	}
	d.submissionMu.Unlock()
	locked = false
	if err != nil || !crossed {
		if crossed {
			d.mu.Lock()
			d.awaitingEcho = nil
			d.mu.Unlock()
			_ = d.traceUnknown(b, "submission_gate_interrupted")
		}
		if err == nil {
			err = &execution.Error{Code: "policy_blocked", Field: "delivery_not_admitted"}
		}
		return err
	}
	defer func() {
		d.mu.Lock()
		if d.awaitingEcho == waiter {
			d.awaitingEcho = nil
		}
		d.mu.Unlock()
	}()
	defer func() {
		timer := time.NewTimer(d.sendCooldown)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-d.ctx.Done():
		}
	}()
	if isConnector {
		return d.finishConnectorDelivery(ctx, postURL, body, action, target, confirmation, waiter)
	}
	status, respText, err := postBridgeDelivery(ctx, d.client, postURL, body)
	responseReceivedAt := time.Now().UTC()
	d.mu.Lock()
	waiter.responsePending = false
	if err == nil && status == 200 && respText == "OK" {
		confirmation.AckReceivedAt = &responseReceivedAt
		if persistErr := d.persistConfirmationsLocked(); persistErr != nil {
			d.mu.Unlock()
			return persistErr
		}
		if confirmation.Echo != nil {
			result := d.confirmEchoLocked(confirmation)
			d.mu.Unlock()
			return result
		}
		if traceErr := d.acceptSubmissionLocked(confirmation); traceErr != nil {
			d.mu.Unlock()
			return traceErr
		}
	} else {
		reason := "bridge_response_unknown"
		if err != nil {
			reason = "bridge_transport_unknown"
		}
		result := d.confirmOrUnknownLocked(confirmation, reason)
		d.mu.Unlock()
		return result
	}
	d.mu.Unlock()
	timer := time.NewTimer(d.echoTimeout)
	defer timer.Stop()
	select {
	case <-waiter.done:
	case <-timer.C:
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.confirmOrUnknownLocked(confirmation, "echo_timeout")
	case <-ctx.Done():
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.confirmOrUnknownLocked(confirmation, "echo_interrupted")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.confirmOrUnknownLocked(confirmation, "echo_evidence_missing")
}

type connectorActionResult struct {
	ActionID   string `json:"action_id"`
	Status     string `json:"status"`
	Submitted  *bool  `json:"submitted,omitempty"`
	ExternalID string `json:"external_id,omitempty"`
	ReasonCode string `json:"reason_code,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func postConnectorAction(ctx context.Context, client *http.Client, url string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 16384))
	resp.Body.Close()
	if readErr != nil {
		return 0, nil, readErr
	}
	return resp.StatusCode, respBody, nil
}

func (d *Dispatcher) finishConnectorDelivery(
	ctx context.Context,
	postURL string,
	body []byte,
	action contracts.ActionRequest,
	target contracts.SourceRef,
	confirmation *awaitingConfirmation,
	waiter *echoWaiter,
) error {
	status, rawBody, err := postConnectorAction(ctx, d.client, postURL, body)
	responseReceivedAt := time.Now().UTC()
	var decoded connectorActionResult
	decodeErr := fmt.Errorf("empty response")
	if err == nil {
		decodeErr = json.Unmarshal(rawBody, &decoded)
	}

	d.mu.Lock()
	waiter.responsePending = false
	if err != nil {
		result := d.confirmOrUnknownLocked(confirmation, "connector_transport_unknown")
		d.mu.Unlock()
		return result
	}
	if decodeErr != nil {
		result := d.confirmOrUnknownLocked(confirmation, "connector_response_unknown")
		d.mu.Unlock()
		return result
	}
	if status == http.StatusOK && decoded.ActionID == action.ActionID {
		switch decoded.Status {
		case "confirmed":
			if contracts.ValidID(decoded.ExternalID) {
				result := d.confirmDirectLocked(confirmation, decoded.ExternalID, responseReceivedAt, action.Text, target.AccountID)
				d.mu.Unlock()
				return result
			}
		case "submitted":
			confirmation.AckReceivedAt = &responseReceivedAt
			if persistErr := d.persistConfirmationsLocked(); persistErr != nil {
				d.mu.Unlock()
				return persistErr
			}
			if confirmation.Echo != nil {
				result := d.confirmEchoLocked(confirmation)
				d.mu.Unlock()
				return result
			}
			if traceErr := d.acceptSubmissionLocked(confirmation); traceErr != nil {
				d.mu.Unlock()
				return traceErr
			}
			d.mu.Unlock()
			timer := time.NewTimer(d.echoTimeout)
			defer timer.Stop()
			select {
			case <-waiter.done:
			case <-timer.C:
				d.mu.Lock()
				defer d.mu.Unlock()
				return d.confirmOrUnknownLocked(confirmation, "echo_timeout")
			case <-ctx.Done():
				d.mu.Lock()
				defer d.mu.Unlock()
				return d.confirmOrUnknownLocked(confirmation, "echo_interrupted")
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			return d.confirmOrUnknownLocked(confirmation, "echo_evidence_missing")
		}
	}
	if status >= 400 && status < 500 &&
		(decoded.ActionID == "" || decoded.ActionID == action.ActionID) &&
		decoded.Status == "failed" && decoded.Submitted != nil && !*decoded.Submitted {
		failReason := sanitizeFailureReason(decoded.ReasonCode)
		traceErr := d.failSubmissionLocked(confirmation, failReason)
		d.mu.Unlock()
		if traceErr != nil {
			return traceErr
		}
		return fmt.Errorf("connector rejected action before submission: %s", failReason)
	}
	result := d.confirmOrUnknownLocked(confirmation, "connector_response_unknown")
	d.mu.Unlock()
	return result
}

// focusRetryDelay is the wait before retrying a delivery the bridge rejected
// with INPUT_FOCUS_FAILED (WeChat window hidden/closed, input focus lost).
// Package-level so tests can shorten it.
var focusRetryDelay = 3 * time.Second

// postBridgeDelivery POSTs the JSON payload to the bridge. When the bridge
// answers 409 INPUT_FOCUS_FAILED (a recoverable focus error), it waits
// focusRetryDelay and retries the same request exactly once. Any other
// non-OK outcome (other 409s, 5xx, malformed body) is returned without retry.
// Network/transport errors are returned as errors; HTTP-level rejections are
// reported via the returned status/body so the caller can classify them.
func postBridgeDelivery(ctx context.Context, client *http.Client, url string, body []byte) (int, string, error) {
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return 0, "", err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return 0, "", err
		}
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 8192))
		resp.Body.Close()
		if readErr != nil {
			return 0, "", readErr
		}
		text := strings.TrimSpace(string(respBody))
		if resp.StatusCode == 200 && text == "OK" {
			return resp.StatusCode, text, nil
		}
		if attempt == 0 && resp.StatusCode == 409 && strings.Contains(text, "INPUT_FOCUS_FAILED") {
			log.Printf("bridge 上报输入框聚焦失败（INPUT_FOCUS_FAILED），%v 后重试同一请求", focusRetryDelay)
			select {
			case <-time.After(focusRetryDelay):
			case <-ctx.Done():
				return resp.StatusCode, text, fmt.Errorf("focus retry interrupted; not retried: %w", ctx.Err())
			}
			continue
		}
		return resp.StatusCode, text, nil
	}
}

func (r HermesResult) actualBackend() string {
	if r.Backend == "Alma" || r.Backend == "Hermes" {
		return r.Backend
	}
	if strings.HasPrefix(r.SessionID, "alma:") {
		return "Alma"
	}
	return "Hermes"
}

// Support events must identify primary Alma success or an explicit, known degradation.
// Legacy groups retain their existing event/session compatibility.
func validSupportBackend(r HermesResult) bool {
	if r.Backend == "Alma" {
		return !r.Fallback && r.FallbackReason == "" && regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`).MatchString(r.RunID) && r.SessionID == "alma:"+r.RunID
	}
	if r.Backend != "Hermes" || !r.Fallback {
		return false
	}
	switch r.FallbackReason {
	case "alma_timeout", "alma_failed", "alma_empty_summary", "alma_invalid_output", "alma_transport_error":
		return true
	}
	return false
}
