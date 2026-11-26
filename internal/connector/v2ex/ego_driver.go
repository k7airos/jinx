package v2ex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

const egoResultMarker = "__JINX_V2EX_EGO_RESULT__:"

type EgoDriver struct {
	cfg       EgoConfig
	runScript func(ctx context.Context, script string) ([]byte, error)
}

func NewEgoDriver(cfg EgoConfig) *EgoDriver {
	d := &EgoDriver{cfg: cfg}
	d.runScript = d.execEgoNodeJS
	return d
}

func resolveEgoBin(configured string) (string, error) {
	if configured != "" {
		info, err := os.Stat(configured)
		if err != nil {
			return "", fmt.Errorf("ego-browser bin_path %q: %w", configured, err)
		}
		if info.IsDir() {
			return "", fmt.Errorf("ego-browser bin_path %q is a directory", configured)
		}
		return configured, nil
	}
	if p, err := exec.LookPath("ego-browser"); err == nil {
		return p, nil
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidate := filepath.Join(home, ".local", "bin", "ego-browser")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("ego-browser binary not found")
}

type syncLimitedBuffer struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	limit int
}

func (b *syncLimitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.buf.Len()+len(p) > b.limit {
		return 0, errors.New("ego output exceeds limit")
	}
	return b.buf.Write(p)
}

func (b *syncLimitedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

func minimalEgoEnv() []string {
	allow := map[string]bool{
		"PATH": true, "HOME": true, "TMPDIR": true,
		"LANG": true, "LC_ALL": true, "LC_CTYPE": true,
		"USER": true, "LOGNAME": true, "SHELL": true,
		"XDG_CONFIG_HOME": true, "XDG_DATA_HOME": true, "XDG_CACHE_HOME": true,
	}
	var env []string
	for _, entry := range os.Environ() {
		if k, _, ok := strings.Cut(entry, "="); ok && allow[k] {
			env = append(env, entry)
		}
	}
	return env
}

func (d *EgoDriver) execEgoNodeJS(ctx context.Context, script string) ([]byte, error) {
	binPath, err := resolveEgoBin(d.cfg.BinPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, binPath, "nodejs")
	cmd.Env = minimalEgoEnv()
	cmd.Stdin = strings.NewReader(script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = 2 * time.Second
	combined := &syncLimitedBuffer{limit: 4 << 20}
	cmd.Stdout = combined
	cmd.Stderr = combined
	err = cmd.Run()
	out := combined.Bytes()
	if err != nil {
		return out, fmt.Errorf("ego-browser nodejs failed: %w", err)
	}
	return out, nil
}

func (d *EgoDriver) Health(_ context.Context) (bool, string) {
	if _, err := resolveEgoBin(d.cfg.BinPath); err != nil {
		return false, "ego_browser_not_found"
	}
	return true, ""
}

type egoFetchConfig struct {
	TaskSpace string `json:"taskSpace"`
	TargetURL string `json:"targetUrl"`
	TopicID   string `json:"topicId"`
	PollLimit int    `json:"pollLimit"`
	KeepPage  bool   `json:"keepPage"`
}

type egoFetchComment struct {
	ReplyID     string `json:"reply_id"`
	Floor       int    `json:"floor"`
	SenderID    string `json:"sender_id"`
	SenderName  string `json:"sender_name"`
	Content     string `json:"content"`
	TimestampMS int64  `json:"timestamp_ms"`
	IsSelf      bool   `json:"is_self"`
}

type egoFetchOutput struct {
	OK             bool              `json:"ok"`
	ViewerUsername string            `json:"viewer_username"`
	Comments       []egoFetchComment `json:"comments"`
	ErrorCode      string            `json:"error_code"`
	Detail         string            `json:"detail"`
}

func (d *EgoDriver) FetchComments(ctx context.Context, target TargetConfig) ([]Comment, string, error) {
	payload := egoFetchConfig{
		TaskSpace: d.cfg.TaskSpace,
		TargetURL: target.URL,
		TopicID:   target.TopicID,
		PollLimit: target.PollLimit,
		KeepPage:  d.cfg.KeepPage,
	}
	script, err := buildEgoFetchScript(payload)
	if err != nil {
		return nil, "", err
	}
	raw, err := d.runScript(ctx, script)
	if err != nil {
		return nil, "", err
	}
	var out egoFetchOutput
	if err := parseEgoMarker(raw, &out); err != nil {
		return nil, "", err
	}
	if !out.OK {
		return nil, out.ViewerUsername, fmt.Errorf("ego fetch failed (%s): %s", out.ErrorCode, out.Detail)
	}
	comments := make([]Comment, 0, len(out.Comments))
	for _, item := range out.Comments {
		replyID := NormalizeReplyID(item.ReplyID)
		if replyID == "" || strings.TrimSpace(item.Content) == "" {
			continue
		}
		senderID := strings.TrimSpace(item.SenderID)
		senderName := strings.TrimSpace(item.SenderName)
		if senderName == "" {
			senderName = senderID
		}
		comments = append(comments, Comment{
			ReplyID:     replyID,
			TopicID:     target.TopicID,
			Floor:       item.Floor,
			SenderID:    senderID,
			SenderName:  senderName,
			Content:     item.Content,
			TimestampMS: item.TimestampMS,
			IsSelf:      item.IsSelf || (out.ViewerUsername != "" && senderID != "" && strings.EqualFold(senderID, out.ViewerUsername)),
		})
	}
	return comments, out.ViewerUsername, nil
}

type egoReplyConfig struct {
	TaskSpace string `json:"taskSpace"`
	TargetURL string `json:"targetUrl"`
	TopicID   string `json:"topicId"`
	Text      string `json:"text"`
	KeepPage  bool   `json:"keepPage"`
}

type egoReplyOutput struct {
	Status     string `json:"status"`
	Submitted  *bool  `json:"submitted,omitempty"`
	ExternalID string `json:"external_id,omitempty"`
	ReasonCode string `json:"reason_code,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func (d *EgoDriver) PostReply(ctx context.Context, req ReplyRequest) (ReplyResult, error) {
	payload := egoReplyConfig{
		TaskSpace: d.cfg.TaskSpace,
		TargetURL: req.Target.URL,
		TopicID:   req.TopicID,
		Text:      req.Text,
		KeepPage:  d.cfg.KeepPage,
	}
	script, err := buildEgoReplyScript(payload)
	if err != nil {
		return ReplyResult{Status: "failed", Submitted: connector.BoolPtr(false), ReasonCode: "script_build_failed", Detail: err.Error()}, err
	}
	raw, err := d.runScript(ctx, script)
	if err != nil {
		return ReplyResult{Status: "unknown", ReasonCode: "transport_unknown", Detail: "driver transport error"}, nil
	}
	var out egoReplyOutput
	if err := parseEgoMarker(raw, &out); err != nil {
		return ReplyResult{Status: "unknown", ReasonCode: "output_parse_unknown", Detail: "missing or invalid driver result"}, nil
	}
	switch out.Status {
	case "confirmed":
		ext := NormalizeReplyID(out.ExternalID)
		if !contracts.ValidID(ext) {
			return ReplyResult{Status: "submitted", Submitted: connector.BoolPtr(true), ReasonCode: "missing_external_id"}, nil
		}
		return ReplyResult{Status: "confirmed", Submitted: connector.BoolPtr(true), ExternalID: ext}, nil
	case "submitted":
		return ReplyResult{Status: "submitted", Submitted: connector.BoolPtr(true), ReasonCode: out.ReasonCode}, nil
	case "failed":
		reason := classifyV2EXProblem(out.Detail, out.ReasonCode)
		if out.Submitted != nil && !*out.Submitted {
			return ReplyResult{
				Status:     "failed",
				Submitted:  connector.BoolPtr(false),
				ReasonCode: reason,
				Detail:     out.Detail,
			}, nil
		}
		return ReplyResult{Status: "unknown", ReasonCode: reason, Detail: out.Detail}, nil
	default:
		reason := out.ReasonCode
		if reason == "" {
			reason = "transport_unknown"
		}
		return ReplyResult{Status: "unknown", ReasonCode: reason, Detail: out.Detail}, nil
	}
}

func classifyV2EXProblem(detail, fallback string) string {
	lower := strings.ToLower(detail)
	switch {
	case strings.Contains(detail, "过于频繁") ||
		strings.Contains(detail, "请不要在短期内") ||
		strings.Contains(detail, "稍后再试") ||
		strings.Contains(lower, "rate") ||
		strings.Contains(lower, "too fast"):
		return "rate_limited"
	case strings.Contains(detail, "重复") ||
		strings.Contains(detail, "已经回复") ||
		strings.Contains(lower, "duplicate") ||
		strings.Contains(lower, "identical"):
		return "duplicate_reply"
	case strings.Contains(detail, "禁止回复") ||
		strings.Contains(detail, "已关闭") ||
		strings.Contains(detail, "已锁定") ||
		strings.Contains(lower, "closed") ||
		strings.Contains(lower, "locked") ||
		strings.Contains(lower, "archived"):
		return "replies_closed"
	case strings.Contains(detail, "登录") ||
		strings.Contains(lower, "sign in") ||
		strings.Contains(lower, "login") ||
		strings.Contains(lower, "2fa"):
		return "not_logged_in"
	case strings.Contains(detail, "验证码") ||
		strings.Contains(lower, "captcha") ||
		strings.Contains(lower, "turnstile") ||
		strings.Contains(lower, "cloudflare"):
		return "captcha_required"
	}
	if fallback != "" && fallback != "problem" && contracts.ValidID(fallback) {
		return fallback
	}
	return "platform_rejected"
}

func parseEgoMarker(output []byte, dst any) error {
	lines := strings.Split(string(output), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, egoResultMarker) {
			jsonPart := strings.TrimPrefix(line, egoResultMarker)
			if err := json.Unmarshal([]byte(jsonPart), dst); err != nil {
				return fmt.Errorf("decode ego marker JSON: %w", err)
			}
			return nil
		}
	}
	return fmt.Errorf("missing %s marker in ego output", egoResultMarker)
}

func buildEgoPageScript(cfg any, userControlResult string, errorResult string, pageBody string) (string, error) {
	rawCfg, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`const CONFIG = %s;
const MARKER = %q;
const emit = (obj) => console.log(MARKER + JSON.stringify(obj));

const existing = (await listTaskSpaces()).find((s) => s.name === CONFIG.taskSpace);
if (existing && existing.ownership === "agentDelegatedToUser") {
  emit(%s);
} else {
  const task = !existing
    ? await taskSpace(CONFIG.taskSpace)
    : existing.ownership === "user"
      ? await claimTaskSpace(existing.id)
      : await taskSpace(existing.id);
  let page = null;
  try {
    const pages = await task.pages();
    page = pages[0] ?? null;
    if (!page) {
      const tabs = await task.tabs();
      page = tabs[0] ? await task.adopt(tabs[0].page) : await task.newPage();
    }
    await page.goto(CONFIG.targetUrl);
%s
  } catch (err) {
    emit(Object.assign(%s, { detail: String((err && err.message) || err) }));
  } finally {
    try {
      await task.finish({ keep: CONFIG.keepPage && page ? [page.label] : [] });
    } catch (_) {}
  }
}
`, string(rawCfg), egoResultMarker, userControlResult, pageBody, errorResult), nil
}

const jsChallengeDetector = `const isChallenge = (doc) => {
  const title = (doc.title || "").toLowerCase();
  if (title.includes("just a moment") || title.includes("attention required")) return true;
  if (doc.querySelector('#challenge-form, #cf-challenge-running, iframe[src*="challenges.cloudflare.com"], .cf-turnstile, .g-recaptcha')) return true;
  return false;
};`

func buildEgoFetchScript(cfg egoFetchConfig) (string, error) {
	pageBody := `    const pageMeta = await page.evaluate(() => {
` + jsChallengeDetector + `
      if (isChallenge(document)) {
        return { challenge: true };
      }
      let currentPage = 1;
      const curEl = document.querySelector("span.page_current");
      if (curEl) {
        const parsed = parseInt((curEl.textContent || "").trim(), 10);
        if (parsed > 0) currentPage = parsed;
      }
      let maxPage = currentPage;
      for (const a of document.querySelectorAll('a.page_normal[href*="?p="]')) {
        const href = a.getAttribute("href") || "";
        const m = href.match(/[?&]p=([0-9]+)/);
        const n = m ? parseInt(m[1], 10) : parseInt((a.textContent || "").trim(), 10);
        if (n > maxPage) maxPage = n;
      }
      return { challenge: false, currentPage, maxPage };
    });

    if (pageMeta.challenge) {
      emit({ ok: false, error_code: "captcha_required", detail: "challenge page detected" });
    } else {
      if (pageMeta.maxPage > pageMeta.currentPage) {
        await page.goto(CONFIG.targetUrl + "?p=" + pageMeta.maxPage);
      }
      const res = await page.evaluate((c) => {
` + jsChallengeDetector + `
        if (isChallenge(document)) {
          return { ok: false, error_code: "captcha_required", detail: "challenge page detected" };
        }
        const topLink = document.querySelector('#Top a[href^="/member/"]');
        const viewerHref = topLink ? (topLink.getAttribute("href") || "") : "";
        const viewerUsername = viewerHref.startsWith("/member/") ? viewerHref.slice("/member/".length).split(/[/?#]/)[0] : "";
        const parseTimestamp = (raw) => {
          const trimmed = (raw || "").trim();
          if (!trimmed) return 0;
          const m = trimmed.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s*([+-]\d{2}:\d{2})$/);
          const iso = m ? (m[1] + "T" + m[2] + m[3]) : trimmed;
          const ms = Date.parse(iso);
          return Number.isFinite(ms) ? ms : 0;
        };
        const items = [];
        for (const el of document.querySelectorAll('div[id^="r_"]')) {
          const replyId = (el.id || "").replace(/^r_/, "").trim();
          const content = (el.querySelector(".reply_content")?.innerText || "").trim();
          if (!replyId || !content) continue;
          const authorLink = el.querySelector('strong a[href^="/member/"]');
          const authorHref = authorLink ? (authorLink.getAttribute("href") || "") : "";
          const senderId = authorHref.startsWith("/member/")
            ? authorHref.slice("/member/".length).split(/[/?#]/)[0]
            : ((authorLink?.textContent || "").trim());
          const senderName = (authorLink?.textContent || "").trim() || senderId;
          const floor = parseInt((el.querySelector(".no")?.textContent || "").trim(), 10) || 0;
          const ts = parseTimestamp(el.querySelector(".ago")?.getAttribute("title") || "");
          items.push({
            reply_id: replyId,
            floor,
            sender_id: senderId,
            sender_name: senderName,
            content,
            timestamp_ms: ts,
            is_self: viewerUsername !== "" && senderId !== "" && senderId.toLowerCase() === viewerUsername.toLowerCase()
          });
        }
        const limit = c.pollLimit > 0 ? c.pollLimit : 20;
        const comments = items.length > limit ? items.slice(items.length - limit) : items;
        return { ok: true, viewer_username: viewerUsername, comments };
      }, CONFIG);
      emit(res);
    }`
	return buildEgoPageScript(
		cfg,
		`{ ok: false, error_code: "user_control_active", detail: "task space is currently delegated to user" }`,
		`{ ok: false, error_code: "ego_exception" }`,
		pageBody,
	)
}

func buildEgoReplyScript(cfg egoReplyConfig) (string, error) {
	pageBody := `    const res = await page.evaluate(async (c) => {
` + jsChallengeDetector + `
      if (isChallenge(document)) {
        return { status: "failed", submitted: false, reason_code: "captcha_required", detail: "challenge page detected" };
      }
      const topMember = document.querySelector('#Top a[href^="/member/"]');
      const hasSignin = !!document.querySelector('#Top a[href="/signin"]');
      if (!topMember || hasSignin) {
        return { status: "failed", submitted: false, reason_code: "not_logged_in", detail: "v2ex session is not logged in" };
      }
      const viewerHref = topMember.getAttribute("href") || "";
      const viewerUsername = viewerHref.startsWith("/member/") ? viewerHref.slice("/member/".length).split(/[/?#]/)[0] : "";

      const form = document.querySelector('form[action^="/t/"]');
      const onceInput = form ? form.querySelector('input[name="once"]') : null;
      const textarea = form ? form.querySelector('textarea[name="content"]') : null;
      const once = onceInput ? (onceInput.value || "").trim() : "";
      const formAction = form ? (form.getAttribute("action") || "") : "";
      if (!form || !once || !textarea || !formAction) {
        return { status: "failed", submitted: false, reason_code: "replies_closed", detail: "reply form or once token unavailable" };
      }

      const toBig = (raw) => {
        try { return /^[0-9]+$/.test(raw) ? BigInt(raw) : 0n; } catch (_) { return 0n; }
      };
      const beforeIds = new Set();
      let maxBeforeId = 0n;
      for (const el of document.querySelectorAll('div[id^="r_"]')) {
        const rid = (el.id || "").replace(/^r_/, "").trim();
        if (!rid) continue;
        beforeIds.add(rid);
        const val = toBig(rid);
        if (val > maxBeforeId) maxBeforeId = val;
      }

      const params = new URLSearchParams();
      params.set("content", c.text);
      params.set("once", once);

      let resp;
      try {
        resp = await fetch(formAction, {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
          body: params.toString(),
          redirect: "follow"
        });
      } catch (netErr) {
        return { status: "unknown", reason_code: "transport_unknown", detail: String((netErr && netErr.message) || netErr) };
      }

      const finalUrl = String(resp.url || "");
      if (finalUrl.includes("/signin") || finalUrl.includes("/2fa")) {
        return { status: "failed", submitted: false, reason_code: "not_logged_in", detail: "redirected to login" };
      }
      if (resp.status >= 500) {
        return { status: "unknown", reason_code: "http_" + String(resp.status), detail: "v2ex returned HTTP " + String(resp.status) };
      }

      let html = "";
      try {
        html = await resp.text();
      } catch (readErr) {
        return { status: "unknown", reason_code: "response_parse_unknown", detail: String((readErr && readErr.message) || readErr) };
      }

      const doc = new DOMParser().parseFromString(html, "text/html");
      if (isChallenge(doc)) {
        return { status: "failed", submitted: false, reason_code: "captcha_required", detail: "challenge page returned after submit" };
      }
      const problemEl = doc.querySelector(".problem");
      const problemText = problemEl ? (problemEl.textContent || "").trim() : "";
      if (problemText) {
        return { status: "failed", submitted: false, reason_code: "problem", detail: problemText };
      }

      let matchedId = "";
      let matchedBig = 0n;
      for (const el of doc.querySelectorAll('div[id^="r_"]')) {
        const rid = (el.id || "").replace(/^r_/, "").trim();
        if (!rid || beforeIds.has(rid)) continue;
        const val = toBig(rid);
        if (val <= maxBeforeId) continue;
        const link = el.querySelector('strong a[href^="/member/"]');
        const href = link ? (link.getAttribute("href") || "") : "";
        const author = href.startsWith("/member/") ? href.slice("/member/".length).split(/[/?#]/)[0] : ((link?.textContent || "").trim());
        if (viewerUsername && author && author.toLowerCase() === viewerUsername.toLowerCase()) {
          if (val >= matchedBig) {
            matchedBig = val;
            matchedId = rid;
          }
        }
      }
      if (matchedId) {
        return { status: "confirmed", submitted: true, external_id: matchedId };
      }
      if (resp.ok && finalUrl.includes("/t/" + c.topicId)) {
        return { status: "submitted", submitted: true, reason_code: "missing_external_id" };
      }
      return { status: "unknown", reason_code: "unexpected_response", detail: "unexpected v2ex reply response" };
    }, CONFIG);
    emit(res);`
	return buildEgoPageScript(
		cfg,
		`{ status: "failed", submitted: false, reason_code: "user_control_active", detail: "task space is currently delegated to user" }`,
		`{ status: "unknown", reason_code: "ego_exception" }`,
		pageBody,
	)
}
