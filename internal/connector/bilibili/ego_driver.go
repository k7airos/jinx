package bilibili

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

const egoResultMarker = "__JINX_EGO_RESULT__:"

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
	TaskSpace   string `json:"taskSpace"`
	TargetURL   string `json:"targetUrl"`
	OID         string `json:"oid"`
	BVID        string `json:"bvid"`
	CommentType int    `json:"commentType"`
	PollLimit   int    `json:"pollLimit"`
	KeepPage    bool   `json:"keepPage"`
}

type egoFetchComment struct {
	RPID        string `json:"rpid"`
	Root        string `json:"root"`
	Parent      string `json:"parent"`
	SenderID    string `json:"sender_id"`
	SenderName  string `json:"sender_name"`
	Content     string `json:"content"`
	TimestampMS int64  `json:"timestamp_ms"`
	IsSelf      bool   `json:"is_self"`
}

type egoFetchOutput struct {
	OK        bool              `json:"ok"`
	OID       string            `json:"oid"`
	ViewerMID string            `json:"viewer_mid"`
	Comments  []egoFetchComment `json:"comments"`
	ErrorCode string            `json:"error_code"`
	Detail    string            `json:"detail"`
}

func (d *EgoDriver) FetchComments(ctx context.Context, target TargetConfig) ([]Comment, string, error) {
	payload := egoFetchConfig{
		TaskSpace:   d.cfg.TaskSpace,
		TargetURL:   target.URL,
		OID:         target.OID,
		BVID:        target.BVID,
		CommentType: target.CommentType,
		PollLimit:   target.PollLimit,
		KeepPage:    d.cfg.KeepPage,
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
		return nil, out.ViewerMID, fmt.Errorf("ego fetch failed (%s): %s", out.ErrorCode, out.Detail)
	}
	comments := make([]Comment, 0, len(out.Comments))
	for _, item := range out.Comments {
		rpid := NormalizeReplyID(item.RPID)
		if rpid == "" || strings.TrimSpace(item.Content) == "" {
			continue
		}
		oid := out.OID
		if oid == "" {
			oid = target.OID
		}
		comments = append(comments, Comment{
			RPID:        rpid,
			OID:         oid,
			BVID:        target.BVID,
			CommentType: target.CommentType,
			RootID:      NormalizeReplyID(item.Root),
			ParentID:    NormalizeReplyID(item.Parent),
			SenderID:    strings.TrimSpace(item.SenderID),
			SenderName:  strings.TrimSpace(item.SenderName),
			Content:     item.Content,
			TimestampMS: item.TimestampMS,
			IsSelf:      item.IsSelf || (out.ViewerMID != "" && strings.TrimSpace(item.SenderID) == out.ViewerMID),
		})
	}
	return comments, out.ViewerMID, nil
}

type egoReplyConfig struct {
	TaskSpace   string `json:"taskSpace"`
	TargetURL   string `json:"targetUrl"`
	OID         string `json:"oid"`
	CommentType int    `json:"commentType"`
	RootID      string `json:"rootId"`
	ParentID    string `json:"parentId"`
	Text        string `json:"text"`
	KeepPage    bool   `json:"keepPage"`
}

type egoReplyOutput struct {
	Status     string `json:"status"`
	Submitted  *bool  `json:"submitted,omitempty"`
	ExternalID string `json:"external_id,omitempty"`
	Code       int    `json:"code,omitempty"`
	ReasonCode string `json:"reason_code,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

func (d *EgoDriver) PostReply(ctx context.Context, req ReplyRequest) (ReplyResult, error) {
	payload := egoReplyConfig{
		TaskSpace:   d.cfg.TaskSpace,
		TargetURL:   req.Target.URL,
		OID:         req.OID,
		CommentType: req.CommentType,
		RootID:      req.RootID,
		ParentID:    req.ParentID,
		Text:        req.Text,
		KeepPage:    d.cfg.KeepPage,
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
		reason := classifyBilibiliError(out.Code, out.ReasonCode)
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

func classifyBilibiliError(code int, fallback string) string {
	switch code {
	case -101, -111:
		return "not_logged_in"
	case 12002:
		return "comments_closed"
	case 12015:
		return "captcha_required"
	case 12022:
		return "comment_deleted"
	case 12025, 12035:
		return "rate_limited"
	}
	if fallback != "" && contracts.ValidID(fallback) {
		return fallback
	}
	if code != 0 {
		return "bilibili_code_" + strconv.Itoa(code)
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

func buildEgoPageScript(cfg any, userControlResult string, errorResult string, evaluateBody string) (string, error) {
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
    const res = await page.evaluate(async (c) => {
%s
    }, CONFIG);
    emit(res);
  } catch (err) {
    emit(Object.assign(%s, { detail: String((err && err.message) || err) }));
  } finally {
    try {
      await task.finish({ keep: CONFIG.keepPage && page ? [page.label] : [] });
    } catch (_) {}
  }
}
`, string(rawCfg), egoResultMarker, userControlResult, evaluateBody, errorResult), nil
}

func buildEgoFetchScript(cfg egoFetchConfig) (string, error) {
	const evaluateBody = `    let viewerMid = "";
    try {
      const navResp = await fetch("https://api.bilibili.com/x/web-interface/nav", { credentials: "include" });
      const navJson = await navResp.json();
      if (navJson && navJson.code === 0 && navJson.data && navJson.data.mid) {
        viewerMid = String(navJson.data.mid);
      }
    } catch (_) {}

    let oid = c.oid || "";
    if (!oid && window.__INITIAL_STATE__) {
      const st = window.__INITIAL_STATE__;
      const aid = st.aid || (st.videoData && st.videoData.aid);
      if (aid) oid = String(aid);
    }
    if (!oid) {
      return { ok: false, error_code: "missing_oid", detail: "could not resolve bilibili oid from config or page state" };
    }

    const api = "https://api.bilibili.com/x/v2/reply?type=" + encodeURIComponent(String(c.commentType || 1)) +
      "&oid=" + encodeURIComponent(oid) + "&sort=0&ps=" + encodeURIComponent(String(c.pollLimit || 20)) + "&pn=1";
    const resp = await fetch(api, { credentials: "include" });
    const data = await resp.json();
    if (!data || data.code !== 0) {
      return {
        ok: false,
        oid,
        viewer_mid: viewerMid,
        error_code: "bilibili_api_error",
        detail: String((data && data.message) || ("code=" + (data && data.code)))
      };
    }

    const out = [];
    const pushItem = (item) => {
      if (!item) return;
      const rpid = String(item.rpid_str || item.rpid || "");
      const content = item.content && typeof item.content.message === "string" ? item.content.message : "";
      if (!rpid || !content) return;
      const mid = item.mid ? String(item.mid) : (item.member && item.member.mid ? String(item.member.mid) : "");
      const uname = item.member && item.member.uname ? String(item.member.uname) : mid;
      const root = item.root_str || (item.root ? String(item.root) : "");
      const parent = item.parent_str || (item.parent ? String(item.parent) : "");
      const ctimeSec = Number(item.ctime || 0);
      out.push({
        rpid,
        root: root === "0" ? "" : root,
        parent: parent === "0" ? "" : parent,
        sender_id: mid,
        sender_name: uname,
        content,
        timestamp_ms: ctimeSec > 0 ? ctimeSec * 1000 : Date.now(),
        is_self: viewerMid !== "" && mid === viewerMid
      });
    };

    const topReplies = Array.isArray(data.data && data.data.replies) ? data.data.replies : [];
    for (const top of topReplies) {
      pushItem(top);
      if (Array.isArray(top && top.replies)) {
        for (const sub of top.replies) {
          pushItem(sub);
        }
      }
    }
    return { ok: true, oid, viewer_mid: viewerMid, comments: out };`
	return buildEgoPageScript(
		cfg,
		`{ ok: false, error_code: "user_control_active", detail: "task space is currently delegated to user" }`,
		`{ ok: false, error_code: "ego_exception" }`,
		evaluateBody,
	)
}

func buildEgoReplyScript(cfg egoReplyConfig) (string, error) {
	const evaluateBody = `    const cookieMatch = document.cookie.match(/(?:^|;\s*)bili_jct=([^;]+)/);
    const csrf = cookieMatch ? decodeURIComponent(cookieMatch[1]) : "";
    if (!csrf) {
      return { status: "failed", submitted: false, code: -101, reason_code: "not_logged_in", detail: "missing bili_jct CSRF cookie" };
    }
    let oid = c.oid || "";
    if (!oid && window.__INITIAL_STATE__) {
      const st = window.__INITIAL_STATE__;
      const aid = st.aid || (st.videoData && st.videoData.aid);
      if (aid) oid = String(aid);
    }
    if (!oid) {
      return { status: "failed", submitted: false, reason_code: "missing_oid", detail: "could not resolve bilibili oid" };
    }

    const params = new URLSearchParams();
    params.set("oid", oid);
    params.set("type", String(c.commentType || 1));
    params.set("message", c.text);
    params.set("plat", "1");
    params.set("csrf", csrf);
    if (c.rootId && c.rootId !== "0") params.set("root", c.rootId);
    if (c.parentId && c.parentId !== "0") params.set("parent", c.parentId);

    let resp;
    try {
      resp = await fetch("https://api.bilibili.com/x/v2/reply/add", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8" },
        body: params.toString()
      });
    } catch (netErr) {
      return { status: "unknown", reason_code: "transport_unknown", detail: String((netErr && netErr.message) || netErr) };
    }

    let data;
    try {
      data = await resp.json();
    } catch (parseErr) {
      return { status: "unknown", reason_code: "response_parse_unknown", detail: String((parseErr && parseErr.message) || parseErr) };
    }

    if (data && data.code === 0) {
      const rpid = data.data && (data.data.rpid_str || (data.data.rpid ? String(data.data.rpid) : ""));
      if (rpid) {
        return { status: "confirmed", submitted: true, external_id: String(rpid) };
      }
      return { status: "submitted", submitted: true, reason_code: "missing_external_id" };
    }
    return {
      status: "failed",
      submitted: false,
      code: Number((data && data.code) || 0),
      detail: String((data && data.message) || "bilibili rejected reply")
    };`
	return buildEgoPageScript(
		cfg,
		`{ status: "failed", submitted: false, reason_code: "user_control_active", detail: "task space is currently delegated to user" }`,
		`{ status: "unknown", reason_code: "ego_exception" }`,
		evaluateBody,
	)
}
