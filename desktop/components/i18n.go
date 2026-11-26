package components

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"jinx-desktop/client"
)

// Filter options in Simplified Chinese paired with exact protocol enum mappings.

var RunPhaseFilterOptions = []string{
	"全部", "已受理", "聚合中", "排队中", "执行中", "检查中", "提交中", "确认中", "已结束", "未知",
}

var RunOutcomeFilterOptions = []string{
	"全部", "待处理", "已回复", "静默忽略", "已拦截", "已抑制", "已过期", "已取消", "失败", "未知",
}

var RunDeliveryFilterOptions = []string{
	"全部", "未开始", "已拦截", "发送中", "已提交", "已确认", "失败", "未知",
}

var ChangeStatusFilterOptions = []string{
	"全部", "已生效", "部分生效", "状态未知", "失败", "待执行",
}

func RunPhaseFilterValue(label string) string {
	switch label {
	case "已受理":
		return "accepted"
	case "聚合中":
		return "aggregating"
	case "排队中":
		return "queued"
	case "执行中":
		return "executing"
	case "检查中":
		return "checking"
	case "提交中":
		return "submitting"
	case "确认中":
		return "confirming"
	case "已结束":
		return "terminal"
	case "未知":
		return "unknown"
	default:
		return ""
	}
}

func RunOutcomeFilterValue(label string) string {
	switch label {
	case "待处理":
		return "pending"
	case "已回复":
		return "replied"
	case "静默忽略":
		return "silent"
	case "已拦截":
		return "blocked"
	case "已抑制":
		return "suppressed"
	case "已过期":
		return "expired"
	case "已取消":
		return "canceled"
	case "失败":
		return "failed"
	case "未知":
		return "unknown"
	default:
		return ""
	}
}

func RunDeliveryFilterValue(label string) string {
	switch label {
	case "未开始":
		return "not_started"
	case "已拦截":
		return "blocked"
	case "发送中":
		return "submitting"
	case "已提交":
		return "submitted"
	case "已确认":
		return "confirmed"
	case "失败":
		return "failed"
	case "未知":
		return "unknown"
	default:
		return ""
	}
}

func ChangeStatusFilterValue(label string) string {
	switch label {
	case "已生效":
		return "succeeded"
	case "部分生效":
		return "partial_apply"
	case "状态未知":
		return "unknown"
	case "失败":
		return "failed"
	case "待执行":
		return "pending"
	default:
		return ""
	}
}

func HealthLabel(v string) string {
	switch v {
	case "ready":
		return "就绪"
	case "running":
		return "运行中"
	case "fresh":
		return "实时"
	case "succeeded":
		return "已生效"
	case "not_ready":
		return "未就绪"
	case "stale":
		return "已过期"
	case "draining":
		return "排空中"
	case "degraded":
		return "降级"
	case "partial_apply":
		return "部分生效"
	case "unreachable":
		return "不可达"
	case "failed":
		return "失败"
	case "disconnected":
		return "已断开"
	case "error":
		return "错误"
	case "unsupported":
		return "未接入"
	case "stopped":
		return "已停止"
	case "pending":
		return "待执行"
	default:
		return "未知"
	}
}

func FreshnessLabel(v string) string {
	switch v {
	case "fresh":
		return "数据实时"
	case "stale":
		return "数据过期"
	case "disconnected":
		return "连接断开"
	default:
		return "状态未知"
	}
}

func SSEStateLabel(v string) string {
	switch v {
	case "fresh":
		return "事件流正常"
	case "stale":
		return "事件流滞后"
	case "disconnected":
		return "事件流未连"
	default:
		return "事件流未知"
	}
}

func PhaseLabel(v string) string {
	switch v {
	case "accepted":
		return "阶段: 已受理"
	case "aggregating":
		return "阶段: 聚合中"
	case "queued":
		return "阶段: 排队中"
	case "executing":
		return "阶段: 执行中"
	case "checking":
		return "阶段: 检查中"
	case "submitting":
		return "阶段: 提交中"
	case "confirming":
		return "阶段: 确认中"
	case "terminal":
		return "阶段: 已结束"
	case "unknown":
		return "阶段: 未知"
	default:
		return "阶段: " + nonEmpty(v, "未知")
	}
}

func OutcomeLabel(v string) string {
	switch v {
	case "pending":
		return "结果: 待处理"
	case "replied":
		return "结果: 已回复"
	case "silent":
		return "结果: 静默忽略"
	case "blocked":
		return "结果: 已拦截"
	case "suppressed":
		return "结果: 已抑制"
	case "expired":
		return "结果: 已过期"
	case "canceled":
		return "结果: 已取消"
	case "failed":
		return "结果: 失败"
	case "unknown":
		return "结果: 未知"
	default:
		return "结果: " + nonEmpty(v, "未知")
	}
}

func DeliveryLabel(v string) string {
	switch v {
	case "not_started":
		return "投递: 未开始"
	case "blocked":
		return "投递: 已拦截"
	case "submitting":
		return "投递: 发送中"
	case "submitted":
		return "投递: 已提交"
	case "confirmed":
		return "投递: 已确认"
	case "failed":
		return "投递: 失败"
	case "unknown":
		return "投递: 未知"
	default:
		return "投递: " + nonEmpty(v, "未知")
	}
}

func CompletenessLabel(v string) string {
	switch v {
	case "complete":
		return "链路完整"
	case "partial":
		return "记录不全"
	default:
		return nonEmpty(v, "未知")
	}
}

func PlatformLabel(v string) string {
	switch v {
	case "wechat":
		return "微信"
	case "bilibili":
		return "B站"
	case "v2ex":
		return "V2EX"
	case "web":
		return "网页/论坛"
	default:
		return nonEmpty(v, "未知来源")
	}
}

func ActionKindLabel(v string) string {
	switch v {
	case "reply_text":
		return "文本回复"
	case "edit_text":
		return "编辑消息"
	case "delete_content":
		return "撤回/删除"
	case "thread_reply":
		return "楼中楼回复"
	default:
		return v
	}
}

func CapabilityLabel(v string) string {
	switch v {
	case "ingest":
		return "消息采集"
	case "adapter_contract":
		return "契约适配器"
	case "send":
		return "发送执行"
	case "durable_admission":
		return "持久准入"
	case "confirmation":
		return "回声确认"
	case "dry_run":
		return "演练开关"
	default:
		return v
	}
}

func CertaintyLabel(v string) string {
	switch v {
	case "observed":
		return "已观测"
	case "missing":
		return "记录缺失"
	case "not_entered":
		return "未进入"
	default:
		return nonEmpty(v, "未知")
	}
}

func RemoteExecLabel(v string) string {
	switch v {
	case "may_be_running":
		return "远端或仍在运行"
	case "unknown":
		return "远端状态未知"
	case "not_applicable":
		return "不适用"
	case "finished":
		return "已结束"
	default:
		return nonEmpty(v, "未知")
	}
}

func AttemptStatusLabel(v string) string {
	switch strings.TrimSpace(v) {
	case "succeeded":
		return "执行成功"
	case "running":
		return "运行中"
	case "queued":
		return "排队中"
	case "failed":
		return "执行失败"
	case "canceled", "cancelled":
		return "已取消"
	case "timed_out":
		return "执行超时"
	case "unknown":
		return "状态未上报"
	default:
		return nonEmpty(v, "未知")
	}
}

// FallbackReasonLabel returns a clear Chinese explanation for backend fallback codes.
func FallbackReasonLabel(code string) string {
	switch strings.TrimSpace(code) {
	case "":
		return ""
	case "alma_transport_error":
		return "主引擎 Alma 通信异常，已降级兜底"
	case "alma_timeout":
		return "主引擎 Alma 超时，已降级兜底"
	case "alma_failed":
		return "主引擎 Alma 执行失败，已降级兜底"
	case "alma_empty_summary":
		return "主引擎 Alma 返回空摘要，已降级兜底"
	case "alma_invalid_output":
		return "主引擎 Alma 输出格式异常，已降级兜底"
	case "adapter_reported_fallback":
		return "主引擎不可用，已切换兜底引擎"
	default:
		return ControlReasonCN(code)
	}
}

// ShortFallbackTag returns a concise pill label for fallback telemetry.
func ShortFallbackTag(code string) string {
	switch strings.TrimSpace(code) {
	case "":
		return ""
	case "alma_transport_error", "alma_timeout", "alma_failed", "alma_empty_summary", "alma_invalid_output":
		return "Alma 异常已降级"
	case "adapter_reported_fallback":
		return "已切换兜底引擎"
	default:
		return "已降级兜底"
	}
}

// ExternalIDLabel formats external runtime handles in readable Chinese while keeping raw tokens when diagnostic.
func ExternalIDLabel(id string) string {
	trimmed := strings.TrimSpace(id)
	switch trimmed {
	case "", "—":
		return "—"
	case "controlled-web":
		return "受控网页工作区 (controlled-web)"
	default:
		return trimmed
	}
}

func OriginLabel(v string) string {
	switch v {
	case "console":
		return "控制台发起"
	case "rollback":
		return "显式回滚"
	case "external":
		return "外部修改"
	default:
		return nonEmpty(v, "未知")
	}
}

func StreamDomainLabel(v string) string {
	switch v {
	case "processing":
		return "编排调度事件流"
	case "source":
		return "采集来源事件流"
	case "action":
		return "动作投递事件流"
	default:
		return v
	}
}

func AttentionCodeLabel(v string) string {
	switch v {
	case "delivery_unknown":
		return "投递状态未知"
	case "publication_partial_apply":
		return "发布部分生效"
	case "drift_detected":
		return "配置发生漂移"
	case "maintenance_blocked":
		return "维护排空受阻"
	default:
		return v
	}
}

func MaintenanceStageLabel(v string) string {
	switch v {
	case "draining":
		return "正在排空"
	case "ready":
		return "可继续维护"
	case "completed":
		return "已完成"
	case "canceled":
		return "已取消"
	default:
		return nonEmpty(v, "未知")
	}
}

func PermissionCapabilityLabel(v string) string {
	switch v {
	case "text_generation":
		return "文本生成推理能力"
	case "auto_text_delivery":
		return "自动文本外发能力"
	case "web_search":
		return "联网搜索能力"
	case "host_write":
		return "宿主写入权限"
	case "execution.tool_use":
		return "工具调用门禁"
	case "host.accessibility":
		return "macOS 辅助功能权限"
	case "host.screen_capture":
		return "macOS 屏幕捕获权限"
	case "prompt.soft_policy":
		return "软提示词规范约束"
	default:
		return v
	}
}

func ProvenanceLabel(v string) string {
	switch v {
	case "policy", "execution_catalog":
		return "策略目录"
	case "backend_contract":
		return "后端运行契约"
	case "bridge_probe", "bridge_gate":
		return "宿主发送网关实测"
	case "prompt_doc", "prompt_policy":
		return "提示词软文档"
	case "not_reported":
		return "未上报"
	default:
		return nonEmpty(v, "未知")
	}
}

func ServiceDisplayName(id string) string {
	switch id {
	case "relay":
		return "Relay 消息中继"
	case "dispatcher":
		return "Dispatcher 调度器"
	case "decision":
		return "Decision 决策服务"
	case "bridge":
		return "Bridge 发送桥接"
	default:
		return id
	}
}

func BackendKindLabel(v string) string {
	switch v {
	case "alma_personal":
		return "Alma 直连后端"
	case "legacy_hermes":
		return "本地智能体桥接后端"
	case "text_only":
		return "纯文本兜底后端"
	case "stream_json":
		return "Stream-JSON 适配器"
	default:
		return nonEmpty(v, "未知类型")
	}
}

func BindingLabel(v string) string {
	switch v {
	case "branch_only":
		return "独立分支隔离"
	case "shared":
		return "共享会话"
	case "unverified":
		return "未验证隔离约束"
	default:
		return nonEmpty(v, "默认隔离")
	}
}

func ScopeLabel(v string) string {
	switch v {
	case "global":
		return "全局范围"
	case "group":
		return "单容器范围"
	default:
		return nonEmpty(v, "未知范围")
	}
}

func VisibilityLabel(v string) string {
	switch v {
	case "visible":
		return "控制台可见"
	case "hidden":
		return "隐藏"
	case "not_observed":
		return "未实测可见性"
	default:
		return nonEmpty(v, "未知")
	}
}

func LabelSourceLabel(v string) string {
	switch v {
	case "config_target_ui_only":
		return "来自前端配置别名"
	case "synthetic":
		return "合成标识"
	case "unverified":
		return "待实测名称"
	case "none":
		return "默认容器编号"
	default:
		return nonEmpty(v, "默认名称")
	}
}

func ReasonLabel(v string) string {
	switch v {
	case "unavailable":
		return "未就绪"
	case "unsupported":
		return "未接入"
	case "unverified":
		return "未验证"
	default:
		return v
	}
}

func FactKeyLabel(k string) string {
	switch k {
	case "status":
		return "状态"
	case "protocol_version":
		return "协议版本"
	case "sse_clients":
		return "SSE 订阅数"
	case "websocket_clients":
		return "WS 连接数"
	case "relay_connected":
		return "Relay 已连接"
	case "pending_messages":
		return "待处理消息"
	case "executing_groups":
		return "执行中容器"
	case "awaiting_echo":
		return "等待回声确认"
	case "sequence":
		return "已处理序号"
	case "last_seq":
		return "最新序号"
	case "started_at":
		return "启动时间"
	case "uptime_sec":
		return "运行时长(秒)"
	case "queue_depth":
		return "队列深度"
	case "groups_count":
		return "纳管容器数"
	case "monitored_groups":
		return "监听容器数"
	case "db_ready":
		return "数据库就绪"
	case "upstream_connected":
		return "上游通道连通"
	case "group_routing":
		return "容器路由"
	case "serial_ui":
		return "串行 UI 发送"
	case "accessibility_trusted":
		return "辅助功能授权"
	case "screen_capture_authorized":
		return "屏幕捕获授权"
	case "dry_run":
		return "演练模式"
	default:
		return k
	}
}

// ShortID shortens long SHA-256 hashes or UUID-like run IDs for readable UI display.
func ShortID(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return "—"
	}
	if strings.HasPrefix(trimmed, "run-") && len(trimmed) > 20 {
		return "#" + trimmed[4:12]
	}
	if len(trimmed) > 20 && !strings.ContainsAny(trimmed, " \t\n") {
		return "#" + trimmed[:8]
	}
	return trimmed
}

// ShortContainerID removes verbose platform prefixes from raw container IDs.
func ShortContainerID(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return "—"
	}
	trimmed = strings.TrimPrefix(trimmed, "bilibili:")
	trimmed = strings.TrimPrefix(trimmed, "web:")
	return trimmed
}

// FormatUint64Ptr safely dereferences *uint64 without printing pointer addresses.
func FormatUint64Ptr(v *uint64) string {
	if v == nil {
		return "—"
	}
	return strconv.FormatUint(*v, 10)
}

// FormatLocalClock formats a timestamp in local HH:MM:SS.
func FormatLocalClock(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "—"
	}
	return t.Local().Format("15:04:05")
}

// FormatLocalDateTime formats a timestamp in local MM-DD HH:MM:SS.
func FormatLocalDateTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "—"
	}
	return t.Local().Format("01-02 15:04:05")
}

// FormatLocalFullTime formats a timestamp in local YYYY-MM-DD HH:MM:SS.
func FormatLocalFullTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "—"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

// FormatDurationBetween returns a concise Chinese elapsed duration between start and end.
func FormatDurationBetween(start, end *time.Time) string {
	if start == nil || start.IsZero() {
		return "—"
	}
	if end == nil || end.IsZero() {
		return "进行中"
	}
	d := end.Sub(*start)
	if d < 0 {
		return "0 ms"
	}
	if d < time.Second {
		return fmt.Sprintf("%d ms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1f 秒", d.Seconds())
	}
	mins := int(d.Minutes())
	secs := int(d.Seconds()) % 60
	return fmt.Sprintf("%d 分 %d 秒", mins, secs)
}

// FormatFactValue renders live service probe facts cleanly in Chinese.
func FormatFactValue(key string, val any) string {
	if val == nil {
		return "—"
	}
	switch v := val.(type) {
	case bool:
		switch key {
		case "dry_run":
			if v {
				return "开启（演练）"
			}
			return "关闭（实盘）"
		case "accessibility_trusted", "screen_capture_authorized":
			if v {
				return "已授权"
			}
			return "未授权"
		case "relay_connected", "upstream_connected":
			if v {
				return "已连接"
			}
			return "未连接"
		case "group_routing", "serial_ui":
			if v {
				return "已启用"
			}
			return "未启用"
		case "awaiting_echo":
			if v {
				return "是（等待中）"
			}
			return "否"
		default:
			if v {
				return "是"
			}
			return "否"
		}
	case float64:
		if math.Trunc(v) == v {
			n := int64(v)
			if key == "protocol_version" {
				return fmt.Sprintf("v%d", n)
			}
			return strconv.FormatInt(n, 10)
		}
		return fmt.Sprintf("%.2f", v)
	case int:
		if key == "protocol_version" {
			return fmt.Sprintf("v%d", v)
		}
		return strconv.Itoa(v)
	case int64:
		if key == "protocol_version" {
			return fmt.Sprintf("v%d", v)
		}
		return strconv.FormatInt(v, 10)
	case uint64:
		if key == "protocol_version" {
			return fmt.Sprintf("v%d", v)
		}
		return strconv.FormatUint(v, 10)
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return "—"
		}
		if t, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
			return t.Local().Format("01-02 15:04:05")
		}
		switch trimmed {
		case "ready":
			return "就绪"
		case "ok":
			return "正常"
		case "running":
			return "运行中"
		case "not_ready":
			return "未就绪"
		default:
			return trimmed
		}
	default:
		return fmt.Sprint(val)
	}
}

// FormatAnyValueCN formats untyped config diff/snapshot values in Chinese.
func FormatAnyValueCN(v any) string {
	if v == nil {
		return "未设置"
	}
	switch x := v.(type) {
	case bool:
		if x {
			return "是"
		}
		return "否"
	case float64:
		if math.Trunc(x) == x {
			return strconv.FormatInt(int64(x), 10)
		}
		return fmt.Sprintf("%.2f", x)
	case []string:
		if len(x) == 0 {
			return "空列表 []"
		}
		return strings.Join(x, "、")
	case []int:
		if len(x) == 0 {
			return "空列表 []"
		}
		parts := make([]string, 0, len(x))
		for _, n := range x {
			parts = append(parts, strconv.Itoa(n))
		}
		return strings.Join(parts, "、")
	case []any:
		if len(x) == 0 {
			return "空列表 []"
		}
		parts := make([]string, 0, len(x))
		for _, item := range x {
			parts = append(parts, FormatAnyValueCN(item))
		}
		return strings.Join(parts, "、")
	case string:
		if strings.TrimSpace(x) == "" {
			return "（空）"
		}
		return x
	default:
		return fmt.Sprint(v)
	}
}

func TriggerLabel(v string) string {
	trimmed := strings.ToLower(strings.TrimSpace(v))
	if src, ok := strings.CutPrefix(trimmed, "direct:"); ok {
		srcName := src
		switch src {
		case "twitter":
			srcName = "Twitter/X"
		case "rss":
			srcName = "RSS"
		case "github":
			srcName = "GitHub"
		case "cron":
			srcName = "定时任务"
		case "api":
			srcName = "脚本接口"
		case "script":
			srcName = "脚本"
		case "forward":
			srcName = "跨平台直通转发"
		case "console":
			srcName = "控制台直发"
		}
		return "直发 · " + srcName + "（跳过 AI）"
	}
	switch trimmed {
	case "":
		return "未记录"
	case "mention":
		return "@ 提及触发"
	case "batch", "debounce", "idle", "batch_idle", "timer":
		return "静默聚合触发"
	case "poll", "comment", "connector_poll":
		return "评论轮询触发"
	case "comment_reply":
		return "评论回复触发"
	case "direct":
		return "直发投递（跳过 AI）"
	case "manual":
		return "人工触发"
	default:
		return v
	}
}

func ReasonCodeLabel(code string) string {
	switch strings.TrimSpace(code) {
	case "":
		return "—"
	case "direct_delivery":
		return "直发投递（跳过门禁与模型推理）"
	case "delivery_disabled":
		return "演练模式或发送关闭，已拦截发送"
	case "connector_dry_run":
		return "连接器演练模式生效，已拦截外发"
	case "gate_blocked":
		return "前置门禁拦截（闲聊、噪音或未达触发条件）"
	case "output_none":
		return "模型主动静默（未输出回复内容）"
	case "other_addressee":
		return "消息指向群内他人，已主动回避"
	case "dry_run":
		return "演练模式生效，已拦截外发"
	case "promo_shadow":
		return "推广防刷保护生效，已静默记录"
	case "rate_limited", "cooldown":
		return "触发频率限制或冷却保护"
	case "paused_boundary", "paused":
		return "处于全局暂停边界，已拦截"
	case "config_changed":
		return "配置发生变更，中止旧批次"
	case "reply_failed", "send_failed":
		return "回复发送失败"
	case "echo_timeout":
		return "等待发送回声确认超时"
	case "upstream_reason_redacted":
		return "上游细节已脱敏"
	case "canceled", "cancelled", "operator_canceled", "operator_cancelled":
		return "管理员已手动取消"
	case "delivery_canceled", "delivery_cancelled":
		return "管理员已拦截本次发送"
	case "backend_unavailable", "adapter_failed":
		return "模型推理后端异常"
	case "self_message":
		return "自身发出的消息，已忽略"
	case "blocked_sender":
		return "发送者位于屏蔽名单"
	case "keyword_blocked", "regex_blocked":
		return "命中关键词或正则屏蔽规则"
	case "mention_required":
		return "仅响应 @ 提及模式已跳过"
	case "disabled":
		return "容器当前未启用"
	case "not_whitelisted":
		return "容器未在白名单中"
	case "adapter_invocation_has_no_external_id":
		return "本地适配器直调模式，无外部任务编号"
	case "unknown":
		return "原因未记录"
	default:
		return code
	}
}

// ShortReasonLabel returns a concise Chinese reason suitable for compact blocked/silent list rows.
func ShortReasonLabel(code string) string {
	switch strings.TrimSpace(code) {
	case "":
		return "—"
	case "direct_delivery":
		return "直发投递"
	case "delivery_disabled":
		return "发送未开启/演练"
	case "connector_dry_run":
		return "连接器演练拦截"
	case "gate_blocked":
		return "门禁拦截（闲聊/噪音）"
	case "output_none":
		return "模型静默未回复"
	case "other_addressee":
		return "指向他人已回避"
	case "dry_run":
		return "演练拦截外发"
	case "promo_shadow":
		return "推广防刷静默"
	case "rate_limited", "cooldown":
		return "冷却限频保护"
	case "paused_boundary", "paused":
		return "全局暂停拦截"
	case "self_message":
		return "自身消息忽略"
	case "blocked_sender":
		return "屏蔽发送者"
	case "keyword_blocked", "regex_blocked":
		return "关键词/正则拦截"
	case "mention_required":
		return "未 @ 提及已跳过"
	default:
		return ReasonCodeLabel(code)
	}
}

// RunShortStatusCN returns a concise 2-5 character status label for pills and headers
// without duplicating the full summary sentence.
func RunShortStatusCN(run client.RunView) string {
	if run.Delivery == "unknown" {
		return "投递未知"
	}
	if run.Phase != "" && run.Phase != "terminal" {
		return strings.TrimPrefix(PhaseLabel(run.Phase), "阶段: ")
	}
	rc := ""
	if run.ReasonCode != nil {
		rc = *run.ReasonCode
	}
	switch run.Outcome {
	case "replied":
		switch run.Delivery {
		case "confirmed":
			return "已回复"
		case "submitted", "submitting":
			return "待确认"
		case "failed":
			return "发送失败"
		case "blocked":
			return "发送被拦"
		default:
			return "已回复"
		}
	case "silent":
		return "静默忽略"
	case "blocked":
		return "已拦截"
	case "suppressed":
		if rc == "dry_run" || rc == "connector_dry_run" || rc == "delivery_disabled" {
			return "演练拦截"
		}
		return "已抑制"
	case "canceled", "cancelled":
		return "已取消"
	case "failed":
		return "执行失败"
	default:
		return strings.TrimPrefix(OutcomeLabel(run.Outcome), "结果: ")
	}
}

func RunHeadlineCN(run client.RunView) string {
	if run.Delivery == "unknown" {
		return "投递状态未知 — 需人工核对"
	}
	if run.Phase != "" && run.Phase != "terminal" {
		return strings.TrimPrefix(PhaseLabel(run.Phase), "阶段: ")
	}
	rc := ""
	if run.ReasonCode != nil {
		rc = *run.ReasonCode
	}
	switch run.Outcome {
	case "replied":
		switch run.Delivery {
		case "confirmed":
			return "已回复并确认送达"
		case "submitted":
			return "已提交发送，等待回声确认"
		case "failed":
			return "回复生成成功，但发送失败"
		case "blocked":
			return "回复已生成，但发送被拦截"
		default:
			return "已生成回复（" + strings.TrimPrefix(DeliveryLabel(run.Delivery), "投递: ") + "）"
		}
	case "blocked":
		if rc != "" {
			return "已拦截：" + ReasonCodeLabel(rc)
		}
		return "前置门禁已拦截"
	case "silent":
		if rc != "" {
			return "静默忽略：" + ReasonCodeLabel(rc)
		}
		return "模型保持静默"
	case "failed":
		if rc != "" {
			return "执行失败：" + ReasonCodeLabel(rc)
		}
		return "执行失败"
	case "canceled", "cancelled":
		if rc != "" {
			return "已取消：" + ReasonCodeLabel(rc)
		}
		return "已取消"
	case "suppressed":
		if rc != "" {
			return "已抑制：" + ReasonCodeLabel(rc)
		}
		return "已抑制"
	default:
		return strings.TrimPrefix(OutcomeLabel(run.Outcome), "结果: ")
	}
}

func RunSummaryCN(run client.RunView) string {
	if containsCJK(run.Summary) {
		return run.Summary
	}
	switch strings.TrimSpace(run.Summary) {
	case "", "Run finished with outcome blocked", "Run finished with outcome silent", "Run finished with outcome replied", "Run finished with outcome failed", "Run finished with outcome canceled", "Run finished with outcome cancelled":
		return RunHeadlineCN(run)
	case "Delivery confirmed by correlated echo":
		return "已匹配到自身回声，确认送达"
	case "Delivery submitted":
		return "回复已提交至发送网关"
	default:
		return RunHeadlineCN(run)
	}
}

func TimelineKindLabel(kind string) string {
	switch strings.TrimSpace(kind) {
	case "run_accepted", "accepted":
		return "受理消息批次"
	case "run_aggregating", "aggregating":
		return "消息聚合等待"
	case "run_gating", "gating":
		return "前置门禁检查"
	case "run_queued", "queued":
		return "进入执行队列"
	case "run_executing", "executing":
		return "调用模型推理"
	case "run_checking", "checking":
		return "复核回复内容"
	case "run_submitting", "submitting":
		return "提交发送回复"
	case "run_confirming", "confirming":
		return "等待回声确认"
	case "run_terminal", "terminal":
		return "流程结束"
	case "attempt_started":
		return "开始模型尝试"
	case "attempt_finished":
		return "完成模型尝试"
	case "delivery_submitted":
		return "已提交发送网关"
	case "delivery_confirmed":
		return "已确认回声送达"
	case "delivery_failed":
		return "发送失败"
	case "run_canceled", "run_cancelled":
		return "流程已取消"
	case "run_unknown":
		return "状态未知"
	default:
		return kind
	}
}

func TimelineSourceLabel(src string) string {
	switch strings.TrimSpace(src) {
	case "dispatcher_trace":
		return "调度器追踪"
	case "relay_echo":
		return "回声核对"
	case "bridge_ack":
		return "桥接回执"
	case "connector":
		return "平台连接器"
	case "console":
		return "控制面"
	default:
		return nonEmpty(src, "系统")
	}
}

func TimelineSummaryCN(item client.TimelineItem) string {
	if containsCJK(item.Summary) {
		return item.Summary
	}
	switch strings.TrimSpace(item.Summary) {
	case "Run accepted", "Run phase accepted":
		return "已受理新消息批次"
	case "Run phase aggregating":
		return "正在静默等待同批次消息聚合"
	case "Run phase gating":
		return "正在执行前置门禁判定"
	case "Run phase queued":
		return "已进入调度队列等待推理"
	case "Run phase executing":
		return "正在调用智能体模型推理"
	case "Run phase checking":
		return "正在复核回复内容与边界规则"
	case "Run phase submitting":
		return "正在向发送网关提交回复"
	case "Run phase confirming":
		return "等待目标平台自身回声确认送达"
	case "Run finished with outcome replied":
		return "本次运行结束：已回复"
	case "Run finished with outcome blocked":
		return "本次运行结束：已拦截"
	case "Run finished with outcome silent":
		return "本次运行结束：静默忽略"
	case "Run finished with outcome failed":
		return "本次运行结束：执行失败"
	case "Run finished with outcome canceled", "Run finished with outcome cancelled":
		return "本次运行结束：已取消"
	case "Delivery confirmed by correlated echo":
		return "已匹配到自身回声，确认送达"
	case "Delivery submitted":
		return "回复已提交至发送网关"
	default:
		base := TimelineKindLabel(item.Kind)
		if item.ReasonCode != nil && *item.ReasonCode != "" {
			return base + "：" + ReasonCodeLabel(*item.ReasonCode)
		}
		return base
	}
}

func SubmissionSourceLabel(src string) string {
	switch strings.TrimSpace(src) {
	case "bridge_ack":
		return "发送网关回执（精确提交时间）"
	case "echo_inferred":
		return "仅由回声反推（未记录提交回执）"
	case "connector_ack":
		return "平台连接器回执"
	case "":
		return "未记录"
	default:
		return src
	}
}

func BackendDisplayName(id string) string {
	switch strings.TrimSpace(id) {
	case "direct_passthrough":
		return "直发通道（未调用 AI）"
	case "reply_adapter":
		return "本地智能体回复适配器"
	case "alma-personal":
		return "Alma 个人智能体"
	case "hermes-alma-bridge":
		return "Hermes/Alma 兼容桥接"
	case "alma":
		return "Alma 智能体"
	case "hermes":
		return "Hermes 智能体"
	default:
		return nonEmpty(id, "未知后端")
	}
}

func StreamIDLabel(id string) string {
	switch strings.TrimSpace(id) {
	case "":
		return "未建立"
	case "runs":
		return "运行事件流"
	case "architecture":
		return "架构状态流"
	default:
		return ShortID(id)
	}
}

func ControlReasonCN(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "—"
	}
	if containsCJK(trimmed) {
		return trimmed
	}
	switch trimmed {
	case "No verified runtime implementation is connected":
		return "当前未启用运行时进程控制（只读展示）"
	case "No authorized memory/disk configuration reader is connected":
		return "当前未接入内存/磁盘配置对比探针（仅展示可观测项）"
	case "Read-only mode":
		return "当前处于只读模式"
	case "No routing provider is connected":
		return "当前使用内置默认执行链路（未接入独立动态路由）"
	case "Internal profile is not exposed":
		return "本地私有身份配置默认脱敏，不对控制台明文暴露"
	case "adapter_invocation_has_no_external_id":
		return "本地适配器直调模式，无外部任务编号"
	case "unsupported":
		return "暂不支持"
	case "unavailable":
		return "暂不可用"
	case "unverified":
		return "未验证"
	case "unknown":
		return "未知"
	case "fetch_failed":
		return "轮询拉取失败"
	case "poll_failed":
		return "轮询异常"
	case "not running", "Not running":
		return "服务当前未运行"
	case "already running", "Already running":
		return "服务当前已在运行"
	case "already stopped", "Already stopped":
		return "服务当前已停止"
	default:
		if strings.Contains(trimmed, "already running") {
			return "服务已在运行中"
		}
		if strings.Contains(trimmed, "not running") {
			return "服务未在运行中"
		}
		if strings.HasPrefix(trimmed, "Dispatcher recorded") {
			return "调度器未记录额外说明（" + trimmed + "）"
		}
		return trimmed
	}
}

func WriteConcurrencyLabel(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "cas":
		return "摘要乐观锁（防并发覆盖）"
	case "remote_cas":
		return "远端摘要乐观锁（CAS 防并发覆盖）"
	case "":
		return "默认并发保护"
	default:
		return v
	}
}

func FieldOriginLabel(v string) string {
	switch strings.TrimSpace(v) {
	case "group":
		return "容器专属配置"
	case "global":
		return "全局默认继承"
	case "default":
		return "系统内置默认"
	case "local_policy":
		return "本地私有策略（只读）"
	case "relay":
		return "中继配置"
	default:
		return nonEmpty(v, "默认")
	}
}

func SnapshotSourceLabel(v string) string {
	switch strings.TrimSpace(v) {
	case "dispatcher_config_snapshot":
		return "调度器运行时配置快照"
	case "console":
		return "控制面快照"
	default:
		return nonEmpty(v, "未知来源")
	}
}

func SnapshotScopeLabel(v string) string {
	switch strings.TrimSpace(v) {
	case "group":
		return "单容器配置"
	case "global":
		return "全局配置"
	case "routing":
		return "执行路由"
	case "permissions":
		return "权限策略"
	default:
		return v
	}
}

func DrainScopeLabel(v string) string {
	switch strings.TrimSpace(v) {
	case "":
		return "不涉及"
	case "global":
		return "全局排空"
	case "group":
		return "单容器排空"
	case "service":
		return "单服务边界"
	default:
		return v
	}
}

func ServiceActionLabel(kind string) string {
	switch strings.TrimSpace(kind) {
	case "start":
		return "启动"
	case "stop":
		return "停止"
	case "restart":
		return "重启"
	case "reload":
		return "重载"
	default:
		return kind
	}
}

func TriStateSummaryCN(t client.TriStateStringList) string {
	switch t.Mode {
	case client.TriUnset, client.TriNull:
		return "继承全局默认"
	case client.TriEmpty:
		return "显式置空"
	case client.TriValues:
		if len(t.Values) == 0 {
			return "显式置空"
		}
		return fmt.Sprintf("已配置 %d 项（%s）", len(t.Values), strings.Join(t.Values, "、"))
	default:
		return "继承全局默认"
	}
}

func FieldPathLabel(path string) string {
	if strings.Contains(path, "rollbackOf.") {
		return path
	}
	trimmed := strings.TrimSpace(path)
	if idx := strings.LastIndex(trimmed, "."); idx >= 0 && !strings.HasPrefix(trimmed, "rollbackOf.") {
		prefix := trimmed[:idx]
		leaf := trimmed[idx+1:]
		if leafLabel := fieldLeafLabel(leaf); leafLabel != leaf {
			return prefix + " · " + leafLabel
		}
	}
	return fieldLeafLabel(trimmed)
}

func fieldLeafLabel(leaf string) string {
	switch leaf {
	case "enabled":
		return "启用状态"
	case "dry_run":
		return "演练模式"
	case "mention_only":
		return "仅 @ 提及触发"
	case "scenario":
		return "场景标识"
	case "response_goal":
		return "回复目标"
	case "tone":
		return "表达语气"
	case "skills":
		return "挂载技能"
	case "tools":
		return "挂载工具"
	case "knowledge_bases":
		return "挂载知识库"
	case "self_sender_ids":
		return "自身账号白名单"
	case "blocked_sender_ids":
		return "发送者屏蔽名单"
	case "keyword_blocklist":
		return "关键词屏蔽表"
	case "regex_blocklist":
		return "正则屏蔽表"
	case "promo_keywords":
		return "推广触发词"
	case "allowed_types":
		return "允许消息类型"
	case "max_reply_runes":
		return "最大回复字数"
	case "decision_threshold":
		return "决策阈值"
	case "batch_size":
		return "批量聚合条数"
	case "support_mode":
		return "客服隔离模式"
	case "hermes_profile":
		return "智能体身份配置"
	case "laya_filter":
		return "Laya 前置过滤"
	case "bypass_mention_decision":
		return "@ 提及直接放行"
	case "clean_content":
		return "清洗冗余正文"
	case "ignore_echo":
		return "忽略自身回声"
	case "monitor_all":
		return "全量监控开关"
	case "forward_rules":
		return "跨平台直通转发规则"
	default:
		return leaf
	}
}

func containsCJK(s string) bool {
	for _, r := range s {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}
