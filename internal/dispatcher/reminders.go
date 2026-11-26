package dispatcher

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

const remindersHeading = "\n【提醒事项实时查询；completed不等于发布；pending_sync不得说已记录】\n"

type reminderCalendar struct {
	ID             string `json:"id"`
	Title          string `json:"title"`
	SourceID       string `json:"source_id"`
	SourceType     int    `json:"source_type"`
	SourceTitle    string `json:"source_title"`
	ICloudVerified bool   `json:"icloud_verified"`
}
type reminderItem struct {
	ID         string `json:"id"`
	CalendarID string `json:"calendar_id"`
	Title      string `json:"title"`
	Notes      string `json:"notes"`
	Completed  bool   `json:"completed"`
}
type reminderRequest struct {
	Operation string `json:"operation"`
	Key       string `json:"key,omitempty"`
	Title     string `json:"title,omitempty"`
	Notes     string `json:"notes,omitempty"`
}
type reminderResponse struct {
	Application   string           `json:"application,omitempty"`
	BundleID      string           `json:"bundle_id,omitempty"`
	Authorization int              `json:"authorization"`
	FullAccess    bool             `json:"full_access"`
	PID           int              `json:"pid,omitempty"`
	UID           int              `json:"uid"`
	Calendar      reminderCalendar `json:"calendar"`
	Item          reminderItem     `json:"item"`
	Items         []reminderItem   `json:"items"`
}
type supportTask struct {
	IntakeID   string `json:"intake_id"`
	MessageID  string `json:"message_id"`
	Kind       string `json:"kind"`
	DedupeKey  string `json:"dedupe_key,omitempty"`
	MarkerKey  string `json:"marker_key,omitempty"`
	Status     string `json:"status"`
	ReminderID string `json:"reminder_id,omitempty"`
	CalendarID string `json:"calendar_id,omitempty"`
	Completed  bool   `json:"completed"`
	CheckedAt  string `json:"checked_at"`
	// Optional operator-maintained state, never inferred from completion or source code.
	MaintainedStatus string `json:"maintained_status,omitempty"`
	StatusUpdatedAt  string `json:"status_updated_at,omitempty"`
}

func (d *Dispatcher) callReminders(req reminderRequest) (reminderResponse, error) {
	policy := d.configSnapshot().supportPolicy()
	settings := policy.Reminders
	if !policy.Enabled || !settings.Enabled {
		return reminderResponse{}, fmt.Errorf("reminders disabled by local policy")
	}
	var response reminderResponse
	var err error
	if d.reminderRunner != nil {
		response, err = d.reminderRunner(req)
	} else {
		response, err = launchReminderApp(d.ctx, settings, req)

	}
	if err != nil {
		return reminderResponse{}, fmt.Errorf("reminders unavailable")
	}

	c := response.Calendar
	if response.Application != settings.Application || response.BundleID != settings.BundleID || c.ID == "" || c.Title != settings.CalendarTitle || c.SourceID == "" || c.SourceType != settings.SourceType || c.SourceTitle != settings.SourceTitle || (settings.RequireCloudVerified && !c.ICloudVerified) {
		return response, fmt.Errorf("unverified reminders identity or calendar")
	}
	return response, nil
}

// Intentionally conservative: ambiguous reports remain untriaged. No LLM can authorize a write.
func supportKind(text string) string {
	if regexp.MustCompile(`(?i)(忽略.{0,12}规则|系统提示词|假装|测试需求|测试提醒|帮我测试|听说|有人说|假设|比如|例如|ignore.{0,20}instructions)`).MatchString(text) {
		return ""
	}
	if regexp.MustCompile(`(希望.{0,12}(增加|添加|支持|能支持|可以支持)|建议.{0,8}(增加|添加|支持)|能否增加|能不能增加|请增加|请添加|想要增加|需要增加).{0,100}(功能|快捷键|同步|搜索|导出|粘贴|选项|模式|按钮|格式)`).MatchString(text) {
		return "feature"
	}

	if strings.ContainsAny(text, "?？") {
		return ""
	}
	if regexp.MustCompile(`(遇到|发现|出现|一直|每次|升级后|更新后|打开后|使用时|我的|我这边|我这里).{0,100}(崩溃|闪退|卡死|报错|无法粘贴|不能粘贴|无法启动|打不开|同步失败|数据丢失|不生效)|(?i)(bug[：:]|故障[：:]|问题反馈[：:])`).MatchString(text) {
		return "bug"
	}
	return ""
}

func redactFeedback(text string) string {
	text = regexp.MustCompile(`(?s)-----BEGIN[^\n]*PRIVATE KEY-----.*?-----END[^\n]*PRIVATE KEY-----`).ReplaceAllString(text, "[凭据已脱敏]")
	text = regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`).ReplaceAllString(text, "${1}[已脱敏]")
	text = regexp.MustCompile(`(?i)((?:api[_-]?key|access[_-]?token|refresh[_-]?token|token|password|passwd|secret|密码|密钥|凭据)["']?\s*[:=：]\s*)[^\s,，;；]+`).ReplaceAllString(text, "${1}[已脱敏]")
	text = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]+`).ReplaceAllString(text, "[凭据已脱敏]")
	text = regexp.MustCompile(`https?://[^\s]+`).ReplaceAllStringFunc(text, func(s string) string {
		if strings.ContainsAny(s, "?@#") {
			return "[含参数链接已脱敏]"
		}
		return s
	})
	return text
}

func reminderPayload(msg RelayMessage, intake, kind string, policy reminderPolicy) reminderRequest {
	text := redactFeedback(msg.Content)
	title := []rune(strings.Join(strings.Fields(text), " "))
	if len(title) > 80 {
		title = title[:80]
	}
	notes := fmt.Sprintf("群：%s (%s)\n反馈人：%s (%s)\n原消息ID：%s\n消息时间：%s\n平台/版本：见下方原反馈；未说明则待补充\n类型：%s\n原收件回执：%s\n复现/预期/实际（仅原反馈，不补造）：\n%s\n%s%s", redactFeedback(msg.GroupName), redactFeedback(msg.GroupID), redactFeedback(msg.SenderName), redactFeedback(msg.SenderID), redactFeedback(msg.ID), time.UnixMilli(msg.Timestamp).Format(time.RFC3339), kind, intake, text, policy.MarkerPrefix, strings.TrimPrefix(intake, "intake-"))
	return reminderRequest{Operation: "upsert", Key: strings.TrimPrefix(intake, "intake-"), Title: "[" + policy.TitlePrefix + " " + kind + "] " + string(title), Notes: notes}
}

func (d *Dispatcher) taskDir() string {
	return filepath.Join(filepath.Dir(d.stateFile), "support-reminders", d.configSnapshot().supportPolicy().GroupID)
}
func (d *Dispatcher) readIntake(id string) (RelayMessage, error) {
	var record struct {
		ID      string       `json:"id"`
		Status  string       `json:"status"`
		Message RelayMessage `json:"message"`
	}
	if !regexp.MustCompile(`^intake-[a-f0-9]{64}$`).MatchString(id) {
		return record.Message, fmt.Errorf("invalid intake id")
	}
	policy := d.configSnapshot().supportPolicy()
	if !policy.Enabled {
		return record.Message, fmt.Errorf("support disabled by local policy")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(d.stateFile), "support", policy.GroupID, id+".json"))
	if err != nil {
		return record.Message, err
	}
	if json.Unmarshal(data, &record) != nil || record.ID != id || record.Status != "received_untriaged" || record.Message.GroupID != policy.GroupID || "intake-"+messageDigest(policy.GroupID+"/"+record.Message.ID) != id {
		return RelayMessage{}, fmt.Errorf("invalid intake evidence")
	}
	return record.Message, nil
}

// One lock covers separate CLI migrations and dispatcher calls. The raw inbox is never changed.
func (d *Dispatcher) syncSupport(ids []string, allowWrite bool) ([]supportTask, error) {
	cfg := d.configSnapshot()
	policy := cfg.supportPolicy()
	if !policy.Enabled || !policy.Reminders.Enabled {
		return nil, fmt.Errorf("support reminders disabled by local policy")
	}
	if err := os.MkdirAll(d.taskDir(), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(d.taskDir(), ".lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, fmt.Errorf("support sync busy")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	tasks := map[string]supportTask{}
	files, err := filepath.Glob(filepath.Join(d.taskDir(), "intake-*.json"))
	if err != nil {
		return nil, err
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		var task supportTask
		if err != nil || json.Unmarshal(data, &task) != nil || filepath.Base(path) != task.IntakeID+".json" {
			return nil, fmt.Errorf("invalid task mapping")
		}
		if !regexp.MustCompile(`^intake-[a-f0-9]{64}$`).MatchString(task.IntakeID) || (task.MarkerKey != "" && !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(task.MarkerKey)) {
			return nil, fmt.Errorf("invalid task identity")
		}
		tasks[task.IntakeID] = task
	}
	for id, task := range tasks {
		if task.ReminderID == "" {
			ids = append(ids, id)
		}
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			continue
		}
		seen[id] = true
		if !regexp.MustCompile(`^intake-[a-f0-9]{64}$`).MatchString(id) {
			continue
		}
		msg, err := d.readIntake(id)
		if err != nil {
			return nil, fmt.Errorf("intake evidence unreadable")
		}
		kind := supportFeedbackKind(msg, cfg)
		if kind == "" {
			continue
		}
		task, exists := tasks[id]
		if exists && task.MessageID != msg.ID {
			return nil, fmt.Errorf("mapping identity mismatch")
		}
		if !exists {
			task = supportTask{IntakeID: id, MessageID: msg.ID, Kind: kind, Status: "pending_sync", DedupeKey: messageDigest(msg.SenderID + "/" + strings.Join(strings.Fields(msg.Content), " ")), MarkerKey: strings.TrimPrefix(id, "intake-")}
			// An exact repeat by the same reporter links to the existing task; new evidence stays in the raw inbox.
			for _, previous := range tasks {
				if previous.DedupeKey == task.DedupeKey {
					task.ReminderID, task.CalendarID, task.MarkerKey = previous.ReminderID, previous.CalendarID, previous.MarkerKey
					task.Status = previous.Status
					break
				}
			}
		}
		if allowWrite && task.ReminderID == "" {
			// Persist intent before EventKit. Its marker recovers save-success/response-loss crashes.
			task.Status = "pending_sync"
			if err := saveSupportTask(d.taskDir(), task); err != nil {
				return nil, err
			}
			sourceIntake := id
			if task.MarkerKey != "" {
				sourceIntake = "intake-" + task.MarkerKey
			}
			sourceMessage, err := d.readIntake(sourceIntake)
			if err != nil {
				return nil, fmt.Errorf("dedupe evidence unreadable")
			}
			req := reminderPayload(sourceMessage, sourceIntake, kind, policy.Reminders)
			response, err := d.callReminders(req)
			item := response.Item
			if err == nil && item.ID != "" && item.CalendarID == response.Calendar.ID && item.Title == req.Title && item.Notes == req.Notes {
				task.Status, task.ReminderID, task.CalendarID = "synced", item.ID, item.CalendarID
			}
		}
		tasks[id] = task
	}
	// Always read fresh state; an old successful mapping is never current evidence.
	response, readErr := d.callReminders(reminderRequest{Operation: "read"})
	if readErr != nil && len(tasks) == 0 {
		return nil, fmt.Errorf("reminders read unavailable")
	}
	var result []supportTask
	for _, task := range tasks {
		task.CheckedAt = time.Now().Format(time.RFC3339)
		task.Completed = false
		if task.ReminderID != "" {
			task.Status = "unavailable"
			if readErr == nil && response.Calendar.ID == task.CalendarID {
				for _, item := range response.Items {
					markerKey := task.MarkerKey
					if markerKey == "" {
						markerKey = strings.TrimPrefix(task.IntakeID, "intake-")
					}
					marker := policy.Reminders.MarkerPrefix + markerKey
					if item.ID == task.ReminderID && item.CalendarID == task.CalendarID && contains(strings.Split(item.Notes, "\n"), marker) {
						task.Status, task.Completed = "synced", item.Completed
					}
				}
			}
			// Keep successful identity even on read failure; never silently recreate a deleted task.
			if err := saveSupportTask(d.taskDir(), task); err != nil {
				return nil, err
			}
		} else if err := saveSupportTask(d.taskDir(), task); err != nil {
			return nil, err
		}
		if _, err := time.Parse(time.RFC3339, task.StatusUpdatedAt); err != nil || task.Status != "synced" {
			task.MaintainedStatus = ""
		}
		switch task.MaintainedStatus {
		case "待核实", "已确认", "开发中", "测试中", "待发布", "已发布", "暂不处理":
		default:
			task.MaintainedStatus = ""
		}
		result = append(result, task)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].IntakeID < result[j].IntakeID })
	return result, nil
}
func saveSupportTask(dir string, task supportTask) error {
	data, err := json.MarshalIndent(task, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(dir, task.IntakeID+".json"), data)
}
