package app

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"jinx-desktop/client"
	"jinx-desktop/components"
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

func (s *Store) renderServicesView(c *ui.Context, p theme.Palette) {
	ui.Column(c).FillWidth().Gap(20).Children(func() {
		components.PageHeader(c, p, "服务状态与维护排空", "白名单进程生命周期管理、全局准入闸门与优雅排空进度", func() {
			if s.LastServiceOp != nil {
				components.StatusPill(c, p,
					fmt.Sprintf("最近操作: %s %s (%s)", components.ServiceDisplayName(s.LastServiceOp.ServiceID), serviceActionCN(s.LastServiceOp.Kind), components.HealthLabel(s.LastServiceOp.State)),
					components.ToneInfo)
			}
		})

		// Active Maintenance Drain Banner
		if s.Controls != nil && s.Controls.Maintenance != nil && s.Controls.Maintenance.Active {
			mt := s.Controls.Maintenance
			components.CardWithHeader(c, p,
				fmt.Sprintf("活动维护排空会话 — %s", mt.MaintenanceID),
				fmt.Sprintf("当前阶段：%s · 边界序号：%d", components.MaintenanceStageLabel(mt.Stage), mt.Boundary),
				nil,
				func() {
					components.AlertBanner(c, p, components.ToneWarn,
						"优雅排空进行中（禁止强杀活跃执行协程）",
						fmt.Sprintf("范围=%s · 待处理=%d · 活跃协程=%d · 投递未知=%d · 远端未定=%d · 未收敛任务=%d",
							components.ScopeLabel(mt.Scope.Kind), mt.Counts.Pending, mt.Counts.ActiveWorkers,
							mt.Counts.UnknownDelivery, mt.Counts.RemoteUnsettled, mt.Counts.UnresolvedRuns))
					for _, blk := range mt.Blockers {
						ui.Text(c, "• 阻塞原因："+blk).FontSize(12).TextColor(p.Danger)
					}
					maintCap := s.Capability("maintenance")
					ui.Row(c).Gap(8).Children(func() {
						if ui.PrimaryButton(c.Key("maint_continue"), "继续维护").Disabled(!maintCap.Supported || !maintCap.Enabled || s.Busy).Clicked() {
							s.MaintainAction(c, mt.MaintenanceID, true)
						}
						if ui.Button(c.Key("maint_cancel"), "取消维护").Disabled(!maintCap.Supported || !maintCap.Enabled || s.Busy).Clicked() {
							s.MaintainAction(c, mt.MaintenanceID, false)
						}
					})
				})
		}

		// Global Pause/Resume Control Gate
		components.CardWithHeader(c, p, "全局准入闸门", "控制所有输入通道的统一暂停与恢复接收边界", nil, func() {
			if s.Controls == nil {
				components.AlertBanner(c, p, components.ToneNeutral, "运行时控制不可用",
					components.ControlReasonCN(s.Capability("controls.read").Reason))
				return
			}
			pauseCap := s.Capability("pause")
			resumeCap := s.Capability("resume")
			paused := s.Controls.Global.Paused
			ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
				ui.Row(c).Gap(10).AlignItems(ui.Center).Children(func() {
					if paused {
						components.StatusPill(c, p, "全局已暂停接入", components.ToneWarn)
					} else {
						components.StatusPill(c, p, "正常接收消息", components.ToneOK)
					}
					ui.Text(c, fmt.Sprintf("控制版本 %s · 边界水位: %d", components.ShortID(s.Controls.Revision), s.Controls.Global.Boundary)).FontSize(12).FontFeatures("tnum").TextColor(p.TextMuted)
				})
				ui.Row(c).Gap(8).Children(func() {
					if ui.Button(c.Key("ctl_pause"), "全局暂停").Disabled(paused || !pauseCap.Supported || !pauseCap.Enabled || s.Busy).Clicked() {
						s.runGlobalPause(c, true)
					}
					if ui.Button(c.Key("ctl_resume"), "恢复接收").Disabled(!paused || !resumeCap.Supported || !resumeCap.Enabled || s.Busy).Clicked() {
						s.runGlobalPause(c, false)
					}
				})
			})
		})

		// Service Health & Lifecycle Controls
		rtMap := map[string]client.RuntimeService{}
		if s.RuntimeSvcs != nil {
			for _, rs := range s.RuntimeSvcs.Services {
				rtMap[rs.ID] = rs
			}
		}

		// Deduplicate identical read-only runtime control gate hints across cards.
		reasonCounts := map[string]int{}
		for _, svc := range s.Services {
			for _, kind := range []string{"start", "stop", "restart"} {
				if a := svc.Actions[kind]; !a.Enabled && a.Reason != "" {
					reasonCounts[a.Reason]++
					break
				}
			}
		}
		sharedGateReason := ""
		if len(reasonCounts) == 1 {
			for r, cnt := range reasonCounts {
				if cnt > 1 {
					sharedGateReason = r
				}
			}
		}
		if sharedGateReason != "" {
			components.AlertBanner(c, p, components.ToneInfo, "当前处于只读运维视图", components.ControlReasonCN(sharedGateReason))
		}

		for _, svc := range s.Services {
			svc := svc
			rt, hasRT := rtMap[svc.ID]
			procLabel := components.HealthLabel(svc.Process)
			if svc.Process == "launchd" {
				procLabel = "launchd 托管"
			}
			subtitle := fmt.Sprintf("运行方式: %s · 探针延迟: %d ms", procLabel, svc.HealthLatencyMS)
			if hasRT {
				labelSuffix := ""
				if rt.Label != "" {
					labelSuffix = " · " + rt.Label
				}
				if rt.PID > 0 {
					subtitle = fmt.Sprintf("进程 PID: %d · 运行状态: %s · 探针延迟: %d ms%s", rt.PID, components.HealthLabel(rt.State), svc.HealthLatencyMS, labelSuffix)
				} else {
					subtitle = fmt.Sprintf("外部托管（无本地 PID） · 运行状态: %s · 探针延迟: %d ms%s", components.HealthLabel(rt.State), svc.HealthLatencyMS, labelSuffix)
				}
			}

			components.CardWithHeader(c, p, components.ServiceDisplayName(svc.ID), subtitle, func() {
				components.HealthPill(c, p, svc.Health)
				for _, kind := range []string{"start", "stop", "restart"} {
					actCap := svc.Actions[kind]
					gwCap := s.Capability("service." + kind)
					allowed := actCap.Supported && actCap.Enabled && (!s.BootPresent() || gwCap.Supported && gwCap.Enabled)
					actText := components.ServiceActionLabel(kind)
					accLabel := actText + " " + svc.ID
					k := kind
					if ui.Button(c.Key("svc_act_"+svc.ID+"_"+k), actText).Label(accLabel).Disabled(!allowed || s.Busy).Clicked() {
						s.PlanServiceAction(c, svc.ID, k)
					}
				}
			}, func() {
				if svc.ID == "bridge" && svc.Health == "unknown" && svc.Facts["accessibility_trusted"] == true && svc.Facts["screen_capture_authorized"] == true {
					components.AlertBanner(c, p, components.ToneInfo, "发送桥接权限已就绪",
						"探针已实测辅助功能与屏幕捕获均为已授权；由于旧版探针未返回状态字段，总健康按真实语义标注为“未知”。")
				}
				keys := make([]string, 0, len(svc.Facts))
				for k := range svc.Facts {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				factParts := make([]string, 0, len(keys))
				for _, k := range keys {
					if svc.Facts[k] != nil {
						factParts = append(factParts, fmt.Sprintf("%s: %s", components.FactKeyLabel(k), components.FormatFactValue(k, svc.Facts[k])))
					}
				}
				if len(factParts) > 0 {
					ui.Text(c, "探针实时事实："+strings.Join(factParts, " · ")).FontSize(12).FontFeatures("tnum").TextColor(p.TextSecondary)
				}
				for _, kind := range []string{"start", "stop", "restart"} {
					if a := svc.Actions[kind]; !a.Enabled && a.Reason != "" {
						if a.Reason != sharedGateReason {
							ui.Text(c, fmt.Sprintf("门控提示（%s 不可操作）：%s", components.ServiceActionLabel(kind), components.ControlReasonCN(a.Reason))).FontSize(11).TextColor(p.TextMuted)
						}
						break
					}
				}
			})
		}
	})
}

func (s *Store) BootPresent() bool { return s.Boot != nil }

func (s *Store) runGlobalPause(c *ui.Context, pause bool) {
	if s.Controls == nil {
		return
	}
	kind := "resume"
	actionText := "恢复"
	if pause {
		kind = "pause"
		actionText = "暂停"
	}
	reqID := s.nextRequestID(kind)
	expectedRev := s.Controls.Revision
	s.Busy = true
	s.runOp(func() {
		_, err := s.client.RunControlAction(context.Background(), client.ControlRequest{
			RequestID:        reqID,
			ExpectedRevision: expectedRev,
			Action:           client.ControlAction{Kind: kind, Scope: client.ControlScope{Kind: "global"}},
		})
		ctl, _ := s.client.GetControls(context.Background())
		s.dispatch(func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "全局"+actionText+"执行失败: "+err.Error())
				return
			}
			s.Controls = &ctl
			s.showToast(c, "全局准入已"+actionText)
		})
	})
}
