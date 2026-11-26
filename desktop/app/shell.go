package app

import (
	"fmt"
	"strings"

	"jinx-desktop/components"
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

// View renders the full-height master-detail desktop shell, integrated macOS titlebar,
// Chinese navigation sidebar, active console view, and confirmation modals.
func (s *Store) View(c *ui.Context) {
	p := s.Palette()
	c.SetTheme(p.UI)
	if s.pendingToast != "" {
		c.Toast(s.pendingToast)
		s.pendingToast = ""
	}

	ui.Row(c).Fill().AlignItems(ui.Stretch).Background(p.WindowBg).Children(func() {
		s.renderSidebarPane(c, p)

		ui.Column(c).Grow(1).FillHeight().Background(p.WindowBg).Children(func() {
			s.renderTopBar(c, p)

			if s.LastError != "" {
				ui.Box(c).Padding(16, 24, 0, 24).Children(func() {
					ui.Column(c).FillWidth().Gap(8).Children(func() {
						components.AlertBanner(c, p, components.ToneDanger, "控制面请求异常", s.LastError)
						ui.Row(c).FillWidth().Justify(ui.End).Children(func() {
							if ui.Button(c.Key("err_open_conn"), "切换连接 / 沙盒模式").Clicked() {
								s.OpenConnectionModal()
							}
						})
					})
				})
			}
			if s.ResetRequired {
				ui.Box(c).Padding(16, 24, 0, 24).Children(func() {
					ui.Row(c).FillWidth().Padding(12, 16).Radius(10).Background(p.DangerSoft).Border(1, p.Danger.Alpha(0.35)).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
						ui.Text(c, "事件流游标已过期（reset_required）："+nonEmptyOr(s.ResetMessage, "需要重新同步全量快照")).FontSize(12.5).FontWeight(500).TextColor(p.Danger)
						if ui.PrimaryButton(c.Key("reload_stream"), "重新同步事件流").Clicked() {
							s.TriggerRefresh(c)
						}
					})
				})
			}

			ui.Scroll(c).Grow(1).FillWidth().Padding(24).Children(func() {
				switch s.ActiveView {
				case ViewSources:
					s.renderSourcesView(c, p)
				case ViewContainers:
					s.renderContainersView(c, p)
				case ViewRuns:
					s.renderRunsView(c, p)
				case ViewServices:
					s.renderServicesView(c, p)
				case ViewChanges:
					s.renderChangesView(c, p)
				default:
					s.renderOverviewView(c, p)
				}
			})
		})
	})

	s.renderDrawers(c, p)
	s.renderModals(c, p)
}

func (s *Store) renderDrawers(c *ui.Context, p theme.Palette) {
	if s.ActiveView == ViewRuns && s.RunDrawerOpen {
		s.renderRunDetailDrawer(c, p)
	}
	if s.ActiveView == ViewChanges && s.ChangeDrawerOpen {
		s.renderChangeDetailDrawer(c, p)
	}
}

func (s *Store) activeViewTitle() string {
	switch s.ActiveView {
	case ViewSources:
		return "来源与动作架构"
	case ViewContainers:
		return "容器、场景与规则编排"
	case ViewRuns:
		return "运行追踪与投递证据"
	case ViewServices:
		return "服务状态与维护排空"
	case ViewChanges:
		return "变更审计与配置漂移"
	default:
		return "系统总览"
	}
}

func (s *Store) renderSidebarPane(c *ui.Context, p theme.Palette) {
	attentionCount := 0
	if s.Overview != nil {
		for _, item := range s.Overview.Data.Attention {
			if !item.Acknowledged {
				attentionCount++
			}
		}
	}
	unknownCount := 0
	if s.Overview != nil && s.Overview.Data.CurrentWork.UnknownDelivery != nil {
		unknownCount = *s.Overview.Data.CurrentWork.UnknownDelivery
	}

	bar := c.TitleBar()
	topInset := bar.Bottom + 10
	if bar.Height > topInset {
		topInset = bar.Height
	}
	if topInset < 46 {
		topInset = 46
	}

	icons := components.Icons()

	ui.Column(c).Width(248).FillHeight().Background(p.SidebarBg).BorderEndWidth(1).BorderColor(p.Border).Children(func() {
		// Drag region + Brand block seated cleanly below macOS traffic lights when inset
		ui.Row(c).FillWidth().Height(topInset).DragWindow()

		ui.Row(c).FillWidth().Padding(4, 16, 14, 16).Gap(10).AlignItems(ui.Center).DragWindow().Children(func() {
			if s.logoBitmap != nil {
				ui.Image(c, s.logoBitmap).Size(32, 32).Radius(8).Border(1, p.Border).Label("JINX Logo")
			}
			ui.Column(c).Grow(1).Gap(1).Children(func() {
				ui.Text(c, "JINX 控制台").FontSize(14).FontWeight(600).TextColor(p.Text).SingleLine()
				ui.Text(c, "多来源编排与监控").FontSize(11).TextColor(p.TextMuted).SingleLine()
			})
		})

		ui.Sidebar(c, &s.ActiveView, func() {
			ui.SidebarSection(c, "运行监控", &s.SecOps, func() {
				ui.SidebarItem(c, ViewOverview, icons.Overview, "系统总览").Children(func() {
					if attentionCount > 0 {
						ui.Badge(c, fmt.Sprintf("%d", attentionCount))
					}
				})
				ui.SidebarItem(c, ViewRuns, icons.Runs, "运行与追踪").Children(func() {
					if unknownCount > 0 {
						ui.Badge(c, fmt.Sprintf("%d", unknownCount))
					}
				})
			})
			ui.SidebarSection(c, "配置编排", &s.SecConfig, func() {
				ui.SidebarItem(c, ViewSources, icons.Sources, "来源与动作")
				ui.SidebarItem(c, ViewContainers, icons.Containers, "容器与规则")
			})
			ui.SidebarSection(c, "运维与审计", &s.SecRuntime, func() {
				ui.SidebarItem(c, ViewServices, icons.Services, "服务与维护")
				ui.SidebarItem(c, ViewChanges, icons.Changes, "变更与漂移")
			})
		}).Grow(1).FillWidth().Background(p.SidebarBg)

		// Footer connection origin card + Connection Settings trigger
		ui.Divider(c)
		ui.Column(c).FillWidth().Padding(12).Children(func() {
			ui.Column(c).FillWidth().Padding(10, 12).Gap(8).Radius(10).Background(p.CardBg).Border(1, p.Border).Children(func() {
				ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
					if s.client.IsMock() {
						components.StatusPill(c, p, "沙盒演示 ("+scenarioCN(strings.TrimPrefix(s.client.Label(), "mock:"))+")", components.ToneWarn)
					} else {
						components.StatusPill(c, p, "本地回环连接", components.ToneInfo)
					}
				})
				if !s.client.IsMock() {
					ui.Text(c, s.client.Label()).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).SingleLine()
				}
				if ui.Button(c.Key("open_conn_modal"), "连接设置").FillWidth().Clicked() {
					s.OpenConnectionModal()
				}
			})
		})
	})
}

func (s *Store) renderTopBar(c *ui.Context, p theme.Palette) {
	icons := components.Icons()
	ui.Column(c).FillWidth().Background(p.TopbarBg).Children(func() {
		ui.Row(c).FillWidth().Height(52).PaddingX(24).Gap(12).AlignItems(ui.Center).Justify(ui.SpaceBetween).DragWindow().Children(func() {
			ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
				ui.Text(c, "控制台").FontSize(12.5).TextColor(p.TextMuted).SingleLine()
				ui.Text(c, "/").FontSize(12.5).TextColor(p.TextMuted).SingleLine()
				ui.Text(c, s.activeViewTitle()).FontSize(13).FontWeight(500).TextColor(p.Text).SingleLine()
				ui.Box(c).Width(4)
				components.StatusPill(c, p, components.FreshnessLabel(s.Freshness), freshnessTone(s.Freshness))
				components.StatusPill(c, p, components.SSEStateLabel(s.SSEState), freshnessTone(s.SSEState))
			})

			ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
				if s.PendingSSEUpdates {
					if ui.PrimaryButton(c.Key("top_apply_sse"), "应用实时更新").Clicked() {
						s.TriggerRefresh(c)
					}
				}
				if ui.Button(c.Key("top_refresh"), "").Disabled(s.Busy).Label("刷新").Children(func() {
					ui.Icon(c, icons.Refresh).FontSize(14).TextColor(p.TextSecondary)
					ui.Text(c, "刷新").FontSize(12.5).TextColor(p.Text)
				}).Clicked() {
					s.TriggerRefresh(c)
				}

				themeLabel := "深色模式"
				themeIcon := icons.Moon
				if s.DarkMode {
					themeLabel = "浅色模式"
					themeIcon = icons.Sun
				}
				if ui.Button(c.Key("top_theme_toggle"), "").Label(themeLabel).Children(func() {
					ui.Icon(c, themeIcon).FontSize(14).TextColor(p.TextSecondary)
					ui.Text(c, themeLabel).FontSize(12.5).TextColor(p.Text)
				}).Clicked() {
					s.DarkMode = !s.DarkMode
					c.SetTheme(s.Palette().UI)
				}
			})
		})
		ui.Divider(c)
	})
}

func (s *Store) renderModals(c *ui.Context, p theme.Palette) {
	// 1. Confirmed Connector dry_run modal
	if ui.Modal(c, &s.DryRunModalOpen, func() {
		ui.Column(c).Width(500).Padding(24).Gap(14).Radius(14).Background(p.CardBg).Border(1, p.BorderStrong).Children(func() {
			ui.Text(c, "确认切换连接器演练模式").FontSize(16).FontWeight(600).TextColor(p.Text)
			if s.DryRunPlan != nil {
				plan := s.DryRunPlan
				components.DataList(c, p, func() {
					components.KVRow(c, p, "目标平台", components.PlatformLabel(plan.Platform)+" ("+plan.Platform+")")
					components.KVRow(c, p, "状态变更", fmt.Sprintf("dry_run: %v → %v", plan.CurrentDryRun, plan.TargetDryRun))
					components.KVRow(c, p, "持久化范围", "仅当前进程生效（persisted: false，重启恢复配置默认值）")
				})
				for _, w := range plan.Warnings {
					components.AlertBanner(c, p, components.ToneWarn, "运行时作用域提醒", w.Message)
				}
				if !plan.TargetDryRun {
					components.AlertBanner(c, p, components.ToneDanger, "实盘外发警告", "关闭演练模式（dry_run=false）后，该连接器将向真实平台账号执行写入/回复动作。")
				}
				ui.Checkbox(c.Key("confirm_dry_run_cb"), &s.DryRunConfirm, "我已确认更新运行中连接器的演练模式状态")
				ui.Row(c).FillWidth().Justify(ui.End).Gap(8).Children(func() {
					if ui.Button(c.Key("dry_run_cancel"), "取消").Clicked() {
						s.DryRunModalOpen = false
						s.DryRunPlan = nil
						s.DryRunConfirm = false
					}
					if ui.PrimaryButton(c.Key("dry_run_apply"), "确认应用演练开关").Disabled(!s.DryRunConfirm || s.Busy).Clicked() {
						s.ApplyConnectorDryRun(c)
					}
				})
			}
		})
	}).Dismissed() {
		s.DryRunPlan = nil
		s.DryRunConfirm = false
	}

	// 2. Change Plan / Diff / Warnings / Publish / Conflict modal
	if ui.Modal(c, &s.ChangePlanModalOpen, func() {
		ui.Column(c).Width(640).Padding(24).Gap(14).Radius(14).Background(p.CardBg).Border(1, p.BorderStrong).Children(func() {
			ui.Text(c, "预览配置变更计划").FontSize(16).FontWeight(600).TextColor(p.Text)
			if s.ActivePlan != nil {
				plan := s.ActivePlan
				affectedNames := make([]string, 0, len(plan.AffectedGroups))
				for _, gid := range plan.AffectedGroups {
					affectedNames = append(affectedNames, s.ContainerDisplayWithPlatform(gid, ""))
				}
				components.DataList(c, p, func() {
					components.KVRow(c, p, "计划标识", components.ShortID(plan.ID))
					components.KVRow(c, p, "基线指纹", components.ShortID(plan.Base.Fingerprint))
					components.KVRow(c, p, "受影响容器", nonEmptyOr(strings.Join(affectedNames, "、"), "全局或中继规则"))
				})
				for _, ef := range plan.Effect {
					ui.Text(c, "• "+components.ControlReasonCN(ef)).FontSize(12).TextColor(p.TextSecondary)
				}
				for _, w := range plan.Warnings {
					components.AlertBanner(c, p, components.ToneWarn, w.Code, w.Message)
				}
				if s.PublishConflict != "" {
					components.AlertBanner(c, p, components.ToneDanger, "409 版本冲突 — 基线配置已过期",
						s.PublishConflict+" — 请先重新拉取最新服务端配置后再生成变更计划。")
				}
				ui.Scroll(c).MaxHeight(240).FillWidth().Padding(12).Radius(8).Background(p.SubtleBg).Border(1, p.Border).Children(func() {
					components.DataList(c, p, func() {
						if len(plan.Changes) == 0 {
							ui.Text(c, "未检测到有效字段差异。").FontSize(12).TextColor(p.TextMuted)
						}
						for i, ch := range plan.Changes {
							ui.Column(c).Key(i).PaddingY(6).Gap(2).Children(func() {
								ui.Text(c, fmt.Sprintf("%s · %s", components.ServiceDisplayName(ch.Component), components.FieldPathLabel(ch.Path))).FontSize(12).FontWeight(600).TextColor(p.Text)
								ui.Text(c, fmt.Sprintf("  变更前: %s  →  变更后: %s", components.FormatAnyValueCN(ch.Before), components.FormatAnyValueCN(ch.After))).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextMuted)
							})
						}
					})
				})
				ui.Row(c).FillWidth().Justify(ui.End).Gap(8).Children(func() {
					if ui.Button(c.Key("plan_close"), "关闭计划").Clicked() {
						s.ChangePlanModalOpen = false
						s.ActivePlan = nil
						s.PublishConflict = ""
					}
					if s.PublishConflict != "" {
						if ui.PrimaryButton(c.Key("plan_reload_conflict"), "重新拉取最新配置").Clicked() {
							s.ChangePlanModalOpen = false
							s.ActivePlan = nil
							s.PublishConflict = ""
							s.Draft.Dirty = false
							s.GlobalRelayDirty = false
							s.TriggerRefresh(c)
						}
					} else {
						if ui.PrimaryButton(c.Key("plan_publish"), "发布变更").Disabled(s.Busy).Clicked() {
							s.PublishActivePlan(c)
						}
					}
				})
			}
		})
	}).Dismissed() {
		s.ActivePlan = nil
		s.PublishConflict = ""
	}

	// 3. Historical ConfigSnapshot modal
	ui.Modal(c, &s.SnapshotModalOpen, func() {
		ui.Column(c).Width(580).Padding(24).Gap(14).Radius(14).Background(p.CardBg).Border(1, p.BorderStrong).Children(func() {
			ui.Text(c, "历史配置快照（只读）").FontSize(16).FontWeight(600).TextColor(p.Text)
			if s.SnapshotMissingReason != "" {
				components.AlertBanner(c, p, components.ToneWarn, "历史快照不可用", s.SnapshotMissingReason)
			}
			if s.ActiveSnapshot != nil {
				snap := s.ActiveSnapshot
				scopes := make([]string, 0, len(snap.Scope))
				for _, sc := range snap.Scope {
					scopes = append(scopes, components.SnapshotScopeLabel(sc))
				}
				components.DataList(c, p, func() {
					components.KVRow(c, p, "快照编号", components.ShortID(snap.ID))
					components.KVRow(c, p, "所属容器", s.ContainerDisplayWithPlatform(snap.Group.ID, snap.Group.Label))
					components.KVRow(c, p, "快照来源", components.SnapshotSourceLabel(snap.Source))
					components.KVRow(c, p, "覆盖范围", nonEmptyOr(strings.Join(scopes, "、"), "—"))
				})
				components.AlertBanner(c, p, components.ToneInfo, "历史快照范围说明", "仅记录该次运行当场生效的公开容器与全局字段值；上游继承链与本地私有策略规则不在快照留存范围内。")
				ui.Scroll(c).MaxHeight(220).FillWidth().Padding(12).Radius(8).Background(p.SubtleBg).Border(1, p.Border).Children(func() {
					components.DataList(c, p, func() {
						for k, fv := range snap.Fields {
							components.KVRow(c, p, "容器 · "+components.FieldPathLabel(k),
								fmt.Sprintf("%s（%s）", components.FormatAnyValueCN(fv.Effective), components.FieldOriginLabel(fv.Origin)))
						}
						for k, fv := range snap.GlobalFields {
							components.KVRow(c, p, "全局 · "+components.FieldPathLabel(k),
								fmt.Sprintf("%s（%s）", components.FormatAnyValueCN(fv.Effective), components.FieldOriginLabel(fv.Origin)))
						}
					})
				})
			}
			ui.Row(c).FillWidth().Justify(ui.End).Children(func() {
				if ui.Button(c.Key("snap_close"), "关闭快照").Clicked() {
					s.SnapshotModalOpen = false
				}
			})
		})
	})

	// 4. Service Plan / Execute modal
	if ui.Modal(c, &s.ServicePlanModalOpen, func() {
		ui.Column(c).Width(520).Padding(24).Gap(14).Radius(14).Background(p.CardBg).Border(1, p.BorderStrong).Children(func() {
			ui.Text(c, "确认服务生命周期计划").FontSize(16).FontWeight(600).TextColor(p.Text)
			if sp := s.ActiveServicePlan; sp != nil {
				eligText := "否（存在前置阻塞）"
				if sp.Eligible {
					eligText = "是（满足执行条件）"
				}
				components.DataList(c, p, func() {
					components.KVRow(c, p, "目标服务", components.ServiceDisplayName(sp.ServiceID))
					components.KVRow(c, p, "计划动作", serviceActionCN(sp.Kind))
					components.KVRow(c, p, "目标版本", components.ShortID(sp.TargetVersion))
					components.KVRow(c, p, "是否需排空", fmt.Sprintf("%s（%s）", boolValCN(sp.DrainRequired), components.DrainScopeLabel(sp.DrainScope)))
					components.KVRow(c, p, "可执行条件", eligText)
				})
				for _, blk := range sp.Blockers {
					components.AlertBanner(c, p, components.ToneDanger, "阻塞条件: "+blk.Code, blk.Message)
				}
				ui.Row(c).FillWidth().Justify(ui.End).Gap(8).Children(func() {
					if ui.Button(c.Key("svc_plan_cancel"), "取消").Clicked() {
						s.ServicePlanModalOpen = false
						s.ActiveServicePlan = nil
					}
					if ui.PrimaryButton(c.Key("svc_plan_exec"), "执行服务计划").Disabled(!sp.Eligible || s.Busy).Clicked() {
						s.ExecuteServicePlan(c)
					}
				})
			}
		})
	}).Dismissed() {
		s.ActiveServicePlan = nil
	}

	// 5. Data source / loopback connection settings modal
	if ui.Modal(c, &s.ConnModalOpen, func() {
		ui.Column(c).Width(480).Padding(24).Gap(14).Radius(14).Background(p.CardBg).Border(1, p.BorderStrong).Children(func() {
			ui.Text(c, "配置数据源连接").FontSize(16).FontWeight(600).TextColor(p.Text)
			ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
				components.SegmentedControl(c, p, "conn_mode", &s.ConnMode, false, "本地控制面", "沙盒演示")
			})
			if s.ConnMode == 0 {
				ui.Column(c).FillWidth().Gap(6).Children(func() {
					ui.Text(c, "控制面 HTTP 地址").FontSize(12).FontWeight(500).TextColor(p.Text)
					ui.TextInput(c.Key("conn_api_url"), &s.ConnAPIURL).Placeholder("http://127.0.0.1:8090").FillWidth()
					ui.Text(c, "安全策略：仅允许本地回环 HTTP 地址（127.0.0.1 / ::1 / localhost）").FontSize(11).TextColor(p.TextMuted)
				})
			} else {
				ui.Column(c).FillWidth().Gap(6).Children(func() {
					ui.Text(c, "沙盒模拟场景").FontSize(12).FontWeight(500).TextColor(p.Text)
					ui.Select(c.Key("conn_scenario"), &s.ConnScenario, []string{"完整多来源", "降级与维护排空", "409 并发冲突", "未知投递证据"})
					ui.Text(c, "内存隔离演练数据源，不访问真实微信、B站账号或本机端口。").FontSize(11).TextColor(p.TextMuted)
				})
			}
			if s.ConnModalErr != "" {
				components.AlertBanner(c, p, components.ToneDanger, "地址校验失败", s.ConnModalErr)
			}
			ui.Row(c).FillWidth().Justify(ui.End).Gap(8).Children(func() {
				if ui.Button(c.Key("conn_cancel"), "取消").Clicked() {
					s.ConnModalOpen = false
					s.ConnModalErr = ""
				}
				if ui.PrimaryButton(c.Key("conn_apply"), "应用并重新连接").Disabled(s.Busy).Clicked() {
					s.ApplyConnectionSettings(c)
				}
			})
		})
	}).Dismissed() {
		s.ConnModalErr = ""
	}
}

func freshnessTone(state string) components.Tone {
	switch state {
	case "fresh", "ready":
		return components.ToneOK
	case "stale", "not_ready":
		return components.ToneWarn
	case "disconnected", "unreachable", "failed":
		return components.ToneDanger
	default:
		return components.ToneNeutral
	}
}

func scenarioCN(sc string) string {
	switch sc {
	case "degraded":
		return "降级与维护排空"
	case "conflict":
		return "409 并发冲突"
	case "unknown-delivery":
		return "未知投递证据"
	default:
		return "完整多来源"
	}
}

func serviceActionCN(kind string) string {
	switch kind {
	case "start":
		return "启动服务"
	case "stop":
		return "停止服务"
	case "restart":
		return "重启服务"
	default:
		return kind
	}
}
