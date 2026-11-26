package app

import (
	"fmt"
	"strings"

	"jinx-desktop/client"
	"jinx-desktop/components"
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

func (s *Store) renderChangesView(c *ui.Context, p theme.Palette) {
	ui.Column(c).FillWidth().Gap(20).Children(func() {
		components.PageHeader(c, p, "变更审计与配置漂移", "变更发布流水记录、内存/磁盘漂移对比及显式回滚计划 · 点击记录打开详情抽屉", func() {
			ui.Text(c, "状态筛选").FontSize(11.5).TextColor(p.TextMuted)
			ui.Box(c).Width(136).Children(func() {
				if ui.Select(c.Key("chg_filter_status"), &s.ChangeStatusFilter, components.ChangeStatusFilterOptions).Changed() {
					s.FilterChanges(c)
				}
			})
		})

		// Configuration Drift cards (dispatcher + relay)
		ui.Row(c).FillWidth().Gap(16).AlignItems(ui.Stretch).Children(func() {
			s.renderDriftCard(c, p, "Dispatcher 配置漂移检查", s.DriftDispatcher)
			s.renderDriftCard(c, p, "Relay 配置漂移检查", s.DriftRelay)
		})

		// Full-Width Changes Journal
		var items []client.ChangeRecord
		if s.ChangesPage != nil {
			items = s.ChangesPage.Data.Items
		}
		components.CardWithHeader(c, p, fmt.Sprintf("变更审计日志 (%d)", len(items)), "点击任意变更卡片查看字段差异、组件回读校验与生成回滚计划", nil, func() {
			if len(items) == 0 {
				ui.Text(c, "当前筛选条件下无历史变更记录。").FontSize(12).TextColor(p.TextMuted)
				return
			}
			ui.Column(c).FillWidth().Gap(8).Children(func() {
				for i, chg := range items {
					i, chg := i, chg
					selected := s.ChangeDrawerOpen && s.SelectedChange != nil && s.SelectedChange.Data.ID == chg.ID
					accent := p.OK
					bg := p.SubtleBg
					border := p.Border
					if chg.Operation.Status == "partial_apply" || chg.Operation.Status == "unknown" || chg.Operation.Status == "failed" {
						accent = p.Danger
						bg = p.DangerSoft
						border = p.Danger.Alpha(0.35)
					} else if chg.Operation.Status == "pending" {
						accent = p.Warn
						bg = p.WarnSoft
						border = p.Warn.Alpha(0.35)
					}
					if selected {
						border = p.Accent
					}

					fieldPreview := s.summarizeChangedFieldsCN(chg.Changes)
					card := ui.Row(c).Key("chg_row_"+chg.ID).FillWidth().Padding(12, 14, 12, 12).Radius(9).Background(bg).Border(1, border).Gap(10).AlignItems(ui.Stretch)
					if card.Hovered() && !selected {
						card.Background(p.RaisedBg).Border(1, p.BorderStrong)
					}
					card.Cursor(ui.CursorPointer).Role(ui.RoleButton).Label("查看 " + components.ShortID(chg.ID)).Children(func() {
						ui.Box(c).Width(4).Radius(2).Background(accent)
						ui.Column(c).Grow(1).Gap(5).Children(func() {
							ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(8).Children(func() {
								ui.Row(c).Grow(1).Gap(8).AlignItems(ui.Center).Children(func() {
									ui.Text(c, components.ShortID(chg.ID)).FontSize(13).FontWeight(600).FontFeatures("tnum").TextColor(p.Text)
									components.HealthPill(c, p, chg.Operation.Status)
									components.TagPill(c, p, "来源: "+components.OriginLabel(chg.Origin), components.ToneNeutral)
									if chg.RollbackOf != nil {
										components.TagPill(c, p, "回滚自: "+components.ShortID(*chg.RollbackOf), components.ToneInfo)
									}
								})
								ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
									ui.Text(c, components.FormatLocalDateTime(&chg.CreatedAt)).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).NoWrap()
									ui.Icon(c, components.Icons().ChevronRight).FontSize(14).TextColor(p.TextMuted)
								})
							})
							ui.Text(c, nonEmptyOr(components.ControlReasonCN(chg.Operation.Summary), "配置发布变更记录")).FontSize(12).TextColor(p.Text)
							if fieldPreview != "" {
								ui.Text(c, fieldPreview).FontSize(11.5).TextColor(p.TextSecondary).SingleLine()
							}
						})
					})
					if card.Clicked() {
						s.OpenChangeDrawer(i)
					}
				}
			})
		})
	})
}

func (s *Store) formatFieldPathCN(path string) string {
	trimmed := strings.TrimSpace(path)
	if strings.HasPrefix(trimmed, "group_rules.") {
		rest := strings.TrimPrefix(trimmed, "group_rules.")
		if idx := strings.LastIndex(rest, "."); idx > 0 {
			gid := rest[:idx]
			leaf := rest[idx+1:]
			return fmt.Sprintf("%s · %s", s.ContainerDisplayName(gid, ""), components.FieldPathLabel(leaf))
		}
	}
	return components.FieldPathLabel(trimmed)
}

func (s *Store) summarizeChangedFieldsCN(changes []client.FieldChange) string {
	if len(changes) == 0 {
		return ""
	}
	parts := make([]string, 0, 3)
	for idx, fc := range changes {
		if idx >= 3 {
			break
		}
		parts = append(parts, s.formatFieldPathCN(fc.Path))
	}
	out := fmt.Sprintf("涉及字段（%d 项）：%s", len(changes), strings.Join(parts, "、"))
	if len(changes) > 3 {
		out += " 等"
	}
	return out
}

func (s *Store) renderChangeDetailDrawer(c *ui.Context, p theme.Palette) {
	title := "变更详情与回滚"
	subtitle := "配置发布流水详情"
	var chgPtr *client.ChangeRecord
	if s.SelectedChange != nil {
		chg := s.SelectedChange.Data
		chgPtr = &chg
		title = fmt.Sprintf("变更详情 — %s", components.ShortID(chg.ID))
		subtitle = fmt.Sprintf("%s · 来源: %s · 涉及 %d 项字段",
			components.FormatLocalFullTime(&chg.CreatedAt),
			components.OriginLabel(chg.Origin),
			len(chg.Changes))
	}

	components.Drawer(c, p, "change_drawer", &s.ChangeDrawerOpen, 640, title, subtitle, func() {
		if chgPtr == nil {
			return
		}
		chg := *chgPtr
		rbCap := s.Capability("rollback")
		canRollback := chg.BeforeSnapshot != nil && rbCap.Supported && rbCap.Enabled
		components.HealthPill(c, p, chg.Operation.Status)
		if ui.PrimaryButton(c.Key("btn_plan_rollback"), "生成回滚计划").Disabled(!canRollback || s.Busy).Clicked() {
			s.PlanRollback(c, chg)
		}
	}, func() {
		if s.SelectedChange == nil {
			components.AlertBanner(c, p, components.ToneInfo, "正在加载变更详情", "请稍候...")
			return
		}
		chg := s.SelectedChange.Data

		ui.Column(c).FillWidth().Gap(14).Children(func() {
			if chg.Operation.Status == "partial_apply" || chg.Operation.Status == "unknown" {
				components.AlertBanner(c, p, components.ToneDanger,
					"部分生效或状态未知 — 绝不做静默自动补偿",
					"跨组件发布未达到原子一致确认。控制台不会私自反向回滚，请先核查各组件实际状态再决定是否生成显式回滚计划。")
			}

			components.Card(c, p, "基线与回读摘要", func() {
				components.DataList(c, p, func() {
					components.KVRow(c, p, "变更说明", nonEmptyOr(components.ControlReasonCN(chg.Operation.Summary), "配置发布变更记录"))
					components.KVRow(c, p, "变更编号", chg.ID)
					components.KVRow(c, p, "记录与请求编号", fmt.Sprintf("%s / %s", components.ShortID(chg.Operation.ID), components.ShortID(chg.Operation.RequestID)))
					components.KVRow(c, p, "基线版本指纹", components.ShortID(chg.Base.Fingerprint))
					if chg.Readback != nil {
						components.KVRow(c, p, "回读校验指纹", components.ShortID(chg.Readback.Fingerprint))
					}
					if chg.RollbackOf != nil {
						components.KVRow(c, p, "回滚目标单号", *chg.RollbackOf)
					}
				})
			})

			components.CardWithHeader(c, p, "各组件持久化回读状态", "", nil, func() {
				components.DataList(c, p, func() {
					for _, comp := range chg.Operation.Components {
						ui.Row(c).Key("cmp_"+comp.ID).FillWidth().PaddingY(8).Justify(ui.SpaceBetween).AlignItems(ui.Center).Children(func() {
							ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
								ui.Text(c, components.ServiceDisplayName(comp.ID)).FontSize(12).FontWeight(600).TextColor(p.Text)
								components.HealthPill(c, p, comp.State)
							})
							ui.Text(c, fmt.Sprintf("回读版本: %s · 持久化校验: %s", components.ShortID(derefStr(comp.ReadbackVersion)), fmtBoolPtrCN(comp.PersistenceVerified))).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextMuted)
						})
					}
				})
			})

			components.CardWithHeader(c, p, fmt.Sprintf("字段级变更差异 (%d)", len(chg.Changes)), "", nil, func() {
				if len(chg.Changes) == 0 {
					ui.Text(c, "无字段级变更差异。").FontSize(12).TextColor(p.TextMuted)
					return
				}
				components.DataList(c, p, func() {
					for idx, fc := range chg.Changes {
						ui.Column(c).Key(idx).FillWidth().PaddingY(8).Gap(2).Children(func() {
							ui.Text(c, fmt.Sprintf("%s · %s", components.ServiceDisplayName(fc.Component), s.formatFieldPathCN(fc.Path))).FontSize(12).FontWeight(600).TextColor(p.Text)
							ui.Text(c, fmt.Sprintf("配置前: %s → 配置后: %s（生效值 %s → %s）",
								components.FormatAnyValueCN(fc.Before), components.FormatAnyValueCN(fc.After),
								components.FormatAnyValueCN(fc.EffectiveBefore), components.FormatAnyValueCN(fc.EffectiveAfter))).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextSecondary)
						})
					}
				})
			})
		})
	})
}

func (s *Store) renderDriftCard(c *ui.Context, p theme.Palette, title string, env *client.Envelope[client.ConfigurationDrift]) {
	ui.Column(c).Grow(1).Children(func() {
		components.Card(c, p, title, func() {
			if env == nil {
				components.AlertBanner(c, p, components.ToneNeutral, "漂移检测暂不可用", "未接收到该组件的内存与磁盘漂移探针数据。")
				return
			}
			d := env.Data
			tone := components.ToneOK
			statusTxt := "内存与磁盘一致"
			if !d.Available || d.Drift == nil {
				tone = components.ToneNeutral
				statusTxt = "不可用"
			} else if *d.Drift {
				tone = components.ToneWarn
				statusTxt = "检测到配置漂移"
			}
			ui.Row(c).Gap(10).AlignItems(ui.Center).Children(func() {
				components.StatusPill(c, p, statusTxt, tone)
				ui.Text(c, fmt.Sprintf("内存 %s · 磁盘 %s", components.ShortID(derefStr(d.MemoryVersion)), components.ShortID(derefStr(d.DiskVersion)))).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextMuted)
			})
			ui.Text(c, components.ControlReasonCN(d.Reason)).FontSize(12).TextColor(p.TextSecondary)
			ui.Text(c, "说明：脱离可见控制台字段范围的本地私有策略配置不纳入此处对比。").FontSize(11).TextColor(p.TextMuted)
		})
	})
}
