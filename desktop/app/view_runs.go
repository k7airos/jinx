package app

import (
	"fmt"
	"strings"

	"jinx-desktop/client"
	"jinx-desktop/components"
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

func (s *Store) renderRunsView(c *ui.Context, p theme.Palette) {
	ui.Column(c).FillWidth().Gap(18).Children(func() {
		covTxt := "正交状态机（阶段 · 结果 · 投递）与提交/回声证据审查（不显示聊天原文）"
		if s.RunsPage != nil {
			covTxt = fmt.Sprintf("追踪覆盖起始时间: %s · 严格区分模型完成与用户可见送达 · 点击任意条目展开详情抽屉",
				components.FormatLocalFullTime(&s.RunsPage.Data.CoverageSince))
		}
		components.PageHeader(c, p, "运行追踪与投递证据", covTxt, nil)

		if s.PendingSSEUpdates {
			ui.Row(c).FillWidth().Padding(12, 16).Radius(10).Background(p.InfoSoft).Border(1, p.Info.Alpha(0.30)).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
				ui.Text(c, "检测到后台 SSE 新增或变更运行状态（保留当前查看选中项，需手动加载生效）。").FontSize(12).FontWeight(500).TextColor(p.Info)
				if ui.Button(c.Key("runs_load_sse"), "加载最新状态").Clicked() {
					s.ApplyRunFilter(c, "")
				}
			})
		}

		// Instant Filter Bar: presets + common dropdowns + Enter-to-search + toggle
		groupOpts := s.SyncRunGroupFilterFromLabel()
		components.Card(c, p, "", func() {
			ui.Column(c).FillWidth().Gap(10).Children(func() {
				ui.Row(c).FillWidth().Wrap().Gap(8).AlignItems(ui.Center).Children(func() {
					for _, preset := range []string{"全部", "已发送回复", "待核对", "已拦截"} {
						preset := preset
						if components.FilterChip(c, p, "preset_"+preset, preset, s.RunPreset == preset) {
							s.ApplyRunPreset(c, preset)
						}
					}
					ui.Box(c).Width(1).Height(18).Background(p.BorderStrong)
					if components.FilterChip(c, p, "chip_hide_blocked", "隐藏已拦截", s.RunHideBlocked) {
						s.RunHideBlocked = !s.RunHideBlocked
					}
					ui.Box(c).Grow(1)
					advText := "高级筛选"
					if s.ShowAdvancedRunFilters {
						advText = "收起高级筛选"
					}
					if ui.Button(c.Key("btn_adv_filters"), "").Label(advText).Children(func() {
						ui.Icon(c, components.Icons().Sliders).FontSize(13).TextColor(p.TextSecondary)
						ui.Text(c, advText).FontSize(12).TextColor(p.TextSecondary)
					}).Clicked() {
						s.ShowAdvancedRunFilters = !s.ShowAdvancedRunFilters
					}
					if s.HasActiveRunFilters() {
						if ui.Button(c.Key("btn_reset_run_filters"), "重置").Clicked() {
							s.ResetRunFilters(c)
						}
					}
				})

				ui.Row(c).FillWidth().Gap(10).AlignItems(ui.Center).Children(func() {
					ui.Box(c).Width(200).Children(func() {
						if ui.Select(c.Key("f_group_sel"), &s.RunGroupFilterLabel, groupOpts).Changed() {
							s.SyncRunGroupFilterFromLabel()
							s.ApplyRunFilter(c, "")
						}
					})
					ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "投递").FontSize(11.5).TextColor(p.TextMuted).NoWrap()
						ui.Box(c).Width(120).Children(func() {
							if ui.Select(c.Key("f_delivery"), &s.RunDeliveryFilter, components.RunDeliveryFilterOptions).Changed() {
								if s.RunDeliveryFilter == "未知" && s.RunOutcomeFilter == "全部" && s.RunPhaseFilter == "全部" {
									s.RunPreset = "待核对"
								} else {
									s.RunPreset = ""
								}
								s.ApplyRunFilter(c, "")
							}
						})
					})
					ui.Row(c).Grow(1).Gap(6).AlignItems(ui.Center).Children(func() {
						ui.Icon(c, components.Icons().Search).FontSize(14).TextColor(p.TextMuted)
						qInput := ui.TextInput(c.Key("f_query"), &s.RunQuery).Placeholder("搜索编号或关键词，回车即时筛选").FillWidth()
						if qInput.Submitted() {
							s.SyncRunGroupFilterFromLabel()
							s.ApplyRunFilter(c, "")
						}
					})
				})

				if s.ShowAdvancedRunFilters {
					ui.Row(c).FillWidth().Gap(10).AlignItems(ui.Center).Padding(4, 0, 0, 0).Children(func() {
						ui.Text(c, "运行阶段").FontSize(11.5).TextColor(p.TextMuted)
						ui.Box(c).Width(140).Children(func() {
							if ui.Select(c.Key("f_phase"), &s.RunPhaseFilter, components.RunPhaseFilterOptions).Changed() {
								s.RunPreset = ""
								s.ApplyRunFilter(c, "")
							}
						})
						ui.Box(c).Width(8)
						ui.Text(c, "执行结果").FontSize(11.5).TextColor(p.TextMuted)
						ui.Box(c).Width(140).Children(func() {
							if ui.Select(c.Key("f_outcome"), &s.RunOutcomeFilter, components.RunOutcomeFilterOptions).Changed() {
								s.RunPreset = ""
								s.ApplyRunFilter(c, "")
							}
						})
					})
				}
			})
		})

		// Full-Width Signal-First Run Feed
		var items []client.RunView
		var nextCursor *string
		if s.RunsPage != nil {
			items = s.RunsPage.Data.Items
			nextCursor = s.RunsPage.Data.NextCursor
		}

		var repliedCount, unknownCount, blockedCount, hiddenQuietCount int
		visible := make([]indexedRun, 0, len(items))
		for i, run := range items {
			if run.Outcome == "replied" {
				repliedCount++
			}
			if run.Delivery == "unknown" {
				unknownCount++
			}
			if run.Outcome == "blocked" || run.Outcome == "silent" {
				blockedCount++
			}
			isBlockedOrSilent := runIsQuiet(run)
			if s.RunHideBlocked && isBlockedOrSilent {
				hiddenQuietCount++
				continue
			}
			visible = append(visible, indexedRun{idx: i, run: run, quiet: isBlockedOrSilent})
		}

		subSummary := fmt.Sprintf("本页 %d 条 · 已回复 %d · 投递未知 %d · 已拦截/静默 %d",
			len(items), repliedCount, unknownCount, blockedCount)
		if s.RunHideBlocked && hiddenQuietCount > 0 {
			subSummary += fmt.Sprintf("（已隐藏 %d 条静默/拦截记录）", hiddenQuietCount)
		}

		components.CardWithHeader(c, p, fmt.Sprintf("运行队列 (%d)", len(visible)), subSummary, nil, func() {
			if len(visible) == 0 {
				ui.Text(c, "当前筛选条件下暂无匹配的运行记录。").FontSize(12.5).TextColor(p.TextMuted)
				return
			}
			ui.Column(c).FillWidth().Gap(8).Children(func() {
				for _, entry := range visible {
					s.renderRunFeedCard(c, p, entry.idx, entry.run, entry.quiet)
				}
			})
			if nextCursor != nil && *nextCursor != "" {
				ui.Box(c).Padding(6, 0, 0, 0).Children(func() {
					if ui.Button(c.Key("runs_next_page"), "加载更多记录").Disabled(s.Busy).Clicked() {
						s.ApplyRunFilter(c, *nextCursor)
					}
				})
			}
		})
	})
}

type indexedRun struct {
	idx   int
	run   client.RunView
	quiet bool
}

func runFlags(run client.RunView) (hasSucceededAttempt, hasMayBeRunning bool) {
	for _, att := range run.Attempts {
		if att.Status == "succeeded" {
			hasSucceededAttempt = true
		}
		if att.RemoteExecution == "may_be_running" || att.RemoteExecution == "unknown" {
			hasMayBeRunning = true
		}
	}
	return hasSucceededAttempt, hasMayBeRunning
}

func runIntentionalNoSend(run client.RunView) bool {
	return (run.Outcome == "silent" || run.Outcome == "blocked" || run.Outcome == "suppressed") &&
		(run.Delivery == "not_started" || run.Delivery == "blocked") &&
		run.DeliveryEvidence.NoSubmission
}

func runFallbackReason(run client.RunView) string {
	for _, att := range run.Attempts {
		if att.FallbackReason != nil && strings.TrimSpace(*att.FallbackReason) != "" {
			return strings.TrimSpace(*att.FallbackReason)
		}
	}
	return ""
}

func runShowAttemptNotDeliveredWarning(run client.RunView, hasSucceeded bool) bool {
	return hasSucceeded && run.Delivery != "confirmed" && !runIntentionalNoSend(run)
}

func runIsQuiet(run client.RunView) bool {
	if run.Delivery == "unknown" || run.Delivery == "submitted" || run.Delivery == "submitting" || run.Delivery == "confirmed" || run.Delivery == "failed" {
		return false
	}
	if run.Phase != "" && run.Phase != "terminal" {
		return false
	}
	if run.Outcome != "blocked" && run.Outcome != "silent" {
		return false
	}
	if run.DeliveryEvidence.NoSubmission {
		return true
	}
	hasSucceeded, hasMayBeRunning := runFlags(run)
	return !hasSucceeded && !hasMayBeRunning
}

func runEndClockTime(run client.RunView) string {
	endTime := run.LastProgressAt
	if endTime == nil && !run.UpdatedAt.IsZero() {
		t := run.UpdatedAt
		endTime = &t
	}
	return components.FormatDurationBetween(run.AcceptedAt, endTime)
}

func (s *Store) renderRunFeedCard(c *ui.Context, p theme.Palette, idx int, run client.RunView, quiet bool) {
	selected := s.RunDrawerOpen && s.SelectedRun != nil && s.SelectedRun.Data.Run.ID == run.ID
	ref := runContainerRef(run)
	containerName := s.ContainerDisplayName(ref.ID, ref.Label)
	plat := s.ContainerPlatform(ref.ID)
	msgCount := runMessageCount(run)
	dur := runEndClockTime(run)
	accLabel := "查看 " + components.ShortID(run.ID)

	fb := runFallbackReason(run)

	if quiet {
		bg := p.SubtleBg
		border := p.Border
		if selected {
			bg = p.AccentSoft
			border = p.Accent
		}
		row := ui.Row(c).Key("run_row_"+run.ID).FillWidth().Padding(8, 12).Radius(8).Background(bg).Border(1, border).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(10)
		if row.Hovered() && !selected {
			row.Background(p.RaisedBg).Border(1, p.BorderStrong)
		}
		rc := ""
		if run.ReasonCode != nil {
			rc = *run.ReasonCode
		}
		shortReason := components.ShortReasonLabel(rc)
		if shortReason == "—" {
			shortReason = components.RunShortStatusCN(run)
		}
		quietTag := "已拦截"
		if run.Outcome == "silent" {
			quietTag = "静默"
		}
		metaTxt := components.TriggerLabel(run.Trigger)
		if msgCount > 0 {
			metaTxt += fmt.Sprintf(" · %d 条", msgCount)
		}
		metaTxt += " · " + dur

		row.Cursor(ui.CursorPointer).Role(ui.RoleButton).Label(accLabel).Children(func() {
			ui.Row(c).Grow(1).Gap(8).AlignItems(ui.Center).Children(func() {
				ui.Box(c).Width(3).Height(16).Radius(2).Background(p.BorderStrong)
				components.TagPill(c, p, quietTag, components.ToneNeutral)
				components.TagPill(c, p, plat, components.ToneInfo)
				ui.Text(c, containerName).FontSize(12.5).FontWeight(500).TextColor(p.Text).SingleLine()
				ui.Text(c, shortReason).FontSize(12).TextColor(p.TextMuted).SingleLine()
				if fb != "" {
					components.TagPill(c, p, components.ShortFallbackTag(fb), components.ToneWarn)
				}
			})
			ui.Row(c).Gap(8).AlignItems(ui.Center).Shrink(0).Children(func() {
				ui.Text(c, metaTxt).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).NoWrap()
				ui.Text(c, components.FormatLocalClock(run.AcceptedAt)).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextSecondary).NoWrap()
				ui.Icon(c, components.Icons().ChevronRight).FontSize(14).TextColor(p.TextMuted)
			})
		})
		if row.Clicked() {
			s.SelectedRunIdx = idx
			s.OpenRunDrawer(run.ID)
		}
		return
	}

	// Tier A: Prominent card for sent/replied, anomalous, or in-flight runs.
	hasSucceeded, hasMayBeRunning := runFlags(run)
	notDeliveredWarn := runShowAttemptNotDeliveredWarning(run, hasSucceeded)
	accent := p.Info
	bg := p.InfoSoft
	border := p.Info.Alpha(0.32)
	verdictTone := components.ToneInfo
	leadIcon := components.Icons().Send

	switch {
	case run.Delivery == "unknown" || run.Outcome == "failed" || run.Delivery == "failed":
		accent = p.Danger
		bg = p.DangerSoft
		border = p.Danger.Alpha(0.42)
		verdictTone = components.ToneDanger
		leadIcon = components.Icons().Alert
	case run.Outcome == "replied" && run.Delivery == "confirmed":
		accent = p.OK
		bg = p.OKSoft
		border = p.OK.Alpha(0.38)
		verdictTone = components.ToneOK
		leadIcon = components.Icons().Check
	case run.Outcome == "canceled" || run.Outcome == "cancelled":
		if hasMayBeRunning {
			accent = p.Danger
			bg = p.DangerSoft
			border = p.Danger.Alpha(0.42)
			verdictTone = components.ToneDanger
		} else {
			accent = p.Warn
			bg = p.WarnSoft
			border = p.Warn.Alpha(0.40)
			verdictTone = components.ToneWarn
		}
		leadIcon = components.Icons().Alert
	case (run.Phase != "" && run.Phase != "terminal") || run.Delivery == "submitted" || run.Delivery == "submitting" || notDeliveredWarn:
		accent = p.Warn
		bg = p.WarnSoft
		border = p.Warn.Alpha(0.40)
		verdictTone = components.ToneWarn
		leadIcon = components.Icons().Alert
	case run.Outcome == "suppressed":
		accent = p.Info
		bg = p.InfoSoft
		border = p.Info.Alpha(0.32)
		verdictTone = components.ToneInfo
		leadIcon = components.Icons().Send
	case hasMayBeRunning:
		accent = p.Danger
		bg = p.DangerSoft
		border = p.Danger.Alpha(0.42)
		verdictTone = components.ToneDanger
		leadIcon = components.Icons().Alert
	}
	if selected {
		border = p.Accent
	}

	card := ui.Row(c).Key("run_row_"+run.ID).FillWidth().Padding(14, 16, 14, 12).Radius(10).Background(bg).Border(1, border).Gap(12).AlignItems(ui.Stretch)
	if card.Hovered() && !selected {
		card.Border(1, accent.Alpha(0.65))
	}

	ev := run.DeliveryEvidence
	modelDesc := ""
	if len(run.Attempts) > 0 {
		lastAtt := run.Attempts[len(run.Attempts)-1]
		modelDesc = components.BackendDisplayName(lastAtt.BackendID)
		if lastAtt.ModelLabel != nil && *lastAtt.ModelLabel != "" {
			modelDesc += " / " + *lastAtt.ModelLabel
		}
	} else if run.PlannedBackendID != nil && *run.PlannedBackendID != "" {
		modelDesc = components.BackendDisplayName(*run.PlannedBackendID)
	}

	card.Cursor(ui.CursorPointer).Role(ui.RoleButton).Label(accLabel).Children(func() {
		ui.Box(c).Width(4).Radius(2).Background(accent)
		ui.Column(c).Grow(1).Gap(7).Children(func() {
			// Line 1: Icon + Platform + Bold Container + Concise Verdict Pill | Time + Duration + Chevron
			ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(10).Children(func() {
				ui.Row(c).Grow(1).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Icon(c, leadIcon).FontSize(15).TextColor(accent)
					components.TagPill(c, p, plat, components.ToneInfo)
					ui.Text(c, containerName).FontSize(14).FontWeight(600).TextColor(p.Text).SingleLine()
					components.StatusPill(c, p, components.RunShortStatusCN(run), verdictTone)
				})
				ui.Row(c).Gap(10).AlignItems(ui.Center).Shrink(0).Children(func() {
					ui.Text(c, fmt.Sprintf("%s · 耗时 %s", components.FormatLocalDateTime(run.AcceptedAt), dur)).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextSecondary).NoWrap()
					ui.Text(c, components.ShortID(run.ID)).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).NoWrap()
					ui.Icon(c, components.Icons().ChevronRight).FontSize(15).TextColor(p.TextSecondary)
				})
			})

			// Line 2: Clear Chinese summary / excerpt
			summaryLine := components.RunSummaryCN(run)
			if run.Excerpt != nil && strings.TrimSpace(*run.Excerpt) != "" {
				summaryLine += " · 摘要: " + strings.TrimSpace(*run.Excerpt)
			}
			ui.Text(c, summaryLine).FontSize(12.5).TextColor(p.Text)

			// Line 3: Rich horizontal execution & delivery telemetry
			ui.Row(c).Wrap().Gap(6).AlignItems(ui.Center).Children(func() {
				if run.Phase != "" && run.Phase != "terminal" {
					components.PhasePill(c, p, run.Phase)
				}
				components.OutcomePill(c, p, run.Outcome)
				components.DeliveryPill(c, p, run.Delivery)

				trigTxt := components.TriggerLabel(run.Trigger)
				if msgCount > 0 {
					trigTxt += fmt.Sprintf("（%d 条消息）", msgCount)
				}
				components.TagPill(c, p, trigTxt, components.ToneNeutral)

				if modelDesc != "" {
					components.TagPill(c, p, "引擎: "+modelDesc, components.ToneNeutral)
				}
				if fb != "" {
					components.TagPill(c, p, components.ShortFallbackTag(fb), components.ToneWarn)
				}
				if run.Action != nil {
					components.TagPill(c, p, "动作: "+components.ActionKindLabel(run.Action.Kind), components.ToneNeutral)
				}
				if ev.SubmittedAt != nil {
					components.TagPill(c, p, "提交 "+components.FormatLocalClock(ev.SubmittedAt), components.ToneInfo)
				} else if ev.SubmittedAtSource != nil && *ev.SubmittedAtSource == "echo_inferred" {
					components.TagPill(c, p, "提交: 仅回声反推", components.ToneWarn)
				}
				if ev.ConfirmedAt != nil {
					components.TagPill(c, p, "回声确认 "+components.FormatLocalClock(ev.ConfirmedAt), components.ToneOK)
				}
			})

			// Line 4: Inline truth/safety warnings & calm fallback annotations
			if notDeliveredWarn {
				ui.Text(c, "警告：模型执行成功 ≠ 回复已送达（尚未收到目标平台确认回声）").FontSize(11.5).FontWeight(600).TextColor(p.Warn)
			}
			if run.Delivery == "unknown" {
				ui.Text(c, "告警：投递状态未知 — 需人工核对（禁止盲目重发）").FontSize(11.5).FontWeight(600).TextColor(p.Danger)
			}
			if hasMayBeRunning {
				if fb != "" && (run.Outcome == "replied" || runIntentionalNoSend(run)) {
					ui.Text(c, "说明："+components.FallbackReasonLabel(fb)+"（原主引擎请求可能仍在后台收尾，不影响本次结果）").FontSize(11.5).TextColor(p.Warn)
				} else {
					ui.Text(c, "告警：远端执行可能仍在运行（本地取消仅停止跟踪）").FontSize(11.5).FontWeight(600).TextColor(p.Danger)
				}
			}
		})
	})

	if card.Clicked() {
		s.SelectedRunIdx = idx
		s.OpenRunDrawer(run.ID)
	}
}

func (s *Store) renderRunDetailDrawer(c *ui.Context, p theme.Palette) {
	title := "运行详情"
	subtitle := "正在同步完整追踪记录..."
	var runPtr *client.RunView
	if s.SelectedRun != nil {
		r := s.SelectedRun.Data.Run
		runPtr = &r
		ref := runContainerRef(r)
		containerName := s.ContainerDisplayName(ref.ID, ref.Label)
		plat := s.ContainerPlatform(ref.ID)
		dur := runEndClockTime(r)
		msgCount := runMessageCount(r)
		trigTxt := components.TriggerLabel(r.Trigger)
		if msgCount > 0 {
			trigTxt += fmt.Sprintf("（%d 条消息）", msgCount)
		}
		title = fmt.Sprintf("%s · %s", containerName, components.RunShortStatusCN(r))
		subtitle = fmt.Sprintf("%s · 受理于 %s · 耗时 %s · %s", plat, components.FormatLocalFullTime(r.AcceptedAt), dur, trigTxt)
	}

	components.Drawer(c, p, "run_drawer", &s.RunDrawerOpen, 680, title, subtitle, func() {
		if runPtr != nil {
			components.DeliveryPill(c, p, runPtr.Delivery)
		}
	}, func() {
		if s.SelectedRun == nil {
			components.AlertBanner(c, p, components.ToneInfo, "正在加载运行详情", "请稍候...")
			return
		}
		detail := s.SelectedRun.Data
		run := detail.Run
		ref := runContainerRef(run)
		msgCount := runMessageCount(run)
		trigTxt := components.TriggerLabel(run.Trigger)
		if msgCount > 0 {
			trigTxt += fmt.Sprintf("（%d 条消息）", msgCount)
		}

		ui.Column(c).FillWidth().Gap(14).Children(func() {
			// Orthogonal state banner + critical safety callouts
			components.Card(c, p, "状态判定与人工操作", func() {
				ui.Row(c).Wrap().Gap(6).AlignItems(ui.Center).Children(func() {
					components.PhasePill(c, p, run.Phase)
					components.OutcomePill(c, p, run.Outcome)
					components.DeliveryPill(c, p, run.Delivery)
					components.TagPill(c, p, components.CompletenessLabel(run.Completeness), components.ToneNeutral)
					components.TagPill(c, p, components.ShortID(run.ID), components.ToneNeutral)
				})
				ui.Text(c, components.RunSummaryCN(run)).FontSize(12.5).TextColor(p.Text)
				components.DataList(c, p, func() {
					components.KVRow(c, p, "所属容器", s.ContainerDisplayWithPlatform(ref.ID, ref.Label))
					components.KVRow(c, p, "触发与聚合", trigTxt)
					if run.ReasonCode != nil && *run.ReasonCode != "" {
						components.KVRow(c, p, "处理原因", components.ReasonCodeLabel(*run.ReasonCode))
					}
				})

				hasSucceededAttempt, hasMayBeRunning := runFlags(run)
				fb := runFallbackReason(run)
				if runShowAttemptNotDeliveredWarning(run, hasSucceededAttempt) {
					components.AlertBanner(c, p, components.ToneWarn,
						"模型执行成功 ≠ 回复已送达",
						fmt.Sprintf("模型执行尝试已成功完成，但投递状态当前为“%s”（尚未确认回声），切勿将其等同于用户已收到回复。", components.DeliveryLabel(run.Delivery)))
				}
				if hasMayBeRunning {
					if fb != "" && (run.Outcome == "replied" || runIntentionalNoSend(run)) {
						components.AlertBanner(c, p, components.ToneWarn,
							"远端执行可能仍在运行（主引擎降级产生）",
							fmt.Sprintf("%s；本次已由兜底引擎完成处理，但调度器仍记录原主引擎调用处于“可能仍在运行/未知”状态。", components.FallbackReasonLabel(fb)))
					} else {
						components.AlertBanner(c, p, components.ToneDanger,
							"远端执行可能仍在运行",
							"至少有一次尝试上报远端进程处于“可能仍在运行/未知”状态。停止调度器跟踪并不能保证终止外部智能体进程。")
					}
				}
				if run.Delivery == "unknown" {
					components.AlertBanner(c, p, components.ToneDanger,
						"投递状态未知 — 需人工核对（禁止盲目重发）",
						"由于提交前传输异常或响应模糊，系统无法确定消息是否已到达目标平台。为防止重复刷屏，控制台严禁提供一键重发，请先查阅 Bridge 与目标平台记录。")
				}

				ui.Row(c).Wrap().Gap(8).PaddingY(4).Children(func() {
					if run.ConfigSnapshotID != nil {
						if ui.Button(c.Key("btn_view_snap"), "查看历史配置快照").Clicked() {
							s.OpenConfigSnapshot(c, *run.ConfigSnapshotID)
						}
					} else if run.MissingSnapshotReason != nil {
						components.TagPill(c, p, "快照说明: "+components.ControlReasonCN(*run.MissingSnapshotReason), components.ToneNeutral)
					}

					blk := run.Actions.BlockDelivery
					if ui.Button(c.Key("btn_block_delivery"), "拦截发送").Disabled(!blk.Supported || !blk.Enabled || s.Busy).Clicked() {
						s.CancelRun(c, run, "block_delivery")
					}
					stp := run.Actions.StopExecution
					if ui.Button(c.Key("btn_stop_exec"), "停止执行").Disabled(!stp.Supported || !stp.Enabled || s.Busy).Clicked() {
						s.CancelRun(c, run, "stop_execution")
					}
				})
				if !run.Actions.BlockDelivery.Enabled || !run.Actions.StopExecution.Enabled {
					ui.Text(c, fmt.Sprintf("门控约束说明：拦截发送（%s） · 停止执行（%s）",
						nonEmptyOr(components.ControlReasonCN(run.Actions.BlockDelivery.Reason), "当前不可操作"),
						nonEmptyOr(components.ControlReasonCN(run.Actions.StopExecution.Reason), "当前不可操作"))).FontSize(11).TextColor(p.TextMuted)
				}
			})

			// Delivery Evidence Card
			components.CardWithHeader(c, p, "投递证据与回执明细", "严格区分提交回执时间与自身回声确认时间", nil, func() {
				ev := run.DeliveryEvidence
				submittedText := "—"
				if ev.SubmittedAt != nil {
					submittedText = fmt.Sprintf("%s（%s）", components.FormatLocalFullTime(ev.SubmittedAt), components.SubmissionSourceLabel(derefStr(ev.SubmittedAtSource)))
				} else if ev.SubmittedAtSource != nil && *ev.SubmittedAtSource == "echo_inferred" {
					submittedText = "提交回执时间未记录（仅由自身回声反推，绝不伪造时间）"
				}
				confirmedText := components.FormatLocalFullTime(ev.ConfirmedAt)
				submitFlag := "已记录发送提交意图"
				if ev.NoSubmission {
					submitFlag = "明确未发起外发提交"
				}
				components.DataList(c, p, func() {
					components.KVRow(c, p, "提交回执时间", submittedText)
					components.KVRow(c, p, "回声确认时间", confirmedText)
					components.KVRow(c, p, "发送提交判定", submitFlag)
					if ev.FailureEvidence != nil && *ev.FailureEvidence != "" {
						components.KVRow(c, p, "异常证据记录", components.ControlReasonCN(*ev.FailureEvidence))
					}
					if run.Action != nil {
						components.KVRow(c, p, "目标发送动作", fmt.Sprintf("%s · %s",
							components.PlatformLabel(run.Action.Platform), components.ActionKindLabel(run.Action.Kind)))
					}
					if len(run.Inputs) > 0 {
						components.KVRow(c, p, "入站消息源", s.formatGroupedInputsCN(run.Inputs))
					}
				})
			})

			// Attempts
			components.CardWithHeader(c, p, fmt.Sprintf("模型执行尝试记录 (%d)", len(run.Attempts)), "", nil, func() {
				if len(run.Attempts) == 0 {
					msg := "本次运行在进入模型推理前已结束（如门禁拦截或防重过滤）。"
					if strings.HasPrefix(run.Trigger, "direct") {
						msg = "本次为直发投递模式，按设计跳过 AI 模型推理，直接交由发送层执行并追踪回声确认。"
					}
					ui.Text(c, msg).FontSize(12).TextColor(p.TextMuted)
					return
				}
				components.DataList(c, p, func() {
					for idx, att := range run.Attempts {
						ui.Column(c).Key("att_"+att.ID).FillWidth().PaddingY(10).Gap(6).Children(func() {
							ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(8).Children(func() {
								title := fmt.Sprintf("第 %d 次推理 · %s", idx+1, components.BackendDisplayName(att.BackendID))
								if att.ModelLabel != nil && *att.ModelLabel != "" {
									title += " · " + *att.ModelLabel
								}
								ui.Text(c, title).FontSize(12.5).FontWeight(600).TextColor(p.Text)
								ui.Row(c).Gap(6).Children(func() {
									components.StatusPill(c, p, components.AttemptStatusLabel(att.Status), attemptTone(att.Status))
									components.StatusPill(c, p, components.RemoteExecLabel(att.RemoteExecution), remoteTone(att.RemoteExecution))
								})
							})
							components.KVRow(c, p, "执行耗时", fmt.Sprintf("%s（始于 %s）",
								components.FormatDurationBetween(att.StartedAt, att.EndedAt),
								components.FormatLocalClock(att.StartedAt)))
							if att.FallbackReason != nil && strings.TrimSpace(*att.FallbackReason) != "" {
								components.KVRow(c, p, "引擎降级原因", components.FallbackReasonLabel(*att.FallbackReason))
							}
							extInfo := components.ExternalIDLabel(derefStr(att.ExternalID))
							if extInfo == "—" {
								extInfo = components.ControlReasonCN(derefStr(att.MissingExternalIDReason))
							}
							components.KVRow(c, p, "外部任务句柄", extInfo)
						})
					}
				})
			})

			// Timeline
			components.CardWithHeader(c, p, fmt.Sprintf("全链路事件时间线 (%d)", len(detail.Timeline)), "按真实本地时序展示各阶段判定与确证依据", nil, func() {
				if len(detail.Timeline) == 0 {
					ui.Text(c, "暂无时间线明细记录。").FontSize(12).TextColor(p.TextMuted)
					return
				}
				components.DataList(c, p, func() {
					for _, item := range detail.Timeline {
						tone := components.ToneOK
						if item.Certainty == "missing" {
							tone = components.ToneWarn
						} else if item.Certainty == "not_entered" {
							tone = components.ToneNeutral
						}
						ui.Row(c).Key("tl_"+item.ID).FillWidth().PaddingY(10).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
							ui.Column(c).Grow(1).Gap(2).Children(func() {
								head := fmt.Sprintf("%s · %s（%s）",
									components.FormatLocalClock(item.OccurredAt),
									components.TimelineKindLabel(item.Kind),
									components.TimelineSourceLabel(item.Source))
								ui.Text(c, head).FontSize(12).FontWeight(600).FontFeatures("tnum").TextColor(p.Text)
								ui.Text(c, components.TimelineSummaryCN(item)).FontSize(11.5).TextColor(p.TextSecondary)
							})
							components.StatusPill(c, p, components.CertaintyLabel(item.Certainty), tone)
						})
					}
				})
			})

			// Collapsible Technical Identifiers & Debug Tokens
			toggleLabel := "展开技术标识与原始 ID"
			if s.ShowRunTechIDs {
				toggleLabel = "收起技术标识与原始 ID"
			}
			components.CardWithHeader(c, p, "技术标识与调试凭证", "默认折叠长哈希与底层字段，仅排查链路定位时展开", func() {
				if ui.Button(c.Key("toggle_run_tech_ids"), toggleLabel).Clicked() {
					s.ShowRunTechIDs = !s.ShowRunTechIDs
				}
			}, func() {
				if !s.ShowRunTechIDs {
					ui.Text(c, fmt.Sprintf("运行编号 %s · 状态版本 %s · 点击右上按钮查看完整哈希与溯源标识。",
						components.ShortID(run.ID), components.ShortID(run.Revision))).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextMuted)
					return
				}
				ev := run.DeliveryEvidence
				components.DataList(c, p, func() {
					components.KVRow(c, p, "完整 Run ID", run.ID)
					components.KVRow(c, p, "状态 Revision", run.Revision)
					components.KVRow(c, p, "原始容器 ID", ref.ID)
					components.KVRow(c, p, "Bridge 请求 ID", derefStr(ev.BridgeRequestID))
					components.KVRow(c, p, "回声消息 ID", derefStr(ev.EchoMessageID))
					if run.Action != nil {
						components.KVRow(c, p, "动作 Action ID", run.Action.ActionID)
					}
					components.KVRow(c, p, "配置基线指纹", nonEmptyOr(run.Versions.Fingerprint, "—"))
					for _, in := range run.Inputs {
						components.KVRow(c, p, "入站 Envelope ID", fmt.Sprintf("%s (%s)", in.EnvelopeID, in.Source.ContainerID))
					}
					for _, att := range run.Attempts {
						components.KVRow(c, p, "Attempt 策略/路由", fmt.Sprintf("%s · policy=%s · routing=%s",
							att.ID, derefStr(att.Versions.Policy), derefStr(att.Versions.Routing)))
					}
				})
			})
		})
	})
}

func (s *Store) formatGroupedInputsCN(inputs []client.InboundReference) string {
	type item struct {
		key   string
		label string
		count int
	}
	var grouped []item
	for _, in := range inputs {
		plat := components.PlatformLabel(in.Source.Platform)
		name := s.ContainerDisplayName(in.Source.ContainerID, in.Source.ContainerLabel)
		clock := components.FormatLocalClock(in.ReceivedAt)
		key := plat + "|" + name + "|" + clock
		if n := len(grouped); n > 0 && grouped[n-1].key == key {
			grouped[n-1].count++
			continue
		}
		grouped = append(grouped, item{
			key:   key,
			label: fmt.Sprintf("%s · %s（%s）", plat, name, clock),
			count: 1,
		})
	}
	parts := make([]string, 0, len(grouped))
	for _, g := range grouped {
		if g.count > 1 {
			parts = append(parts, strings.TrimSuffix(g.label, "）")+fmt.Sprintf("，共 %d 条）", g.count))
		} else {
			parts = append(parts, g.label)
		}
	}
	return strings.Join(parts, "；")
}

func attemptTone(status string) components.Tone {
	switch status {
	case "succeeded":
		return components.ToneOK
	case "running", "queued":
		return components.ToneInfo
	case "canceled", "cancelled", "timed_out":
		return components.ToneWarn
	case "unknown":
		return components.ToneNeutral
	default:
		return components.ToneDanger
	}
}

func remoteTone(state string) components.Tone {
	switch state {
	case "may_be_running", "unknown":
		return components.ToneDanger
	case "finished":
		return components.ToneOK
	default:
		return components.ToneNeutral
	}
}
