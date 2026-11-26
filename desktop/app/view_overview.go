package app

import (
	"fmt"
	"math"

	"jinx-desktop/components"
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

func (s *Store) renderOverviewView(c *ui.Context, p theme.Palette) {
	ui.Column(c).FillWidth().Gap(20).Children(func() {
		controlsCap := s.Capability("pause")
		if s.Overview != nil {
			controlsCap = s.Overview.Data.Controls
		}
		components.PageHeader(c, p, "系统总览", "实时任务快照与持久化累计漏斗（口径严格分离）", func() {
			ui.Text(c, "控制面门禁").FontSize(12).TextColor(p.TextMuted)
			components.AvailabilityPill(c, p, controlsCap)
			if controlsCap.Reason != "" {
				ui.Text(c, "（"+components.ControlReasonCN(controlsCap.Reason)+"）").FontSize(11.5).TextColor(p.TextMuted)
			}
		})

		if s.Overview == nil {
			components.AlertBanner(c, p, components.ToneWarn, "总览数据暂不可用", "尚未从 JINX 控制面接收到总览快照。")
			return
		}
		ov := s.Overview.Data
		cw := ov.CurrentWork

		// 1. Top 4 Key Metric Cards
		ui.Row(c).FillWidth().Gap(16).AlignItems(ui.Stretch).Children(func() {
			activeVal := "—"
			if cw.Aggregating != nil || cw.Queued != nil || cw.Executing != nil || cw.Submitting != nil || cw.Confirming != nil {
				sum := ptrIntVal(cw.Aggregating) + ptrIntVal(cw.Queued) + ptrIntVal(cw.Executing) + ptrIntVal(cw.Submitting) + ptrIntVal(cw.Confirming)
				activeVal = fmt.Sprintf("%d", sum)
			}
			activeCap := fmt.Sprintf("聚合 %s · 排队 %s · 执行 %s · 提交 %s · 确认 %s",
				components.FormatIntPtr(cw.Aggregating),
				components.FormatIntPtr(cw.Queued),
				components.FormatIntPtr(cw.Executing),
				components.FormatIntPtr(cw.Submitting),
				components.FormatIntPtr(cw.Confirming))
			components.MetricCard(c, p, "当前活跃任务", activeVal, activeCap, components.ToneInfo, false)

			unkCount := ptrIntVal(cw.UnknownDelivery)
			components.MetricCard(c, p, "投递状态未知", components.FormatIntPtr(cw.UnknownDelivery),
				"仅限人工核对现场，禁止盲目重发", components.ToneDanger, unkCount > 0)

			remCount := ptrIntVal(cw.RemoteUnsettled)
			components.MetricCard(c, p, "远端状态未定", components.FormatIntPtr(cw.RemoteUnsettled),
				"外部执行器可能仍在运行", components.ToneWarn, remCount > 0)

			components.MetricCard(c, p, "在途与待处理消息", components.FormatFloatPtr(ov.Stats.PendingMessages),
				"含待聚合及正在发送的消息总数", components.ToneInfo, false)
		})

		// 2. Asymmetric 2:1 Hero Row: Message Processing Funnel Chart + Service Health List
		ui.Row(c).FillWidth().Gap(16).AlignItems(ui.Stretch).Children(func() {
			ui.Column(c).Grow(2).Children(func() {
				components.CardWithHeader(c, p, "消息处理漏斗（累计统计）",
					"持久化累计统计，与顶部实时活跃任务口径严格隔离",
					func() {
						if !ov.StatsAvailable {
							components.StatusPill(c, p, "计数器不可用", components.ToneWarn)
						}
					},
					func() {
						if !ov.StatsAvailable {
							components.AlertBanner(c, p, components.ToneWarn, "累计统计暂不可用", "Dispatcher 统计接口未返回有效计数器数据。")
							return
						}

						received := ptrFloatVal(ov.Stats.TotalReceived)
						triggered := ptrFloatVal(ov.Stats.TotalTriggered)
						submitted := ptrFloatVal(ov.Stats.TotalSubmitted)
						replied := ptrFloatVal(ov.Stats.TotalReplied)
						dryRun := ptrFloatVal(ov.Stats.TotalDryRun)
						failed := ptrFloatVal(ov.Stats.TotalSendFailed)

						ui.Row(c).FillWidth().Gap(20).Wrap().Children(func() {
							summaryMetric(c, p, "已接收", components.FormatFloatPtr(ov.Stats.TotalReceived), p.Text)
							summaryMetric(c, p, "已触发", components.FormatFloatPtr(ov.Stats.TotalTriggered), p.Text)
							summaryMetric(c, p, "已提交", components.FormatFloatPtr(ov.Stats.TotalSubmitted), p.Text)
							summaryMetric(c, p, "已确认回复", components.FormatFloatPtr(ov.Stats.TotalReplied), p.Accent)
							summaryMetric(c, p, "演练模拟", components.FormatFloatPtr(ov.Stats.TotalDryRun), p.Warn)
							summaryMetric(c, p, "发送失败", components.FormatFloatPtr(ov.Stats.TotalSendFailed), p.Danger)
						})

						maxV := 1.0
						for _, v := range []float64{received, triggered, submitted, replied, dryRun, failed} {
							if v > maxV {
								maxV = v
							}
						}
						stages := []struct {
							label string
							val   float64
							pct   string
							color ui.Color
						}{
							{"已接收", received, "入站基准 100%", p.ChartNeutral},
							{"已触发", triggered, formatRateBadge("触发率", triggered, received), p.Info.Alpha(0.55)},
							{"已提交", submitted, formatRateBadge("提交率", submitted, triggered), p.Info},
							{"已回复", replied, formatRateBadge("确认率", replied, submitted), p.OK},
							{"演练", dryRun, formatRateBadge("占触发", dryRun, triggered), p.Warn},
							{"失败", failed, formatRateBadge("失败率", failed, submitted+failed), p.Danger},
						}

						ui.Column(c).FillWidth().Gap(9).Padding(6, 0, 2, 0).Children(func() {
							for _, st := range stages {
								st := st
								fillPct := float32(0)
								if st.val > 0 {
									fillPct = float32(math.Sqrt(st.val/maxV) * 100)
									if fillPct < 4 {
										fillPct = 4
									}
									if fillPct > 100 {
										fillPct = 100
									}
								}
								ui.Row(c).FillWidth().Gap(12).AlignItems(ui.Center).Children(func() {
									ui.Text(c, st.label).Width(64).FontSize(12).FontWeight(500).TextColor(p.TextSecondary).NoWrap()
									ui.Box(c).Grow(1).Height(12).Radius(6).Background(p.SubtleBg).Border(1, p.Border).Children(func() {
										if fillPct > 0 {
											ui.Box(c).Height(10).WidthPercent(fillPct).Radius(5).Background(st.color)
										}
									})
									ui.Row(c).Width(156).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(8).Shrink(0).Children(func() {
										ui.Text(c, fmt.Sprintf("%.0f", st.val)).FontSize(12.5).FontWeight(600).FontFeatures("tnum").TextColor(p.Text).NoWrap()
										ui.Text(c, st.pct).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).NoWrap()
									})
								})
							}
						})
					})
			})

			ui.Column(c).Grow(1).Children(func() {
				svcs := s.Services
				if len(svcs) == 0 {
					svcs = ov.Services
				}
				components.CardWithHeader(c, p, "核心服务健康", fmt.Sprintf("共 %d 个托管与依赖组件", len(svcs)), nil, func() {
					if len(svcs) == 0 {
						ui.Text(c, "暂无服务探针上报数据。").FontSize(12).TextColor(p.TextMuted)
						return
					}
					components.DataList(c, p, func() {
						for i, svc := range svcs {
							svc := svc
							row := ui.Row(c).Key(i).FillWidth().Padding(10, 8).Radius(8).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(8)
							if row.Hovered() {
								row.Background(p.RaisedBg)
							}
							row.Cursor(ui.CursorPointer).Role(ui.RoleButton).Label("查看服务 " + svc.ID).Children(func() {
								ui.Column(c).Grow(1).Gap(2).Children(func() {
									ui.Text(c, components.ServiceDisplayName(svc.ID)).FontSize(12.5).FontWeight(500).TextColor(p.Text).SingleLine()
									sub := fmt.Sprintf("探测延迟 %d ms", svc.HealthLatencyMS)
									if svc.ID == "bridge" && svc.Health == "unknown" && svc.Facts["accessibility_trusted"] == true && svc.Facts["screen_capture_authorized"] == true {
										sub += " · 权限已就绪（辅助功能/屏幕捕获）"
									}
									ui.Text(c, sub).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).SingleLine()
								})
								ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
									components.HealthPill(c, p, svc.Health)
									ui.Icon(c, components.Icons().ChevronRight).FontSize(13).TextColor(p.TextMuted)
								})
							})
							if row.Clicked() {
								s.ActiveView = ViewServices
							}
						}
					})
				})
			})
		})

		// 3. Asymmetric 2:1 Bottom Row: Spacious Recent Runs Feed + Attention Queue
		ui.Row(c).FillWidth().Gap(16).AlignItems(ui.Start).Children(func() {
			ui.Column(c).Grow(2).Children(func() {
				components.CardWithHeader(c, p, "最近运行记录", "点击任意运行卡片可直达详情抽屉", nil, func() {
					if len(ov.RecentRuns) == 0 {
						ui.Text(c, "暂无可追踪的运行记录。").FontSize(12).TextColor(p.TextMuted)
						return
					}
					limit := len(ov.RecentRuns)
					if limit > 8 {
						limit = 8
					}
					ui.Column(c).FillWidth().Gap(8).Children(func() {
						for idx, run := range ov.RecentRuns[:limit] {
							idx, run := idx, run
							ref := runContainerRef(run)
							containerName := s.ContainerDisplayName(ref.ID, ref.Label)
							plat := s.ContainerPlatform(ref.ID)
							msgCount := runMessageCount(run)
							accLabel := "查看总览运行 " + components.ShortID(run.ID)
							fb := runFallbackReason(run)

							if runIsQuiet(run) {
								row := ui.Row(c).Key(run.ID).FillWidth().Padding(8, 10).Radius(8).Background(p.SubtleBg).Border(1, p.Border).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(8)
								if row.Hovered() {
									row.Background(p.RaisedBg).Border(1, p.BorderStrong)
								}
								rc := ""
								if run.ReasonCode != nil {
									rc = *run.ReasonCode
								}
								quietTag := "已拦截"
								if run.Outcome == "silent" {
									quietTag = "静默"
								}
								row.Cursor(ui.CursorPointer).Role(ui.RoleButton).Label(accLabel).Children(func() {
									ui.Row(c).Grow(1).Gap(6).AlignItems(ui.Center).Children(func() {
										components.TagPill(c, p, quietTag, components.ToneNeutral)
										components.TagPill(c, p, plat, components.ToneInfo)
										ui.Text(c, containerName).FontSize(12.5).FontWeight(500).TextColor(p.Text).SingleLine()
										ui.Text(c, components.ShortReasonLabel(rc)).FontSize(11.5).TextColor(p.TextMuted).SingleLine()
										if fb != "" {
											components.TagPill(c, p, components.ShortFallbackTag(fb), components.ToneWarn)
										}
									})
									ui.Row(c).Gap(6).AlignItems(ui.Center).Shrink(0).Children(func() {
										ui.Text(c, components.FormatLocalClock(run.AcceptedAt)).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).NoWrap()
										ui.Icon(c, components.Icons().ChevronRight).FontSize(13).TextColor(p.TextMuted)
									})
								})
								if row.Clicked() {
									s.ActiveView = ViewRuns
									s.SelectedRunIdx = idx
									s.OpenRunDrawer(run.ID)
								}
								continue
							}

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

							card := ui.Row(c).Key(run.ID).FillWidth().Padding(10, 12, 10, 10).Radius(9).Background(bg).Border(1, border).Gap(10).AlignItems(ui.Stretch)
							if card.Hovered() {
								card.Border(1, accent.Alpha(0.65))
							}
							card.Cursor(ui.CursorPointer).Role(ui.RoleButton).Label(accLabel).Children(func() {
								ui.Box(c).Width(3).Radius(2).Background(accent)
								ui.Column(c).Grow(1).Gap(5).Children(func() {
									ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(6).Children(func() {
										ui.Row(c).Grow(1).Gap(6).AlignItems(ui.Center).Children(func() {
											ui.Icon(c, leadIcon).FontSize(14).TextColor(accent)
											components.TagPill(c, p, plat, components.ToneInfo)
											ui.Text(c, containerName).FontSize(13).FontWeight(600).TextColor(p.Text).SingleLine()
											components.StatusPill(c, p, components.RunShortStatusCN(run), verdictTone)
										})
										ui.Row(c).Gap(6).AlignItems(ui.Center).Shrink(0).Children(func() {
											ui.Text(c, components.FormatLocalClock(run.AcceptedAt)).FontSize(11).FontFeatures("tnum").TextColor(p.TextSecondary).NoWrap()
											ui.Icon(c, components.Icons().ChevronRight).FontSize(13).TextColor(p.TextSecondary)
										})
									})
									ui.Text(c, components.RunSummaryCN(run)).FontSize(12).TextColor(p.Text).SingleLine()
									ui.Row(c).Wrap().Gap(5).AlignItems(ui.Center).Children(func() {
										components.OutcomePill(c, p, run.Outcome)
										components.DeliveryPill(c, p, run.Delivery)
										if fb != "" {
											components.TagPill(c, p, components.ShortFallbackTag(fb), components.ToneWarn)
										}
										trigTxt := components.TriggerLabel(run.Trigger)
										if msgCount > 0 {
											trigTxt += fmt.Sprintf(" · %d 条", msgCount)
										}
										ui.Text(c, trigTxt+" · 耗时 "+runEndClockTime(run)).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).SingleLine()
									})
								})
							})
							if card.Clicked() {
								s.ActiveView = ViewRuns
								s.SelectedRunIdx = idx
								s.OpenRunDrawer(run.ID)
							}
						}
					})
				})
			})

			ui.Column(c).Grow(1).Children(func() {
				ackCap := s.Capability("attention.acknowledge")
				components.CardWithHeader(c, p, "待处理关注事项", fmt.Sprintf("当前队列共 %d 项", len(ov.Attention)), nil, func() {
					if len(ov.Attention) == 0 {
						ui.Text(c, "当前无待处理的异常或人工确认事项。").FontSize(12).TextColor(p.TextMuted)
						return
					}
					components.DataList(c, p, func() {
						for i, item := range ov.Attention {
							item := item
							ui.Row(c).Key(item.ID).FillWidth().PaddingY(10).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
								ui.Column(c).Grow(1).Gap(4).Children(func() {
									ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
										components.StatusPill(c, p, components.AttentionCodeLabel(item.Code), components.ToneWarn)
										if item.Target != nil && item.Target.Kind == "group" {
											components.TagPill(c, p, s.ContainerDisplayName(item.Target.ID, ""), components.ToneInfo)
										}
										if item.Acknowledged {
											components.StatusPill(c, p, "已确认", components.ToneOK)
										}
										ui.Text(c, components.FormatLocalClock(&item.OccurredAt)).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted)
									})
									ui.Text(c, components.ControlReasonCN(item.Summary)).FontSize(12).TextColor(p.TextSecondary)
								})
								btnLabel := fmt.Sprintf("确认 %d", i+1)
								if item.Acknowledged {
									btnLabel = "已确认"
								}
								if ui.Button(c.Key("ack_"+item.ID), btnLabel).Disabled(item.Acknowledged || !ackCap.Supported || !ackCap.Enabled || s.Busy).Clicked() {
									s.AcknowledgeAttention(c, item)
								}
							})
						}
					})
				})
			})
		})
	})
}

func formatRateBadge(label string, part, total float64) string {
	if total <= 0 {
		return label + " —"
	}
	pct := (part / total) * 100
	if pct > 100 {
		pct = 100
	}
	return fmt.Sprintf("%s %.1f%%", label, pct)
}

func summaryMetric(c *ui.Context, p theme.Palette, label, val string, valColor ui.Color) {
	ui.Row(c).Gap(6).AlignItems(ui.Baseline).Children(func() {
		ui.Text(c, label).FontSize(11.5).TextColor(p.TextMuted)
		ui.Text(c, val).FontSize(14).FontWeight(600).FontFeatures("tnum").TextColor(valColor)
	})
}

func ptrIntVal(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

func ptrFloatVal(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}
