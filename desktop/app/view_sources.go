package app

import (
	"fmt"
	"slices"
	"strings"

	"jinx-desktop/components"
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

func (s *Store) renderSourcesView(c *ui.Context, p theme.Palette) {
	ui.Column(c).FillWidth().Gap(20).Children(func() {
		revTxt := "等待架构投影加载"
		if s.Arch != nil {
			revTxt = fmt.Sprintf("三层信息来源 → 处理编排 → 回复动作拓扑 · 最近观测 %s · 版本 %s",
				components.FormatLocalClock(&s.Arch.ObservedAt), components.ShortID(s.Arch.Revision))
		}
		components.PageHeader(c, p, "来源与动作架构", revTxt, nil)

		if s.LastDryRunResult != nil {
			res := s.LastDryRunResult
			components.AlertBanner(c, p, components.ToneOK,
				fmt.Sprintf("%s连接器演练模式已切换（%v → %v）", components.PlatformLabel(res.Platform), res.PreviousDryRun, res.DryRun),
				fmt.Sprintf("回读校验通过：%v | 持久化：%v（仅当前进程有效，重启后恢复配置文件默认值）", res.ReadbackVerified, res.Persisted))
		}

		// Layer 1 & Layer 2
		if s.Arch == nil {
			components.AlertBanner(c, p, components.ToneWarn, "架构投影不可用", "未能获取 /console/v1/architecture 接口响应。")
		} else {
			ui.Row(c).FillWidth().Gap(16).AlignItems(ui.Stretch).Children(func() {
				ui.Column(c).Grow(1).Children(func() {
					components.CardWithHeader(c, p, "第一层：信息来源矩阵", "各平台消息采集通道状态与契约支持能力", nil, func() {
						components.DataList(c, p, func() {
							for _, src := range s.Arch.Sources {
								ui.Column(c).Key("src_"+src.Platform).FillWidth().PaddingY(10).Gap(6).Children(func() {
									ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Children(func() {
										ui.Text(c, components.PlatformLabel(src.Platform)).FontSize(13).FontWeight(600).TextColor(p.Text)
										components.HealthPill(c, p, src.State)
									})
									if src.AccountID != nil {
										components.KVRow(c, p, "采集账号", *src.AccountID)
									}
									if len(src.ContainerIDs) > 0 {
										names := make([]string, 0, len(src.ContainerIDs))
										for _, cid := range src.ContainerIDs {
											names = append(names, s.ContainerDisplayName(cid, ""))
										}
										components.KVRow(c, p, fmt.Sprintf("监听容器 (%d)", len(names)), strings.Join(names, "、"))
									}
									ui.Row(c).Wrap().Gap(6).Children(func() {
										for _, capItem := range src.Capabilities {
											tone := components.ToneOK
											if !capItem.Supported {
												tone = components.ToneNeutral
											} else if capItem.Reason != "" {
												tone = components.ToneWarn
											}
											label := components.CapabilityLabel(capItem.ID)
											if !capItem.Supported {
												label += "（未接入）"
											} else if capItem.Reason != "" {
												label += "（" + components.ReasonLabel(capItem.Reason) + "）"
											}
											components.StatusPill(c, p, label, tone)
										}
									})
								})
							}
						})
					})
				})

				ui.Column(c).Grow(1).Children(func() {
					components.CardWithHeader(c, p, "第二层：处理后端与模型目录", "智能体执行引擎及可用推理模型边界", nil, func() {
						components.DataList(c, p, func() {
							for _, bk := range s.Arch.Processing.Backends {
								ui.Column(c).Key("pbk_"+bk.ID).FillWidth().PaddingY(10).Gap(6).Children(func() {
									ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(8).Children(func() {
										ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
											ui.Text(c, components.BackendDisplayName(bk.ID)).FontSize(13).FontWeight(600).TextColor(p.Text)
											components.TagPill(c, p, components.BackendKindLabel(bk.Kind), components.ToneNeutral)
										})
										if bk.Available {
											components.HealthPill(c, p, "ready")
										} else {
											components.HealthPill(c, p, "not_ready")
										}
									})
									components.KVRow(c, p, "隔离绑定策略", components.BindingLabel(bk.AgentBinding))
									modelsTxt := strings.Join(bk.ModelIDs, "、")
									if len(bk.ModelIDs) == 0 {
										modelsTxt = "由下游智能体配置决定"
									}
									components.KVRow(c, p, "挂载模型列表", modelsTxt)
								})
							}
						})
					})
				})
			})
		}

		// Platform Connectors & Confirmed dry_run Control (Prominent above the fold)
		components.CardWithHeader(c, p, "平台连接器与确认式演练开关", "切换实时演练模式（dry_run）需先计划预览并二次勾选确认", nil, func() {
			if len(s.Connectors) == 0 {
				ui.Text(c, "当前控制面未配置外部连接器（可通过 -bilibili-connector / -web-connector 参数接入）。").FontSize(12).TextColor(p.TextMuted)
				return
			}
			components.DataList(c, p, func() {
				for _, conn := range s.Connectors {
					conn := conn
					ui.Column(c).Key("conn_"+conn.Platform).FillWidth().PaddingY(12).Gap(8).Children(func() {
						ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
							ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
								ui.Text(c, components.PlatformLabel(conn.Platform)+"连接器").FontSize(13.5).FontWeight(600).TextColor(p.Text)
								components.HealthPill(c, p, conn.State)
								if conn.DryRun != nil {
									tone := components.ToneWarn
									lbl := "演练模式开启（安全拦截外发）"
									if !*conn.DryRun {
										tone = components.ToneInfo
										lbl = "实盘外发模式（dry_run 关闭）"
									}
									components.StatusPill(c, p, lbl, tone)
								}
							})
							canToggle := conn.Configured && conn.State != "unsupported" && conn.State != "unreachable" &&
								conn.DryRun != nil && slices.Contains(conn.Controls, "dry_run")
							btnText := fmt.Sprintf("切换演练模式 (%s)", components.PlatformLabel(conn.Platform))
							if ui.Button(c.Key("toggle_dry_run_"+conn.Platform), btnText).Disabled(!canToggle || s.Busy).Clicked() {
								s.PlanConnectorDryRun(c, conn)
							}
						})
						if conn.ConnectorName != nil {
							components.KVRow(c, p, "连接器 / 账号", fmt.Sprintf("%s  ·  %s", *conn.ConnectorName, derefStr(conn.AccountID)))
						}
						caps := make([]string, 0, len(conn.Capabilities))
						for _, cp := range conn.Capabilities {
							caps = append(caps, components.ActionKindLabel(cp))
						}
						ctrls := make([]string, 0, len(conn.Controls))
						for _, ct := range conn.Controls {
							ctrls = append(ctrls, components.CapabilityLabel(ct))
						}
						components.KVRow(c, p, "动作支持 / 控制项", fmt.Sprintf("支持动作: %s  ·  支持控制: %s", nonEmptyOr(strings.Join(caps, "、"), "无"), nonEmptyOr(strings.Join(ctrls, "、"), "无")))
						if conn.LastPollAt != nil {
							pollStatus := "轮询正常"
							if conn.LastPollErrorCode != nil && *conn.LastPollErrorCode != "" {
								pollStatus = "异常: " + components.ControlReasonCN(*conn.LastPollErrorCode)
							}
							components.KVRow(c, p, "最近轮询时间", fmt.Sprintf("%s（%s）", components.FormatLocalClock(conn.LastPollAt), pollStatus))
						} else if conn.LastPollErrorCode != nil || conn.Reason != nil {
							components.KVRow(c, p, "异常或未启用原因", fmt.Sprintf("状态: %s · 说明: %s",
								components.ControlReasonCN(derefStr(conn.LastPollErrorCode)),
								components.ControlReasonCN(derefStr(conn.Reason))))
						}
						if len(conn.Targets) > 0 {
							ui.Row(c).FillWidth().Gap(12).PaddingY(4).AlignItems(ui.Start).Children(func() {
								ui.Text(c, fmt.Sprintf("监听目标 (%d)", len(conn.Targets))).Width(124).FontSize(12).TextColor(p.TextMuted)
								ui.Row(c).Grow(1).Wrap().Gap(6).AlignItems(ui.Center).Children(func() {
									for _, t := range conn.Targets {
										lbl := s.ContainerDisplayName(t.ContainerID, derefStr(t.Label))
										stateTxt := "已禁用"
										tone := components.ToneNeutral
										if t.Enabled {
											stateTxt = "已启用"
											tone = components.ToneOK
										}
										components.TagPill(c, p, fmt.Sprintf("%s · %s", lbl, stateTxt), tone)
									}
								})
							})
						}
					})
				}
			})
		})

		if s.Arch != nil {
			// Layer 3: Actions Capability Matrix
			components.CardWithHeader(c, p, "第三层：平台动作能力矩阵", "如实展示发送、持久准入、回声确认与契约适配器就绪情况", nil, func() {
				components.DataList(c, p, func() {
					for _, act := range s.Arch.Actions {
						ui.Row(c).Key(fmt.Sprintf("%s_%s", act.Platform, act.Kind)).FillWidth().PaddingY(8).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
							ui.Row(c).Gap(10).AlignItems(ui.Center).Children(func() {
								ui.Text(c, components.PlatformLabel(act.Platform)).Width(72).FontSize(12).FontWeight(600).TextColor(p.Text)
								ui.Text(c, components.ActionKindLabel(act.Kind)).Width(96).FontSize(12).TextColor(p.TextSecondary)
								components.HealthPill(c, p, act.State)
							})
							ui.Row(c).Wrap().Gap(6).Children(func() {
								for _, capItem := range act.Capabilities {
									tone := components.ToneNeutral
									if capItem.Supported && capItem.Reason == "" {
										tone = components.ToneOK
									} else if capItem.Supported && capItem.Reason != "" {
										tone = components.ToneWarn
									}
									components.TagPill(c, p, components.CapabilityLabel(capItem.ID), tone)
								}
							})
						})
					}
				})
			})

			// EventStreams Boundary Card
			components.CardWithHeader(c, p, "架构事件流边界（SSE 域支持状态）", "未持久化流保持为暂不支持，不伪造空事件流", nil, func() {
				ui.Row(c).FillWidth().Gap(12).Children(func() {
					for _, st := range s.Arch.EventStreams {
						st := st
						tone := components.ToneOK
						statusLabel := "已支持"
						if !st.Supported {
							tone = components.ToneNeutral
							statusLabel = "暂不支持"
						}
						ui.Column(c).Key("stream_"+st.Domain).Grow(1).Padding(12).Gap(6).Radius(8).Background(p.SubtleBg).Border(1, p.Border).Children(func() {
							ui.Row(c).Justify(ui.SpaceBetween).AlignItems(ui.Center).FillWidth().Children(func() {
								ui.Text(c, components.StreamDomainLabel(st.Domain)).FontSize(12).FontWeight(600).TextColor(p.Text)
								components.StatusPill(c, p, statusLabel, tone)
							})
							streamName := "未建立事件流"
							if st.StreamID != nil && *st.StreamID != "" {
								streamName = components.StreamIDLabel(*st.StreamID)
							}
							ui.Text(c, fmt.Sprintf("事件流: %s  ·  最新序号: %s", streamName, components.FormatUint64Ptr(st.LastSeq))).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted)
						})
					}
				})
			})
		}
	})
}

func derefStr(p *string) string {
	if p == nil || *p == "" {
		return "—"
	}
	return *p
}
