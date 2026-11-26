package app

import (
	"fmt"
	"strings"

	"jinx-desktop/client"
	"jinx-desktop/components"
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

func (s *Store) renderContainersView(c *ui.Context, p theme.Palette) {
	ui.Column(c).FillWidth().Gap(20).Children(func() {
		components.PageHeader(c, p, "容器、场景与规则编排", "按群聊 / 频道 / 帖子容器独立管理准入、三态过滤、逻辑挂载与四级权限", func() {
			if s.LastOperation != nil {
				components.StatusPill(c, p, "最近发布: "+s.LastOperation.ChangeID+" ("+components.HealthLabel(s.LastOperation.Status)+")", components.ToneOK)
			}
		})

		ui.Row(c).FillWidth().Gap(16).AlignItems(ui.Start).Children(func() {
			// Left: Container list with quiet scannable status hierarchy
			ui.Column(c).Width(296).Gap(10).Children(func() {
				components.CardWithHeader(c, p, fmt.Sprintf("容器列表 (%d)", len(s.Groups)), "点击卡片切换查看或编辑规则", nil, func() {
					if len(s.Groups) == 0 {
						ui.Text(c, "控制面未返回容器配置。").FontSize(12).TextColor(p.TextMuted)
						return
					}
					ui.Column(c).FillWidth().Gap(8).Children(func() {
						for i, g := range s.Groups {
							i, g := i, g
							selected := i == s.SelectedGroupIdx
							bg := p.SubtleBg
							border := p.Border
							strip := p.BorderStrong
							if selected {
								bg = p.AccentSoft
								border = p.Accent
								strip = p.Accent
							}
							name := s.ContainerDisplayName(g.ID, g.Group.Label)
							plat := s.ContainerPlatform(g.ID)
							modeBadge := "实盘中"
							modeTone := components.ToneOK
							if !g.Enabled {
								modeBadge = "已停用"
								modeTone = components.ToneNeutral
							} else if g.EffectiveDryRun {
								modeBadge = "演练中"
								modeTone = components.ToneWarn
							}
							routeMark := "✓"
							if !g.RouteExists {
								routeMark = "✗"
							}
							wlMark := "✓"
							if !g.Whitelisted {
								wlMark = "✗"
							}
							metaLine := fmt.Sprintf("路由%s · 白名单%s · %s", routeMark, wlMark, components.ShortContainerID(g.ID))

							card := ui.Row(c).Key("grp_card_"+g.ID).FillWidth().Padding(10, 12, 10, 10).Radius(8).Background(bg).Border(1, border).Gap(10).AlignItems(ui.Stretch)
							if card.Hovered() && !selected {
								card.Background(p.RaisedBg).Border(1, p.BorderStrong)
							}
							card.Cursor(ui.CursorPointer).Role(ui.RoleButton).Label("选择容器 " + g.ID).Children(func() {
								ui.Box(c).Width(3).Radius(2).Background(strip)
								ui.Column(c).Grow(1).Gap(5).Children(func() {
									ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(6).Children(func() {
										ui.Row(c).Grow(1).Gap(6).AlignItems(ui.Center).Children(func() {
											components.TagPill(c, p, plat, components.ToneInfo)
											ui.Text(c, name).FontSize(13).FontWeight(600).TextColor(p.Text).SingleLine()
										})
										ui.Icon(c, components.Icons().ChevronRight).FontSize(14).TextColor(p.TextMuted)
									})
									ui.Row(c).FillWidth().Gap(6).AlignItems(ui.Center).Children(func() {
										components.StatusPill(c, p, modeBadge, modeTone)
										if !g.RouteExists {
											components.TagPill(c, p, "无路由", components.ToneDanger)
										}
										if !g.Whitelisted {
											components.TagPill(c, p, "未加白", components.ToneWarn)
										}
										ui.Text(c, metaLine).FontSize(11).FontFeatures("tnum").TextColor(p.TextMuted).SingleLine()
									})
								})
							})
							if card.Clicked() {
								s.SelectGroupByIndex(i, c)
							}
						}
					})
				})
			})

			// Right: 5-tab Rule, Capability & Passthrough Forwarding Editor
			ui.Column(c).Grow(1).Gap(12).Children(func() {
				g, ok := s.SelectedGroup()

				ui.Tabs(c.Key("container_tabs"), &s.ContainerTab, "基础与过滤", "场景与挂载", "路由与权限", "全局与中继规则", "直通转发")

				switch s.ContainerTab {
				case 0:
					if !ok {
						components.AlertBanner(c, p, components.ToneInfo, "请选择容器", "从左侧列表选择微信群、B站视频或网页帖子容器开始查看或编辑规则。")
						return
					}
					s.renderContainerBasicTab(c, p, g)
				case 1:
					if !ok {
						components.AlertBanner(c, p, components.ToneInfo, "请选择容器", "从左侧列表选择微信群、B站视频或网页帖子容器开始查看或编辑规则。")
						return
					}
					s.renderContainerScenarioTab(c, p, g)
				case 2:
					if !ok {
						components.AlertBanner(c, p, components.ToneInfo, "请选择容器", "从左侧列表选择微信群、B站视频或网页帖子容器开始查看或编辑规则。")
						return
					}
					s.renderContainerRoutingTab(c, p, g)
				case 3:
					s.renderGlobalRelayTab(c, p)
				case 4:
					s.renderForwardRulesTab(c, p)
				}
			})
		})
	})
}

func fmtBoolCN(prefix string, v bool) string {
	if v {
		return prefix + ": 是"
	}
	return prefix + ": 否"
}

func boolTone(v bool) components.Tone {
	if v {
		return components.ToneOK
	}
	return components.ToneNeutral
}

func fieldEditable(g client.GroupView, name string) (bool, string) {
	f, ok := g.Fields[name]
	if !ok {
		return true, ""
	}
	return f.Editable, f.Reason
}

func (s *Store) renderContainerBasicTab(c *ui.Context, p theme.Palette, g client.GroupView) {
	containerName := s.ContainerDisplayName(g.ID, g.Group.Label)
	plat := s.ContainerPlatform(g.ID)
	components.CardWithHeader(c, p,
		fmt.Sprintf("基础参数与三态过滤 — %s（%s）", containerName, plat),
		fmt.Sprintf("标识: %s  ·  配置版本 %s", components.ShortContainerID(g.ID), components.ShortID(g.Revision)),
		func() {
			if ui.PrimaryButton(c.Key("plan_basic_edits"), "预览并对比差异").Disabled(s.Busy).Clicked() {
				s.PlanGroupDraft(c)
			}
		},
		func() {
			ui.Row(c).Wrap().Gap(6).AlignItems(ui.Center).Children(func() {
				components.TagPill(c, p, fmtBoolCN("路由", g.RouteExists), boolTone(g.RouteExists))
				components.TagPill(c, p, fmtBoolCN("启用", g.Enabled), boolTone(g.Enabled))
				components.TagPill(c, p, fmtBoolCN("白名单", g.Whitelisted), boolTone(g.Whitelisted))
				components.TagPill(c, p, components.LabelSourceLabel(g.LabelSource), components.ToneInfo)
				components.TagPill(c, p, components.VisibilityLabel(g.Visibility), components.ToneNeutral)
				if s.Draft.Dirty {
					components.StatusPill(c, p, "存在未提交草稿", components.ToneWarn)
				}
			})

			ui.Row(c).Wrap().Gap(20).PaddingY(4).Children(func() {
				enEdit, _ := fieldEditable(g, "enabled")
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Text(c, "启用容器").FontSize(12.5).TextColor(p.Text)
					if ui.Switch(c.Key("sw_enabled"), &s.Draft.Enabled).Disabled(!enEdit).Changed() {
						s.Draft.Dirty = true
					}
				})
				drEdit, _ := fieldEditable(g, "dry_run")
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Text(c, "演练模式").FontSize(12.5).TextColor(p.Text)
					if ui.Switch(c.Key("sw_dry_run"), &s.Draft.DryRun).Disabled(!drEdit).Changed() {
						s.Draft.Dirty = true
					}
				})
				moEdit, _ := fieldEditable(g, "mention_only")
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Text(c, "仅 @ 提及触发").FontSize(12.5).TextColor(p.Text)
					if ui.Switch(c.Key("sw_mention_only"), &s.Draft.MentionOnly).Disabled(!moEdit).Changed() {
						s.Draft.Dirty = true
					}
				})
			})

			ui.Row(c).FillWidth().Gap(12).Children(func() {
				ui.Column(c).Grow(1).Gap(4).Children(func() {
					ui.Text(c, "最大回复字数").FontSize(11.5).TextColor(p.TextMuted)
					if ui.TextInput(c.Key("in_max_runes"), &s.Draft.MaxReplyRunes).FillWidth().Changed() {
						s.Draft.Dirty = true
					}
				})
				ui.Column(c).Grow(1).Gap(4).Children(func() {
					ui.Text(c, "聚合批次条数").FontSize(11.5).TextColor(p.TextMuted)
					if ui.TextInput(c.Key("in_batch_size"), &s.Draft.BatchSize).FillWidth().Changed() {
						s.Draft.Dirty = true
					}
				})
				ui.Column(c).Grow(1).Gap(4).Children(func() {
					ui.Text(c, "触发决策阈值").FontSize(11.5).TextColor(p.TextMuted)
					if ui.TextInput(c.Key("in_threshold"), &s.Draft.DecisionThreshold).FillWidth().Changed() {
						s.Draft.Dirty = true
					}
				})
			})

			hpEdit, hpReason := fieldEditable(g, "hermes_profile")
			smEdit, smReason := fieldEditable(g, "support_mode")
			if !hpEdit || !smEdit {
				smText := "未开启"
				if b, ok := g.Fields["support_mode"].Effective.(bool); ok && b {
					smText = "已开启"
				}
				hpText := "由本地私有策略托管（已脱敏）"
				if sVal, ok := g.Fields["hermes_profile"].Effective.(string); ok && strings.TrimSpace(sVal) != "" {
					hpText = sVal
				}
				components.AlertBanner(c, p, components.ToneInfo, "受保护的本地私有策略绑定（只读）",
					fmt.Sprintf("客服隔离模式：%s（%s） · 智能体身份配置：%s（%s）",
						smText, nonEmptyOr(components.ControlReasonCN(smReason), "由本地私有策略控制"),
						hpText, nonEmptyOr(components.ControlReasonCN(hpReason), "已脱敏屏蔽")))
			}

			if components.TriStateStringEditor(c, p, "self_ids", "自身账号白名单",
				&s.Draft.SelfSenderIDs, &s.Draft.SelfSenderQuery, nil, true,
				"“继承默认”沿用全局自身账号列表；“显式置空”明确禁用本地自身过滤。") {
				s.Draft.Dirty = true
			}

			ui.Column(c).FillWidth().Gap(8).Padding(12).Radius(8).Background(p.SubtleBg).Border(1, p.Border).Children(func() {
				ui.Text(c, "允许接收的消息类型编号").FontSize(12.5).FontWeight(600).TextColor(p.Text)
				ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
					if components.SegmentedControl(c, p, "allowed_types_mode", &s.Draft.AllowedTypesMode, false, "继承默认", "显式置空", "自定义列表").Changed() {
						s.Draft.Dirty = true
					}
				})
				if s.Draft.AllowedTypesMode == 2 {
					if ui.TextInput(c.Key("allowed_types_text"), &s.Draft.AllowedTypesText).Placeholder("例如: 1, 49, 81").FillWidth().Changed() {
						s.Draft.Dirty = true
					}
				} else {
					summary := "当前生效语义：继承默认消息类型集合（JSON null）"
					if s.Draft.AllowedTypesMode == 1 {
						summary = "当前生效语义：显式置空允许列表（JSON []）"
					}
					ui.Text(c, summary).FontSize(11.5).TextColor(p.TextMuted)
				}
			})

			if components.TriStateStringEditor(c, p, "blocked_ids", "发送者屏蔽名单",
				&s.Draft.BlockedSenderIDs, &s.Draft.BlockedQuery, nil, true, "") {
				s.Draft.Dirty = true
			}
			if components.TriStateStringEditor(c, p, "kw_block", "屏蔽关键词",
				&s.Draft.KeywordBlocklist, &s.Draft.KeywordQuery, nil, true, "") {
				s.Draft.Dirty = true
			}
			if components.TriStateStringEditor(c, p, "rx_block", "屏蔽正则表达式",
				&s.Draft.RegexBlocklist, &s.Draft.RegexQuery, nil, true, "") {
				s.Draft.Dirty = true
			}
		})
}

func (s *Store) renderContainerScenarioTab(c *ui.Context, p theme.Palette, g client.GroupView) {
	containerName := s.ContainerDisplayName(g.ID, g.Group.Label)
	components.CardWithHeader(c, p,
		fmt.Sprintf("场景模板与逻辑挂载 — %s", containerName),
		"仅保存脱敏后的逻辑标识，敏感提示词与知识原文仍由私有策略外置管理",
		func() {
			if ui.PrimaryButton(c.Key("plan_mount_edits"), "预览挂载变更").Disabled(s.Busy).Clicked() {
				s.PlanGroupDraft(c)
			}
		},
		func() {
			catCap := s.Capability("catalog")
			ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
				ui.Text(c, "逻辑目录状态").FontSize(12).TextColor(p.TextMuted)
				components.AvailabilityPill(c, p, catCap)
			})

			components.AlertBanner(c, p, components.ToneInfo, "目录规范化与安全隔离说明",
				"技能、工具、知识库及推广词列表上限为 16 个逻辑标识；显式置空将在调度器回读时规范化为继承默认。受限管理工具在客服隔离模式下会被强制拦截。")

			ui.Column(c).FillWidth().Gap(4).Children(func() {
				ui.Text(c, "场景标识（来自逻辑目录）").FontSize(11.5).TextColor(p.TextMuted)
				opts := s.ScenarioOptions(s.Draft.Scenario)
				if ui.Select(c.Key("sel_scenario"), &s.Draft.Scenario, opts).Changed() {
					s.Draft.Dirty = true
				}
			})

			ui.Row(c).FillWidth().Gap(12).Children(func() {
				ui.Column(c).Grow(1).Gap(4).Children(func() {
					ui.Text(c, "回复目标").FontSize(11.5).TextColor(p.TextMuted)
					if ui.TextInput(c.Key("in_response_goal"), &s.Draft.ResponseGoal).FillWidth().Changed() {
						s.Draft.Dirty = true
					}
				})
				ui.Column(c).Grow(1).Gap(4).Children(func() {
					ui.Text(c, "表达语气").FontSize(11.5).TextColor(p.TextMuted)
					if ui.TextInput(c.Key("in_tone"), &s.Draft.Tone).FillWidth().Changed() {
						s.Draft.Dirty = true
					}
				})
			})

			if components.TriStateStringEditor(c, p, "mount_skills", "挂载技能标识",
				&s.Draft.Skills, &s.Draft.SkillsQuery, s.CatalogSkillIDs(), true, "") {
				s.Draft.Dirty = true
			}
			if components.TriStateStringEditor(c, p, "mount_tools", "挂载工具标识",
				&s.Draft.Tools, &s.Draft.ToolsQuery, s.CatalogToolIDs(), true, "") {
				s.Draft.Dirty = true
			}
			if components.TriStateStringEditor(c, p, "mount_kbs", "挂载只读知识库",
				&s.Draft.KnowledgeBases, &s.Draft.KBQuery, s.CatalogKBIDs(), true, "") {
				s.Draft.Dirty = true
			}
			if components.TriStateStringEditor(c, p, "mount_promo", "场景推广触发词",
				&s.Draft.PromoKeywords, &s.Draft.PromoQuery, nil, true, "") {
				s.Draft.Dirty = true
			}
		})
}

func (s *Store) renderContainerRoutingTab(c *ui.Context, p theme.Palette, g client.GroupView) {
	containerName := s.ContainerDisplayName(g.ID, g.Group.Label)
	ui.Column(c).FillWidth().Gap(16).Children(func() {
		components.CardWithHeader(c, p, "执行路由策略 — "+containerName, "主备模型链路与单次运行会话策略", nil, func() {
			if s.SelectedRouting == nil {
				components.AlertBanner(c, p, components.ToneNeutral, "路由信息不可用",
					nonEmptyOr(components.ControlReasonCN(g.MissingRoutingReason), "控制面网关未返回该容器的执行路由配置。"))
			} else {
				rt := s.SelectedRouting
				components.DataList(c, p, func() {
					components.KVRow(c, p, "路由 / 策略标识", fmt.Sprintf("%s / %s（版本 %s）", rt.Routing.RouteID, rt.Routing.PolicyID, components.ShortID(rt.Revision)))
					components.KVRow(c, p, "主执行后端", fmt.Sprintf("%s / %s（超时 %d 秒）",
						components.BackendDisplayName(rt.Routing.Primary.BackendID), rt.Routing.Primary.ModelID, rt.Routing.Primary.TimeoutSec))
					for idx, fb := range rt.Routing.Fallbacks {
						components.KVRow(c, p, fmt.Sprintf("降级兜底 %d", idx+1), fmt.Sprintf("%s / %s（超时 %d 秒，遵循权限不扩大原则）",
							components.BackendDisplayName(fb.BackendID), fb.ModelID, fb.TimeoutSec))
					}
				})
			}
		})

		components.CardWithHeader(c, p, "四级权限证据链（硬拦截 / 宿主门控 / 软规则 / 未验证）", "严格区分运行时硬约束与只读提示词软规范，绝不混同", nil, func() {
			components.AlertBanner(c, p, components.ToneWarn, "软约束只读边界提示",
				"软提示词规则仅作为审查证据展示，禁止在控制台编辑，且永远无法覆盖运行时硬边界或宿主安全门控。")
			if s.SelectedPerms == nil {
				ui.Text(c, "当前容器未加载到网关权限证据数据。").FontSize(12).TextColor(p.TextMuted)
			} else {
				components.DataList(c, p, func() {
					for _, perm := range s.SelectedPerms.Permissions {
						ui.Row(c).Key("perm_"+perm.Capability).FillWidth().PaddingY(10).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
							ui.Column(c).Grow(1).Gap(4).Children(func() {
								ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
									ui.Text(c, components.PermissionCapabilityLabel(perm.Capability)).FontSize(12.5).FontWeight(600).TextColor(p.Text)
									components.EnforcementPill(c, p, perm.Enforcement)
								})
								ui.Text(c, fmt.Sprintf("依据来源: %s · 策略版本 %s · %s",
									components.ProvenanceLabel(perm.Provenance), components.ShortID(perm.PolicyVersion), components.ControlReasonCN(perm.Reason))).FontSize(11).TextColor(p.TextMuted)
							})
							ui.Text(c, fmt.Sprintf("申请: %s · 生效: %s · 可改: %s", fmtBoolPtrCN(perm.Requested), fmtBoolPtrCN(perm.Effective), boolValCN(perm.Editable))).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextSecondary)
						})
					}
				})
			}
		})

		components.CardWithHeader(c, p, fmt.Sprintf("后端执行引擎目录 (%d)", len(s.Backends)), "当前控制面注入的执行后端与可选模型", nil, func() {
			components.DataList(c, p, func() {
				for _, bk := range s.Backends {
					models := make([]string, 0, len(bk.Models))
					for _, m := range bk.Models {
						models = append(models, nonEmptyOr(m.Label, m.ID))
					}
					modelsTxt := strings.Join(models, "、")
					if modelsTxt == "" {
						modelsTxt = "由下游智能体配置决定"
					}
					components.KVRow(c, p, components.BackendDisplayName(bk.ID),
						fmt.Sprintf("%s · 可用: %s · %s · 模型: %s",
							components.BackendKindLabel(bk.Kind), boolValCN(bk.Available), components.BindingLabel(bk.AgentBinding), modelsTxt))
				}
			})
		})
	})
}

func (s *Store) renderGlobalRelayTab(c *ui.Context, p theme.Palette) {
	ui.Column(c).FillWidth().Gap(16).Children(func() {
		components.CardWithHeader(c, p, "全局调度器规则（Dispatcher）", "控制全局演练、前置 Laya 过滤及 @ 触发直通开关", func() {
			if s.GlobalRules != nil {
				if ui.PrimaryButton(c.Key("plan_global_rules"), "预览全局规则变更").Disabled(s.Busy).Clicked() {
					s.PlanGlobalRulesDraft(c)
				}
			}
		}, func() {
			if s.GlobalRules == nil {
				ui.Text(c, "全局调度器规则暂不可用。").FontSize(12).TextColor(p.TextMuted)
			} else {
				ui.Text(c, "基线版本指纹: "+components.ShortID(s.GlobalRules.Data.Versions.Fingerprint)).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextMuted)
				ui.Row(c).Wrap().Gap(20).PaddingY(4).Children(func() {
					ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "全局演练模式").FontSize(12.5).TextColor(p.Text)
						if ui.Switch(c.Key("g_dry_run"), &s.GlobalDryRun).Changed() {
							s.GlobalRelayDirty = true
						}
					})
					ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "Laya 前置过滤").FontSize(12.5).TextColor(p.Text)
						if ui.Switch(c.Key("g_laya"), &s.GlobalLayaFilter).Changed() {
							s.GlobalRelayDirty = true
						}
					})
					ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "@ 提及直接放行决策").FontSize(12.5).TextColor(p.Text)
						if ui.Switch(c.Key("g_bypass"), &s.GlobalBypassMention).Changed() {
							s.GlobalRelayDirty = true
						}
					})
				})

				compat := s.GlobalRules.Data.Compatibility
				components.AlertBanner(c, p, components.ToneInfo, "兼容旧映射投影（只读不可编辑）",
					fmt.Sprintf("白名单数量: %d · 默认身份: %s · 容器映射数: %d · 他机器人过滤数: %d",
						len(compat.GroupWhitelist), derefStr(compat.DefaultProfile), len(compat.GroupProfiles), len(compat.GroupOtherBots)))
			}
		})

		components.CardWithHeader(c, p, "中继过滤器与群控开关（Relay）", "采用摘要预检乐观锁保证并发安全写入", func() {
			if s.RelayRules != nil {
				if ui.PrimaryButton(c.Key("plan_relay_rules"), "预览中继规则变更").Disabled(s.Busy).Clicked() {
					s.PlanRelayDraft(c)
				}
			}
		}, func() {
			if s.RelayRules == nil {
				ui.Text(c, "Relay 中继规则未连接或暂不可用。").FontSize(12).TextColor(p.TextMuted)
			} else {
				rr := s.RelayRules.Data
				ui.Text(c, "并发写入机制: "+components.WriteConcurrencyLabel(rr.WriteConcurrency)).FontSize(11.5).TextColor(p.TextMuted)
				ui.Row(c).Wrap().Gap(20).PaddingY(4).Children(func() {
					ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "清洗冗余正文").FontSize(12.5).TextColor(p.Text)
						if ui.Switch(c.Key("r_clean"), &s.RelayCleanContent).Changed() {
							s.GlobalRelayDirty = true
						}
					})
					ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "忽略自身回声").FontSize(12.5).TextColor(p.Text)
						if ui.Switch(c.Key("r_echo"), &s.RelayIgnoreEcho).Changed() {
							s.GlobalRelayDirty = true
						}
					})
					ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
						ui.Text(c, "全量监控开关").FontSize(12.5).TextColor(p.Text)
						if ui.Switch(c.Key("r_mon"), &s.RelayMonitorAll).Changed() {
							s.GlobalRelayDirty = true
						}
					})
				})
				components.DataList(c, p, func() {
					components.KVRow(c, p, "关键字过滤列表", components.TriStateSummaryCN(rr.Rules.Filters.KeywordBlocklist))
					components.KVRow(c, p, "正则过滤列表", components.TriStateSummaryCN(rr.Rules.Filters.RegexBlocklist))
					components.KVRow(c, p, "发送者屏蔽列表", components.TriStateSummaryCN(rr.Rules.Filters.SenderBlocklist))
				})
			}
		})
	})
}

func boolValCN(v bool) string {
	if v {
		return "是"
	}
	return "否"
}

func fmtBoolPtrCN(v *bool) string {
	if v == nil {
		return "未声明 (null)"
	}
	return boolValCN(*v)
}

func (s *Store) renderForwardRulesTab(c *ui.Context, p theme.Palette) {
	targetOpts := s.TargetContainerOptions()
	knownSources := s.KnownSourceContainers()

	ui.Column(c).FillWidth().Gap(16).Children(func() {
		components.CardWithHeader(c, p,
			"跨平台直通转发规则（跳过过滤与 AI）",
			"将采集来源消息绕过门禁、防抖与模型推理，直接投递到已配置路由的目标微信群",
			func() {
				if s.GlobalRules != nil {
					if ui.PrimaryButton(c.Key("plan_forward_rules"), "预览并发布转发规则").Disabled(s.Busy).Clicked() {
						s.PlanForwardRulesDraft(c)
					}
				}
			},
			func() {
				ui.Row(c).Wrap().Gap(8).AlignItems(ui.Center).Children(func() {
					components.TagPill(c, p, fmt.Sprintf("当前规则数: %d", len(s.ForwardRulesDraft)), components.ToneInfo)
					if s.ForwardRulesDirty {
						components.StatusPill(c, p, "存在未发布草稿", components.ToneWarn)
					}
				})

				components.AlertBanner(c, p, components.ToneInfo, "直通转发边界说明",
					"来源容器无需加入 Dispatcher 白名单（只需 Relay 收录）；目标微信群必须已在左侧容器路由中存在，且受目标群演练开关、幂等防重和自身回声确认保护。模板可用变量：{{platform}}、{{group}}、{{sender}}、{{content}}。")

				if len(s.ForwardRulesDraft) == 0 {
					ui.Text(c, "尚未配置直通转发规则。可在下方添加来源容器与目标微信群的直通映射。").FontSize(12).TextColor(p.TextMuted)
				} else {
					ui.Column(c).FillWidth().Gap(8).Children(func() {
						for i := range s.ForwardRulesDraft {
							i := i
							rule := &s.ForwardRulesDraft[i]
							ui.Column(c).Key("fwd_item_"+rule.ID).FillWidth().Padding(12).Gap(8).Radius(8).Background(p.SubtleBg).Border(1, p.Border).Children(func() {
								ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(8).Children(func() {
									ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
										ui.Text(c, rule.ID).FontSize(13).FontWeight(600).TextColor(p.Text)
										components.TagPill(c, p, rule.SourceContainerID+" → "+s.ContainerDisplayName(rule.TargetContainerID, ""), components.ToneInfo)
										if rule.DryRun {
											components.StatusPill(c, p, "演练拦截", components.ToneWarn)
										} else if rule.Enabled {
											components.StatusPill(c, p, "实盘直发", components.ToneOK)
										} else {
											components.StatusPill(c, p, "已停用", components.ToneNeutral)
										}
									})
									if ui.Button(c.Key("fwd_del_"+rule.ID), "删除").Disabled(s.Busy).Clicked() {
										s.RemoveForwardRuleDraft(i)
									}
								})
								ui.Row(c).Wrap().Gap(18).AlignItems(ui.Center).Children(func() {
									ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
										ui.Text(c, "启用").FontSize(12).TextColor(p.Text)
										if ui.Switch(c.Key("fwd_en_"+rule.ID), &rule.Enabled).Changed() {
											s.ForwardRulesDirty = true
										}
									})
									ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
										ui.Text(c, "演练模式").FontSize(12).TextColor(p.Text)
										if ui.Switch(c.Key("fwd_dry_"+rule.ID), &rule.DryRun).Changed() {
											s.ForwardRulesDirty = true
										}
									})
									ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
										ui.Text(c, "包含自身回声").FontSize(12).TextColor(p.Text)
										if ui.Switch(c.Key("fwd_echo_"+rule.ID), &rule.IncludeEcho).Changed() {
											s.ForwardRulesDirty = true
										}
									})
								})
								if strings.TrimSpace(rule.Template) != "" {
									ui.Text(c, "模板: "+rule.Template).FontSize(11.5).TextColor(p.TextMuted)
								}
							})
						}
					})
				}

				// Add new passthrough forwarding rule form
				ui.Column(c).FillWidth().Padding(12).Gap(10).Radius(8).Background(p.SubtleBg).Border(1, p.Border).Children(func() {
					ui.Text(c, "新增直通转发规则").FontSize(12.5).FontWeight(600).TextColor(p.Text)

					ui.Row(c).FillWidth().Gap(10).Children(func() {
						ui.Column(c).Width(180).Gap(4).Children(func() {
							ui.Text(c, "规则 ID（留空自动生成）").FontSize(11.5).TextColor(p.TextMuted)
							ui.TextInput(c.Key("new_fwd_id"), &s.NewFwdID).Placeholder("例如: v2ex-to-wx").FillWidth()
						})
						ui.Column(c).Grow(1).Gap(4).Children(func() {
							ui.Text(c, "来源容器 ID").FontSize(11.5).TextColor(p.TextMuted)
							ui.TextInput(c.Key("new_fwd_src"), &s.NewFwdSource).Placeholder("例如: v2ex:t-100001 或 bilibili:BV1...").FillWidth()
						})
					})

					if len(knownSources) > 0 {
						ui.Row(c).Wrap().Gap(6).AlignItems(ui.Center).Children(func() {
							ui.Text(c, "快捷填入已知来源:").FontSize(11).TextColor(p.TextMuted)
							for _, srcID := range knownSources {
								srcID := srcID
								if ui.Button(c.Key("pick_src_"+srcID), srcID).Clicked() {
									s.NewFwdSource = srcID
								}
							}
						})
					}

					ui.Row(c).FillWidth().Gap(10).AlignItems(ui.End).Children(func() {
						ui.Column(c).Grow(1).Gap(4).Children(func() {
							ui.Text(c, "目标容器（优先微信群）").FontSize(11.5).TextColor(p.TextMuted)
							if len(targetOpts) > 0 {
								ui.Select(c.Key("new_fwd_dst"), &s.NewFwdTargetLabel, targetOpts)
							} else {
								ui.Text(c, "暂无可选目标容器，请先在左侧配置微信群路由。").FontSize(11.5).TextColor(p.Danger)
							}
						})
						ui.Column(c).Grow(1).Gap(4).Children(func() {
							ui.Text(c, "转发模板（可选）").FontSize(11.5).TextColor(p.TextMuted)
							ui.TextInput(c.Key("new_fwd_tmpl"), &s.NewFwdTemplate).Placeholder("[{{platform}}] {{group}} · {{sender}}: {{content}}").FillWidth()
						})
					})

					ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Wrap().Gap(12).Children(func() {
						ui.Row(c).Wrap().Gap(16).AlignItems(ui.Center).Children(func() {
							ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
								ui.Text(c, "立即启用").FontSize(12).TextColor(p.Text)
								ui.Switch(c.Key("new_fwd_en"), &s.NewFwdEnabled)
							})
							ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
								ui.Text(c, "演练模式").FontSize(12).TextColor(p.Text)
								ui.Switch(c.Key("new_fwd_dry"), &s.NewFwdDryRun)
							})
							ui.Row(c).Gap(6).AlignItems(ui.Center).Children(func() {
								ui.Text(c, "包含回声").FontSize(12).TextColor(p.Text)
								ui.Switch(c.Key("new_fwd_echo"), &s.NewFwdIncludeEcho)
							})
						})
						if ui.Button(c.Key("add_fwd_draft"), "添加到草稿列表").Disabled(s.Busy || len(targetOpts) == 0).Clicked() {
							s.AddForwardRuleDraft(c)
						}
					})
				})
			})

		components.CardWithHeader(c, p,
			"手动直发测试（跳过过滤与 AI）",
			"通过 /console/v1/direct-deliver 直接向目标容器发送一条测试文本并生成可追踪运行记录",
			func() {
				if ui.PrimaryButton(c.Key("send_direct_deliver"), "发送直发测试").Disabled(s.Busy || len(targetOpts) == 0).Clicked() {
					s.SendDirectDeliver(c)
				}
			},
			func() {
				ui.Row(c).FillWidth().Gap(12).AlignItems(ui.End).Children(func() {
					ui.Column(c).Width(280).Gap(4).Children(func() {
						ui.Text(c, "目标容器").FontSize(11.5).TextColor(p.TextMuted)
						if len(targetOpts) > 0 {
							ui.Select(c.Key("direct_dst"), &s.DirectTargetLabel, targetOpts)
						} else {
							ui.Text(c, "暂无可选目标容器").FontSize(11.5).TextColor(p.Danger)
						}
					})
					ui.Column(c).Grow(1).Gap(4).Children(func() {
						ui.Text(c, "发送文本").FontSize(11.5).TextColor(p.TextMuted)
						ui.TextInput(c.Key("direct_text"), &s.DirectText).Placeholder("输入要直发测试的内容...").FillWidth()
					})
				})
				ui.Row(c).Gap(8).AlignItems(ui.Center).Children(func() {
					ui.Text(c, "本次演练模式（dry_run，仅校验链路不真实发群）").FontSize(12).TextColor(p.Text)
					ui.Switch(c.Key("direct_dry"), &s.DirectDryRun)
				})
				if s.LastDirectResult != nil {
					res := s.LastDirectResult
					tone := components.ToneOK
					if res.DryRun {
						tone = components.ToneWarn
					} else if res.Outcome == "failed" || res.Outcome == "unknown" {
						tone = components.ToneDanger
					}
					components.AlertBanner(c, p, tone,
						fmt.Sprintf("最近直发结果: %s（%s）", components.OutcomeLabel(res.Outcome), components.DeliveryLabel(res.Delivery)),
						fmt.Sprintf("运行 ID: %s · 目标: %s · 触发: %s · 原因: %s",
							components.ShortID(res.RunID), res.TargetContainerID, components.TriggerLabel(res.Trigger), components.ShortReasonLabel(res.ReasonCode)))
				}
			})
	})
}
