// Package components provides high-craft native MyGo widgets, semantic dot
// pills, metric cards, hairline-divided lists, alert callouts, and tri-state
// editors localized for the JINX desktop console.
package components

import (
	"fmt"
	"strings"

	"jinx-desktop/client"
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

type Tone int

const (
	ToneNeutral Tone = iota
	ToneOK
	ToneWarn
	ToneDanger
	ToneInfo
)

func toneColors(p theme.Palette, tone Tone) (fg, bg, border ui.Color) {
	switch tone {
	case ToneOK:
		return p.OK, p.OKSoft, p.OK.Alpha(0.28)
	case ToneWarn:
		return p.Warn, p.WarnSoft, p.Warn.Alpha(0.30)
	case ToneDanger:
		return p.Danger, p.DangerSoft, p.Danger.Alpha(0.30)
	case ToneInfo:
		return p.Info, p.InfoSoft, p.Info.Alpha(0.28)
	default:
		return p.Muted, p.MutedSoft, p.Border
	}
}

// StatusPill renders a crisp status badge with a 6px solid semantic dot and Chinese label.
func StatusPill(c *ui.Context, p theme.Palette, text string, tone Tone) ui.Element {
	fg, bg, border := toneColors(p, tone)
	return ui.Row(c).PaddingX(8).PaddingY(3).Gap(6).Radius(6).Background(bg).Border(1, border).AlignItems(ui.Center).Shrink(0).Children(func() {
		ui.Box(c).Size(6, 6).Radius(999).Background(fg)
		ui.Text(c, text).FontSize(11.5).FontWeight(500).TextColor(fg).SingleLine()
	})
}

// TagPill renders a subtle metadata chip without a status dot.
func TagPill(c *ui.Context, p theme.Palette, text string, tone Tone) ui.Element {
	fg, bg, border := toneColors(p, tone)
	return ui.Box(c).PaddingX(8).PaddingY(3).Radius(6).Background(bg).Border(1, border).Shrink(0).Children(func() {
		ui.Text(c, text).FontSize(11.5).FontWeight(500).TextColor(fg).SingleLine()
	})
}

// IconPill renders a badge with a stroke SVG icon and label (for enforcement & lock states).
func IconPill(c *ui.Context, p theme.Palette, svg *ui.SVG, text string, tone Tone) ui.Element {
	fg, bg, border := toneColors(p, tone)
	return ui.Row(c).PaddingX(8).PaddingY(3).Gap(5).Radius(6).Background(bg).Border(1, border).AlignItems(ui.Center).Shrink(0).Children(func() {
		if svg != nil {
			ui.Icon(c, svg).FontSize(12).TextColor(fg)
		}
		ui.Text(c, text).FontSize(11.5).FontWeight(500).TextColor(fg).SingleLine()
	})
}

func HealthPill(c *ui.Context, p theme.Palette, health string) ui.Element {
	label := HealthLabel(health)
	switch health {
	case "ready", "running", "fresh", "succeeded":
		return StatusPill(c, p, label, ToneOK)
	case "not_ready", "stale", "draining", "degraded", "partial_apply":
		return StatusPill(c, p, label, ToneWarn)
	case "unreachable", "failed", "disconnected", "error":
		return StatusPill(c, p, label, ToneDanger)
	case "unsupported":
		return StatusPill(c, p, label, ToneNeutral)
	default:
		return StatusPill(c, p, label, ToneNeutral)
	}
}

func PhasePill(c *ui.Context, p theme.Palette, phase string) ui.Element {
	label := PhaseLabel(phase)
	switch phase {
	case "terminal":
		return StatusPill(c, p, label, ToneInfo)
	case "unknown":
		return StatusPill(c, p, label, ToneDanger)
	default:
		return StatusPill(c, p, label, ToneWarn)
	}
}

func OutcomePill(c *ui.Context, p theme.Palette, outcome string) ui.Element {
	label := OutcomeLabel(outcome)
	switch outcome {
	case "replied":
		return StatusPill(c, p, label, ToneOK)
	case "pending":
		return StatusPill(c, p, label, ToneInfo)
	case "silent", "suppressed", "blocked":
		return StatusPill(c, p, label, ToneNeutral)
	case "expired", "canceled":
		return StatusPill(c, p, label, ToneWarn)
	case "failed", "unknown":
		return StatusPill(c, p, label, ToneDanger)
	default:
		return StatusPill(c, p, label, ToneNeutral)
	}
}

func DeliveryPill(c *ui.Context, p theme.Palette, delivery string) ui.Element {
	label := DeliveryLabel(delivery)
	switch delivery {
	case "confirmed":
		return StatusPill(c, p, label, ToneOK)
	case "submitted", "submitting":
		return StatusPill(c, p, label, ToneInfo)
	case "not_started":
		return StatusPill(c, p, label, ToneNeutral)
	case "blocked":
		return StatusPill(c, p, label, ToneWarn)
	case "failed", "unknown":
		return StatusPill(c, p, label, ToneDanger)
	default:
		return StatusPill(c, p, label, ToneNeutral)
	}
}

// EnforcementPill renders the mandatory 4-level enforcement taxonomy without blurring hard vs soft.
func EnforcementPill(c *ui.Context, p theme.Palette, enforcement string) ui.Element {
	icons := Icons()
	switch enforcement {
	case "hard":
		return IconPill(c, p, icons.Lock, "硬拦截 (不可绕过)", ToneOK)
	case "host":
		return IconPill(c, p, icons.Shield, "宿主权限门控", ToneInfo)
	case "soft":
		return IconPill(c, p, icons.Alert, "软规则 (仅只读证据)", ToneWarn)
	default:
		return StatusPill(c, p, "未验证", ToneNeutral)
	}
}

func AvailabilityPill(c *ui.Context, p theme.Palette, a client.Availability) ui.Element {
	if !a.Supported {
		return StatusPill(c, p, "暂不支持", ToneNeutral)
	}
	if !a.Enabled {
		return StatusPill(c, p, "已禁用", ToneWarn)
	}
	return StatusPill(c, p, "已启用", ToneOK)
}

// PageHeader renders a clean top header inside a console page with optional right action controls.
func PageHeader(c *ui.Context, p theme.Palette, title, subtitle string, rightFn func()) {
	ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(16).Children(func() {
		ui.Column(c).Grow(1).Gap(4).Children(func() {
			ui.Text(c, title).FontSize(20).FontWeight(600).TextColor(p.Text)
			if subtitle != "" {
				ui.Text(c, subtitle).FontSize(12).TextColor(p.TextMuted)
			}
		})
		if rightFn != nil {
			ui.Row(c).Gap(8).AlignItems(ui.Center).Children(rightFn)
		}
	})
}

func applyCardSurface(el ui.Element, p theme.Palette) ui.Element {
	el = el.Padding(20).Gap(12).Radius(12).Background(p.CardBg).Border(1, p.Border)
	if !p.Dark {
		el = el.Shadow(0, 1, 3, 0, ui.RGBA(15, 23, 42, 0.04))
	}
	return el
}

// Card draws a high-craft neutral container with hairline border and balanced spacing.
func Card(c *ui.Context, p theme.Palette, title string, body func()) ui.Element {
	return applyCardSurface(ui.Column(c).FillWidth(), p).Children(func() {
		if title != "" {
			ui.Text(c, title).FontSize(14).FontWeight(600).TextColor(p.Text)
		}
		if body != nil {
			body()
		}
	})
}

// CardWithHeader draws a card with title, muted subtitle, optional right widget, and body.
func CardWithHeader(c *ui.Context, p theme.Palette, title, subtitle string, rightFn, body func()) ui.Element {
	return applyCardSurface(ui.Column(c).FillWidth(), p).Children(func() {
		if title != "" || subtitle != "" || rightFn != nil {
			ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
				ui.Column(c).Grow(1).Gap(2).Children(func() {
					if title != "" {
						ui.Text(c, title).FontSize(14).FontWeight(600).TextColor(p.Text)
					}
					if subtitle != "" {
						ui.Text(c, subtitle).FontSize(11.5).TextColor(p.TextMuted)
					}
				})
				if rightFn != nil {
					ui.Row(c).Gap(8).AlignItems(ui.Center).Children(rightFn)
				}
			})
		}
		if body != nil {
			body()
		}
	})
}

// MetricCard renders a spacious KPI card on a quiet neutral surface with tabular numerals.
func MetricCard(c *ui.Context, p theme.Palette, label, value, caption string, tone Tone, emphasize bool) ui.Element {
	valColor := p.Text
	if emphasize {
		fg, _, _ := toneColors(p, tone)
		valColor = fg
	}
	el := ui.Column(c).Grow(1).Padding(16).Gap(6).Radius(12).Background(p.CardBg).Border(1, p.Border)
	if !p.Dark {
		el = el.Shadow(0, 1, 2, 0, ui.RGBA(15, 23, 42, 0.03))
	}
	return el.Children(func() {
		ui.Text(c, label).FontSize(12).FontWeight(500).TextColor(p.TextSecondary)
		ui.Text(c, value).FontSize(26).FontWeight(600).FontFeatures("tnum").TextColor(valColor).SingleLine()
		if caption != "" {
			ui.Text(c, caption).FontSize(11).TextColor(p.TextMuted)
		}
	})
}

// DataList groups rows vertically with clean 1px hairline dividers.
func DataList(c *ui.Context, p theme.Palette, fn func()) ui.Element {
	return ui.Column(c).FillWidth().Dividers(1, p.Border).Children(fn)
}

// AlertBanner draws a semantic callout with a 3px left accent stripe, icon, title, and body.
func AlertBanner(c *ui.Context, p theme.Palette, tone Tone, title, message string) ui.Element {
	fg, bg, border := toneColors(p, tone)
	icon := Icons().Alert
	if tone == ToneOK {
		icon = Icons().Check
	}
	return ui.Row(c).FillWidth().Radius(10).Background(bg).Border(1, border).Padding(10, 14, 10, 10).Gap(10).AlignItems(ui.Stretch).Children(func() {
		ui.Box(c).Width(3).Radius(2).Background(fg)
		ui.Box(c).Padding(2, 0, 0, 0).Children(func() {
			ui.Icon(c, icon).FontSize(15).TextColor(fg)
		})
		ui.Column(c).Grow(1).Gap(2).Children(func() {
			if title != "" {
				ui.Text(c, title).FontSize(12.5).FontWeight(600).TextColor(fg)
			}
			if message != "" {
				ui.Text(c, message).FontSize(12).TextColor(p.TextSecondary)
			}
		})
	})
}

// KVRow renders an aligned key/value row with fixed key width and tabular values.
func KVRow(c *ui.Context, p theme.Palette, key, value string) {
	ui.Row(c).FillWidth().Gap(12).PaddingY(4).AlignItems(ui.Start).Children(func() {
		ui.Text(c, key).Width(124).FontSize(12).TextColor(p.TextMuted)
		ui.Text(c, nonEmpty(value, "—")).Grow(1).FontSize(12).FontFeatures("tnum").TextColor(p.Text)
	})
}

// SegmentedControl renders a high-contrast segmented switch whose active choice
// remains unmistakably highlighted in both Light and Dark surfaces.
func SegmentedControl(c *ui.Context, p theme.Palette, key string, selected *int, disabled bool, labels ...string) ui.Element {
	parts := ui.SegmentedBase(c.Key(key), selected, len(labels))
	trackBg := p.RaisedBg
	activeBg := p.CardBg
	if p.Dark {
		trackBg = p.WindowBg
		activeBg = p.RaisedBg
	}
	track := parts.Track.AlignItems(ui.Stretch).Padding(3).Gap(3).Radius(8).Background(trackBg).Border(1, p.Border)
	if disabled {
		track.Disabled(true)
	}
	track.Children(func() {
		for i, label := range labels {
			active := selected != nil && *selected == i
			seg := parts.Segment(i).PaddingX(12).PaddingY(5).Radius(6).Cursor(ui.CursorPointer)
			if disabled {
				seg.Disabled(true)
			}
			fg := p.TextSecondary
			weight := 500
			if active {
				fg = p.AccentText
				weight = 600
				seg.Background(activeBg).Border(1, p.Accent.Alpha(0.50))
				if !p.Dark {
					seg.Shadow(0, 1, 2, 0, ui.RGBA(15, 23, 42, 0.10))
				}
			} else if seg.Hovered() && !disabled {
				seg.Background(activeBg.Alpha(0.55))
			}
			seg.Children(func() {
				ui.Text(c, label).FontSize(12).FontWeight(weight).TextColor(fg).SingleLine()
			})
		}
	})
	return track
}

// TriStateStringEditor edits 继承默认 (null) vs 显式置空 ([]) vs 自定义列表 without collapsing [].
func TriStateStringEditor(
	c *ui.Context,
	p theme.Palette,
	keyPrefix string,
	title string,
	state *client.TriStateStringList,
	inputDraft *string,
	suggestions []string,
	editable bool,
	note string,
) (changed bool) {
	ui.Column(c).FillWidth().Gap(8).Padding(12).Radius(8).Background(p.SubtleBg).Border(1, p.Border).Children(func() {
		ui.Row(c).FillWidth().Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(8).Children(func() {
			ui.Text(c, title).FontSize(12.5).FontWeight(600).TextColor(p.Text)
			if !editable {
				IconPill(c, p, Icons().Lock, "只读锁定", ToneNeutral)
			}
		})
		if note != "" {
			ui.Text(c, note).FontSize(11).TextColor(p.TextMuted)
		}

		modeIdx := 0
		switch state.Mode {
		case client.TriEmpty:
			modeIdx = 1
		case client.TriValues:
			modeIdx = 2
		}

		// High-contrast compact SegmentedControl inside a Row so it retains intrinsic width.
		ui.Row(c).FillWidth().AlignItems(ui.Center).Children(func() {
			seg := SegmentedControl(c, p, keyPrefix+"_mode", &modeIdx, !editable, "继承默认", "显式置空", "自定义列表")
			if seg.Changed() && editable {
				changed = true
				switch modeIdx {
				case 0:
					*state = client.TriStringsNull()
				case 1:
					*state = client.TriStringsEmpty()
				case 2:
					vals := append([]string(nil), state.Values...)
					*state = client.TriStateStringList{Mode: client.TriValues, Values: vals}
				}
			}
		})

		if state.Mode == client.TriValues {
			tokens := append([]string(nil), state.Values...)
			tf := ui.TokenField(c.Key(keyPrefix+"_tokens"), &tokens, suggestions).Label(title).FillWidth().Disabled(!editable)
			if inputDraft != nil {
				tf = tf.Query(inputDraft)
			}
			if tf.Changed() && editable {
				changed = true
				state.Values = tokens
			}
		} else {
			summary := "当前生效语义：继承全局／上级配置（JSON null）"
			if state.Mode == client.TriEmpty {
				summary = "当前生效语义：显式清空覆盖（JSON []）"
			}
			ui.Text(c, summary).FontSize(11.5).TextColor(p.TextMuted)
		}
	})
	return changed
}

func FormatFloatPtr(v *float64) string {
	if v == nil {
		return "—"
	}
	if *v == float64(int64(*v)) {
		return fmt.Sprintf("%d", int64(*v))
	}
	return fmt.Sprintf("%.2f", *v)
}

func FormatIntPtr(v *int) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("%d", *v)
}

func nonEmpty(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
