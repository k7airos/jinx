package components

import (
	"jinx-desktop/theme"

	"github.com/egoist/mygo/ui"
)

// Drawer renders a macOS right-docked slide-over panel with dimmed scrim, smooth
// slide animation, Escape-to-dismiss, and sticky header over a scrollable body.
func Drawer(
	c *ui.Context,
	p theme.Palette,
	key string,
	open *bool,
	width float32,
	title string,
	subtitle string,
	headerActions func(),
	body func(),
) {
	if open == nil || !*open {
		return
	}
	scrimAlpha := float32(0.26)
	if p.Dark {
		scrimAlpha = 0.48
	}
	ui.Overlay(c, func() {
		// Scrim and panel are siblings inside Overlay so panel clicks never dismiss the scrim.
		scrim := ui.Box(c).Key(key + "_scrim").
			Absolute().Top(0).Right(0).Bottom(0).Left(0).
			Background(ui.Hex("#000000").Alpha(scrimAlpha))
		if scrim.Clicked() {
			*open = false
		}

		panel := ui.Column(c).Key(key + "_panel").
			Absolute().Top(0).Right(0).Bottom(0).Width(width).
			Background(p.CardBg).
			BorderStartWidth(1).BorderColor(p.BorderStrong).
			Shadow(-12, 0, 32, 0, ui.Hex("#000000").Alpha(0.18))

		entered := ui.Local(panel, "entered", func() bool { return false })
		pose := ui.Pose{}
		if !*entered {
			pose, *entered = ui.Pose{X: 28, Fade: 1}, true
			c.Invalidate()
		}
		panel.AnimatePose(pose, ui.Snappy)
		if panel.OverlayShortcut(0, ui.KeyEscape) {
			*open = false
		}

		panel.Children(func() {
			ui.Row(c).FillWidth().Padding(16, 20).Justify(ui.SpaceBetween).AlignItems(ui.Center).Gap(12).Children(func() {
				ui.Column(c).Grow(1).Gap(2).Children(func() {
					ui.Text(c, title).FontSize(15).FontWeight(600).TextColor(p.Text).SingleLine()
					if subtitle != "" {
						ui.Text(c, subtitle).FontSize(11.5).FontFeatures("tnum").TextColor(p.TextMuted).SingleLine()
					}
				})
				ui.Row(c).Gap(8).AlignItems(ui.Center).Shrink(0).Children(func() {
					if headerActions != nil {
						headerActions()
					}
					if ui.Button(c.Key(key+"_close"), "").Label("关闭抽屉").Children(func() {
						ui.Icon(c, Icons().Close).FontSize(14).TextColor(p.TextSecondary)
					}).Clicked() {
						*open = false
					}
				})
			})
			ui.Divider(c)
			ui.Scroll(c).Key(key + "_scroll").Grow(1).FillWidth().Padding(20).Children(func() {
				if body != nil {
					body()
				}
			})
		})
	})
}

// FilterChip renders an interactive toggle/preset pill with smooth hover feedback.
func FilterChip(c *ui.Context, p theme.Palette, key, label string, active bool) bool {
	bg := p.SubtleBg
	border := p.Border
	fg := p.TextSecondary
	weight := 500
	if active {
		bg = p.AccentSoft
		border = p.Accent
		fg = p.AccentText
		weight = 600
	}
	chip := ui.Row(c).Key(key).Padding(6, 12).Radius(999).Background(bg).Border(1, border).AlignItems(ui.Center).Gap(6)
	if chip.Hovered() && !active {
		chip.Background(p.RaisedBg).Border(1, p.BorderStrong)
	}
	chip.Cursor(ui.CursorPointer).Role(ui.RoleButton).Label(label).Children(func() {
		if active {
			ui.Box(c).Size(6, 6).Radius(999).Background(p.Accent)
		}
		ui.Text(c, label).FontSize(12).FontWeight(weight).TextColor(fg).NoWrap()
	})
	return chip.Clicked()
}
