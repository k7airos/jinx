// Package theme provides neutral graphite light and dark palettes paired with
// JINX cerulean blue accents, hairline borders, and high-craft telemetry tokens.
package theme

import "github.com/egoist/mygo/ui"

// Palette defines the complete semantic color and surface tokens for Light and Dark modes.
type Palette struct {
	Dark          bool
	UI            *ui.Theme
	WindowBg      ui.Color
	SidebarBg     ui.Color
	TopbarBg      ui.Color
	CardBg        ui.Color
	SubtleBg      ui.Color
	RaisedBg      ui.Color
	Border        ui.Color
	BorderStrong  ui.Color
	Text          ui.Color
	TextSecondary ui.Color
	TextMuted     ui.Color
	Accent        ui.Color
	AccentSoft    ui.Color
	AccentText    ui.Color
	OK            ui.Color
	OKSoft        ui.Color
	Warn          ui.Color
	WarnSoft      ui.Color
	Danger        ui.Color
	DangerSoft    ui.Color
	Info          ui.Color
	InfoSoft      ui.Color
	Muted         ui.Color
	MutedSoft     ui.Color
	ChartNeutral  ui.Color
}

// Light returns the default crisp neutral graphite light palette.
func Light() Palette {
	textPrimary := ui.Hex("#171717")
	hairline := textPrimary.Alpha(0.08)

	t := ui.LightTheme()
	t.Dark = false
	t.Background = ui.Hex("#F7F8FA")
	t.Surface = ui.Hex("#FFFFFF")
	t.SurfaceHover = ui.Hex("#F2F3F5")
	t.SurfacePressed = ui.Hex("#E8EAEE")
	t.Border = textPrimary.Alpha(0.10)
	t.Text = textPrimary
	t.TextMuted = ui.Hex("#6B7280")
	t.Accent = ui.Hex("#0284C7")
	t.AccentHover = ui.Hex("#0369A1")
	t.AccentPressed = ui.Hex("#075985")
	t.AccentText = ui.Hex("#FFFFFF")
	t.InactiveAccent = ui.Hex("#3898CC")
	t.Danger = ui.Hex("#DC2626")
	t.Warning = ui.Hex("#B45309")
	t.Success = ui.Hex("#15803D")
	t.Focus = ui.Hex("#0284C7")
	t.Spacing = 4
	t.Radius = 8
	t.FontSize = 13

	return Palette{
		Dark:          false,
		UI:            t,
		WindowBg:      ui.Hex("#F7F8FA"),
		SidebarBg:     ui.Hex("#F2F3F5"),
		TopbarBg:      ui.Hex("#FFFFFF"),
		CardBg:        ui.Hex("#FFFFFF"),
		SubtleBg:      ui.Hex("#F4F5F7"),
		RaisedBg:      ui.Hex("#ECEEF2"),
		Border:        hairline,
		BorderStrong:  textPrimary.Alpha(0.14),
		Text:          textPrimary,
		TextSecondary: ui.Hex("#4B5563"),
		TextMuted:     ui.Hex("#6B7280"),
		Accent:        ui.Hex("#0284C7"),
		AccentSoft:    ui.Hex("#0284C7").Alpha(0.10),
		AccentText:    ui.Hex("#0369A1"),
		OK:            ui.Hex("#15803D"),
		OKSoft:        ui.Hex("#16A34A").Alpha(0.10),
		Warn:          ui.Hex("#B45309"),
		WarnSoft:      ui.Hex("#D97706").Alpha(0.11),
		Danger:        ui.Hex("#DC2626"),
		DangerSoft:    ui.Hex("#DC2626").Alpha(0.10),
		Info:          ui.Hex("#0284C7"),
		InfoSoft:      ui.Hex("#0284C7").Alpha(0.10),
		Muted:         ui.Hex("#52525B"),
		MutedSoft:     textPrimary.Alpha(0.05),
		ChartNeutral:  ui.Hex("#D1D5DB"),
	}
}

// Dark returns the high-contrast neutral graphite dark palette.
func Dark() Palette {
	textPrimary := ui.Hex("#ECEEF2")
	hairline := textPrimary.Alpha(0.08)

	t := ui.DarkTheme()
	t.Dark = true
	t.Background = ui.Hex("#15171A")
	t.Surface = ui.Hex("#1C1F24")
	t.SurfaceHover = ui.Hex("#252930")
	t.SurfacePressed = ui.Hex("#2E333C")
	t.Border = textPrimary.Alpha(0.10)
	t.Text = textPrimary
	t.TextMuted = ui.Hex("#88919E")
	t.Accent = ui.Hex("#38BDF8")
	t.AccentHover = ui.Hex("#7DD3FC")
	t.AccentPressed = ui.Hex("#0EA5E9")
	t.AccentText = ui.Hex("#0C1219")
	t.InactiveAccent = ui.Hex("#2E86B5")
	t.Danger = ui.Hex("#F16A6F")
	t.Warning = ui.Hex("#F5B74B")
	t.Success = ui.Hex("#4CC38A")
	t.Focus = ui.Hex("#38BDF8")
	t.Spacing = 4
	t.Radius = 8
	t.FontSize = 13

	return Palette{
		Dark:          true,
		UI:            t,
		WindowBg:      ui.Hex("#15171A"),
		SidebarBg:     ui.Hex("#101114"),
		TopbarBg:      ui.Hex("#181A1F"),
		CardBg:        ui.Hex("#1C1F24"),
		SubtleBg:      ui.Hex("#181B20"),
		RaisedBg:      ui.Hex("#252930"),
		Border:        hairline,
		BorderStrong:  textPrimary.Alpha(0.15),
		Text:          textPrimary,
		TextSecondary: ui.Hex("#A1A8B3"),
		TextMuted:     ui.Hex("#88919E"),
		Accent:        ui.Hex("#38BDF8"),
		AccentSoft:    ui.Hex("#38BDF8").Alpha(0.14),
		AccentText:    ui.Hex("#7DD3FC"),
		OK:            ui.Hex("#4CC38A"),
		OKSoft:        ui.Hex("#4CC38A").Alpha(0.14),
		Warn:          ui.Hex("#F5B74B"),
		WarnSoft:      ui.Hex("#F5B74B").Alpha(0.14),
		Danger:        ui.Hex("#F16A6F"),
		DangerSoft:    ui.Hex("#F16A6F").Alpha(0.14),
		Info:          ui.Hex("#38BDF8"),
		InfoSoft:      ui.Hex("#38BDF8").Alpha(0.14),
		Muted:         ui.Hex("#9CA3AF"),
		MutedSoft:     textPrimary.Alpha(0.07),
		ChartNeutral:  ui.Hex("#4B5563"),
	}
}

// Get returns Dark() when dark is true, otherwise Light().
func Get(dark bool) Palette {
	if dark {
		return Dark()
	}
	return Light()
}
