package components

import (
	"sync"

	"github.com/egoist/mygo/ui"
)

type iconSet struct {
	Overview   *ui.SVG
	Runs       *ui.SVG
	Sources    *ui.SVG
	Containers *ui.SVG
	Services   *ui.SVG
	Changes    *ui.SVG
	Refresh    *ui.SVG
	Moon       *ui.SVG
	Sun        *ui.SVG
	Lock         *ui.SVG
	Shield       *ui.SVG
	Alert        *ui.SVG
	Check        *ui.SVG
	Close        *ui.SVG
	ChevronRight *ui.SVG
	Send         *ui.SVG
	Search       *ui.SVG
	Sliders      *ui.SVG
}

var navIcons = sync.OnceValue(func() iconSet {
	stroked := func(paths string) *ui.SVG {
		return ui.MustParseSVG([]byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">` + paths + `</svg>`))
	}
	return iconSet{
		Overview:   stroked(`<rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/>`),
		Runs:       stroked(`<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>`),
		Sources:    stroked(`<circle cx="6" cy="6" r="3"/><circle cx="18" cy="18" r="3"/><path d="M8.5 7.5 15.5 16.5"/><circle cx="18" cy="6" r="3"/><path d="M15 6H9"/>`),
		Containers: stroked(`<path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z"/><path d="m3.3 7 8.7 5 8.7-5"/><path d="M12 22V12"/>`),
		Services:   stroked(`<rect width="20" height="8" x="2" y="2" rx="2"/><rect width="20" height="8" x="2" y="14" rx="2"/><line x1="6" x2="6.01" y1="6" y2="6"/><line x1="6" x2="6.01" y1="18" y2="18"/>`),
		Changes:    stroked(`<path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l3 2"/>`),
		Refresh:    stroked(`<path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 16h5v5"/>`),
		Moon:       stroked(`<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>`),
		Sun:        stroked(`<circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="m4.93 4.93 1.41 1.41"/><path d="m17.66 17.66 1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="m6.34 17.66-1.41 1.41"/><path d="m19.07 4.93-1.41 1.41"/>`),
		Lock:       stroked(`<rect width="18" height="11" x="3" y="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>`),
		Shield:     stroked(`<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/><path d="m9 12 2 2 4-4"/>`),
		Alert:        stroked(`<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/>`),
		Check:        stroked(`<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>`),
		Close:        stroked(`<path d="M18 6 6 18"/><path d="m6 6 12 12"/>`),
		ChevronRight: stroked(`<path d="m9 18 6-6-6-6"/>`),
		Send:         stroked(`<path d="M14.536 21.686a.5.5 0 0 0 .937-.024l6.5-19a.496.496 0 0 0-.635-.635l-19 6.5a.5.5 0 0 0-.024.937l7.93 3.18a2 2 0 0 1 1.112 1.11z"/><path d="m21.854 2.147-10.94 10.939"/>`),
		Search:       stroked(`<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>`),
		Sliders:      stroked(`<line x1="4" x2="4" y1="21" y2="14"/><line x1="4" x2="4" y1="10" y2="3"/><line x1="12" x2="12" y1="21" y2="12"/><line x1="12" x2="12" y1="8" y2="3"/><line x1="20" x2="20" y1="21" y2="16"/><line x1="20" x2="20" y1="12" y2="3"/><line x1="2" x2="6" y1="14" y2="14"/><line x1="10" x2="14" y1="8" y2="8"/><line x1="18" x2="22" y1="16" y2="16"/>`),
	}
})

func Icons() iconSet { return navIcons() }
