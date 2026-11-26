// Package assets embeds static branding assets for the JINX desktop console.
package assets

import _ "embed"

// LogoPNG holds the embedded PNG bytes for the JINX desktop application logo.
//
//go:embed jinx-logo.png
var LogoPNG []byte

// AppIconPNG holds the Apple HIG padded squircle PNG used for the macOS Dock
// and window icon so it matches standard macOS application proportions.
//
//go:embed jinx-app-icon.png
var AppIconPNG []byte
