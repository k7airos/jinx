// Package assets embeds static branding assets for the JINX desktop console.
package assets

import _ "embed"

// LogoPNG holds the embedded PNG bytes for the JINX desktop application logo.
//
//go:embed jinx-logo.png
var LogoPNG []byte
