//go:build e2e

// Package main links MyGo's native UI automation plugin for e2e driving and
// screenshot inspection when built with `-tags e2e`. Production builds omit
// this file so release binaries never expose an automation socket.
package main

import _ "github.com/egoist/mygo/plugins/automation"
