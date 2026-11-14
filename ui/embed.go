// Package ui embeds the notification console so the API server serves it from the single binary.
package ui

import "embed"

//go:embed *.html *.css *.js
var Files embed.FS
