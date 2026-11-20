//go:build frontend

package frontend

import "embed"

// EmbedFS is the server's web UI, in app/dist, which make builds
//
//go:embed app/dist
var EmbedFS embed.FS
