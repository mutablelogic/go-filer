// Package frontend is go-filer's web UI: its modules, such as Files, which
// the package @mutablelogic/go-filer exports, and the server's app, which
// make builds into app/dist, and which a command built with the "frontend"
// tag embeds, as EmbedFS. It's tracked, so go mod tidy, which reads every
// tag's imports, finds it before the UI is built.
package frontend
