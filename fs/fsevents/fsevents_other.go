//go:build !linux && !windows && !(darwin && cgo)

package fsevents

import (
	"context"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// Watching isn't supported on this platform, or on macOS without cgo
const supported = false

///////////////////////////////////////////////////////////////////////////////
// TYPES

// watcher can't watch on this platform, or on macOS without cgo
type watcher struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newWatcher(string) (*watcher, error) {
	return nil, ErrNotSupported
}

func (w *watcher) close() error {
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (w *watcher) run(context.Context, func(Event)) error {
	return ErrNotSupported
}
