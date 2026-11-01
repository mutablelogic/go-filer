package fs

import (
	"context"
	"errors"
	"io/fs"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	fsevents "github.com/mutablelogic/go-filer/filer/fs/fsevents"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// pathFS is a filesystem rooted at a directory, which can be watched
type pathFS interface {
	Path() string
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects sends each change to the objects in the filesystem to the
// channel, until the context is cancelled, or the filesystem is closed, when
// fs.ErrClosed is returned. Changes to files which aren't listed, such as
// hidden files, aren't sent, and a change within a package is an update to
// the package. Returns filer.ErrNotImplemented when the filesystem isn't a
// directory, or can't be watched on this platform.
func (self *FS) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if err := self.checkRequest(ctx); err != nil {
		return err
	}
	fsys, ok := self.fs.(pathFS)
	if !ok || fsys.Path() == "" {
		return filer.ErrNotImplemented.With("the filesystem isn't a directory, so can't be watched")
	}

	// Stop watching when the filesystem is closed
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-self.done:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Watch the directory, until the context is cancelled or watching fails
	events, done := make(chan fsevents.Event), make(chan error, 1)
	go func() {
		done <- fsevents.Watch(ctx, fsys.Path(), events)
	}()
	for {
		select {
		case err := <-done:
			if self.isClosed() {
				return fs.ErrClosed
			} else if errors.Is(err, fsevents.ErrNotSupported) {
				return filer.ErrNotImplemented.Withf("%w", err)
			}
			return err
		case event := <-events:
			if event, ok := self.event(event); ok {
				// Send the event to the channel, but don't block when the
				// context is done
				select {
				case ch <- event:
				case <-ctx.Done():
				}
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// event returns the change to an object for the change to a file, or false
// when the file isn't listed. The changes to files are the changes to
// objects, with the same values (see Test_eventTypes). A change within a package is an update to the
// package, which is a file. A file which is created or updated must be one
// which is listed, so not a socket, or a link which is broken or leads out of
// the filesystem, but a deleted file can't be checked, so only its name is.
func (self *FS) event(event fsevents.Event) (schema.Event, bool) {
	name, eventType := event.Path, schema.EventType(event.Op)
	if pkg, ok := self.packageOf(name); ok {
		name, eventType = pkg, schema.EventTypeUpdated
	}
	switch {
	case eventType < schema.EventTypeCreated || eventType > schema.EventTypeRescan:
		return schema.Event{}, false
	case name != "" && self.skipPath(name):
		return schema.Event{}, false
	case name != "" && (eventType == schema.EventTypeCreated || eventType == schema.EventTypeUpdated):
		if _, err := self.stat("watch", name); err != nil {
			return schema.Event{}, false
		}
	}
	return schema.Event{
		EventType: eventType,
		ObjectKey: schema.ObjectKey{Path: "/" + name},
	}, true
}
