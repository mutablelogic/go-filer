package fs

import (
	"context"
	"path"

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
//
// The first change is a rescan of the root, once the directory is watched,
// so a change after it is sent. The calls share one watch of the directory,
// so a second call doesn't watch it again: one which joins a watch which is
// running is sent its rescan at once. Each is sent each change, and one
// which falls behind is sent a rescan of the directory, rather than holding
// up the others.
func (self *FS) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if err := self.checkRequest(ctx); err != nil {
		return err
	}
	fsys, ok := self.fs.(pathFS)
	if !ok || fsys.Path() == "" {
		return filer.ErrNotImplemented.With("the filesystem isn't a directory, so can't be watched")
	}

	// Share the directory's watch with the other calls, which starts it,
	// when there's none
	w, err := self.join(ctx, fsys.Path())
	if err != nil {
		return nil
	}
	defer self.leave(w)
	return self.subscribe(ctx, w, ch)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// event returns the change to an object for the change to a file, or false
// when the file isn't listed, of the object's key, which is its path, with a
// "/" after a folder's, which is created, or updated, as a folder which is
// deleted can't be checked. The changes to files are the changes to
// objects, with the same values (see Test_eventTypes). A change within a package is an update to the
// package, which is a file. A file which is created or updated must be one
// which is listed, so not a socket, or a link, or beneath one, but a deleted
// file can't be checked, so only its name is.
func (self *FS) event(event fsevents.Event) (schema.Event, bool) {
	name, eventType := event.Path, schema.EventType(event.Op)
	if pkg, ok := self.packageOf(name); ok {
		name, eventType = pkg, schema.EventTypeUpdated
	}
	key := "/" + name
	switch {
	case eventType < schema.EventTypeCreated || eventType > schema.EventTypeRescan:
		return schema.Event{}, false
	case name != "" && self.skipPath(name):
		return schema.Event{}, false
	case name != "" && (eventType == schema.EventTypeCreated || eventType == schema.EventTypeUpdated):
		info, err := self.stat("watch", name)
		if err != nil {
			return schema.Event{}, false
		} else if info.IsDir() && !self.isPackage(path.Base(name)) {
			key += "/"
		}
	}
	return schema.Event{
		EventType: eventType,
		ObjectKey: schema.ObjectKey{Path: key},
	}, true
}
