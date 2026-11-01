// Package fsevents watches a directory, and everything beneath it, for files
// and directories which are created, updated and deleted, using the efficient
// mechanism of each platform: FSEvents on macOS, ReadDirectoryChangesW on
// Windows, and inotify on Linux.
package fsevents

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Op is what happened to a file or directory
type Op uint8

// Opt is an option of Watch
type Opt func(*opt)

type opt struct {
	ready func()
}

// Event is a change to a file or directory beneath the watched directory
type Event struct {
	// What happened
	Op Op

	// The path of the file or directory, relative to the watched directory,
	// with forward slashes, or empty for the watched directory itself
	Path string

	// True when the path is a directory. For a delete, it's always false on
	// Windows, which doesn't report whether what was deleted was a directory.
	IsDir bool
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Create is a file or directory which is created, or moved into the
	// watched directory, or renamed to the path. Everything beneath a
	// directory which is moved in is created too.
	Create Op = iota + 1

	// Update is a file whose contents, or attributes, have changed
	Update

	// Delete is a file or directory which is deleted, or moved out of the
	// watched directory, or renamed from the path. Everything beneath a
	// directory is deleted with it, which may not be reported.
	Delete

	// Rescan is a directory whose changes may have been missed, such as
	// when they came faster than they could be read, so everything beneath
	// it should be compared with what's known about it
	Rescan
)

var (
	// ErrRootRemoved is returned when the watched directory is deleted or
	// moved
	ErrRootRemoved = errors.New("the watched directory was removed")

	// ErrNotSupported is returned on a platform which can't be watched
	ErrNotSupported = errors.New("watching isn't supported on this platform")
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Supported reports whether watching is supported on this platform. On
// macOS, it needs cgo. When it isn't, Watch returns ErrNotSupported.
func Supported() bool {
	return supported
}

// Watch sends each change to the directory, and everything beneath it, to the
// channel, in order, until the context is cancelled. Symbolic links aren't
// followed, so a link is reported as a file. Changes wait until the channel
// is ready, and it isn't closed. Returns ErrRootRemoved when the directory is
// deleted or moved, or ErrNotSupported on a platform which can't be watched.
//
// With WithReady, its function is called once the directory is watched,
// before any change is sent, so a change after it returns is sent.
func Watch(ctx context.Context, dir string, ch chan<- Event, opts ...Opt) error {
	var o opt
	for _, fn := range opts {
		fn(&o)
	}

	// The directory's real path, so that paths reported by the platform,
	// which are real, are beneath it
	root, err := filepath.Abs(dir)
	if err != nil {
		return err
	} else if root, err = filepath.EvalSymlinks(root); err != nil {
		return err
	} else if info, err := os.Stat(root); err != nil {
		return err
	} else if !info.IsDir() {
		return &fs.PathError{Op: "watch", Path: dir, Err: errors.New("not a directory")}
	}

	// Start watching, and send the changes until the context is cancelled
	w, err := newWatcher(root)
	if err != nil {
		return err
	} else if o.ready != nil {
		o.ready()
	}
	return errors.Join(w.run(ctx, func(event Event) {
		select {
		case ch <- event:
		case <-ctx.Done():
		}
	}), w.close())
}

// WithReady calls the function once the directory is watched, before any
// change is sent
func WithReady(fn func()) Opt {
	return func(o *opt) {
		o.ready = fn
	}
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (op Op) String() string {
	switch op {
	case Create:
		return "create"
	case Update:
		return "update"
	case Delete:
		return "delete"
	case Rescan:
		return "rescan"
	default:
		return "unknown"
	}
}

func (e Event) String() string {
	if e.IsDir {
		return e.Op.String() + " " + e.Path + "/"
	}
	return e.Op.String() + " " + e.Path
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// created reports the file or directory at the path, relative to the root, as
// created. A directory is walked, and everything beneath it is reported as
// created too, as a directory moved in is reported without what's in it. The
// directory function, if not nil, is called with each directory before what's
// in it is read, so that it can be watched first. A path which no longer
// exists isn't reported.
func created(root, rel string, fn func(Event), dir func(string) error) error {
	return filepath.WalkDir(filepath.Join(root, filepath.FromSlash(rel)), func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			// What's removed while it's walked isn't reported
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
				return nil
			}
			return err
		}
		rel, err := filepath.Rel(root, name)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.IsDir() && dir != nil {
			if err := dir(rel); err != nil {
				return err
			}
		}
		fn(Event{Op: Create, Path: rel, IsDir: entry.IsDir()})
		return nil
	})
}
