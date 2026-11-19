package fsevents

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unsafe"

	// Packages
	unix "golang.org/x/sys/unix"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// watcher watches with inotify, which has a watch for each directory, all
// read from one file descriptor. The descriptor is kept, as getting it from
// the file would make the file blocking, so closing it wouldn't stop a read.
type watcher struct {
	root string
	fd   int
	file *os.File

	// The directory of each watch, relative to the root, and the watch of
	// each directory
	dirs map[int32]string
	wds  map[string]int32

	// The watch of the root's parent, or -1, and the root's name in it. The
	// root's own watch doesn't report it's removed while it's open, such as
	// by a filesystem rooted at it, so its parent does.
	parent int32
	base   string
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Watching is supported on this platform
	supported = true

	// The changes watched for in each directory. Files are updated when
	// they're closed after writing, rather than on each write, and when their
	// attributes change, such as their modified time.
	watchMask = unix.IN_CREATE | unix.IN_DELETE | unix.IN_MOVED_FROM | unix.IN_MOVED_TO |
		unix.IN_CLOSE_WRITE | unix.IN_ATTRIB | unix.IN_DELETE_SELF | unix.IN_MOVE_SELF |
		unix.IN_ONLYDIR | unix.IN_DONT_FOLLOW | unix.IN_EXCL_UNLINK

	// The changes watched for in the root's parent, which are the root being
	// deleted or moved, or the parent itself
	parentMask = unix.IN_DELETE | unix.IN_MOVED_FROM | unix.IN_DELETE_SELF | unix.IN_MOVE_SELF |
		unix.IN_ONLYDIR | unix.IN_DONT_FOLLOW

	// The size of the buffer events are read into, which holds hundreds
	readSize = 64 * 1024
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newWatcher(root string) (*watcher, error) {
	// The file descriptor is non-blocking, so it's read with the runtime's
	// poller, and closing it stops a read
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return nil, os.NewSyscallError("inotify_init1", err)
	}
	w := &watcher{
		root:   root,
		fd:     fd,
		file:   os.NewFile(uintptr(fd), "inotify"),
		dirs:   make(map[int32]string),
		wds:    make(map[string]int32),
		parent: -1,
	}

	// Watch every directory
	if err := w.watchAll(); err != nil {
		return nil, errors.Join(err, w.file.Close())
	}

	// Watch the root's parent, for the root being removed, when it can be,
	// such as when the root isn't the top of the filesystem
	if abs, err := filepath.Abs(root); err == nil {
		if parent := filepath.Dir(abs); parent != abs {
			if wd, err := unix.InotifyAddWatch(fd, parent, parentMask); err == nil {
				w.parent, w.base = int32(wd), filepath.Base(abs)
			}
		}
	}
	return w, nil
}

func (w *watcher) close() error {
	if err := w.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (w *watcher) run(ctx context.Context, fn func(Event)) error {
	// Closing the file stops the read when the context is cancelled
	stop := context.AfterFunc(ctx, func() { w.file.Close() })
	defer stop()

	buf := make([]byte, readSize)
	for {
		n, err := w.file.Read(buf)
		if ctx.Err() != nil {
			return nil
		} else if err != nil {
			return err
		}
		if err := w.events(buf[:n], fn); err != nil {
			return err
		}
	}
}

// events reports the events read
func (w *watcher) events(buf []byte, fn func(Event)) error {
	for len(buf) >= unix.SizeofInotifyEvent {
		event := (*unix.InotifyEvent)(unsafe.Pointer(&buf[0]))
		end := unix.SizeofInotifyEvent + int(event.Len)
		name := string(bytes.TrimRight(buf[unix.SizeofInotifyEvent:end], "\x00"))
		buf = buf[end:]
		if err := w.event(event.Wd, event.Mask, name, fn); err != nil {
			return err
		}
	}
	return nil
}

// event reports an event for the name in the directory of the watch
func (w *watcher) event(wd int32, mask uint32, name string, fn func(Event)) error {
	// Events were lost, so watch any directories which aren't watched, and
	// rescan everything
	if mask&unix.IN_Q_OVERFLOW != 0 {
		if err := w.watchAll(); err != nil {
			return err
		}
		fn(Event{Op: Rescan, IsDir: true})
		return nil
	}

	// The root is deleted or moved, or its parent is, which the root's
	// parent reports
	if w.parent >= 0 && wd == w.parent {
		switch {
		case mask&unix.IN_IGNORED != 0:
			w.parent = -1
		case mask&(unix.IN_DELETE_SELF|unix.IN_MOVE_SELF) != 0:
			return ErrRootRemoved
		case mask&(unix.IN_DELETE|unix.IN_MOVED_FROM) != 0 && name == w.base:
			return ErrRootRemoved
		}
		return nil
	}

	// The directory of the watch
	dir, exists := w.dirs[wd]
	switch {
	case !exists:
		return nil
	case mask&unix.IN_IGNORED != 0:
		// The watch is removed, when its directory is deleted, or it's
		// unwatched
		delete(w.dirs, wd)
		if w.wds[dir] == wd {
			delete(w.wds, dir)
		}
		return nil
	case mask&(unix.IN_DELETE_SELF|unix.IN_MOVE_SELF) != 0:
		// A directory is deleted or moved, which its parent reports, unless
		// it's the root
		if dir == "" {
			return ErrRootRemoved
		}
		return nil
	}

	// The file or directory in the directory
	rel, isDir := join(dir, name), mask&unix.IN_ISDIR != 0
	switch {
	case mask&(unix.IN_CREATE|unix.IN_MOVED_TO) != 0:
		if isDir {
			return created(w.root, rel, fn, w.watch)
		}
		fn(Event{Op: Create, Path: rel})
	case mask&(unix.IN_DELETE|unix.IN_MOVED_FROM) != 0:
		// A directory moved out is still watched, so its watches are removed
		if isDir {
			w.unwatch(rel)
		}
		fn(Event{Op: Delete, Path: rel, IsDir: isDir})
	case mask&(unix.IN_CLOSE_WRITE|unix.IN_ATTRIB) != 0 && !isDir:
		fn(Event{Op: Update, Path: rel})
	}
	return nil
}

// watchAll watches every directory beneath the root, and stops watching
// directories which no longer exist
func (w *watcher) watchAll() error {
	for dir, wd := range w.wds {
		if info, err := os.Lstat(filepath.Join(w.root, filepath.FromSlash(dir))); err != nil || !info.IsDir() {
			unix.InotifyRmWatch(w.fd, uint32(wd))
			delete(w.wds, dir)
			delete(w.dirs, wd)
		}
	}
	return filepath.WalkDir(w.root, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
				return nil
			}
			return err
		} else if !entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(w.root, name)
		if err != nil {
			return err
		} else if rel == "." {
			rel = ""
		}
		return w.watch(filepath.ToSlash(rel))
	})
}

// watch watches the directory, relative to the root. A directory which no
// longer exists isn't watched.
func (w *watcher) watch(dir string) error {
	wd, err := unix.InotifyAddWatch(w.fd, filepath.Join(w.root, filepath.FromSlash(dir)), watchMask)
	switch {
	case errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.EACCES):
		return nil
	case errors.Is(err, unix.ENOSPC):
		return fmt.Errorf("watch %q: too many directories to watch, so raise fs.inotify.max_user_watches: %w", dir, err)
	case err != nil:
		return &fs.PathError{Op: "watch", Path: dir, Err: err}
	}

	// A directory moved within the root keeps its watch, under its new path,
	// and a directory replaced at the path, when events were lost, has its
	// watch removed, as it may have moved out of the root
	if old, exists := w.dirs[int32(wd)]; exists && w.wds[old] == int32(wd) {
		delete(w.wds, old)
	}
	if old, exists := w.wds[dir]; exists && old != int32(wd) {
		unix.InotifyRmWatch(w.fd, uint32(old))
		delete(w.dirs, old)
	}
	w.dirs[int32(wd)], w.wds[dir] = dir, int32(wd)
	return nil
}

// unwatch stops watching the directory, relative to the root, and the
// directories beneath it
func (w *watcher) unwatch(dir string) {
	for rel, wd := range w.wds {
		if beneath(rel, dir) {
			unix.InotifyRmWatch(w.fd, uint32(wd))
			delete(w.wds, rel)
			delete(w.dirs, wd)
		}
	}
}

// join returns the path of the name in the directory, relative to the root
func join(dir, name string) string {
	if dir == "" {
		return name
	}
	return path.Join(dir, name)
}

// beneath returns true when the path is the directory, or beneath it
func beneath(rel, dir string) bool {
	return dir == "" || rel == dir || strings.HasPrefix(rel, dir+"/")
}
