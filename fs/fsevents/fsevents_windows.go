package fsevents

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"unsafe"

	// Packages
	windows "golang.org/x/sys/windows"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// watcher watches with ReadDirectoryChangesW, which has one handle for the
// directory and everything beneath it. It doesn't report the directory itself
// being renamed, so the directory's parent is watched too. Reads are
// overlapped, so that they can be stopped.
type watcher struct {
	root string

	// The directory, and its identity, which is checked when its parent
	// changes, and its parent, unless it's the root of a drive
	dir    *read
	id     identity
	parent *read

	// The event which is signalled when the context is cancelled
	cancel windows.Handle
}

// read is a directory which is read for changes, with the read which is
// pending, which signals its event when it's complete
type read struct {
	handle     windows.Handle
	buf        []byte
	subtree    bool
	mask       uint32
	overlapped windows.Overlapped
	pending    bool
}

// identity is what identifies a directory, as its path can change
type identity struct {
	volume uint32
	index  uint64
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Watching is supported on this platform
	supported = true

	// The changes watched for: names of files and directories, and when
	// files are written, or their attributes or permissions change
	watchMask = windows.FILE_NOTIFY_CHANGE_FILE_NAME | windows.FILE_NOTIFY_CHANGE_DIR_NAME |
		windows.FILE_NOTIFY_CHANGE_LAST_WRITE | windows.FILE_NOTIFY_CHANGE_ATTRIBUTES |
		windows.FILE_NOTIFY_CHANGE_SECURITY

	// The changes watched for in the parent, where the directory is renamed
	// or deleted
	parentMask = windows.FILE_NOTIFY_CHANGE_DIR_NAME

	// The size of the buffer changes are read into, which is the most a
	// network share can return, and the parent's, which is only waited on
	readSize   = 64 * 1024
	parentSize = 4 * 1024
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newWatcher(root string) (*watcher, error) {
	w := &watcher{root: root}
	var err error
	if w.cancel, err = windows.CreateEvent(nil, 1, 0, nil); err != nil {
		return nil, err
	}

	// Read the directory, and everything beneath it, so that changes are
	// kept until they're read
	if w.dir, err = newRead(root, true, watchMask, readSize); err != nil {
		return nil, errors.Join(err, w.close())
	} else if w.id, err = identityOf(w.dir.handle); err != nil {
		return nil, errors.Join(err, w.close())
	}

	// Read the parent, unless the directory is the root of a drive
	if parent := filepath.Dir(root); parent != root {
		if w.parent, err = newRead(parent, false, parentMask, parentSize); err != nil {
			return nil, errors.Join(err, w.close())
		}
	}
	return w, nil
}

// newRead opens the directory, and starts reading it for the changes
func newRead(path string, subtree bool, mask uint32, size int) (*read, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(name, windows.FILE_LIST_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OVERLAPPED, 0)
	if err != nil {
		return nil, &os.PathError{Op: "watch", Path: path, Err: err}
	}
	r := &read{handle: handle, buf: make([]byte, size), subtree: subtree, mask: mask}

	// The event, which is reset by hand
	if r.overlapped.HEvent, err = windows.CreateEvent(nil, 1, 0, nil); err != nil {
		return nil, errors.Join(err, r.close())
	} else if err := r.read(); err != nil {
		return nil, errors.Join(err, r.close())
	}
	return r, nil
}

func (w *watcher) close() error {
	var result error
	for _, r := range []*read{w.dir, w.parent} {
		if r != nil {
			result = errors.Join(result, r.close())
		}
	}
	if w.cancel != 0 {
		result = errors.Join(result, windows.CloseHandle(w.cancel))
		w.cancel = 0
	}
	return result
}

// close stops the pending read, and waits for it to stop, before the buffer
// it reads into is released, and closes the directory
func (r *read) close() error {
	if r.pending {
		var n uint32
		windows.CancelIoEx(r.handle, &r.overlapped)
		windows.GetOverlappedResult(r.handle, &r.overlapped, &n, true)
		r.pending = false
	}
	var result error
	for _, handle := range []*windows.Handle{&r.handle, &r.overlapped.HEvent} {
		if *handle != 0 {
			result = errors.Join(result, windows.CloseHandle(*handle))
			*handle = 0
		}
	}
	return result
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (w *watcher) run(ctx context.Context, fn func(Event)) error {
	// Signal the cancel event when the context is cancelled, and wait for
	// that to finish before returning, as the event is closed afterwards
	called := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(called)
		windows.SetEvent(w.cancel)
	})
	defer func() {
		if !stop() {
			<-called
		}
	}()

	// Wait for the directory's changes, the parent's, or the context
	handles := []windows.Handle{w.dir.overlapped.HEvent, w.cancel}
	if w.parent != nil {
		handles = append(handles, w.parent.overlapped.HEvent)
	}
	for {
		event, err := windows.WaitForMultipleObjects(handles, false, windows.INFINITE)
		if err != nil {
			return err
		}
		switch event {
		case windows.WAIT_OBJECT_0:
			if err := w.changes(fn); err != nil {
				return err
			}
		case windows.WAIT_OBJECT_0 + 1:
			return nil
		case windows.WAIT_OBJECT_0 + 2:
			if err := w.parentChanged(); err != nil {
				return err
			}
		}
	}
}

// changes reads the changes to the directory, and reads the next. Reading
// fails when the directory is being deleted, which can be once its changes
// have been read, as what's in it is deleted first.
func (w *watcher) changes(fn func(Event)) error {
	// None means changes were lost, as they didn't fit in the buffer, so
	// everything is rescanned
	n, err := w.dir.result()
	switch {
	case errors.Is(err, windows.ERROR_NOTIFY_ENUM_DIR) || (err == nil && n == 0):
		fn(Event{Op: Rescan, IsDir: true})
	case err != nil:
		return w.removed(err)
	default:
		w.events(w.dir.buf[:n], fn)
	}
	return w.removed(w.dir.next())
}

// removed returns ErrRootRemoved when reading the directory failed, as it's no
// longer at the root's path, such as when it's being deleted, or the error
func (w *watcher) removed(err error) error {
	if err != nil && (errors.Is(err, windows.ERROR_ACCESS_DENIED) || !w.exists()) {
		return ErrRootRemoved
	}
	return err
}

// parentChanged checks the directory is still where it was, when a directory
// in its parent is renamed or deleted, and reads the parent's next changes
func (w *watcher) parentChanged() error {
	if _, err := w.parent.result(); err != nil && !errors.Is(err, windows.ERROR_NOTIFY_ENUM_DIR) {
		return err
	} else if !w.exists() {
		return ErrRootRemoved
	}
	return w.parent.next()
}

// exists returns true when the directory at the root's path is the one which
// is watched
func (w *watcher) exists() bool {
	name, err := windows.UTF16PtrFromString(w.root)
	if err != nil {
		return false
	}
	handle, err := windows.CreateFile(name, windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return false
	}
	defer windows.CloseHandle(handle)
	id, err := identityOf(handle)
	return err == nil && id == w.id
}

// read starts reading the changes to the directory
func (r *read) read() error {
	if err := windows.ReadDirectoryChanges(r.handle, &r.buf[0], uint32(len(r.buf)), r.subtree, r.mask, nil, &r.overlapped, 0); err != nil {
		return os.NewSyscallError("ReadDirectoryChangesW", err)
	}
	r.pending = true
	return nil
}

// result returns the number of bytes of changes read, once the read is
// complete
func (r *read) result() (uint32, error) {
	var n uint32
	err := windows.GetOverlappedResult(r.handle, &r.overlapped, &n, false)
	r.pending = false
	return n, err
}

// next starts reading the next changes
func (r *read) next() error {
	if err := windows.ResetEvent(r.overlapped.HEvent); err != nil {
		return err
	}
	return r.read()
}

// identityOf returns the identity of the open directory
func identityOf(handle windows.Handle) (identity, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return identity{}, err
	}
	return identity{volume: info.VolumeSerialNumber, index: uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)}, nil
}

// events reports the changes read
func (w *watcher) events(buf []byte, fn func(Event)) {
	for offset := uint32(0); int(offset) < len(buf); {
		info := (*windows.FileNotifyInformation)(unsafe.Pointer(&buf[offset]))
		name := windows.UTF16ToString(unsafe.Slice(&info.FileName, info.FileNameLength/2))
		w.event(info.Action, filepath.ToSlash(name), fn)
		if info.NextEntryOffset == 0 {
			break
		}
		offset += info.NextEntryOffset
	}
}

// event reports a change to the path, relative to the root. A directory which
// is modified, as what's in it changed, isn't reported.
func (w *watcher) event(action uint32, rel string, fn func(Event)) {
	switch action {
	case windows.FILE_ACTION_ADDED, windows.FILE_ACTION_RENAMED_NEW_NAME:
		if info, err := os.Lstat(filepath.Join(w.root, filepath.FromSlash(rel))); err != nil {
			return
		} else if info.IsDir() {
			created(w.root, rel, fn, nil)
		} else {
			fn(Event{Op: Create, Path: rel})
		}
	case windows.FILE_ACTION_REMOVED, windows.FILE_ACTION_RENAMED_OLD_NAME:
		fn(Event{Op: Delete, Path: rel})
	case windows.FILE_ACTION_MODIFIED:
		if info, err := os.Lstat(filepath.Join(w.root, filepath.FromSlash(rel))); err == nil && !info.IsDir() {
			fn(Event{Op: Update, Path: rel})
		}
	}
}
