//go:build darwin && cgo

package fsevents

/*
#cgo LDFLAGS: -framework CoreServices
#include <CoreServices/CoreServices.h>
#include <dispatch/dispatch.h>
#include <stdint.h>
#include <stdlib.h>

extern void fseventsCallback(uintptr_t handle, size_t n, char **paths, FSEventStreamEventFlags *flags);

static void fseventsStreamCallback(ConstFSEventStreamRef stream, void *info, size_t n, void *paths, const FSEventStreamEventFlags flags[], const FSEventStreamEventId ids[]) {
	fseventsCallback((uintptr_t)info, n, (char **)paths, (FSEventStreamEventFlags *)flags);
}

// fseventsStart starts a stream of the changes to the path, and everything
// beneath it, from now, with an event for each file, on a serial queue
static FSEventStreamRef fseventsStart(uintptr_t handle, const char *path, double latency, dispatch_queue_t *queue) {
	CFStringRef cfpath = CFStringCreateWithCString(NULL, path, kCFStringEncodingUTF8);
	CFArrayRef paths = CFArrayCreate(NULL, (const void **)&cfpath, 1, &kCFTypeArrayCallBacks);
	FSEventStreamContext context = {0, (void *)handle, NULL, NULL, NULL};
	FSEventStreamRef stream = FSEventStreamCreate(NULL, fseventsStreamCallback, &context, paths,
		kFSEventStreamEventIdSinceNow, latency,
		kFSEventStreamCreateFlagFileEvents | kFSEventStreamCreateFlagNoDefer | kFSEventStreamCreateFlagWatchRoot);
	CFRelease(paths);
	CFRelease(cfpath);
	if (stream == NULL) {
		return NULL;
	}
	*queue = dispatch_queue_create("fsevents", DISPATCH_QUEUE_SERIAL);
	FSEventStreamSetDispatchQueue(stream, *queue);
	if (!FSEventStreamStart(stream)) {
		FSEventStreamInvalidate(stream);
		FSEventStreamRelease(stream);
		dispatch_release(*queue);
		return NULL;
	}
	return stream;
}

// fseventsStop stops the stream, once its callbacks have returned
static void fseventsStop(FSEventStreamRef stream, dispatch_queue_t queue) {
	FSEventStreamStop(stream);
	FSEventStreamInvalidate(stream);
	FSEventStreamRelease(stream);
	dispatch_sync(queue, ^{});
	dispatch_release(queue);
}
*/
import "C"

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime/cgo"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// watcher watches with FSEvents, which has one stream for the directory and
// everything beneath it, with an event for each file
type watcher struct {
	root   string
	handle cgo.Handle
	stream C.FSEventStreamRef
	queue  C.dispatch_queue_t

	// The events from the stream's queue, which stop being sent when done is
	// closed
	events chan []rawEvent
	done   chan struct{}
	once   sync.Once

	// The inodes of the paths reported as created, as later events for a
	// path still have the flag that it was created, oldest first
	created map[string]uint64
	order   []string
}

// rawEvent is an event from the stream, with the path and its flags
type rawEvent struct {
	path  string
	flags C.FSEventStreamEventFlags
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Watching is supported on this platform
	supported = true

	// How long the stream waits to send events, in seconds, as events are
	// sent together
	latency = 0.05

	// The most paths remembered as created. When one is forgotten, a later
	// update to it is reported as created.
	maxCreated = 64 * 1024

	// Events which were lost, and which need a rescan
	lostFlags = C.kFSEventStreamEventFlagMustScanSubDirs | C.kFSEventStreamEventFlagUserDropped | C.kFSEventStreamEventFlagKernelDropped

	// Events where an item was created, removed or renamed, and where it
	// was changed
	nameFlags   = C.kFSEventStreamEventFlagItemCreated | C.kFSEventStreamEventFlagItemRemoved | C.kFSEventStreamEventFlagItemRenamed
	changeFlags = C.kFSEventStreamEventFlagItemModified | C.kFSEventStreamEventFlagItemInodeMetaMod |
		C.kFSEventStreamEventFlagItemChangeOwner | C.kFSEventStreamEventFlagItemXattrMod
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newWatcher(root string) (*watcher, error) {
	w := &watcher{root: root, events: make(chan []rawEvent, 16), done: make(chan struct{}), created: make(map[string]uint64)}
	w.handle = cgo.NewHandle(w)

	path := C.CString(root)
	defer C.free(unsafe.Pointer(path))
	if w.stream = C.fseventsStart(C.uintptr_t(w.handle), path, latency, &w.queue); w.stream == nil {
		w.handle.Delete()
		return nil, &os.PathError{Op: "watch", Path: root, Err: errors.New("the FSEvents stream couldn't be started")}
	}
	return w, nil
}

func (w *watcher) close() error {
	w.once.Do(func() {
		close(w.done)
		C.fseventsStop(w.stream, w.queue)
		w.handle.Delete()
	})
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (w *watcher) run(ctx context.Context, fn func(Event)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case events := <-w.events:
			for _, event := range events {
				if err := w.event(event, fn); err != nil {
					return err
				}
			}
		}
	}
}

// event reports an event for the path. FSEvents combines the flags of changes
// to a path, so the path is checked: it's deleted when it doesn't exist,
// created when it was created, removed or renamed and exists, unless it's
// already been reported as created, and otherwise updated when it changed.
func (w *watcher) event(event rawEvent, fn func(Event)) error {
	rel, ok := w.rel(event.path)
	switch {
	case event.flags&C.kFSEventStreamEventFlagRootChanged != 0:
		return ErrRootRemoved
	case event.flags&lostFlags != 0:
		// Events were lost, beneath the path, or anywhere
		if !ok || event.flags&C.kFSEventStreamEventFlagMustScanSubDirs == 0 {
			rel = ""
		}
		fn(Event{Op: Rescan, Path: rel, IsDir: true})
		return nil
	case !ok || rel == "":
		return nil
	}

	info, err := os.Lstat(event.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		delete(w.created, rel)
		fn(Event{Op: Delete, Path: rel, IsDir: event.flags&C.kFSEventStreamEventFlagItemIsDir != 0})
	case err != nil:
		return nil
	case event.flags&nameFlags != 0 && w.create(rel, info):
		if info.IsDir() {
			return created(w.root, rel, fn, nil)
		}
		fn(Event{Op: Create, Path: rel})
	case event.flags&(nameFlags|changeFlags) != 0 && !info.IsDir():
		fn(Event{Op: Update, Path: rel})
	}
	return nil
}

// create returns true when the path is created, which is when it hasn't been
// reported as created with its inode, and remembers it
func (w *watcher) create(rel string, info os.FileInfo) bool {
	var inode uint64
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		inode = stat.Ino
	}
	if existing, exists := w.created[rel]; exists && existing == inode {
		return false
	} else if !exists {
		w.order = append(w.order, rel)
	}
	w.created[rel] = inode

	// Forget the oldest paths, which may have been deleted already
	for len(w.order) > maxCreated {
		delete(w.created, w.order[0])
		w.order = w.order[1:]
	}
	return true
}

// rel returns the path relative to the root, with forward slashes, or false
// when it isn't beneath the root. The root is matched without case, as the
// path has the case of the filesystem. The root can end with a separator,
// when it's the filesystem's root.
func (w *watcher) rel(path string) (string, bool) {
	if strings.EqualFold(path, w.root) {
		return "", true
	}
	prefix := w.root
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	if len(path) > len(prefix) && strings.EqualFold(path[:len(prefix)], prefix) {
		return filepath.ToSlash(path[len(prefix):]), true
	}
	return "", false
}

///////////////////////////////////////////////////////////////////////////////
// CALLBACKS

//export fseventsCallback
func fseventsCallback(handle C.uintptr_t, n C.size_t, paths **C.char, flags *C.FSEventStreamEventFlags) {
	w := cgo.Handle(handle).Value().(*watcher)
	cpaths, cflags := unsafe.Slice(paths, n), unsafe.Slice(flags, n)
	events := make([]rawEvent, n)
	for i := range events {
		events[i] = rawEvent{path: C.GoString(cpaths[i]), flags: cflags[i]}
	}
	select {
	case w.events <- events:
	case <-w.done:
	}
}
