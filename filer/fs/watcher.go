package fs

import (
	"context"
	"errors"
	"io/fs"
	"sync/atomic"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	fsevents "github.com/mutablelogic/go-filer/filer/fs/fsevents"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	broadcaster "github.com/mutablelogic/go-server/broadcaster"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// watcher is the watch of a filesystem's directory, which the calls to
// WatchObjects share: the first starts it, the others join it, and the last
// to leave stops it, or closing the filesystem does. Its changes are
// published to a broadcaster, so each call is sent each change, and one
// which falls behind is sent a rescan, rather than holding up the others.
type watcher struct {
	b      *broadcaster.Broadcaster
	cancel context.CancelFunc
	ready  chan struct{} // Closed once the directory is watched
	done   chan struct{} // Closed once the watch ends, when err is why
	err    error
	users  int // The calls which share it, which the filesystem's lock guards
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// watchTable is the table of the changes the broadcaster is sent, which
	// is only the watcher's
	watchTable = "fs"

	// watchQueue is how many changes can wait for each call to WatchObjects,
	// after which it's sent a rescan of the directory
	watchQueue = 1024
)

// watching is how many watches of directories are running, for tests
var watching atomic.Int32

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// join returns the watcher of the directory, which is started when there's
// none, or the last ended, such as when it failed, and records the call
// which shares it, which must leave it
func (self *FS) join(dir string) *watcher {
	self.watchMu.Lock()
	defer self.watchMu.Unlock()
	if w := self.watcher; w != nil {
		select {
		case <-w.done:
		default:
			w.users++
			return w
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcher{b: broadcaster.New(), cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}), users: 1}
	self.watcher = w
	go func() { _ = w.b.Run(ctx) }()
	go self.watch(ctx, w, dir)
	return w
}

// leave records that a call no longer shares the watcher, which stops it,
// once it's the last
func (self *FS) leave(w *watcher) {
	self.watchMu.Lock()
	defer self.watchMu.Unlock()
	if w.users--; w.users == 0 {
		w.cancel()
		if self.watcher == w {
			self.watcher = nil
		}
	}
}

// watch watches the directory, and publishes the changes to its objects,
// until the context is cancelled, or the filesystem is closed, or watching
// fails, when it ends, with why
func (self *FS) watch(ctx context.Context, w *watcher, dir string) {
	watching.Add(1)
	defer watching.Add(-1)
	defer w.cancel()
	events, done := make(chan fsevents.Event), make(chan error, 1)
	go func() {
		done <- fsevents.Watch(ctx, dir, events, fsevents.WithReady(func() { close(w.ready) }))
	}()
	closing := self.done
	for {
		select {
		case err := <-done:
			if self.isClosed() {
				err = fs.ErrClosed
			} else if errors.Is(err, fsevents.ErrNotSupported) {
				err = filer.ErrNotImplemented.Withf("%w", err)
			}
			w.err = err
			close(w.done)
			return
		case <-closing:
			// The filesystem is closed, so the watch is stopped, once
			w.cancel()
			closing = nil
		case event := <-events:
			if event, ok := self.event(event); ok {
				event.Name = watchTable
				w.b.Publish(event.Change())
			}
		}
	}
}

// subscribe sends the changes of the watcher to the channel, once the
// directory is watched, after a rescan of the root, until the context is
// cancelled, when it returns nil, or the filesystem is closed, which is
// fs.ErrClosed, or the watch fails, with why
func (self *FS) subscribe(ctx context.Context, w *watcher, ch chan<- schema.Event) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan changes.Change, watchQueue)
	stopped, err := w.b.Watch(ctx, changes.ChangeFilter{Schema: schema.ChangeSchema, Table: watchTable}, queue)
	if err != nil {
		return self.watchEnded(w)
	}

	// Wait until the directory is watched, so a change after ready is sent
	select {
	case <-w.ready:
	case <-w.done:
		return self.watchEnded(w)
	case <-ctx.Done():
		return nil
	}
	select {
	case ch <- schema.Rescan(""):
	case <-ctx.Done():
		return nil
	}

	// Send the changes, without their table, which is the watcher's
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.done:
			return self.watchEnded(w)
		case <-stopped:
			return self.watchEnded(w)
		case change := <-queue:
			event, ok := schema.ChangeEvent(change)
			if !ok {
				continue
			}
			event.Name = ""
			select {
			case ch <- event:
			case <-ctx.Done():
				return nil
			case <-w.done:
				return self.watchEnded(w)
			}
		}
	}
}

// watchEnded returns why the watcher ended, which is fs.ErrClosed when the
// filesystem is closed, once it has
func (self *FS) watchEnded(w *watcher) error {
	<-w.done
	if self.isClosed() {
		return fs.ErrClosed
	}
	return w.err
}
