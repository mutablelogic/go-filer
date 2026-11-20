package google

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// taskSnapshot is the entity tags of the lists and tasks, by their paths,
// which are compared, to find what changed
type taskSnapshot map[string]string

// taskChange is a change the filer made, which is sent to its watches as
// it's made: a task or list which is deleted, or a task which is created,
// or updated, with its entity tag, and its sequence, in the order the filer
// made them
type taskChange struct {
	eventType schema.EventType
	path      string
	etag      string
	seq       uint64
}

// taskWatch is a watch's changes, which the filer made, which it hasn't
// sent: the latest of each path, so an earlier one isn't sent after a later
// one, and the signal that there are some
type taskWatch struct {
	mu      sync.Mutex
	pending map[string]taskChange
	signal  chan struct{}
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects polls the lists and tasks, at the refresh interval, and sends
// the changes, until the context is cancelled: a list or task which is
// created, or whose entity tag changes, or which is deleted. A list which is
// deleted is the deletion of each of its tasks, and of the list. What the
// filer creates, updates or deletes is sent as it's changed, rather than
// when it's refreshed, and isn't sent again when it is. Returns
// ErrNotImplemented when the refresh interval is zero, and the error of
// reading them, such as an AuthError, when a sign-in is needed.
func (self *TaskFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if self.refreshInterval == 0 {
		return gofiler.ErrNotImplemented.Withf("volume %q isn't watched, as its %s is zero", self.name, paramRefreshInterval)
	}

	// The changes the filer makes, from when it's watching
	watch := self.watch()
	defer self.unwatch(watch)

	// What's there is read, and then the first change is a rescan of the
	// root, so a change after it is sent. A change the filer made before a
	// snapshot is read is in it, so isn't sent.
	seen := self.sequence()
	last, err := self.snapshot(ctx)
	if err != nil {
		return err
	}

	// What was got before it's watched may be out of date, of the snapshot,
	// which isn't sent, so it's got again, of the rescan
	self.clearCache()
	select {
	case ch <- schema.Rescan(self.name):
	case <-ctx.Done():
		return ctx.Err()
	}
	ticker := time.NewTicker(self.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-watch.signal:
			// What the filer changed is sent now, in the order it was made,
			// unless the snapshot has it already, and isn't sent again when
			// it's refreshed
			for _, change := range watch.take(seen) {
				etag, exists := last[change.path]
				switch {
				case change.eventType == schema.EventTypeDeleted && !exists:
					continue
				case change.eventType == schema.EventTypeDeleted:
					delete(last, change.path)
				case exists && etag == change.etag:
					continue
				default:
					last[change.path] = change.etag
					if exists {
						change.eventType = schema.EventTypeUpdated
					} else {
						change.eventType = schema.EventTypeCreated
					}
				}
				select {
				case ch <- self.event(change.eventType, change.path):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			continue
		case <-ticker.C:
		}
		started := self.sequence()
		next, err := self.snapshot(ctx)
		if err != nil {
			return err
		}
		changes := self.changes(last, next)
		if len(changes) > 0 {
			// What was got from Google is out of date, so it's got again,
			// before what changed is sent, and read
			self.clearCache()
		}
		for _, event := range changes {
			select {
			case ch <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		last, seen = next, started
	}
}

// watch returns a watch, which is sent the changes the filer makes, until
// unwatch removes it
func (self *TaskFiler) watch() *taskWatch {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.watches == nil {
		self.watches = make(map[*taskWatch]struct{})
	}
	watch := &taskWatch{pending: make(map[string]taskChange), signal: make(chan struct{}, 1)}
	self.watches[watch] = struct{}{}
	return watch
}

func (self *TaskFiler) unwatch(watch *taskWatch) {
	self.mu.Lock()
	defer self.mu.Unlock()
	delete(self.watches, watch)
}

// sequence returns the sequence of the last change the filer made
func (self *TaskFiler) sequence() uint64 {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.seq
}

// changed numbers the changes the filer made, in order, and gives them to
// its watches, without waiting, where each keeps the latest of a path
func (self *TaskFiler) changed(changes ...taskChange) {
	self.clearCache()
	self.mu.Lock()
	defer self.mu.Unlock()
	for i := range changes {
		self.seq++
		changes[i].seq = self.seq
	}
	for watch := range self.watches {
		watch.mu.Lock()
		for _, change := range changes {
			watch.pending[change.path] = change
		}
		watch.mu.Unlock()
		select {
		case watch.signal <- struct{}{}:
		default:
		}
	}
}

// deleted gives the paths the filer deleted to its watches
func (self *TaskFiler) deleted(paths []string) {
	changes := make([]taskChange, 0, len(paths))
	for _, path := range paths {
		changes = append(changes, taskChange{eventType: schema.EventTypeDeleted, path: path})
	}
	self.changed(changes...)
}

// take returns the changes a watch hasn't sent, in the order they were made,
// without those made before its snapshot was read, which it has
func (self *taskWatch) take(seen uint64) []taskChange {
	self.mu.Lock()
	pending := self.pending
	self.pending = make(map[string]taskChange)
	self.mu.Unlock()
	result := make([]taskChange, 0, len(pending))
	for _, change := range pending {
		if change.seq > seen {
			result = append(result, change)
		}
	}
	slices.SortFunc(result, func(a, b taskChange) int { return cmp.Compare(a.seq, b.seq) })
	return result
}

// snapshot returns the entity tags of the lists, and of their tasks, by
// their paths
func (self *TaskFiler) snapshot(ctx context.Context) (taskSnapshot, error) {
	lists, err := self.client.TaskLists(ctx)
	if err != nil {
		return nil, self.apiError("watch", "/", err)
	}
	snapshot := make(taskSnapshot)
	for _, list := range lists {
		if !validId(list.Id) {
			continue
		}
		tasks, err := self.client.Tasks(ctx, list.Id)
		if err != nil {
			return nil, self.apiError("watch", listPath(list.Id), err)
		}

		// A list's directory is updated when the count of its tasks, of its
		// metadata, changes, as well as its entity tag
		snapshot[listPath(list.Id)] = self.listTag(&list, tasks)
		for _, task := range tasks {
			if self.visible(&task) {
				snapshot[taskFilePath(list.Id, task.Id)] = task.ETag
			}
		}
	}
	return snapshot, nil
}

// changes returns the events of what changed between two snapshots, in the
// order of their paths, so a list is created before its tasks, and its
// tasks are deleted before it
func (self *TaskFiler) changes(last, next taskSnapshot) []schema.Event {
	var created, deleted []schema.Event
	for path, etag := range next {
		if old, exists := last[path]; !exists {
			created = append(created, self.event(schema.EventTypeCreated, path))
		} else if old != etag {
			created = append(created, self.event(schema.EventTypeUpdated, path))
		}
	}
	for path := range last {
		if _, exists := next[path]; !exists {
			deleted = append(deleted, self.event(schema.EventTypeDeleted, path))
		}
	}
	slices.SortFunc(created, func(a, b schema.Event) int { return compare(a.Path, b.Path) })
	slices.SortFunc(deleted, func(a, b schema.Event) int { return compare(b.Path, a.Path) })
	return append(deleted, created...)
}

// event returns an event of a path of the volume
func (self *TaskFiler) event(eventType schema.EventType, path string) schema.Event {
	return schema.Event{EventType: eventType, ObjectKey: schema.ObjectKey{Name: self.name, Path: path}}
}

// compare compares two paths
func compare(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
