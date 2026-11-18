package google

import (
	"context"
	"slices"
	"strconv"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// taskSnapshot is the entity tags of the lists and tasks, by their paths,
// which are compared, to find what changed
type taskSnapshot map[string]string

// taskChange is a change the filer made, which is sent to its watches as
// it's made: a task or list which is deleted, or a task which is created,
// or updated, with its entity tag
type taskChange struct {
	eventType schema.EventType
	path      string
	etag      string
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// deletedBuffer is how many deletions a watch keeps, which it hasn't sent,
// beyond which they're sent when it refreshes
const changeBuffer = 16

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
	changed := self.watch()
	defer self.unwatch(changed)

	// What's there is read, and then the first change is a rescan of the
	// root, so a change after it is sent
	last, err := self.snapshot(ctx)
	if err != nil {
		return err
	}
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
		case changes := <-changed:
			// What the filer changed is sent now, unless the snapshot has it
			// already, and isn't sent again when it's refreshed
			for _, change := range changes {
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
		next, err := self.snapshot(ctx)
		if err != nil {
			return err
		}
		for _, event := range self.changes(last, next) {
			select {
			case ch <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		last = next
	}
}

// watch returns the channel of a watch, which is sent the changes the
// filer makes, until unwatch removes it
func (self *TaskFiler) watch() chan []taskChange {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.watches == nil {
		self.watches = make(map[chan []taskChange]struct{})
	}
	ch := make(chan []taskChange, changeBuffer)
	self.watches[ch] = struct{}{}
	return ch
}

func (self *TaskFiler) unwatch(ch chan []taskChange) {
	self.mu.Lock()
	defer self.mu.Unlock()
	delete(self.watches, ch)
}

// changed sends the changes the filer made to its watches, without
// waiting, so a watch which is behind is told when it refreshes instead
func (self *TaskFiler) changed(changes ...taskChange) {
	self.mu.Lock()
	defer self.mu.Unlock()
	for ch := range self.watches {
		select {
		case ch <- changes:
		default:
		}
	}
}

// deleted sends the paths the filer deleted to its watches
func (self *TaskFiler) deleted(paths []string) {
	changes := make([]taskChange, 0, len(paths))
	for _, path := range paths {
		changes = append(changes, taskChange{eventType: schema.EventTypeDeleted, path: path})
	}
	self.changed(changes...)
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
		snapshot[listPath(list.Id)] = list.ETag + "/" + strconv.Itoa(countTasks(tasks))
		for _, task := range tasks {
			if !task.Deleted && validId(task.Id) {
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
