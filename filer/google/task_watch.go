package google

import (
	"context"
	"slices"
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

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects polls the lists and tasks, at the refresh interval, and sends
// the changes, until the context is cancelled: a list or task which is
// created, or whose entity tag changes, or which is deleted. A list which is
// deleted is the deletion of each of its tasks, and of the list. Returns
// ErrNotImplemented when the refresh interval is zero, and the error of
// reading them, such as an AuthError, when a sign-in is needed.
func (self *TaskFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if self.refreshInterval == 0 {
		return gofiler.ErrNotImplemented.Withf("volume %q isn't watched, as its %s is zero", self.name, paramRefreshInterval)
	}

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

// snapshot returns the entity tags of the lists, and of their tasks, by
// their paths
func (self *TaskFiler) snapshot(ctx context.Context) (taskSnapshot, error) {
	lists, err := self.client.TaskLists(ctx)
	if err != nil {
		return nil, apiError("watch", "/", err)
	}
	snapshot := make(taskSnapshot)
	for _, list := range lists {
		if !validId(list.Id) {
			continue
		}
		snapshot[listPath(list.Id)] = list.ETag
		tasks, err := self.client.Tasks(ctx, list.Id)
		if err != nil {
			return nil, apiError("watch", listPath(list.Id), err)
		}
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
