package schema

import (
	"time"

	// Packages
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// QueuePush is work of a volume, which is pushed
type QueuePush struct {
	Volume string
	store.Work
}

// QueueDue selects the work of a volume, of the kinds, which is due, and
// isn't leased, at the time, up to the limit
type QueueDue struct {
	Volume string
	Kinds  []store.Kind
	Now    time.Time
	Limit  int
}

// QueueKey selects work of a volume, of its key and kind, which is leased,
// done, or failed, of its lease
type QueueKey struct {
	Volume string
	store.Work
}

// QueueLease is the lease of work, until when it ends
type QueueLease time.Time

// QueueFailure is why work failed, and when it's due again
type QueueFailure struct {
	Error string
	After time.Time
}

// QueuePending selects whether a key of a volume has work which would
// change its index
type QueuePending struct {
	Volume  string
	Key     string
	Beneath bool // Work beneath the folder's key too, of its listing
}

// Pending is whether a key has work which would change its index
type Pending bool

// WorkList is the work which is read
type WorkList []store.Work

////////////////////////////////////////////////////////////////////////////////
// WRITERS

// Insert binds the work, with the folders above it, of a rescan, or of an
// index, and its own key, too, as a rescan of a folder's key does it
func (push QueuePush) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("volume", push.Volume)
	bind.Set("key", push.Key)
	bind.Set("kind", string(push.Kind))
	bind.Set("after", push.After.UTC())
	ancestors := store.Ancestors(push.Key)
	if push.Kind == store.KindRescan {
		lower, upper := store.Subtree(push.Key)
		bind.Set("lower", lower)
		bind.Set("upper", upper)
		bind.Set("ancestors", ancestors)
		return bind.Query("queue.push_rescan"), nil
	}
	bind.Set("ancestors", append(ancestors, push.Key))
	return bind.Query("queue.push_index"), nil
}

func (push QueuePush) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("work is pushed")
}

func (lease QueueLease) Insert(*sqlite.Bind) (string, error) {
	return "", sqlite.ErrNotImplemented.With("work is leased")
}

// Update binds the lease, until when it ends
func (lease QueueLease) Update(bind *sqlite.Bind) error {
	bind.Set("leased", time.Time(lease).UTC())
	return nil
}

func (failure QueueFailure) Insert(*sqlite.Bind) (string, error) {
	return "", sqlite.ErrNotImplemented.With("work fails")
}

// Update binds why the work failed, and when it's due again
func (failure QueueFailure) Update(bind *sqlite.Bind) error {
	bind.Set("error", failure.Error)
	bind.Set("after", failure.After.UTC())
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// SELECTORS

func (due QueueDue) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.List {
		return "", sqlite.ErrNotImplemented.Withf("queue: %v", op)
	}
	kinds := make([]string, 0, len(due.Kinds))
	for _, kind := range due.Kinds {
		kinds = append(kinds, string(kind))
	}
	bind.Set("volume", due.Volume)
	bind.Set("kinds", kinds)
	bind.Set("now", due.Now.UTC())
	if due.Limit > 0 {
		bind.Set("offsetlimit", "LIMIT "+bind.Set("limit", due.Limit))
	}
	return bind.Query("queue.due"), nil
}

// Select binds the work, of its lease, which is leased, done or failed
func (key QueueKey) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("volume", key.Volume)
	bind.Set("key", key.Key)
	bind.Set("kind", string(key.Kind))
	switch op {
	case sqlite.Update:
		// A lease, or a failure, which binds its own
		if bind.Has("leased") && !bind.Has("error") {
			return bind.Query("queue.lease"), nil
		}
		bind.Set("leased", key.Leased.UTC())
		return bind.Query("queue.failed"), nil
	case sqlite.Delete:
		bind.Set("leased", key.Leased.UTC())
		return bind.Query("queue.done"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("queue: %v", op)
}

func (pending QueuePending) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Get {
		return "", sqlite.ErrNotImplemented.Withf("queue pending: %v", op)
	}
	bind.Set("volume", pending.Volume)
	bind.Set("ancestors", append(store.Ancestors(pending.Key), pending.Key))
	lower, upper := store.Subtree(pending.Key)
	bind.Set("beneath", pending.Beneath)
	bind.Set("lower", lower)
	bind.Set("upper", upper)
	return bind.Query("queue.pending"), nil
}

////////////////////////////////////////////////////////////////////////////////
// READERS

func (pending *Pending) Scan(row sqlite.Row) error {
	return row.Scan((*bool)(pending))
}

func (list *WorkList) Scan(row sqlite.Row) error {
	var work store.Work
	var kind string
	if err := row.Scan(&work.Key, &kind, &work.After, &work.Attempts, &work.Error, &work.Leased); err != nil {
		return err
	}
	work.Kind = store.Kind(kind)
	*list = append(*list, work)
	return nil
}
