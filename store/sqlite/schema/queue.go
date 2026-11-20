package schema

import (
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer/schema"
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
	Error  string
	Outage bool      // It failed as its backend was unavailable, which isn't an attempt
	Failed time.Time // When it failed
	After  time.Time // When it's due again
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
	outage := 0
	if failure.Outage {
		outage = 1
	}
	bind.Set("outage", outage)
	bind.Set("failed", failure.Failed.UTC())
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
	if err := row.Scan(&work.Key, &kind, &work.After, &work.Attempts, &work.Outages, &work.Error, &work.Leased, &work.Revision); err != nil {
		return err
	}
	work.Kind = store.Kind(kind)
	*list = append(*list, work)
	return nil
}

// QueueDid is the revision of a file which a task's work was done of
type QueueDid struct {
	Volume   string
	Key      string
	Kind     store.Kind
	Revision string
	Error    string // Why it was given up, or empty when it was done
	Now      time.Time
}

// QueueTasks is the work of a task of a volume's files, which is read
type QueueTasks struct {
	Volume string
	Kind   store.Kind
	filer.TaskState
}

// QueueStale is the work of a task of the files beneath a folder, which are
// stale
type QueueStale struct {
	Volume string
	Folder string
	Kind   store.Kind
	Now    time.Time
}

// Did returns the kind of the rows of the revisions which a task's work is
// done of, which is its kind, with _done after it
func Did(kind store.Kind) string {
	return string(kind) + "_done"
}

// Insert binds the file, and the kind of its task
func (did QueueDid) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("volume", did.Volume)
	bind.Set("key", did.Key)
	bind.Set("did", Did(did.Kind))
	bind.Set("revision", did.Revision)
	bind.Set("error", did.Error)
	bind.Set("now", did.Now.UTC())
	return bind.Query("queue.did"), nil
}

// Select binds the volume, and the kinds of the task's work, and what it did
func (tasks QueueTasks) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("volume", tasks.Volume)
	bind.Set("kind", string(tasks.Kind))
	bind.Set("did", Did(tasks.Kind))
	switch op {
	case sqlite.Get:
		return bind.Query("queue.tasks"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("tasks: %v", op)
}

// Scan reads the work of the task
func (tasks *QueueTasks) Scan(row sqlite.Row) error {
	return row.Scan(&tasks.Queued, &tasks.Indexed, &tasks.Failed, &tasks.Error)
}

func (did QueueDid) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("what a task did is put")
}

// Insert binds the folder, and the kind of the task
func (stale QueueStale) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("volume", stale.Volume)
	bind.Set("kind", string(stale.Kind))
	bind.Set("did", Did(stale.Kind))
	bind.Set("now", stale.Now.UTC())
	lower, upper := store.Subtree(stale.Folder)
	bind.Set("lower", lower)
	bind.Set("upper", upper)
	return bind.Query("queue.push_stale"), nil
}

func (stale QueueStale) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("stale work is pushed")
}
