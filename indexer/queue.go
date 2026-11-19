package indexer

import (
	"context"
	"sync"
	"time"

	// Packages
	store "github.com/mutablelogic/go-filer/store"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// queue is the work of indexing a volume, of the store's queue, which
// outlasts a restart. It wakes the worker when work is pushed, and limits
// how often a folder is rescanned, of when it was last, which it keeps.
type queue struct {
	store    store.QueueStore
	volume   string
	debounce time.Duration // How long work waits, for more of its path, which coalesces with it
	every    time.Duration // How often a folder is rescanned, at most
	now      func() time.Time

	// wake is sent, without waiting, when work is pushed, and tasks when the
	// work of a task is
	wake  chan struct{}
	tasks chan struct{}

	// When each folder was last rescanned, by its path
	mu      sync.Mutex
	scanned map[string]time.Time
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newQueue(s store.QueueStore, volume string, debounce, every time.Duration) *queue {
	return &queue{
		store:    s,
		volume:   volume,
		debounce: debounce,
		every:    every,
		now:      time.Now,
		wake:     make(chan struct{}, 1),
		tasks:    make(chan struct{}, 1),
		scanned:  make(map[string]time.Time),
	}
}

////////////////////////////////////////////////////////////////////////////////
// METHODS

// index queues the index of each key, after the debounce, so a burst of
// changes of a key, within it, is one
func (q *queue) index(ctx context.Context, keys ...string) error {
	after := q.now().Add(q.debounce)
	work := make([]store.Work, 0, len(keys))
	for _, key := range keys {
		work = append(work, store.Work{Key: key, Kind: store.KindIndex, After: after})
	}
	return q.push(ctx, work...)
}

// rescan queues a rescan of the folder's key, after the debounce, and no sooner
// than the minimum interval since it was last rescanned, so it's deferred,
// rather than dropped
func (q *queue) rescan(ctx context.Context, folder string) error {
	after := q.now().Add(q.debounce)
	q.mu.Lock()
	if last, exists := q.scanned[folder]; exists && last.Add(q.every).After(after) {
		after = last.Add(q.every)
	}
	q.mu.Unlock()
	return q.push(ctx, store.Work{Key: folder, Kind: store.KindRescan, After: after})
}

// rescanned records that the folder is rescanned now, for the minimum
// interval of the next
func (q *queue) rescanned(folder string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.scanned[folder] = q.now()
}

// stale queues the work of the task of the kind of the files beneath the
// folder's key which are new, or changed, since it was done of them, and
// wakes its workers
func (q *queue) stale(ctx context.Context, folder string, kind store.Kind) error {
	if err := q.store.PushStale(ctx, q.volume, folder, kind); err != nil {
		return err
	}
	q.wakeTasks()
	return nil
}

// task queues the work of the task of the kind of each key, and wakes its
// workers
func (q *queue) task(ctx context.Context, kind store.Kind, keys ...string) error {
	work := make([]store.Work, 0, len(keys))
	for _, key := range keys {
		work = append(work, store.Work{Key: key, Kind: kind})
	}
	if len(work) == 0 {
		return nil
	} else if err := q.store.Push(ctx, q.volume, work...); err != nil {
		return err
	}
	q.wakeTasks()
	return nil
}

// wakeTasks wakes the workers of the tasks, without waiting
func (q *queue) wakeTasks() {
	select {
	case q.tasks <- struct{}{}:
	default:
	}
}

// push queues the work, and wakes the worker
func (q *queue) push(ctx context.Context, work ...store.Work) error {
	if len(work) == 0 {
		return nil
	}
	if err := q.store.Push(ctx, q.volume, work...); err != nil {
		return err
	}
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return nil
}
