package indexer

import (
	"context"
	"errors"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Task enriches a file of a volume: it reads the file from the backend, and
// stores what it finds in the store, of the task's kind, such as its
// metadata. It returns an error when it fails, when it's done again, after a
// while, which doubles with each attempt, until it's given up, until the file
// changes. The object is the index's.
type Task func(ctx context.Context, backend gofiler.Filer, store MetadataStore, object *schema.Object) error

// MetadataStore is where a task stores the metadata it finds of a volume's
// files, of its kind, which a read of a file merges with the backend's,
// replacing the backend's of the same namespace and key
type MetadataStore interface {
	// SetMetadata replaces the metadata the task found of the file of the
	// key, or returns ErrNotFound when the file isn't indexed
	SetMetadata(ctx context.Context, key string, meta []schema.Meta) error
}

// taskStore is the metadata store of a task, of a volume, of its kind
type taskStore struct {
	store  store.MetadataStore
	volume string
	kind   store.Kind
}

var _ MetadataStore = taskStore{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// taskBatch is how much of a task's work is claimed at a time
	taskBatch = 10

	// taskAttempts is how many times a task fails of a file before it's
	// given up, until the file changes
	taskAttempts = 5

	// DefaultTaskWorkers is how many workers do the work of tasks
	DefaultTaskWorkers = 2
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// SetMetadata replaces the metadata the task found of the file of the key
func (self taskStore) SetMetadata(ctx context.Context, key string, meta []schema.Meta) error {
	return self.store.SetMetadata(ctx, self.volume, key, string(self.kind), meta)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// task returns the task of the kind, when the volume's mode does it, or nil
func (self *Indexer) task(kind store.Kind) Task {
	if kind == store.KindMetadata && self.mode.Level() >= ModeMetadata.Level() {
		return self.tasks[kind]
	}
	return nil
}

// tasked returns true when the volume does a task
func (self *Indexer) tasked() bool {
	return self.task(store.KindMetadata) != nil
}

// enrich runs the workers of the tasks, until the context is cancelled,
// which do their work, apart from the work of indexing, so a task which is
// slow doesn't hold up indexing
func (self *Indexer) enrich(ctx context.Context) {
	var wg sync.WaitGroup
	for range max(self.taskWorkers, 1) {
		wg.Go(func() { self.tasker(ctx, store.KindMetadata) })
	}
	wg.Wait()
}

// tasker does the work of a task, until the context is cancelled
func (self *Indexer) tasker(ctx context.Context, kind store.Kind) {
	task := self.task(kind)
	ts := taskStore{store: self.store, volume: self.name, kind: kind}
	for {
		claimed, err := self.store.Claim(ctx, self.name, []store.Kind{kind}, taskBatch, workLease)
		if ctx.Err() != nil {
			return
		} else if err != nil || len(claimed) == 0 {
			if !self.idleTask(ctx, workIdle) {
				return
			}
			continue
		}
		done := make([]store.Work, 0, len(claimed))
		for i, w := range claimed {
			err := self.doTask(ctx, task, ts, w)
			if ctx.Err() != nil {
				self.release(claimed[i:])
				break
			} else if err != nil && w.Attempts+1 < taskAttempts {
				_ = self.store.Failed(ctx, self.name, w, err, min(self.retry<<min(w.Attempts, 16), maxRetry))
			} else {
				// It's done, or given up, until the file changes
				done = append(done, w)
			}
		}
		if err := self.store.Done(context.WithoutCancel(ctx), self.name, done...); err != nil {
			self.failed(ctx, err)
		}
	}
}

// doTask does the task of the file of the work, which is done when the file
// isn't indexed any more
func (self *Indexer) doTask(ctx context.Context, task Task, ts taskStore, w store.Work) error {
	object, err := self.store.GetObject(ctx, self.name, w.Key)
	if errors.Is(err, gofiler.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	} else if object.IsDir {
		return nil
	}
	object.Name = self.name
	if err := task(ctx, self.Filer, ts, object); errors.Is(err, gofiler.ErrNotFound) {
		return nil
	} else {
		return err
	}
}

// idleTask waits for a task's work to be pushed, or the time to pass, and
// returns false when the context is cancelled
func (self *Indexer) idleTask(ctx context.Context, wait time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-self.queue.tasks:
		return true
	case <-time.After(wait):
		return true
	}
}
