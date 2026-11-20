package indexer

import (
	"context"
	"errors"
	"io/fs"
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
// changes, or ErrBadParameter (of errors.Is) when the file can't be read, as
// it isn't valid, such as a PDF which is cut short, which is given up at
// once, as doing it again wouldn't change it. Failures are logged, to the
// logger which Run is given. The object is the index's.
type Task func(ctx context.Context, backend gofiler.Filer, store MetadataStore, object *schema.Object) error

// MetadataStore is where a task stores the metadata it finds of a volume's
// files, of its kind, which a read of a file merges with the backend's,
// replacing the backend's of the same namespace and key
type MetadataStore interface {
	// SetMetadata replaces what the task found of the file of the key: its
	// metadata, the shapes of where it is, such as where a photo was taken,
	// and the times of its events, or returns ErrNotFound when the file
	// isn't indexed, or ErrBadParameter when a shape, or an occurrence,
	// isn't valid
	SetMetadata(ctx context.Context, key string, found schema.TaskMeta) error
}

// taskStore is the metadata store of a task, of a volume, of its kind, of
// the file it's done of, and the version of it which it read, so a task
// which is superseded, as the file changed since, doesn't replace what a
// later one found
type taskStore struct {
	store  store.MetadataStore
	volume string
	kind   store.Kind
	key    string
	of     schema.ObjectAttr
}

var _ MetadataStore = taskStore{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// taskLease is how long a task's work is leased for, which is of one
	// file, which can be slow to read, such as a large video of a bucket
	taskLease = 10 * time.Minute

	// taskAttempts is how many times a task fails of a file before it's
	// given up, until the file changes, apart from when its backend is
	// unavailable, which is tried until it isn't
	taskAttempts = 5

	// DefaultTaskWorkers is how many workers do the work of tasks
	DefaultTaskWorkers = 2
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// SetMetadata replaces the metadata the task found of the file of the key,
// which is the file it's done of, when it's the version it read
func (self taskStore) SetMetadata(ctx context.Context, key string, found schema.TaskMeta) error {
	if key != self.key {
		return gofiler.ErrBadParameter.Withf("a task of %q sets the metadata of %q", self.key, key)
	}
	return self.store.SetMetadata(ctx, self.volume, key, string(self.kind), &self.of, found)
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

// tasker does the work of a task, until the context is cancelled, of one
// file at a time, which is done, or failed, once it's done, so its lease is
// of one file
func (self *Indexer) tasker(ctx context.Context, kind store.Kind) {
	task := self.task(kind)
	for {
		claimed, err := self.store.Claim(ctx, self.name, []store.Kind{kind}, 1, taskLease)
		if ctx.Err() != nil {
			return
		} else if err != nil || len(claimed) == 0 {
			if !self.idleTask(ctx, workIdle) {
				return
			}
			continue
		}
		w := claimed[0]
		err = self.doTask(ctx, task, kind, w)
		switch {
		case ctx.Err() != nil:
			self.release(claimed)
			return
		case err != nil && retry(err, w.Attempts+1):
			self.log().DebugContext(ctx, "a task failed of a file, which is tried again", "task", string(kind), "key", w.Key, "attempt", w.Attempts+1, "error", err.Error())
			_ = self.store.Failed(ctx, self.name, w, err, min(self.retry<<min(w.Attempts, 16), maxRetry))
		default:
			// It's done, or given up, until the file changes, with why, as
			// it failed too often, or can't be read, which won't change
			if err != nil {
				w.Error = err.Error()
				self.log().WarnContext(ctx, "a task gave up of a file, until it changes", "task", string(kind), "key", w.Key, "attempts", w.Attempts+1, "error", err.Error())
			} else {
				w.Error = ""
			}
			if err := self.store.Done(context.WithoutCancel(ctx), self.name, w); err != nil {
				self.failed(ctx, err)
			}
		}
	}
}

// doTask does the task of the file of the work, which is done when the file
// isn't indexed, or isn't there, any more, or changed since it was read, as
// its change queued the task again
func (self *Indexer) doTask(ctx context.Context, task Task, kind store.Kind, w store.Work) error {
	object, err := self.store.GetObject(ctx, self.name, w.Key)
	if errors.Is(err, gofiler.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	} else if object.IsDir {
		return nil
	}
	object.Name = self.name
	ts := taskStore{store: self.store, volume: self.name, kind: kind, key: w.Key, of: object.ObjectAttr}
	if err := task(ctx, self.Filer, ts, object); errors.Is(err, gofiler.ErrNotFound) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, gofiler.ErrPreconditionFailed) {
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

// retry returns true when a task which failed of a file is tried again: an
// error of its backend, which is unavailable, such as a server which is
// busy, or offline, for a while, is, however many times it fails, at most
// once each maxRetry, and others, apart from a file which can't be read, until
// it's failed taskAttempts times
func retry(err error, attempts uint) bool {
	switch {
	case errors.Is(err, gofiler.ErrBadParameter):
		return false
	case errors.Is(err, gofiler.ErrServiceUnavailable):
		return true
	default:
		return attempts < taskAttempts
	}
}
