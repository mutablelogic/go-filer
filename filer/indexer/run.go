package indexer

import (
	"context"
	"errors"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Run indexes the backend's folders, and follows its changes, until the
// context is cancelled. It watches the backend, and scans it once it's
// watching, so a change while it scans isn't missed, and scans a folder
// again when the backend says its changes may have been missed. A backend
// which can't be watched is scanned again at the rescan interval. When
// watching fails, it's watched, and scanned, again, after a while, which
// doubles each time, up to a minute. The changes are queued, in the store,
// and indexed by a worker, in the background, so the work which is queued
// when it stops is done when it runs again.
func (self *Indexer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	worked := make(chan struct{})
	go func() {
		defer close(worked)
		self.work(ctx)
	}()
	defer func() {
		cancel()
		<-worked
	}()
	retry := self.retry
	for {
		err := self.watch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, gofiler.ErrNotImplemented) {
			return self.poll(ctx)
		}
		self.failed(ctx, err)
		self.fail(err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retry):
		}
		retry = min(retry*2, maxRetry)
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// watch watches the backend, and scans it, once it's watching, which is its
// first change, a rescan of the root, and follows its changes, until
// watching fails, or the context is cancelled
func (self *Indexer) watch(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, done := make(chan schema.Event, 64), make(chan error, 1)
	go func() {
		done <- self.Filer.WatchObjects(ctx, events)
	}()
	for {
		select {
		case err := <-done:
			return err
		case event := <-events:
			// The change is queued, which is quick, and then sent to the
			// watchers, who aren't held up by indexing it, which is done in
			// the background, and a watcher which doesn't read its changes
			// doesn't hold up the index. When it can't be queued, the watch
			// is started again, with a rescan, so it isn't missed.
			if err := self.apply(ctx, event); err != nil {
				return err
			}
			self.forward(ctx, event)
		}
	}
}

// poll queues a scan of a backend which can't be watched, now, and at the
// rescan interval, until the context is cancelled, and tells its watchers
// to compare what they know with it, as its changes aren't known
func (self *Indexer) poll(ctx context.Context) error {
	for {
		if err := self.queue.rescan(ctx, "/"); err != nil && ctx.Err() == nil {
			self.failed(ctx, err)
		}
		self.forward(ctx, schema.Rescan(self.name))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(self.rescanInterval):
		}
	}
}

// apply queues the work of a change to the backend: a rescan of its folder,
// or the index of an object which is created, updated or deleted
func (self *Indexer) apply(ctx context.Context, event schema.Event) error {
	switch event.EventType {
	case schema.EventTypeRescan:
		return self.queue.rescan(ctx, folderPath(event.Path))
	case schema.EventTypeCreated, schema.EventTypeUpdated, schema.EventTypeDeleted:
		return self.queue.index(ctx, folderPath(event.Path))
	default:
		return nil
	}
}

// work does the work of the queue, until the context is cancelled: a
// rescan scans the folder, and an index refreshes the object, with its
// folders. Work which fails is done again, after a while, which doubles
// with each attempt, and work which isn't done when it's cancelled is
// pushed again, so it's done when it runs again, rather than once its
// lease has passed.
func (self *Indexer) work(ctx context.Context) {
	retry := self.retry
	for {
		claimed, err := self.store.Claim(ctx, self.name, kinds, workBatch, workLease)
		if ctx.Err() != nil {
			return
		} else if err != nil {
			self.failed(ctx, err)
			if !self.idle(ctx, retry) {
				return
			}
			retry = min(retry*2, maxRetry)
			continue
		}
		retry = self.retry
		if len(claimed) == 0 {
			if !self.idle(ctx, workIdle) {
				return
			}
			continue
		}

		// Do the work, which is done, or failed, of its claim
		done := make([]store.Work, 0, len(claimed))
		for i, w := range claimed {
			err := self.do(ctx, w)
			if ctx.Err() != nil {
				self.release(claimed[i:])
				break
			} else if err != nil {
				self.failed(ctx, err)
				_ = self.store.Failed(ctx, self.name, w, err, min(self.retry<<min(w.Attempts, 16), maxRetry))
			} else {
				done = append(done, w)
			}
		}
		if err := self.store.Done(context.WithoutCancel(ctx), self.name, done...); err != nil {
			self.failed(ctx, err)
		}
	}
}

// do does work, of the queue
func (self *Indexer) do(ctx context.Context, w store.Work) error {
	switch w.Kind {
	case store.KindRescan:
		self.queue.rescanned(w.Path)
		return self.scan(ctx, w.Path)
	case store.KindIndex:
		// The event of the change isn't kept, as changes are coalesced, so a
		// folder is listed, as when it's created, which can have folders,
		// such as one which is moved there
		self.mu.Lock()
		defer self.mu.Unlock()
		return self.refresh(ctx, w.Path, schema.EventTypeCreated)
	default:
		return nil
	}
}

// release pushes work which is claimed, but isn't done, again, so it's
// claimed again at once, rather than once its lease has passed
func (self *Indexer) release(work []store.Work) {
	for i := range work {
		work[i].Attempts, work[i].Error = 0, ""
	}
	_ = self.store.Push(context.Background(), self.name, work...)
}

// idle waits for work to be pushed, which is due once the debounce has
// passed, which it waits for too, or the time to pass, for work which is
// due later, and returns false when the context is cancelled
func (self *Indexer) idle(ctx context.Context, wait time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-self.queue.wake:
		if self.debounce > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(self.debounce):
			}
		}
		return true
	case <-time.After(wait):
		return true
	}
}

// forward sends a change, which is indexed, to the watchers, each until it's
// sent, or the watcher, or Run, is cancelled, so a watcher which doesn't
// read its changes doesn't hold Run up, once it's cancelled
func (self *Indexer) forward(ctx context.Context, event schema.Event) {
	self.smu.Lock()
	watchers := make([]*watcher, 0, len(self.watchers))
	for w := range self.watchers {
		watchers = append(watchers, w)
	}
	self.smu.Unlock()
	for _, w := range watchers {
		select {
		case w.ch <- event:
		case <-w.ctx.Done():
		case <-ctx.Done():
			return
		}
	}
}

// fail tells the watchers the backend's watch failed, with why
func (self *Indexer) fail(err error) {
	self.smu.Lock()
	defer self.smu.Unlock()
	for w := range self.watchers {
		select {
		case w.failed <- err:
		default:
		}
	}
}

// failed records why the index's scan, or watch, failed
func (self *Indexer) failed(ctx context.Context, err error) {
	if state, e := self.store.State(ctx, self.name); e == nil {
		state.Error = err.Error()
		_ = self.store.SetState(ctx, self.name, state)
	}
}
