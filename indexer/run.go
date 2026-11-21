package indexer

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	attribute "go.opentelemetry.io/otel/attribute"
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
func (self *Indexer) Run(ctx context.Context, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	self.logger.Store(logger.With("volume", self.name))

	// The worker of the index, and the workers of the tasks, apart from it,
	// which are waited for once it's cancelled
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	workers.Go(func() { self.work(ctx) })
	if self.mode.Level() < ModeMetadata.Level() {
		// A volume which isn't enriched doesn't keep what it was enriched
		// with, of when it was, so it's enriched of every file again, when it
		// is again
		if err := self.store.DeleteMetadata(ctx, self.name, string(store.KindMetadata)); err != nil {
			self.failed(ctx, err)
		}
	} else if self.tasked() {
		// The files which are indexed, and not enriched, such as those of a
		// volume whose files were indexed, which is enriched now, are queued
		// at once, rather than once it's scanned again, as their revisions are
		// known, and what's new, or changed, is queued by its scan
		sctx, end := self.span(ctx, "indexer.stale", "", attribute.String("kind", string(store.KindMetadata)))
		err := self.queue.stale(sctx, "/", store.KindMetadata)
		end(err)
		if err != nil {
			self.failed(ctx, err)
		}
		workers.Go(func() { self.enrich(ctx) })
	}
	defer func() {
		cancel()
		workers.Wait()
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
			ectx, end := self.span(ctx, "indexer.event", "", attribute.String("event.type", event.EventType.String()), attribute.String("path", event.Path))
			err := self.apply(ectx, event)
			end(err)
			if err != nil {
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
		pctx, end := self.span(ctx, "indexer.poll", "")
		err := self.queue.rescan(pctx, "/")
		end(err)
		if err != nil && ctx.Err() == nil {
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
// or the index of an object which is created, updated or deleted, of its
// key, which a change of a folder's path, with a "/" after it, is, and
// otherwise a file's, which is created, or updated, or, which is deleted,
// those the index has of its path, as a change of what's deleted can't say
// what it was
func (self *Indexer) apply(ctx context.Context, event schema.Event) error {
	switch event.EventType {
	case schema.EventTypeRescan:
		return self.queue.rescan(ctx, store.FolderKey(folderPath(event.Path)))
	case schema.EventTypeCreated, schema.EventTypeUpdated:
		if event.Path == "" || event.Path == "/" {
			return self.queue.index(ctx, "/")
		}
		return self.queue.index(ctx, event.Path)
	case schema.EventTypeDeleted:
		return self.queue.index(ctx, self.keysAt(ctx, event.Path)...)
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
	var recounted time.Time
	var uncounted bool
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
			if uncounted {
				self.recount(context.WithoutCancel(ctx))
				uncounted, recounted = false, time.Now()
			}
			if !self.idle(ctx, workIdle) {
				return
			}
			continue
		}

		// Do the work, which is done, or failed, of its claim
		done := make([]store.Work, 0, len(claimed))
		for i, w := range claimed {
			wctx, end := self.workSpan(ctx, w)
			err := self.do(wctx, w)
			end(err)
			if ctx.Err() != nil {
				self.release(claimed[i:])
				break
			} else if err != nil {
				self.failed(ctx, err)
				_ = self.store.Failed(context.WithoutCancel(ctx), self.name, w, err, min(self.retry<<min(w.Attempts+w.Outages, 16), maxRetry))
			} else {
				done = append(done, w)
			}
		}
		if err := self.store.Done(context.WithoutCancel(ctx), self.name, done...); err != nil {
			self.failed(ctx, err)
		}

		// The files which changed, while it's watched, are counted, of the
		// changes, and the rescans of their folders, as a scan of the whole
		// volume counts them only when it's done, at most every
		// recountInterval, and once there's no more work
		if len(done) > 0 {
			uncounted = true
			if time.Since(recounted) >= recountInterval {
				self.recount(context.WithoutCancel(ctx))
				uncounted, recounted = false, time.Now()
			}
		}

		// The changes are sent to the watchers again, once they're indexed,
		// as they were sent before, so a watcher which listed what changed
		// then may have the listing from before
		for _, event := range self.indexed(context.WithoutCancel(ctx), done) {
			self.forward(ctx, event)
		}
	}
}

// do does work, of the queue
func (self *Indexer) do(ctx context.Context, w store.Work) error {
	switch w.Kind {
	case store.KindRescan:
		self.queue.rescanned(w.Key)
		return self.scan(ctx, w.Key)
	case store.KindIndex:
		// The event of the change isn't kept, as changes are coalesced, so a
		// folder is listed, as when it's created, which can have folders,
		// such as one which is moved there
		self.mu.Lock()
		defer self.mu.Unlock()
		return self.refresh(ctx, w.Key)
	default:
		return nil
	}
}

// release pushes work which is claimed, but isn't done, again, so it's
// claimed again at once, rather than once its lease has passed
func (self *Indexer) release(work []store.Work) {
	for i := range work {
		// Its trace is cleared, so a newer one, of a push while it was done,
		// is kept, rather than replaced by the one it was claimed with
		work[i].Attempts, work[i].Outages, work[i].Error, work[i].Trace = 0, 0, "", ""
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

// recount sets how many files the index's state has, as the index has
// now, once work is done, such as the changes which are watched, while
// it's ready, and isn't scanned, of a volume whose files are indexed. Its
// files are counted of their keys, as a folder can have a type of its own.
func (self *Indexer) recount(ctx context.Context) {
	if !self.mode.Files() {
		return
	}
	idle := func(state *schema.IndexState) bool {
		return state.Status == schema.IndexStatusReady && state.Phase == schema.IndexPhaseIdle
	}
	if state, err := self.store.State(ctx, self.name); err != nil || !idle(&state) {
		return
	}
	files, err := self.store.CountObjects(ctx, self.name, store.ListRequest{Parent: "/", Recursive: true, Objects: true})
	if err != nil {
		return
	}
	_ = self.updateState(ctx, func(state *schema.IndexState) bool {
		if !idle(state) || state.Objects == files {
			return false
		}
		state.Objects = files
		return true
	})
}

// failed records why the index's scan, or watch, failed
func (self *Indexer) failed(ctx context.Context, err error) {
	// It's logged as a warning when it's why it failed, for the first time,
	// rather than of each attempt, such as while its backend is unavailable,
	// as its state says why it last failed, until a rescan clears it
	again := false
	_ = self.updateState(ctx, func(state *schema.IndexState) bool {
		if again = state.Error == err.Error(); again {
			return false
		}
		state.Error = err.Error()
		return true
	})
	if again {
		self.log().DebugContext(ctx, "indexing failed, again", "error", err.Error())
	} else {
		self.log().WarnContext(ctx, "indexing failed", "error", err.Error())
	}
}

// updateState reads the index's state, and writes it, when the function
// changes it, which it returns, while no other change of it is made, so a
// change of one of its fields isn't written over of another's, which was
// read before it
func (self *Indexer) updateState(ctx context.Context, fn func(*schema.IndexState) bool) error {
	self.statemu.Lock()
	defer self.statemu.Unlock()
	state, err := self.store.State(ctx, self.name)
	if err != nil {
		return err
	} else if !fn(&state) {
		return nil
	}
	return self.store.SetState(ctx, self.name, state)
}

// log returns the logger which Run was given, of the volume, or the
// default, before it runs
func (self *Indexer) log() *slog.Logger {
	if logger := self.logger.Load(); logger != nil {
		return logger
	}
	return slog.Default().With("volume", self.name)
}

// indexed returns the changes of the work which is done, once it's
// indexed, once each: an object which is indexed is updated, or deleted,
// when the index doesn't have it, and a folder which is scanned is updated,
// other than the root, whose change would be a rescan of the volume, and
// files, unless they're indexed
func (self *Indexer) indexed(ctx context.Context, work []store.Work) []schema.Event {
	var result []schema.Event
	seen := make(map[string]bool)
	for _, w := range work {
		if w.Key == "" || w.Key == "/" || seen[w.Key] {
			continue
		}
		seen[w.Key] = true
		event := schema.Event{EventType: schema.EventTypeUpdated, ObjectKey: schema.ObjectKey{Name: self.name, Path: w.Key}}
		switch w.Kind {
		case store.KindRescan:
			event.Path = store.FolderKey(w.Key)
		case store.KindIndex:
			// A file isn't indexed, unless the files are, when it's listed
			// of the backend, which isn't out of date
			if !strings.HasSuffix(w.Key, "/") && !self.mode.Files() {
				continue
			} else if _, err := self.store.GetObject(ctx, self.name, w.Key); errors.Is(err, gofiler.ErrNotFound) {
				event.EventType = schema.EventTypeDeleted
			} else if err != nil {
				continue
			}
		default:
			continue
		}
		result = append(result, event)
	}
	return result
}
