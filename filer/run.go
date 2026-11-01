package filer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// watched is a volume's watch which has ended, with why
type watched struct {
	e      *entry
	volume string
	err    error
}

// readyWatcher is a backend which reports when it's watching, so the
// changes after it are sent
type readyWatcher interface {
	WatchObjectsReady(context.Context, chan<- schema.Event, func()) error
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	// watchQueue is how many changes can wait for each call to WatchObjects
	watchQueue = 1024

	// rescanInterval is how often the rescans which subscribers are owed are
	// sent, when there are no changes to send them with
	rescanInterval = 100 * time.Millisecond

	// watchRetry is how long to wait before opening a volume again, when
	// watching it fails, which doubles each time it can't be, up to
	// watchRetryMax
	watchRetry    = time.Second
	watchRetryMax = time.Minute
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Run processes events on the runloop, until the context is cancelled
func (self *Registry) Run(ctx context.Context, logger *slog.Logger) error {
	if !self.running.CompareAndSwap(false, true) {
		return gofiler.ErrConflict.With("registry is already running")
	}
	defer self.running.Store(false)
	if logger == nil {
		logger = slog.Default()
	}

	// The watches of the volumes, which send their changes, and when they
	// end, to Run
	watches := make(map[*entry]context.CancelFunc)
	events, ended := make(chan schema.Event), make(chan watched)
	defer func() {
		// Stop watching, and wait for the watches to end, and end the
		// subscriptions, as there are no more changes
		for _, cancel := range watches {
			cancel()
		}
		for len(watches) > 0 {
			select {
			case <-events:
			case w := <-ended:
				delete(watches, w.e)
			}
		}
		self.broadcaster.end(false)
	}()

	// Watch the volumes, and send their changes, until it's stopped
	ticker := time.NewTicker(rescanInterval)
	defer ticker.Stop()
	self.startWatches(watches, events, ended)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-self.done:
			return nil
		case <-self.changed:
			self.startWatches(watches, events, ended)
		case event := <-events:
			if dropped := self.broadcaster.publish(event); dropped > 0 {
				logger.DebugContext(ctx, "subscribers were slow, so a change was dropped, and they'll be sent a rescan", "volume", event.Name, "subscribers", dropped)
			}
		case w := <-ended:
			// A watch which fails, while its volume is registered, opens the
			// volume again, which is watched again, with a rescan
			delete(watches, w.e)
			if w.err != nil && !errors.Is(w.err, gofiler.ErrNotImplemented) && w.e.ctx.Err() == nil {
				logger.WarnContext(ctx, "watching a volume failed, opening it again", "volume", w.volume, "error", w.err.Error())
				go self.reopen(w.e, w.volume)
			}
		case <-ticker.C:
			self.broadcaster.flush()
		}
	}
}

// WatchObjects sends each change to the objects of every volume to the
// channel
func (self *Registry) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	return self.broadcaster.watch(ctx, ch, watchQueue)
}

// Running returns true when Run is running
func (self *Registry) Running() bool {
	return self.running.Load()
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// startWatches watches the volumes which are registered, and aren't watched,
// until they're removed, or Run cancels them, as it stops, which sends their
// changes to events, and when their watches end to ended. A watch is waited
// for before its volume is closed, so it's added to the entry's calls with
// the registry locked, while it isn't being removed.
func (self *Registry) startWatches(watches map[*entry]context.CancelFunc, events chan<- schema.Event, ended chan<- watched) {
	self.Lock()
	defer self.Unlock()
	for volume, e := range self.backend {
		if _, exists := watches[e]; exists || !e.ready || e.closing {
			continue
		}
		ctx, cancel := context.WithCancel(e.ctx)
		watches[e] = cancel
		rescan := e.rescan
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			defer cancel()
			ended <- watched{e: e, volume: volume, err: self.watch(ctx, e, volume, rescan, events)}
		}()
	}
}

// watch watches the volume, sending each change to events, with the name of
// its volume, until watching fails, or the context is cancelled. With
// rescan, a rescan of the volume is sent first, once it's watched, or when
// it starts, for a backend which doesn't report when it's watching.
func (self *Registry) watch(ctx context.Context, e *entry, volume string, rescan bool, events chan<- schema.Event) error {
	send := func(event schema.Event) {
		event.Name = volume
		select {
		case events <- event:
		case <-ctx.Done():
		}
	}
	ready := func() {
		if rescan {
			send(rescanEvent(volume))
		}
	}
	changes, done := make(chan schema.Event), make(chan error, 1)
	go func() {
		if b, ok := e.Filer.(readyWatcher); ok {
			done <- b.WatchObjectsReady(ctx, changes, ready)
		} else {
			ready()
			done <- e.WatchObjects(ctx, changes)
		}
	}()
	for {
		select {
		case err := <-done:
			if ctx.Err() != nil {
				return nil
			}
			return err
		case event := <-changes:
			send(event)
		}
	}
}

// changedVolumes tells Run that a volume is added, or updated, so it's
// watched, without waiting
func (self *Registry) changedVolumes() {
	select {
	case self.changed <- struct{}{}:
	default:
	}
}

// reopen opens the volume again, after a while, until it can be, which
// doubles each time it can't, or the volume is removed, or changed
func (self *Registry) reopen(e *entry, volume string) {
	retry := watchRetry
	for {
		select {
		case <-time.After(retry):
		case <-e.ctx.Done():
			return
		}
		// Opening it again ends when it's opened, or the volume isn't the
		// entry's, as it's removed, or changed, or the registry is closed,
		// but not when its backend is unavailable, such as a server which is
		// down, which it's tried again for
		if _, err := self.update(context.Background(), e, volume, nil); err == nil || !self.registered(e, volume) {
			return
		}
		retry = min(retry*2, watchRetryMax)
	}
}

// registered returns true when the volume is the entry's, which isn't being
// removed, and the registry isn't closed
func (self *Registry) registered(e *entry, volume string) bool {
	self.RLock()
	defer self.RUnlock()
	return self.backend != nil && self.backend[volume] == e && !e.closing
}
