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

// watching is a volume which Run watches, whose changes it publishes, until
// cancel is called, or the volume is removed
type watching struct {
	volume string
	cancel context.CancelFunc
}

// watched is a volume's watch which has ended, with why
type watched struct {
	e   *entry
	err error
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

// Run watches the volumes, which are watched when they're added, or updated,
// and not once they're removed, and sends their changes, with the names of
// their volumes, to the subscriptions (see Subscribe), logging with the
// logger, until the context is cancelled, or the registry is closed, when
// it stops watching, and every subscription ends. A volume which is updated,
// or whose watching fails, and is opened again, is sent a rescan of, as its
// changes may have been missed. A backend which can't be watched isn't.
// Only one can run at a time, which is otherwise ErrConflict.
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
	watches := make(map[*entry]watching)
	events, ended := make(chan schema.Event), make(chan watched)
	defer func() {
		// Stop watching, and wait for the watches to end, and end the
		// subscriptions, as there are no more changes
		for _, w := range watches {
			w.cancel()
		}
		for len(watches) > 0 {
			select {
			case <-events:
			case w := <-ended:
				delete(watches, w.e)
			}
		}
		self.broadcaster.end(false)
		self.setListening(false)
	}()
	self.setListening(true)

	// Watch the volumes, and send their changes, until it's stopped
	ticker := time.NewTicker(rescanInterval)
	defer ticker.Stop()
	self.startWatches(ctx, watches, events, ended)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-self.done:
			return nil
		case <-self.changed:
			self.startWatches(ctx, watches, events, ended)
		case event := <-events:
			if dropped := self.broadcaster.publish(event); dropped > 0 {
				logger.DebugContext(ctx, "subscribers were slow, so a change was dropped, and they'll be sent a rescan", "volume", event.Name, "subscribers", dropped)
			}
		case w := <-ended:
			// A watch which fails, while its volume is registered, opens the
			// volume again, which is watched again, with a rescan
			volume := watches[w.e].volume
			delete(watches, w.e)
			if w.err != nil && !errors.Is(w.err, gofiler.ErrNotImplemented) && w.e.ctx.Err() == nil {
				logger.WarnContext(ctx, "watching a volume failed, opening it again", "volume", volume, "error", w.err.Error())
				go self.reopen(w.e, volume)
			}
		case <-ticker.C:
			self.broadcaster.flush()
		}
	}
}

// Subscribe sends the changes to the objects of the volumes which match the
// filter, or every volume, when it has none, to ch, from when it returns,
// until the context is cancelled, or Run stops, or the registry is closed. It
// returns a channel which is sent nil when the context is cancelled, or
// fs.ErrClosed when Run stops, or the registry is closed, once the
// subscription has ended, after which no more changes are sent to ch, so it
// can be closed. A subscription made before Run runs lasts until it stops,
// and one made once the registry is closed isn't made, which is fs.ErrClosed.
// A channel is subscribed once at a time, so subscribing it while it's
// subscribed is ErrConflict. A change isn't waited for ch to receive, so
// when it's full, a rescan of the volume is sent in place of the changes it
// misses, once there's room, so ch should be buffered.
func (self *Registry) Subscribe(ctx context.Context, filter schema.EventFilter, ch chan<- schema.Event) (<-chan error, error) {
	return self.broadcaster.subscribe(ctx, filter, ch)
}

// WatchObjects sends each change to the objects of every volume to the
// channel, with the name of its volume, waiting for it to receive each,
// until the context is cancelled, or Run stops, or the registry is closed,
// when fs.ErrClosed is returned. When changes may have been missed, such as
// when watching a volume fails, and it's watched again, or the channel isn't
// read fast enough, a rescan of the volume is sent in their place.
func (self *Registry) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	return self.broadcaster.watch(ctx, ch, watchQueue)
}

// Running returns true when Run is running
func (self *Registry) Running() bool {
	return self.running.Load()
}

// WaitListening waits until Run is running, so the changes from then are
// sent to the subscriptions, or returns the context's error when it's
// cancelled first
func (self *Registry) WaitListening(ctx context.Context) error {
	self.RLock()
	listening := self.listening
	self.RUnlock()
	select {
	case <-listening:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribers returns how many subscriptions there are
func (self *Registry) Subscribers() int {
	return self.broadcaster.len()
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// startWatches watches the volumes which are registered, and aren't watched,
// until they're removed, or Run stops, which sends their changes to events,
// and when their watches end to ended. A watch is waited for before its
// volume is closed, so it's added to the entry's calls with the registry
// locked, while it isn't being removed.
func (self *Registry) startWatches(ctx context.Context, watches map[*entry]watching, events chan<- schema.Event, ended chan<- watched) {
	self.Lock()
	defer self.Unlock()
	for volume, e := range self.backend {
		if _, exists := watches[e]; exists || !e.ready || e.closing {
			continue
		}
		wctx, cancel := context.WithCancel(e.ctx)
		stop := context.AfterFunc(ctx, cancel)
		watches[e] = watching{volume: volume, cancel: cancel}
		rescan := e.rescan
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			defer stop()
			defer cancel()
			ended <- watched{e: e, err: self.watch(wctx, e, volume, rescan, events)}
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

// setListening sets whether Run is running, closing the channel which
// WaitListening waits for, once it is, or replacing it, once it isn't
func (self *Registry) setListening(listening bool) {
	self.Lock()
	defer self.Unlock()
	select {
	case <-self.listening:
		if !listening {
			self.listening = make(chan struct{})
		}
	default:
		if listening {
			close(self.listening)
		}
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
