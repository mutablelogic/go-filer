package filer

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"slices"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// subscriber is a subscription's filter, and its channel, which is sent the
// changes which match it, without waiting, and the volumes whose changes
// were dropped, as its channel was full, which it's sent a rescan of, once
// there's room, and the channel which is closed when it ends, as Run stops
type subscriber struct {
	filter schema.EventFilter
	ch     chan<- schema.Event
	rescan map[string]bool
	stop   chan struct{}
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	// eventQueue is how many changes can wait for Run, which the volumes'
	// watchers wait for when it's full
	eventQueue = 1024

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

// Run sends the changes to the objects of every volume to the subscribers,
// with the names of their volumes, logging with the logger, until the
// context is cancelled, or the registry is closed. A change isn't waited for
// a subscriber to receive, so when its channel is full, it, and the
// volume's other changes, are dropped, until it's sent a rescan of the
// volume, which says changes may have been missed, once there's room. When
// it stops, every subscription ends. Only one can run at a time, which is
// ErrConflict.
func (self *Registry) Run(ctx context.Context, logger *slog.Logger) error {
	if !self.running.CompareAndSwap(false, true) {
		return gofiler.ErrConflict.With("registry is already running")
	}
	defer self.running.Store(false)
	defer self.unsubscribeAll()
	if logger == nil {
		logger = slog.Default()
	}
	self.setListening(true)
	defer self.setListening(false)

	// Send the changes, and the rescans which are owed, until it's stopped
	ticker := time.NewTicker(rescanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-self.done:
			return nil
		case event := <-self.events:
			if dropped := self.broadcast(event); dropped > 0 {
				logger.DebugContext(ctx, "subscribers were slow, so a change was dropped, and they'll be sent a rescan", "volume", event.Name, "subscribers", dropped)
			}
		case <-ticker.C:
			self.flushAll()
		}
	}
}

// Subscribe sends the changes to the objects of the volumes which match the
// filter to ch, from when it returns, until the context is cancelled, or Run
// stops, or the registry is closed. It returns a channel which is sent nil
// when the context is cancelled, or fs.ErrClosed when Run stops, or the
// registry is closed, once the subscription has ended, after which no more
// changes are sent to ch, so it can be closed. A subscription made before
// Run runs lasts until it stops, and one made once the registry is closed
// isn't made, which is fs.ErrClosed. A channel is subscribed once at a time,
// so subscribing it while it's subscribed is ErrConflict. A change isn't
// waited for ch to receive, so when it's full, a rescan of the volume is
// sent in place of the changes it misses (see Run), so ch should be
// buffered.
func (self *Registry) Subscribe(ctx context.Context, filter schema.EventFilter, ch chan<- schema.Event) (<-chan error, error) {
	if ch == nil {
		return nil, gofiler.ErrBadParameter.With("missing channel")
	}
	done := make(chan error, 1)
	s := &subscriber{filter: filter, ch: ch, rescan: make(map[string]bool), stop: make(chan struct{})}
	self.Lock()
	select {
	case <-self.done:
		self.Unlock()
		return nil, fs.ErrClosed
	default:
	}
	if _, exists := self.subscribers[ch]; exists {
		self.Unlock()
		return nil, gofiler.ErrConflict.With("channel is already subscribed")
	}
	self.subscribers[ch] = s
	self.Unlock()

	// The subscription ends when the context is cancelled, or Run stops, or
	// the registry is closed. It's removed before that's sent, under the
	// lock changes are sent under, so once it's received, no more changes
	// are sent to ch.
	go func() {
		var err error
		select {
		case <-ctx.Done():
		case <-s.stop:
			err = fs.ErrClosed
		case <-self.done:
			err = fs.ErrClosed
		}
		self.Lock()
		if self.subscribers[ch] == s {
			delete(self.subscribers, ch)
		}
		self.Unlock()
		done <- err
	}()
	return done, nil
}

// WatchObjects sends each change to the objects of every volume to the
// channel, with the name of its volume, waiting for it to receive each, until
// the context is cancelled, or Run stops, or the registry is closed, when
// fs.ErrClosed is returned. Changes to the volumes which are added while it's
// watching are sent too. When changes may have been missed, such as when
// watching a volume fails, and it's watched again, or the channel isn't read
// fast enough, a rescan of the volume is sent in their place.
func (self *Registry) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan schema.Event, watchQueue)
	done, err := self.Subscribe(ctx, schema.EventFilter{}, queue)
	if err != nil {
		return err
	}
	for {
		select {
		case err := <-done:
			return err
		case event := <-queue:
			select {
			case ch <- event:
			case err := <-done:
				return err
			}
		}
	}
}

// Running returns true when Run is running
func (self *Registry) Running() bool {
	return self.running.Load()
}

// WaitListening waits until Run is running, so the changes from then are
// sent to the subscribers, or returns the context's error when it's
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
	self.RLock()
	defer self.RUnlock()
	return len(self.subscribers)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// watch publishes the changes to the volume's objects, which Run sends to
// the subscribers, until it's removed, with a rescan of the volume first, once
// it's watched, when rescan is set. When watching fails, such as when the
// volume's directory is removed, the volume is opened again, which watches
// it again, with a rescan, as changes may have been missed, and as the
// backend may no longer be of the directory at its path, when it's replaced.
// A backend which can't be watched isn't.
func (self *Registry) watch(e *entry, volume string, rescan bool) {
	err := self.watchOnce(e, volume, rescan)
	if e.ctx.Err() != nil || errors.Is(err, gofiler.ErrNotImplemented) {
		return
	}

	// Open the volume again, from a goroutine of its own, as opening it
	// closes the backend, which waits for this one
	go self.reopen(e, volume)
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

// readyWatcher is a backend which reports when it's watching, so the
// changes after it are sent
type readyWatcher interface {
	WatchObjectsReady(context.Context, chan<- schema.Event, func()) error
}

// watchOnce watches the volume, publishing each change, until watching fails
// or the volume is removed. With rescan, a rescan of the volume is published
// first, once it's watched, or when it starts, for a backend which doesn't
// report when it's watching.
func (self *Registry) watchOnce(e *entry, volume string, rescan bool) error {
	ready := func() {
		if rescan {
			self.publish(e, rescanEvent(volume))
		}
	}
	events, done := make(chan schema.Event), make(chan error, 1)
	go func() {
		if b, ok := e.Filer.(readyWatcher); ok {
			done <- b.WatchObjectsReady(e.ctx, events, ready)
		} else {
			ready()
			done <- e.WatchObjects(e.ctx, events)
		}
	}()
	for {
		select {
		case err := <-done:
			return err
		case event := <-events:
			event.Name = volume
			self.publish(e, event)
		}
	}
}

// publish queues the change of the entry's volume for Run, waiting while the
// queue is full, unless the volume is removed
func (self *Registry) publish(e *entry, event schema.Event) {
	select {
	case self.events <- event:
	case <-e.ctx.Done():
	}
}

// broadcast sends the change to each subscriber whose filter it matches,
// without waiting, and returns how many it was dropped for, as their
// channels were full, which they're owed a rescan of its volume for
func (self *Registry) broadcast(event schema.Event) int {
	self.Lock()
	defer self.Unlock()
	dropped := 0
	for _, s := range self.subscribers {
		if !s.filter.Match(event) {
			continue
		} else if !s.send(event) {
			dropped++
		}
	}
	return dropped
}

// flushAll sends the rescans which subscribers are owed, while there's room
func (self *Registry) flushAll() {
	self.Lock()
	defer self.Unlock()
	for _, s := range self.subscribers {
		s.flush()
	}
}

// unsubscribeAll ends every subscription, as Run stops
func (self *Registry) unsubscribeAll() {
	self.Lock()
	defer self.Unlock()
	for ch, s := range self.subscribers {
		close(s.stop)
		delete(self.subscribers, ch)
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

// send sends the change, without waiting, once the rescans which are owed
// are, and returns false when it's dropped, as the channel is full, or a
// rescan of its volume is owed, which it's then owed. The registry must be
// locked.
func (s *subscriber) send(event schema.Event) bool {
	s.flush()
	if s.rescan[event.Name] {
		return false
	}
	select {
	case s.ch <- event:
		return true
	default:
		s.rescan[event.Name] = true
		return false
	}
}

// flush sends the rescans of the volumes whose changes were dropped, while
// there's room, in the order of their names. The registry must be locked.
func (s *subscriber) flush() {
	if len(s.rescan) == 0 {
		return
	}
	names := make([]string, 0, len(s.rescan))
	for name := range s.rescan {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		select {
		case s.ch <- rescanEvent(name):
			delete(s.rescan, name)
		default:
			return
		}
	}
}

// rescanEvent returns a rescan of the volume
func rescanEvent(volume string) schema.Event {
	return schema.Event{EventType: schema.EventTypeRescan, ObjectKey: schema.ObjectKey{Name: volume, Path: "/"}}
}
