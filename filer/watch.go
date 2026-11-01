package filer

import (
	"context"
	"errors"
	"io/fs"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// subscriber is a call to WatchObjects, which is sent the changes to every
// volume, through a queue, so a caller which is slow doesn't hold up
// watching, or the other callers. When its queue is full, the changes to a
// volume are dropped, and it's sent a rescan of the volume once there's room.
type subscriber struct {
	sync.Mutex
	queue  chan schema.Event
	rescan map[string]bool // the volumes whose changes were dropped
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	// watchQueue is how many changes can wait for each call to WatchObjects
	watchQueue = 1024

	// watchRetry is how long to wait before opening a volume again, when
	// watching it fails, which doubles each time it can't be, up to
	// watchRetryMax
	watchRetry    = time.Second
	watchRetryMax = time.Minute
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects sends each change to the objects of every volume to the
// channel, with the name of its volume, until the context is cancelled, or
// the registry is closed, when fs.ErrClosed is returned. Changes to the
// volumes which are added while it's watching are sent too. When changes
// may have been missed, such as when watching a volume fails, and it's
// watched again, or the channel isn't read fast enough, a rescan of the
// volume is sent in their place.
func (self *Registry) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	s := &subscriber{queue: make(chan schema.Event, watchQueue), rescan: make(map[string]bool)}

	// Register the call, until it returns
	self.Lock()
	select {
	case <-self.done:
		self.Unlock()
		return fs.ErrClosed
	default:
		self.subscribers[s] = struct{}{}
	}
	self.Unlock()
	defer func() {
		self.Lock()
		delete(self.subscribers, s)
		self.Unlock()
	}()

	// Send the changes to the channel
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-self.done:
			return fs.ErrClosed
		case event := <-s.queue:
			select {
			case ch <- event:
			case <-ctx.Done():
				return nil
			case <-self.done:
				return fs.ErrClosed
			}

			// There's room in the queue for the rescans of the volumes whose
			// changes were dropped
			s.Lock()
			s.flush()
			s.Unlock()
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// watch sends the changes to the volume's objects to the calls to
// WatchObjects until it's removed, with a rescan of the volume first, once
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
		_, err := self.update(context.Background(), e, volume, nil)
		if err == nil || errors.Is(err, gofiler.ErrNotFound) || errors.Is(err, gofiler.ErrServiceUnavailable) {
			return
		}
		retry = min(retry*2, watchRetryMax)
	}
}

// readyWatcher is a backend which reports when it's watching, so the
// changes after it are sent
type readyWatcher interface {
	WatchObjectsReady(context.Context, chan<- schema.Event, func()) error
}

// watchOnce watches the volume, sending each change to the calls to
// WatchObjects, until watching fails or the volume is removed. With rescan,
// a rescan of the volume is sent first, once it's watched, or when it
// starts, for a backend which doesn't report when it's watching.
func (self *Registry) watchOnce(e *entry, volume string, rescan bool) error {
	ready := func() {
		if rescan {
			self.broadcast(rescanEvent(volume))
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
			self.broadcast(event)
		}
	}
}

// broadcast sends the change to each call to WatchObjects, without waiting
func (self *Registry) broadcast(event schema.Event) {
	self.RLock()
	defer self.RUnlock()
	for s := range self.subscribers {
		s.send(event)
	}
}

// send queues the change, without waiting. When the queue is full, it's
// dropped, as are the volume's other changes, until there's room for a
// rescan of the volume, which is queued before them.
func (s *subscriber) send(event schema.Event) {
	s.Lock()
	defer s.Unlock()
	s.flush()
	if s.rescan[event.Name] {
		return
	}
	select {
	case s.queue <- event:
	default:
		s.rescan[event.Name] = true
	}
}

// flush queues the rescans of the volumes whose changes were dropped, while
// there's room. The subscriber must be locked.
func (s *subscriber) flush() {
	for name := range s.rescan {
		select {
		case s.queue <- rescanEvent(name):
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
