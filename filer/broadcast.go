package filer

import (
	"context"
	"io/fs"
	"slices"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// broadcaster sends the changes which are published to it to its
// subscriptions, whose filters they match, without waiting, so a subscriber
// whose channel is full misses the changes of a volume, and is sent a rescan
// of the volume, once there's room
type broadcaster struct {
	sync.Mutex
	subscribers map[chan<- schema.Event]*subscriber
	closed      bool
}

// subscriber is a subscription's filter, and its channel, and the volumes
// whose changes were dropped, as its channel was full, which it's sent a
// rescan of, once there's room, and the channel which is closed when it
// ends, with fs.ErrClosed
type subscriber struct {
	filter schema.EventFilter
	ch     chan<- schema.Event
	rescan map[string]bool
	stop   chan struct{}
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newBroadcaster returns a broadcaster, without subscriptions
func newBroadcaster() *broadcaster {
	return &broadcaster{subscribers: make(map[chan<- schema.Event]*subscriber)}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// subscribe sends the changes which match the filter to ch, from when it
// returns, until the context is cancelled, or the subscriptions end (see
// end). It returns a channel which is sent nil when the context is
// cancelled, or fs.ErrClosed when the subscriptions end, once it's ended,
// after which no more changes are sent to ch. A channel is subscribed once
// at a time, which is otherwise ErrConflict, and once the broadcaster is
// closed, a subscription isn't made, which is fs.ErrClosed.
func (self *broadcaster) subscribe(ctx context.Context, filter schema.EventFilter, ch chan<- schema.Event) (<-chan error, error) {
	if ch == nil {
		return nil, gofiler.ErrBadParameter.With("missing channel")
	}
	s := &subscriber{filter: filter, ch: ch, rescan: make(map[string]bool), stop: make(chan struct{})}
	self.Lock()
	if self.closed {
		self.Unlock()
		return nil, fs.ErrClosed
	} else if _, exists := self.subscribers[ch]; exists {
		self.Unlock()
		return nil, gofiler.ErrConflict.With("channel is already subscribed")
	}
	self.subscribers[ch] = s
	self.Unlock()

	// The subscription is removed before it's ended, under the lock changes
	// are sent under, so once it's received, no more changes are sent
	done := make(chan error, 1)
	go func() {
		var err error
		select {
		case <-ctx.Done():
		case <-s.stop:
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

// watch sends the changes to ch, waiting for it to receive each, through a
// buffered channel, so the changes it misses, when it's slow, are a rescan,
// until the context is cancelled, when it returns nil, or the subscriptions
// end, when it returns fs.ErrClosed
func (self *broadcaster) watch(ctx context.Context, ch chan<- schema.Event, buffer int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	queue := make(chan schema.Event, buffer)
	done, err := self.subscribe(ctx, schema.EventFilter{}, queue)
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

// publish sends the change to each subscriber whose filter it matches,
// without waiting, and returns how many it was dropped for, as their
// channels were full, which they're owed a rescan of its volume for
func (self *broadcaster) publish(event schema.Event) int {
	self.Lock()
	defer self.Unlock()
	dropped := 0
	for _, s := range self.subscribers {
		if s.filter.Match(event) && !s.send(event) {
			dropped++
		}
	}
	return dropped
}

// flush sends the rescans which subscribers are owed, while there's room
func (self *broadcaster) flush() {
	self.Lock()
	defer self.Unlock()
	for _, s := range self.subscribers {
		s.flush()
	}
}

// end ends every subscription, with fs.ErrClosed, and when final is set,
// later subscriptions aren't made
func (self *broadcaster) end(final bool) {
	self.Lock()
	defer self.Unlock()
	for ch, s := range self.subscribers {
		close(s.stop)
		delete(self.subscribers, ch)
	}
	self.closed = self.closed || final
}

// len returns how many subscriptions there are
func (self *broadcaster) len() int {
	self.Lock()
	defer self.Unlock()
	return len(self.subscribers)
}

// send sends the change, without waiting, once the rescans which are owed
// are, and returns false when it's dropped, as the channel is full, or a
// rescan of its volume is owed, which it's then owed. The broadcaster must be
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
// there's room, in the order of their names. The broadcaster must be locked.
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
