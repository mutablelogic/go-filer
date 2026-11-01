package filer

import (
	"slices"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// debouncer holds the changes to an object, until its changes are quiet for
// the delay, or for at most debounceMax, merging them, so an object which
// changes many times, such as a file which is written in parts, is one
// change, and a rescan of a volume is sent at once, in place of its changes
// which are held
type debouncer struct {
	delay   time.Duration
	pending map[string]*pending
}

// pending is a change which is held, since its first change, and its last
type pending struct {
	event       schema.Event
	first, last time.Time
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	// debounceDelay is how long a change is held for, by default, until its
	// object is quiet, and debounceMax is the most it's held for
	debounceDelay = 250 * time.Millisecond
	debounceMax   = 2 * time.Second
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newDebouncer returns a debouncer of the delay, which is zero to send the
// changes at once
func newDebouncer(delay time.Duration) *debouncer {
	return &debouncer{delay: delay, pending: make(map[string]*pending)}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// add holds the change, merged with the change of its object which is held,
// and returns the changes to send at once, which is the change, when there's
// no delay, or it's a rescan, when its volume's changes which are held are
// dropped
func (self *debouncer) add(event schema.Event, now time.Time) []schema.Event {
	switch {
	case self.delay <= 0:
		return []schema.Event{event}
	case event.EventType == schema.EventTypeRescan:
		for key, p := range self.pending {
			if p.event.Name == event.Name {
				delete(self.pending, key)
			}
		}
		return []schema.Event{event}
	}

	// Merge the change with the one which is held, or hold it
	key := event.Name + "\x00" + event.Path
	p, exists := self.pending[key]
	if !exists {
		self.pending[key] = &pending{event: event, first: now, last: now}
		return nil
	}
	if eventType, ok := merge(p.event.EventType, event.EventType); ok {
		p.event.EventType, p.last = eventType, now
	} else {
		delete(self.pending, key)
	}
	return nil
}

// due returns the changes whose objects are quiet for the delay, or which
// are held for debounceMax, in the order of their first changes, which are
// no longer held, or every change, when all is set
func (self *debouncer) due(now time.Time, all bool) []schema.Event {
	var due []*pending
	for key, p := range self.pending {
		if all || now.Sub(p.last) >= self.delay || now.Sub(p.first) >= max(debounceMax, self.delay) {
			due = append(due, p)
			delete(self.pending, key)
		}
	}
	slices.SortFunc(due, func(a, b *pending) int {
		return a.first.Compare(b.first)
	})
	events := make([]schema.Event, 0, len(due))
	for _, p := range due {
		events = append(events, p.event)
	}
	return events
}

// merge returns the change of an object which is held, followed by
// another, and false when they cancel, as it's created, and deleted
func merge(held, next schema.EventType) (schema.EventType, bool) {
	switch {
	case held == schema.EventTypeCreated && next == schema.EventTypeDeleted:
		return schema.EventTypeNone, false
	case held == schema.EventTypeCreated:
		return schema.EventTypeCreated, true
	case next == schema.EventTypeDeleted:
		return schema.EventTypeDeleted, true
	default:
		// Updated, or deleted and created again, which is replaced
		return schema.EventTypeUpdated, true
	}
}
