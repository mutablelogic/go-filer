package google

import (
	"errors"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// cache keeps what's got from Google, by key, such as the contacts of an
// address book, or the events of a calendar's month, for as long as ttl
// returns, which is none when it's zero, so what's listed again soon, or
// read after it's listed, isn't got again. A value is got by
// one request at a time, so it's got once, however many ask for it, which
// is got without holding the cache, so what doesn't wait for it, such as
// clearing it when what it's got from changes, doesn't wait for it to be got.
type cache[T any] struct {
	mu      sync.Mutex
	now     func() time.Time
	ttl     func() time.Duration
	values  map[string]cached[T]
	loading map[string]*loading[T]
}

// cached is a value which is kept, and when it was got
type cached[T any] struct {
	value T
	at    time.Time
}

// loading is a value which is being got, which is closed once it's got, and
// isn't kept when the cache was cleared while it was got, as it may be from
// before what it's got from changed, when what asks for it next gets it
// again, rather than waiting for it
type loading[T any] struct {
	done    chan struct{}
	value   T
	err     error
	cleared bool
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// errLoadPanicked is returned to those waiting for a value whose getting
// panicked
var errLoadPanicked = errors.New("getting the value panicked")

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newCache returns a cache which keeps values for as long as ttl returns, at
// the time now returns
func newCache[T any](now func() time.Time, ttl func() time.Duration) *cache[T] {
	return &cache[T]{now: now, ttl: ttl, values: make(map[string]cached[T]), loading: make(map[string]*loading[T])}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// get returns the value kept with the key, or when there's none, or it's
// been kept for longer than the cache keeps values, gets it with fn, and
// keeps it, unless fn returns an error, or the cache was cleared while it
// was got. When it's already being got, it waits for it, and returns it, or
// when it couldn't be got, such as when what got it was cancelled, gets it.
func (c *cache[T]) get(key string, fn func() (T, error)) (T, error) {
	if c == nil {
		// A cache which there isn't keeps nothing
		return fn()
	}
	for {
		c.mu.Lock()
		if value, exists := c.values[key]; exists && c.now().Sub(value.at) < c.ttl() {
			c.mu.Unlock()
			return value.value, nil
		} else if l, exists := c.loading[key]; exists {
			c.mu.Unlock()
			if <-l.done; l.err == nil {
				return l.value, nil
			}
			continue
		}
		l := &loading[T]{done: make(chan struct{})}
		c.loading[key] = l
		c.mu.Unlock()
		return c.load(key, l, fn)
	}
}

// load gets the value being got with fn, without holding the cache, and then
// keeps it, and returns it to those waiting for it, even when fn panics
func (c *cache[T]) load(key string, l *loading[T], fn func() (T, error)) (T, error) {
	defer func() {
		c.mu.Lock()
		if c.loading[key] == l {
			delete(c.loading, key)
		}
		if l.err == nil && !l.cleared {
			c.values[key] = cached[T]{value: l.value, at: c.now()}
		}
		c.mu.Unlock()
		close(l.done)
	}()
	l.err = errLoadPanicked
	l.value, l.err = fn()
	return l.value, l.err
}

// peek returns the value kept with the key, without getting it, or false
// when there's none, or it's been kept for longer than the cache keeps values
func (c *cache[T]) peek(key string) (T, bool) {
	if c == nil {
		var zero T
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if value, exists := c.values[key]; exists && c.now().Sub(value.at) < c.ttl() {
		return value.value, true
	}
	var zero T
	return zero, false
}

// clear removes the values kept, so they're got again, such as when what
// they were got from changed, and those being got aren't kept, once they're
// got, and aren't waited for by what asks for them after, which gets them
// again
func (c *cache[T]) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.values)
	for key, l := range c.loading {
		l.cleared = true
		delete(c.loading, key)
	}
}
