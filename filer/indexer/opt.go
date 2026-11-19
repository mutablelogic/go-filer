package indexer

import (
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is an option of an indexer
type Opt func(*opt) error

type opt struct {
	retry          time.Duration // How long to wait to watch again, when watching fails, which doubles each time
	rescanInterval time.Duration // How often a backend which can't be watched is scanned again
	debounce       time.Duration // How long work waits, for more of its path, which coalesces with it
	rescanMin      time.Duration // How often a folder is rescanned, at most
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// DefaultRescanInterval is how often a backend which can't be watched,
	// such as a bucket, is scanned again
	DefaultRescanInterval = 15 * time.Minute

	// DefaultDebounce is how long the work of a path waits, for more of it,
	// which coalesces with it, so a burst of changes is indexed once
	DefaultDebounce = 250 * time.Millisecond

	// DefaultRescanMinInterval is how often a folder is rescanned, at most,
	// so a backend which sends a lot of rescans isn't scanned each time
	DefaultRescanMinInterval = time.Minute

	// defaultRetry and maxRetry are how long to wait to watch a backend
	// again, when watching it fails, or to do work again, when it fails, at
	// first, and at most
	defaultRetry = time.Second
	maxRetry     = time.Minute
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// WithRescanInterval sets how often a backend which can't be watched is
// scanned again, which is DefaultRescanInterval
func WithRescanInterval(interval time.Duration) Opt {
	return func(o *opt) error {
		if interval <= 0 {
			return gofiler.ErrBadParameter.Withf("rescan interval: %v", interval)
		}
		o.rescanInterval = interval
		return nil
	}
}

// WithDebounce sets how long the work of a path waits, for more of it, which
// coalesces with it, which is DefaultDebounce, or zero not to wait
func WithDebounce(debounce time.Duration) Opt {
	return func(o *opt) error {
		if debounce < 0 {
			return gofiler.ErrBadParameter.Withf("debounce: %v", debounce)
		}
		o.debounce = debounce
		return nil
	}
}

// WithRescanMinInterval sets how often a folder is rescanned, at most,
// which is DefaultRescanMinInterval, or zero not to limit it. A rescan
// which is sooner is deferred until it's due, rather than dropped.
func WithRescanMinInterval(interval time.Duration) Opt {
	return func(o *opt) error {
		if interval < 0 {
			return gofiler.ErrBadParameter.Withf("rescan min interval: %v", interval)
		}
		o.rescanMin = interval
		return nil
	}
}

// WithRetry sets how long to wait to watch a backend again, when watching
// it fails, or to do work again, when it fails, which doubles each time, up
// to a minute
func WithRetry(retry time.Duration) Opt {
	return func(o *opt) error {
		if retry <= 0 {
			return gofiler.ErrBadParameter.Withf("retry: %v", retry)
		}
		o.retry = retry
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (o *opt) apply(opts []Opt) error {
	o.retry, o.rescanInterval, o.debounce, o.rescanMin = defaultRetry, DefaultRescanInterval, DefaultDebounce, DefaultRescanMinInterval
	for _, fn := range opts {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}
