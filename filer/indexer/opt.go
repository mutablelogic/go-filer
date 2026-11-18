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
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// DefaultRescanInterval is how often a backend which can't be watched,
	// such as a bucket, is scanned again
	DefaultRescanInterval = 15 * time.Minute

	// defaultRetry and maxRetry are how long to wait to watch a backend
	// again, when watching it fails, at first, and at most
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

// WithRetry sets how long to wait to watch a backend again, when watching
// it fails, which doubles each time, up to a minute
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
	o.retry, o.rescanInterval = defaultRetry, DefaultRescanInterval
	for _, fn := range opts {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}
