package indexer

import (
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	store "github.com/mutablelogic/go-filer/store"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is an option of an indexer
type Opt func(*opt) error

type opt struct {
	retry          time.Duration       // How long to wait to watch again, when watching fails, which doubles each time
	rescanInterval time.Duration       // How often a backend which can't be watched is scanned again
	debounce       time.Duration       // How long work waits, for more of its path, which coalesces with it
	rescanMin      time.Duration       // How often a folder is rescanned, at most
	mode           Mode                // What's indexed, which is ModeFolders, by default
	tasks          map[store.Kind]Task // The tasks of the files, of their kinds, such as KindMetadata
	taskWorkers    int                 // How many workers do the tasks' work
	tracer         trace.Tracer        // The tracer of the work, which is linked to what queued it, or nil
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

// WithMode sets what's indexed, which is ModeFolders, by default, or
// ModeObjects, of the files too, or ModeMetadata, of the files, which are
// enriched with their metadata, when a task is set (see WithMetadataTask),
// or ModeArtwork, which are enriched with their artwork too
func WithMode(mode Mode) Opt {
	return func(o *opt) error {
		if mode != ModeFolders && mode != ModeObjects && mode != ModeMetadata && mode != ModeArtwork {
			return gofiler.ErrBadParameter.Withf("an indexer's mode is %s, %s, %s or %s, not %q", ModeFolders, ModeObjects, ModeMetadata, ModeArtwork, mode)
		}
		o.mode = mode
		return nil
	}
}

// WithMetadataTask sets the task which enriches a volume's files with their
// metadata, of ModeMetadata, which reads a file of the backend, and stores
// what it finds, when it's new, or changed, apart from indexing it
func WithMetadataTask(task Task) Opt {
	return func(o *opt) error {
		if task == nil {
			return gofiler.ErrBadParameter.With("a metadata task is nil")
		}
		if o.tasks == nil {
			o.tasks = make(map[store.Kind]Task)
		}
		o.tasks[store.KindMetadata] = task
		return nil
	}
}

// WithTaskWorkers sets how many workers do the work of tasks, which is
// DefaultTaskWorkers
func WithTaskWorkers(n int) Opt {
	return func(o *opt) error {
		if n <= 0 {
			return gofiler.ErrBadParameter.Withf("task workers: %d", n)
		}
		o.taskWorkers = n
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

// WithTracer sets the tracer of the index's work: each change it's told of,
// each scan it's queued, and each piece of work it does, which is linked to
// the span which queued it, so it's known where work came from
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.tracer = tracer
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (o *opt) apply(opts []Opt) error {
	o.retry, o.rescanInterval, o.debounce, o.rescanMin, o.mode, o.taskWorkers = defaultRetry, DefaultRescanInterval, DefaultDebounce, DefaultRescanMinInterval, ModeFolders, DefaultTaskWorkers
	for _, fn := range opts {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}
