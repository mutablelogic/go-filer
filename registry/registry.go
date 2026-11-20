package registry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/indexer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Registry struct {
	sync.RWMutex
	opt
	backend map[string]*entry

	// The channel which tells Run a volume is added or updated, and whether
	// Run is running
	changed chan struct{}
	running atomic.Bool

	// The metrics, which Run counts the changes with, or nil, without a
	// meter
	metrics *metrics

	// The backends which are being closed, by Del, Update, or Run, when it
	// closes the registry, which it waits for, as a backend which Update is
	// closing is no longer the volume's in the registry
	teardown map[*entry]struct{}

	// done is closed when Run closes the registry, which ends the calls to
	// WatchObjects, and Run can't run it again
	done chan struct{}

	// failed is why the volumes which are enabled can't be mounted, by their
	// names, which is read and set with the registry locked
	failed map[string]error

	// logger is the logger which Run is given, which the indexers of
	// volumes are run with, or nil before it runs
	logger atomic.Pointer[slog.Logger]

	// The volumes which Run syncs with the store, and its workers which
	// sync them
	syncMu  sync.Mutex
	syncs   syncs
	workers sync.WaitGroup

	// opener creates the backends which Run mounts, in place of openURL, in
	// tests, which set it before it runs
	opener func(ctx context.Context, name, volumeURL string) (gofiler.Filer, error)
}

// entry is a registered backend, with the calls to With which are using it
type entry struct {
	gofiler.Filer

	// Calls to With in progress, and watching the volume, which are waited
	// for before the backend is closed, and a context which is cancelled to
	// stop them
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// Whether the backend is being added, when its volume is registered, so
	// it can't be added twice, but can't be used, is ready, or is being
	// removed, when it can't be used, but is still registered until it's
	// closed, so its volume can't be added again until then. It's only read
	// or set with the registry locked.
	state entryState

	// closed is closed once the backend is closed, and removed from the
	// registry, when err is why it couldn't be closed
	closed chan struct{}
	err    error

	// offline is why the volume's backend isn't working, when watching it
	// failed, until it's opened again. It's only read or set with the
	// registry locked.
	offline error

	// forget is true when the volume's index is removed, once its backend
	// is closed, and before its name is released, as it isn't indexed any
	// more, or its backend is another. It's only set with the registry
	// locked, before it's closed.
	forget bool

	// created is when the stored volume which the backend was mounted from
	// was created, so a volume which is deleted, and created again, of the
	// same URL, is mounted again. It's only set before it's registered.
	created time.Time
}

// entryState is how far a backend is added, or removed
type entryState int

const (
	stateAdding  entryState = iota // Being added, so it can't be used yet
	stateReady                     // Can be used
	stateClosing                   // Being removed, so it can't be used
)

// Ensure Registry is a filer of every volume
var _ gofiler.Filer = (*Registry)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a registry of the backends of volumes
func New(ctx context.Context, opts ...Opt) (*Registry, error) {
	self := &Registry{
		backend:  make(map[string]*entry),
		changed:  make(chan struct{}, 1),
		teardown: make(map[*entry]struct{}),
		done:     make(chan struct{}),
		failed:   make(map[string]error),
		syncs:    syncs{volumes: make(map[string]*syncState)},
	}

	// Apply options
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}

	// Register the metrics, with the meter
	if metrics, err := newMetrics(self.meter, self.worker); err != nil {
		return nil, err
	} else {
		self.metrics = metrics
	}

	// Return success
	return self, nil
}

// close removes and closes all the backends, cancelling and waiting for any
// calls to With which are using them, and waiting for those which Del is
// closing, once Run has stopped watching them. Calls to WatchObjects return,
// and backends cannot be added after the registry is closed. Run calls it
// when its context is cancelled, which isn't from a function passed to
// With, as it would wait for the function to return.
func (self *Registry) close() error {
	var result error

	// End the calls to WatchObjects, and those made later, and remove all
	// the backends, and close those which aren't already being closed, while
	// the registry is locked, so they're each closed once. Those which are
	// being added are closed by Add.
	self.Lock()
	select {
	case <-self.done:
	default:
		close(self.done)
	}
	started := make(map[*entry]bool, len(self.backend))
	for volume, e := range self.backend {
		if e.state == stateReady {
			self.startClose(e, volume)
			started[e] = true
		}
	}
	entries := make([]*entry, 0, len(self.teardown))
	for e := range self.teardown {
		entries = append(entries, e)
	}
	self.backend = nil
	self.Unlock()

	// Wait for each backend to be closed, including those which Del or
	// Update are closing, returning the errors of those which were closed
	// here
	for _, e := range entries {
		<-e.closed
		if started[e] {
			result = errors.Join(result, e.err)
		}
	}

	// Return any errors encountered while closing backends
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Keys returns the volumes which are registered, and can be used, in order,
// which doesn't include those which are being added or removed
func (self *Registry) Keys() []string {
	self.RLock()
	defer self.RUnlock()
	keys := make([]string, 0, len(self.backend))
	for volume, e := range self.backend {
		if e.state == stateReady {
			keys = append(keys, volume)
		}
	}
	slices.Sort(keys)
	return keys
}

// VolumeName returns the name of the volume of a URL, which is its name
// option, when it's set, such as s3://my.bucket/?name=photos, or otherwise
// its host, such as file://media/mnt/media or s3://photos/, or empty, when it
// has neither
func VolumeName(u *url.URL) string {
	if u == nil {
		return ""
	} else if name := u.Query().Get("name"); name != "" {
		return name
	}
	return u.Host
}

// metaOf returns what's stored of a volume, of its backend: its name, its
// URL, which a backend can be created again from, with the placeholders of
// its credentials, when it has them, and its index option, and that it's
// enabled
func (self *Registry) metaOf(name string, b gofiler.Filer) schema.VolumeMeta {
	mode, backend := indexer.ModeNone, b
	if i, ok := b.(*indexer.Indexer); ok {
		mode, backend = i.Mode(), i.Backend()
	}
	u := backend.URL()
	if c, ok := backend.(interface{ URLWithCredentials() *url.URL }); ok {
		u = c.URLWithCredentials()
	}
	query := u.Query()
	query.Set(indexer.IndexParam, string(mode))
	u.RawQuery = query.Encode()
	return schema.VolumeMeta{Name: name, URL: u.String(), Enabled: true}
}

// setOffline records why the volume of the entry is offline
func (self *Registry) setOffline(e *entry, err error) {
	self.Lock()
	defer self.Unlock()
	e.offline = err
}

// Schemes returns the schemes of the URLs which backends can be created from,
// in order, such as "file", of those which are built in, and those of
// WithBackend
func (self *Registry) Schemes() []string {
	return slices.Sorted(maps.Keys(self.backends))
}

// forget removes a volume's index, and its state, from the registry's store,
// when it isn't indexed any more, or its backend is another, which doesn't
// write the state when there's none
func (self *Registry) forget(ctx context.Context, volume string) {
	_ = self.store.DeleteObject(ctx, volume, "/")
	if state, err := self.store.State(ctx, volume); err == nil && state != (schema.IndexState{}) {
		_ = self.store.SetState(ctx, volume, schema.IndexState{})
	}
}

// With calls fn with the backend for the specified volume, or returns an error
// if the volume is not registered. The backend is not closed until fn returns,
// so fn must finish using the backend before it returns. The context passed
// to fn is cancelled when ctx is cancelled, or when the volume is removed, so
// that long-running operations do not delay removing it. The registry is not
// locked while fn runs, so fn may call other registry methods, but Del or
// Update of its own volume, with the context passed to it, is ErrConflict,
// as they'd wait for fn to return.
func (self *Registry) With(ctx context.Context, volume string, fn func(context.Context, gofiler.Filer) error) error {
	// Find the backend, and record that it is in use
	e, err := self.use(ctx, volume)
	if err != nil {
		return err
	}
	defer e.wg.Done()

	// Cancel the context passed to fn when the volume is removed
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()

	return fn(ctx, e.Filer)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// new creates a backend from a URL, whose path must be absolute, which is
// indexed, of its index option, such as index=folders, which is the
// registry's, so the backend doesn't see it. The caller must close it.
func (self *Registry) new(ctx context.Context, u *url.URL) (gofiler.Filer, error) {
	if u == nil {
		return nil, gofiler.ErrBadParameter.With("url is nil")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid options: %v", err)
	}
	mode, err := self.indexMode(query)
	if err != nil {
		return nil, err
	}
	stripped := *u
	query.Del(indexer.IndexParam)
	stripped.RawQuery = query.Encode()
	b, err := self.newBackend(ctx, &stripped)
	if err != nil {
		return nil, err
	}
	return self.indexed(VolumeName(u), b, mode)
}

// indexMode returns what's indexed of a volume, of its options
func (self *Registry) indexMode(query url.Values) (indexer.Mode, error) {
	if !query.Has(indexer.IndexParam) {
		return indexer.ModeNone, nil
	}
	return indexer.ParseMode(query.Get(indexer.IndexParam))
}

// indexed returns the backend of a volume, which is indexed, unless the
// mode is ModeNone, and is closed when it can't be
func (self *Registry) indexed(volume string, b gofiler.Filer, mode indexer.Mode) (gofiler.Filer, error) {
	if mode == indexer.ModeNone {
		return b, nil
	}
	opts := []indexer.Opt{indexer.WithMode(mode), indexer.WithTracer(self.backendOpts.Tracer)}
	if self.metadata != nil {
		opts = append(opts, indexer.WithMetadataTask(self.metadata))
	}
	i, err := indexer.New(volume, b, self.store, opts...)
	if err != nil {
		return nil, errors.Join(err, closeFiler(b))
	}
	return i, nil
}

// newBackend creates a backend from a URL, without the registry's options,
// of the backend of its scheme (see WithBackend)
func (self *Registry) newBackend(ctx context.Context, u *url.URL) (gofiler.Filer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	} else if u == nil {
		return nil, gofiler.ErrBadParameter.With("url is nil")
	}

	// The options, which are rejected when they can't be parsed, rather than
	// those which can't being left out, so a backend can read them of the URL
	if _, err := url.ParseQuery(u.RawQuery); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid options: %v", err)
	}
	open, exists := self.backends[u.Scheme]
	if !exists {
		return nil, gofiler.ErrBadParameter.Withf("unsupported scheme: %q", u.Scheme)
	}
	return open(ctx, u, self.backendOpts)
}

// register registers the backend with the entry of the volume, unless the
// registry was closed in the meantime, when it's closed, and watches it until
// it's removed. Its watch sends a rescan first, once it's watching, as its
// changes may have been missed, such as when it's updated, or opened again.
func (self *Registry) register(e *entry, volume string, b gofiler.Filer) (gofiler.Filer, error) {
	self.Lock()
	defer self.Unlock()
	if self.backend == nil {
		return nil, errors.Join(gofiler.ErrServiceUnavailable.With("registry is closed"), closeFiler(b))
	}
	e.Filer, e.state = b, stateReady
	e.ctx, e.cancel = context.WithCancel(context.Background())

	// An indexed volume is indexed until it's removed, which waits for it
	if i, ok := b.(*indexer.Indexer); ok {
		e.wg.Add(1)
		go func(ctx context.Context) {
			defer e.wg.Done()
			_ = i.Run(ctx, self.logger.Load())
		}(e.ctx)
	}
	self.changedVolumes()
	return b, nil
}

// unreserve unregisters the volume, which was registered with the entry
// while it was added, unless the registry is closed
func (self *Registry) unreserve(e *entry, volume string) {
	self.Lock()
	defer self.Unlock()
	if self.backend != nil && self.backend[volume] == e {
		delete(self.backend, volume)
	}
}

// startClose marks the backend as closing, so it can't be used, and closes
// it in the background, once the calls to With which are using it, and
// watching it, which are cancelled, have returned, and then removes it from the
// registry, and closes e.closed. The registry must be locked.
func (self *Registry) startClose(e *entry, volume string) {
	e.state = stateClosing
	e.cancel()
	self.teardown[e] = struct{}{}
	go func() {
		e.wg.Wait()
		e.err = closeFiler(e.Filer)

		// Its index is removed, when it's deleted, or isn't indexed any
		// more, once its indexer has stopped, and before its name is
		// released, with a context which outlives the request
		if e.forget {
			self.forget(context.Background(), volume)
		}
		self.Lock()
		if self.backend != nil && self.backend[volume] == e {
			delete(self.backend, volume)
		}
		delete(self.teardown, e)
		self.Unlock()
		close(e.closed)
	}()
}

// closeFiler closes a filer, when it holds resources, which it releases
func closeFiler(f gofiler.Filer) error {
	if closer, ok := f.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
