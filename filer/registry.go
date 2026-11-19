package filer

import (
	"context"
	"errors"
	"io"
	"maps"
	"net/url"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/filer/fs"
	google "github.com/mutablelogic/go-filer/filer/google"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	s3 "github.com/mutablelogic/go-filer/filer/s3"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
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
	// is closed, and before its name is released, as it's deleted, or isn't
	// indexed any more. It's only set with the registry locked, before it's
	// closed.
	forget bool

	// remove is true when the volume is removed from the registry's store,
	// with its index, once its backend is closed, as it's deleted. It's only
	// set with the registry locked, before it's closed.
	remove bool
}

// usingKey is the key of the value in the context which With passes to its
// function, so Del or Update of the volume within it is a conflict, rather
// than waiting for the function to return
type usingKey struct {
	e *entry
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

// schemes are the schemes of the URLs which backends are created from
var schemes = []string{schema.SchemeFile, schema.SchemeS3, google.TasksScheme, google.ContactsScheme}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a registry of the backends of volumes
func New(ctx context.Context, opts ...Opt) (*Registry, error) {
	self := &Registry{
		backend:  make(map[string]*entry),
		changed:  make(chan struct{}, 1),
		teardown: make(map[*entry]struct{}),
		done:     make(chan struct{}),
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

// Volumes returns the volumes which are registered, and can be used, in
// order of their names, with the URLs of their backends
func (self *Registry) Volumes() []schema.Volume {
	self.RLock()
	defer self.RUnlock()
	volumes := make([]schema.Volume, 0, len(self.backend))
	for name, e := range self.backend {
		if e.state == stateReady {
			volumes = append(volumes, self.volumeOf(name, e))
		}
	}
	slices.SortFunc(volumes, func(a, b schema.Volume) int {
		return strings.Compare(a.Name, b.Name)
	})
	return volumes
}

// Volume returns the volume, which is registered, and can be used, with the
// URL of its backend, or ErrNotFound when it isn't registered, or is being
// added or removed
func (self *Registry) Volume(name string) (*schema.Volume, error) {
	self.RLock()
	defer self.RUnlock()
	if e, exists := self.backend[name]; !exists || e.state != stateReady {
		return nil, gofiler.ErrNotFound.Withf("volume %q is not mounted", name)
	} else {
		return types.Ptr(self.volumeOf(name, e)), nil
	}
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

// volumeOf returns the volume of an entry, with the URL of its backend,
// whether it's enabled, which it isn't while it's offline, with why, and its
// index's state, when it's indexed. The registry must be locked.
func (self *Registry) volumeOf(name string, e *entry) schema.Volume {
	volume := schema.Volume{VolumeMeta: self.metaOf(name, e.Filer)}
	if e.offline != nil {
		volume.Enabled, volume.Error = false, e.offline.Error()
	}
	if i, ok := e.Filer.(*indexer.Indexer); ok {
		volume.Index = &schema.VolumeIndex{Mode: string(indexer.ModeFolders)}
		if state, err := i.State(context.Background()); err != nil {
			// Its state can't be read, which is why it isn't indexed
			volume.Index.Error = err.Error()
		} else {
			volume.Index.IndexState = state
		}
	}
	return volume
}

// metaOf returns what's stored of a volume, of its backend: its name, its
// URL, which, with the registry's store, has index=none when it isn't
// indexed, and that it's enabled
func (self *Registry) metaOf(name string, b gofiler.Filer) schema.VolumeMeta {
	u := b.URL()
	if _, indexed := b.(*indexer.Indexer); !indexed && self.indexStore != nil {
		query := u.Query()
		query.Set(indexer.IndexParam, string(indexer.ModeNone))
		u.RawQuery = query.Encode()
	}
	return schema.VolumeMeta{Name: name, URL: u.String(), Enabled: true}
}

// setOffline records why the volume of the entry is offline
func (self *Registry) setOffline(e *entry, err error) {
	self.Lock()
	defer self.Unlock()
	e.offline = err
}

// Schemes returns the schemes of the URLs which backends can be created from,
// in order, such as "file"
func (self *Registry) Schemes() []string {
	result := slices.Clone(schemes)
	slices.Sort(result)
	return result
}

// Validate returns the URL of a backend, after checking a backend can be
// created from it, which is then closed. The URL is the backend's, with
// each of its options, defaults too, which a backend can be added from.
func (self *Registry) Validate(ctx context.Context, url *url.URL) (*url.URL, error) {
	backend, err := self.new(ctx, url)
	if err != nil {
		return nil, err
	}

	// Get the URL, and then close the backend
	result := backend.URL()
	if err := closeFiler(backend); err != nil {
		return nil, err
	}
	return result, nil
}

// Add a backend to the registry for the specified volume, created from the
// URL, and return it. The volume is watched until it's removed, and its
// changes are sent to the calls to WatchObjects. Returns an error if a backend already exists for the volume, or
// is being added, or the URL does not name the volume. The registry isn't
// locked while the backend is created, which is cancelled when ctx is.
func (self *Registry) Add(ctx context.Context, volume string, url *url.URL) (gofiler.Filer, error) {
	// Check the registry is open, and the volume is not already registered,
	// and register it, so it isn't added twice, but can't be used yet
	e := &entry{closed: make(chan struct{})}
	self.Lock()
	if self.backend == nil {
		self.Unlock()
		return nil, gofiler.ErrServiceUnavailable.With("registry is closed")
	} else if _, exists := self.backend[volume]; exists {
		self.Unlock()
		return nil, gofiler.ErrConflict.Withf("volume already exists: %q", volume)
	}
	self.backend[volume] = e
	self.Unlock()

	// Create the backend, and register it, or unregister the volume when it
	// can't be
	b, err := self.open(ctx, e, volume, url)
	if err != nil {
		self.unreserve(e, volume)
		return nil, err
	}
	return b, nil
}

// Update changes the options of a volume, which are the query of the
// URL of its backend, and returns it. Each key of the patch replaces the
// option's values with its own, or, when it has none, removes the option, so
// it's the default (see gofiler.PatchValues). The backend is cloned with the
// options first, which checks them, so a volume isn't changed when they're
// invalid, or would rename it. Then the volume's backend is closed, as Del
// closes it, waiting for the calls to With which are using it, and the clone
// is registered in its place. Its name is kept while it's closed, so it isn't
// added by another call. Returns ErrNotImplemented when the backend can't be
// cloned, such as a filesystem which isn't a directory, ErrNotFound when the volume isn't registered, or
// is being added or removed, and ErrConflict when it's called from a
// function passed to With which is using the volume, which it'd wait for.
func (self *Registry) Update(ctx context.Context, volume string, patch url.Values) (*schema.Volume, error) {
	e, err := self.use(volume)
	if err != nil {
		return nil, err
	}
	e.wg.Done()
	if ctx.Value(usingKey{e}) != nil {
		return nil, gofiler.ErrConflict.Withf("volume %q can't be updated by a function which is using it", volume)
	}
	return self.update(ctx, e, volume, patch)
}

// update changes the options of the volume, whose backend is the entry's,
// with the patch, as Update does, or opens it again, with a nil patch, when
// watching it fails. Returns ErrNotFound when the volume's backend isn't the
// entry's, as it's been removed, or changed. Once the clone is registered,
// it's watched, with a rescan, as changes are missed while the backend is
// closed, and the options can change which objects it has.
func (self *Registry) update(ctx context.Context, e *entry, volume string, patch url.Values) (*schema.Volume, error) {
	// Clone the backend with the options, which checks them, while it's still
	// used, so it isn't changed when they're invalid, or would rename it
	clone, err := self.clone(ctx, volume, e.Filer, patch)
	if err != nil {
		return nil, err
	} else if name := VolumeName(clone.URL()); name != volume {
		return nil, errors.Join(gofiler.ErrBadParameter.Withf("url %q is not for volume %q", clone.URL(), volume), closeFiler(clone))
	} else if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, closeFiler(clone))
	} else if err := self.put(ctx, volume, clone); err != nil {
		return nil, err
	}

	// Close the backend, keeping its name, unless it's changed meanwhile
	pending := &entry{closed: make(chan struct{})}
	self.Lock()
	if self.backend == nil {
		self.Unlock()
		return nil, errors.Join(gofiler.ErrServiceUnavailable.With("registry is closed"), closeFiler(clone))
	} else if current, exists := self.backend[volume]; !exists || current != e || e.state == stateClosing {
		self.Unlock()
		return nil, errors.Join(gofiler.ErrNotFound.Withf("volume %q is not mounted", volume), closeFiler(clone))
	}
	// A volume which isn't indexed any more doesn't keep its index, which
	// is removed once it's closed, while the pending entry has its name
	_, wasIndexed := e.Filer.(*indexer.Indexer)
	_, indexed := clone.(*indexer.Indexer)
	e.forget = wasIndexed && !indexed
	self.startClose(e, volume)
	self.backend[volume] = pending
	self.Unlock()
	<-e.closed

	// Register the clone in its place, which only fails when the registry is
	// closed in the meantime
	if _, err := self.register(pending, volume, clone); err != nil {
		self.unreserve(pending, volume)
		return nil, err
	}

	// Return the volume, which isn't offline, as it's opened again
	self.RLock()
	defer self.RUnlock()
	return types.Ptr(self.volumeOf(volume, pending)), nil
}

// Del removes the backend for the specified volume from the registry, and
// closes it once any calls to With which are using it have returned, which
// are cancelled, and it's no longer watched. It can't be used once Del is called, but
// stays registered until it's closed, so a volume which isn't registered has
// released what it used, such as its directory. Del waits until the backend
// is closed, or ctx is cancelled. Returns ErrConflict, and doesn't remove
// it, when it's called from a function passed to With which is using the
// volume, which it'd wait for.
// Does nothing if there is no backend for the volume, or it's being added.
func (self *Registry) Del(ctx context.Context, volume string) error {
	self.Lock()
	e, exists := self.backend[volume]
	if exists && ctx.Value(usingKey{e}) != nil {
		self.Unlock()
		return gofiler.ErrConflict.Withf("volume %q can't be removed by a function which is using it", volume)
	}
	if exists = exists && e.state != stateAdding; exists && e.state == stateReady {
		// It's removed from the store, with its index, once it's closed,
		// before its name is released
		e.remove = true
		self.startClose(e, volume)
	}
	self.Unlock()
	if !exists {
		return nil
	}

	select {
	case <-e.closed:
		return e.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// forget removes a volume's index, and its state, from the registry's store
func (self *Registry) forget(ctx context.Context, volume string) {
	if self.indexStore != nil {
		_ = self.indexStore.Delete(ctx, volume, "/")
		_ = self.indexStore.SetState(ctx, volume, schema.IndexState{})
	}
}

// remove removes a volume, with its index, from the registry's store
func (self *Registry) remove(ctx context.Context, volume string) {
	if self.indexStore != nil {
		_ = self.indexStore.DeleteVolume(ctx, volume)
	}
}

// put stores a volume, of its backend, in the registry's store, so it's
// mounted when the server starts: its URL, with its index option, and that
// it's enabled. The backend is closed when it can't be.
func (self *Registry) put(ctx context.Context, volume string, b gofiler.Filer) error {
	if self.indexStore == nil {
		return nil
	}
	if _, err := self.indexStore.PutVolume(ctx, self.metaOf(volume, b)); err != nil {
		return errors.Join(err, closeFiler(b))
	}
	return nil
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
	e, err := self.use(volume)
	if err != nil {
		return err
	}
	defer e.wg.Done()

	// Cancel the context passed to fn when the volume is removed, and mark
	// it as using the volume, so it can't remove or update it
	ctx, cancel := context.WithCancel(context.WithValue(ctx, usingKey{e}, true))
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

// indexMode returns what's indexed of a volume, of its options, which must
// be ModeNone without the registry's store
func (self *Registry) indexMode(query url.Values) (indexer.Mode, error) {
	if !query.Has(indexer.IndexParam) {
		return indexer.ModeNone, nil
	}
	mode, err := indexer.ParseMode(query.Get(indexer.IndexParam))
	if err != nil {
		return mode, err
	} else if mode != indexer.ModeNone && self.indexStore == nil {
		return mode, gofiler.ErrBadParameter.Withf("%s=%s needs the registry's indexer store", indexer.IndexParam, mode)
	}
	return mode, nil
}

// indexed returns the backend of a volume, which is indexed, unless the
// mode is ModeNone, and is closed when it can't be
func (self *Registry) indexed(volume string, b gofiler.Filer, mode indexer.Mode) (gofiler.Filer, error) {
	if mode == indexer.ModeNone {
		return b, nil
	}
	i, err := indexer.New(volume, b, self.indexStore)
	if err != nil {
		return nil, errors.Join(err, closeFiler(b))
	}
	return i, nil
}

// clone returns a clone of a volume's backend, with the patch, which can
// change its index option, so the backend which is indexed is cloned, and
// then indexed, or not
func (self *Registry) clone(ctx context.Context, volume string, b gofiler.Filer, patch url.Values) (gofiler.Filer, error) {
	idx, indexed := b.(*indexer.Indexer)
	mode := indexer.ModeNone
	if indexed {
		mode, b = indexer.ModeFolders, idx.Backend()
	}
	if patch.Has(indexer.IndexParam) {
		var err error
		if mode, err = self.indexMode(url.Values{indexer.IndexParam: patch[indexer.IndexParam]}); err != nil {
			return nil, err
		}
		patch = maps.Clone(patch)
		delete(patch, indexer.IndexParam)
	}
	clone, err := b.Clone(ctx, patch)
	if err != nil {
		return nil, err
	}
	return self.indexed(volume, clone, mode)
}

// newBackend creates a backend from a URL, without the registry's options
func (self *Registry) newBackend(ctx context.Context, u *url.URL) (gofiler.Filer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	} else if u == nil {
		return nil, gofiler.ErrBadParameter.With("url is nil")
	}

	// The options, which are rejected when they can't be parsed, rather than
	// those which can't being left out
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid options: %v", err)
	}
	switch u.Scheme {
	case schema.SchemeFile:
		if types.IsIdentifier(u.Host) == false {
			return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", u.Host)
		}

		// The path is absolute, so a URL without one isn't the directory the
		// registry is running in. On Windows, it's /C:/media, which is
		// converted to C:/media.
		path := u.Path
		if runtime.GOOS == "windows" {
			path = types.FilePath(&url.URL{Scheme: schema.SchemeFile, Path: u.Path})
		}
		if path == "" || !filepath.IsAbs(filepath.FromSlash(path)) {
			return nil, gofiler.ErrBadParameter.Withf("path of volume %q isn't absolute: %q", u.Host, path)
		}
		if b, err := fs.New(u.Host, path, query, self.backendOpts); err != nil {
			return nil, err
		} else {
			return b, nil
		}
	case schema.SchemeS3:
		if b, err := s3.New(ctx, u, self.backendOpts); err != nil {
			return nil, err
		} else {
			return b, nil
		}
	case google.TasksScheme:
		// The tasks of a Google account, of its sign-in, of the provider of
		// credentials, which is a SignInError, with how to sign in, when
		// it's needed
		if u.Path != "" && u.Path != "/" {
			return nil, gofiler.ErrBadParameter.Withf("a volume of Google Tasks has no path: %q", u.Path)
		}
		// The registry's options of its backends: its credentials, of the
		// sign-in, and its client options, as they are
		if b, err := google.NewTaskFiler(ctx, u.Host, query, self.backendOpts); err != nil {
			return nil, err
		} else {
			return b, nil
		}
	case google.ContactsScheme:
		// The contacts of a Google account, of its sign-in, as for tasks
		if u.Path != "" && u.Path != "/" {
			return nil, gofiler.ErrBadParameter.Withf("a volume of Google Contacts has no path: %q", u.Path)
		}
		if b, err := google.NewContactFiler(ctx, u.Host, query, self.backendOpts); err != nil {
			return nil, err
		} else {
			return b, nil
		}
	default:
		return nil, gofiler.ErrBadParameter.Withf("unsupported scheme: %q", u.Scheme)
	}
}

// open creates the backend of the volume, which is registered with the
// entry, and checks it's for the volume, and registers it, and watches it
// until it's removed. The volume is still registered when it can't be, so
// the caller can open it again, or unreserve it. Returns ErrServiceUnavailable
// when the registry was closed meanwhile.
func (self *Registry) open(ctx context.Context, e *entry, volume string, u *url.URL) (gofiler.Filer, error) {
	b, err := self.new(ctx, u)
	if err != nil {
		return nil, err
	} else if name := VolumeName(b.URL()); name != volume {
		return nil, errors.Join(gofiler.ErrBadParameter.Withf("url %q is not for volume %q", b.URL(), volume), closeFiler(b))
	} else if err := self.put(ctx, volume, b); err != nil {
		return nil, err
	}
	return self.register(e, volume, b)
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
			_ = i.Run(ctx)
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
		if e.remove {
			self.remove(context.Background(), volume)
		} else if e.forget {
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
