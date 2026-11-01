package filer

import (
	"context"
	"errors"
	"io"
	"net/url"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/filer/fs"
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

	// The calls to WatchObjects in progress, which are sent the changes to
	// every volume
	subscribers map[*subscriber]struct{}

	// The backends which are being closed, by Del, Update or Close, which
	// Close waits for, as a backend which Update is closing is no longer the
	// volume's in the registry
	teardown map[*entry]struct{}

	// done is closed when the registry is closed, which stops the calls to
	// WatchObjects
	done      chan struct{}
	closeOnce sync.Once
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

	// Set once the backend is created, as the volume is registered while
	// it's being added, so it can't be added twice, but can't be used until
	// then. It's only read or set with the registry locked.
	ready bool

	// Set while the backend is being removed, when it can't be used, but is
	// still registered until it's closed, so its volume can't be added again
	// until then. It's only read or set with the registry locked.
	closing bool

	// closed is closed once the backend is closed, and removed from the
	// registry, when err is why it couldn't be closed
	closed chan struct{}
	err    error
}

// usingKey is the key of the value in the context of a function which is
// using a backend, passed to With, so removing the volume within it doesn't
// wait for it to return
type usingKey struct {
	e *entry
}

// Ensure Registry implements io.Closer, and is a filer of every volume
var _ io.Closer = (*Registry)(nil)
var _ gofiler.Filer = (*Registry)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// schemes are the schemes of the URLs which backends are created from
var schemes = []string{schema.SchemeFile, schema.SchemeS3}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a registry of the backends of volumes
func New(ctx context.Context, opts ...Opt) (*Registry, error) {
	self := &Registry{
		backend:     make(map[string]*entry),
		subscribers: make(map[*subscriber]struct{}),
		teardown:    make(map[*entry]struct{}),
		done:        make(chan struct{}),
	}

	// Apply options
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

// Close removes and closes all the backends, cancelling and waiting for any
// calls to With which are using them, and stopping watching them, and
// waiting for those which Del is closing. Calls to WatchObjects return, and
// backends cannot be added after the registry is closed. Close can't be
// called from a function passed to With, as it would wait for the function
// to return.
func (self *Registry) Close() error {
	var result error

	// Stop the calls to WatchObjects
	self.closeOnce.Do(func() {
		close(self.done)
	})

	// Remove all the backends, and close those which aren't already being
	// closed, while the registry is locked, so they're each closed once.
	// Those which are being added are closed by Add.
	self.Lock()
	started := make(map[*entry]bool, len(self.backend))
	for volume, e := range self.backend {
		if e.ready && !e.closing {
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
		if e.ready && !e.closing {
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
		if e.ready && !e.closing {
			volumes = append(volumes, volumeOf(name, e.Filer))
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
	if e, exists := self.backend[name]; !exists || !e.ready || e.closing {
		return nil, gofiler.ErrNotFound.Withf("volume %q is not mounted", name)
	} else {
		return types.Ptr(volumeOf(name, e.Filer)), nil
	}
}

// VolumeName returns the name of the volume of a URL, which is its name
// option, when it's set, such as s3://my-bucket/?name=photos, or for a
// directory, its host, such as file://media/mnt/media, or empty, when it has
// neither
func VolumeName(u *url.URL) string {
	if u == nil {
		return ""
	} else if name := u.Query().Get("name"); name != "" {
		return name
	} else if u.Scheme == schema.SchemeFile {
		return u.Host
	}
	return ""
}

// volumeOf returns the volume of a backend, with its URL, and whether it's
// read-only, when the backend says
func volumeOf(name string, b gofiler.Filer) schema.Volume {
	volume := schema.Volume{Name: name, URL: b.URL().String()}
	if b, ok := b.(interface{ Readonly() bool }); ok {
		volume.Readonly = b.Readonly()
	}
	return volume
}

// Schemes returns the schemes of the URLs which backends can be created from,
// in order, such as "file"
func (self *Registry) Schemes() []string {
	result := slices.Clone(schemes)
	slices.Sort(result)
	return result
}

// Validate returns the URL of a backend, after checking a backend can be
// created from it, which is then closed. The URL is the backend's, with the
// options which aren't the defaults, which a backend can be added from.
func (self *Registry) Validate(ctx context.Context, url *url.URL) (*url.URL, error) {
	backend, err := self.new(ctx, url)
	if err != nil {
		return nil, err
	}

	// Get the URL, and then close the backend
	result := backend.URL()
	if err := backend.Close(); err != nil {
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
	b, err := self.open(ctx, e, volume, url, false)
	if err != nil {
		self.unreserve(e, volume)
		return nil, err
	}
	return b, nil
}

// Update changes the options of a volume, which are the query of the
// URL of its backend, and returns it. Each key of the patch replaces the
// option's values with its own, or, when it has none, removes the option, so
// it's the default. The URL with the options is checked first, by creating a
// backend from it, which is closed, so a volume isn't changed when they're
// invalid. Then the volume's backend is closed, as Del closes it, waiting for
// the calls to With which are using it, and the volume is added again, with
// the options. Its name is kept while it's closed, so it isn't added by
// another call. When the volume can't be added with the options, it's added
// again as it was. Returns ErrNotFound when the volume isn't registered, or
// is being added or removed, and ErrConflict when it's called from a
// function passed to With which is using the volume, which it would wait for.
func (self *Registry) Update(ctx context.Context, volume string, patch url.Values) (*schema.Volume, error) {
	e, err := self.use(volume)
	if err != nil {
		return nil, err
	}
	e.wg.Done()
	if ctx.Value(usingKey{e}) != nil {
		return nil, gofiler.ErrConflict.Withf("volume %q can't be updated while it's used by the caller", volume)
	}
	return self.update(ctx, e, volume, patch)
}

// update changes the options of the volume, whose backend is the entry's,
// with the patch, as Update does. Returns ErrNotFound when the volume's
// backend isn't the entry's, as it's been removed, or changed. Once it's
// opened again, it's watched again, with a rescan, as changes are missed
// while it's closed, and the options can change which objects it has.
func (self *Registry) update(ctx context.Context, e *entry, volume string, patch url.Values) (*schema.Volume, error) {
	// The URL with the options, which is checked
	old := e.URL()
	u := *old
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid options of volume %q: %v", volume, err)
	}
	for key, values := range patch {
		if len(values) == 0 {
			query.Del(key)
		} else {
			query[key] = values
		}
	}
	u.RawQuery = query.Encode()
	if _, err := self.Validate(ctx, &u); err != nil {
		return nil, err
	} else if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Close the backend, keeping its name, unless it's changed meanwhile
	pending := &entry{closed: make(chan struct{})}
	self.Lock()
	if self.backend == nil {
		self.Unlock()
		return nil, gofiler.ErrServiceUnavailable.With("registry is closed")
	} else if current, exists := self.backend[volume]; !exists || current != e || e.closing {
		self.Unlock()
		return nil, gofiler.ErrNotFound.Withf("volume %q is not mounted", volume)
	}
	self.startClose(e, volume)
	self.backend[volume] = pending
	self.Unlock()
	<-e.closed

	// Add it again with the options, or as it was when it can't be, which
	// isn't cancelled, so the volume isn't lost
	b, err := self.open(ctx, pending, volume, &u, true)
	if err != nil {
		if _, err2 := self.open(context.WithoutCancel(ctx), pending, volume, old, true); err2 != nil {
			self.unreserve(pending, volume)
			return nil, errors.Join(err, err2)
		}
		return nil, err
	}

	// Return the volume
	return types.Ptr(volumeOf(volume, b)), nil
}

// Del removes the backend for the specified volume from the registry, and
// closes it once any calls to With which are using it have returned, which
// are cancelled, and it's no longer watched. It can't be used once Del is called, but
// stays registered until it's closed, so a volume which isn't registered has
// released what it used, such as its directory. Del waits until the backend
// is closed, or ctx is cancelled, except when it's called from a function
// passed to With, which uses the backend, when it returns once the backend
// can't be used, and the backend is closed once the function returns.
// Does nothing if there is no backend for the volume, or it's being added.
func (self *Registry) Del(ctx context.Context, volume string) error {
	self.Lock()
	e, exists := self.backend[volume]
	if exists = exists && e.ready; exists && !e.closing {
		self.startClose(e, volume)
	}
	self.Unlock()
	if !exists {
		return nil
	}

	// Don't wait for the backend to be closed when it's being used by the
	// caller, which it waits for
	if ctx.Value(usingKey{e}) != nil {
		return nil
	}
	select {
	case <-e.closed:
		return e.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// With calls fn with the backend for the specified volume, or returns an error
// if the volume is not registered. The backend is not closed until fn returns,
// so fn must finish using the backend before it returns. The context passed
// to fn is cancelled when ctx is cancelled, or when the volume is removed, so
// that long-running operations do not delay removing it. The registry is not
// locked while fn runs, so fn may call other registry methods, including Del
// for the volume, with the context passed to it, but not Close.
func (self *Registry) With(ctx context.Context, volume string, fn func(context.Context, gofiler.Filer) error) error {
	// Find the backend, and record that it is in use
	e, err := self.use(volume)
	if err != nil {
		return err
	}
	defer e.wg.Done()

	// Cancel the context passed to fn when the volume is removed, and mark it
	// as using the backend
	ctx, cancel := context.WithCancel(context.WithValue(ctx, usingKey{e}, true))
	defer cancel()
	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()

	return fn(ctx, e.Filer)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// new creates a backend from a URL, whose path must be absolute. The caller
// must close it.
func (self *Registry) new(ctx context.Context, u *url.URL) (gofiler.Filer, error) {
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
		if b, err := fs.New(u.Host, path, query); err != nil {
			return nil, err
		} else {
			return b, nil
		}
	case schema.SchemeS3:
		if b, err := s3.New(ctx, u); err != nil {
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
// until it's removed, with a rescan once it's watched, when rescan is set. The volume is still registered when it can't be, so
// the caller can open it again, or unreserve it. Returns ErrServiceUnavailable
// when the registry was closed meanwhile.
func (self *Registry) open(ctx context.Context, e *entry, volume string, u *url.URL, rescan bool) (gofiler.Filer, error) {
	b, err := self.new(ctx, u)
	if err == nil {
		if name := VolumeName(b.URL()); name != volume {
			err = errors.Join(gofiler.ErrBadParameter.Withf("url %q is not for volume %q", u, volume), b.Close())
		}
	}

	// Register the backend, unless it couldn't be created, or the registry
	// was closed in the meantime, and watch it until it's removed
	self.Lock()
	defer self.Unlock()
	if self.backend == nil {
		if err == nil {
			err = errors.Join(gofiler.ErrServiceUnavailable.With("registry is closed"), b.Close())
		}
		return nil, err
	} else if err != nil {
		return nil, err
	}
	e.Filer, e.ready = b, true
	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		self.watch(e, volume, rescan)
	}()
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
	e.closing = true
	e.cancel()
	self.teardown[e] = struct{}{}
	go func() {
		e.wg.Wait()
		e.err = e.Filer.Close()
		self.Lock()
		if self.backend != nil && self.backend[volume] == e {
			delete(self.backend, volume)
		}
		delete(self.teardown, e)
		self.Unlock()
		close(e.closed)
	}()
}
