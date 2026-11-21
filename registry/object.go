package registry

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// listIterator is an iterator of a volume's objects, which sets the name of
// the volume on each, and keeps the volume's backend from being closed until
// it's closed, or the volume is removed, when it's closed, so an iterator
// which isn't closed doesn't stop the volume being removed
type listIterator struct {
	sync.Mutex
	schema.ListObjectsIterator
	ctx     context.Context // the volume's, which is cancelled when it's removed
	name    string
	release func()
	stop    func() bool
	closed  bool
}

var _ schema.ListObjectsIterator = (*listIterator)(nil)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// URL returns the URL of the registry, which is filer:, as it's the filer of
// every volume, whose URLs are those of their backends
func (self *Registry) URL() *url.URL {
	return &url.URL{Scheme: "filer"}
}

// Actions returns nil, as a registry's actions are of its volumes, each of
// its own backend's
func (self *Registry) Actions() []gofiler.Action {
	return nil
}

// Description returns what the registry is, its volumes, as each volume's
// backend has its own description
func (self *Registry) Description() string {
	return "Volumes"
}

// Clone returns ErrNotImplemented, as a registry can't be cloned: its
// volumes are cloned to change their options, with Update
func (self *Registry) Clone(context.Context, url.Values) (gofiler.Filer, error) {
	return nil, gofiler.ErrNotImplemented.With("a registry can't be cloned")
}

// CreateObject creates an object in the volume named by the request
func (self *Registry) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	var obj *schema.Object
	if err := self.With(ctx, req.Name, func(ctx context.Context, b gofiler.Filer) (err error) {
		obj, err = b.CreateObject(ctx, req)
		return err
	}); err != nil {
		return nil, err
	}
	obj.Name = req.Name
	return obj, nil
}

// GetObject returns an object in the volume named by the request
func (self *Registry) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	var obj *schema.Object
	if err := self.With(ctx, req.Name, func(ctx context.Context, b gofiler.Filer) (err error) {
		obj, err = b.GetObject(ctx, req)
		return err
	}); err != nil {
		return nil, err
	}
	obj.Name = req.Name
	return obj, nil
}

// ReadObject returns an object in the volume named by the request, and a
// function which reads its content, which keeps the volume's backend from
// being closed while it reads, and is cancelled when the volume is removed.
// Returns ErrNotFound from the function when the volume was removed, or
// added again, since the object was returned.
func (self *Registry) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	var read gofiler.ReadObjectFunc
	var obj *schema.Object
	var backend gofiler.Filer
	if err := self.With(ctx, req.Name, func(ctx context.Context, b gofiler.Filer) (err error) {
		read, obj, err = b.ReadObject(ctx, req)
		backend = b
		return err
	}); err != nil {
		return nil, nil, err
	}
	obj.Name = req.Name

	// Read the content with the backend the object was returned by
	return func(ctx context.Context, fn func(io.Reader) error) error {
		return self.With(ctx, req.Name, func(ctx context.Context, b gofiler.Filer) error {
			if b != backend {
				return gofiler.ErrNotFound.Withf("volume %q was removed", req.Name)
			}
			return read(ctx, fn)
		})
	}, obj, nil
}

// Markdown reads the content of the object, which ReadObject returned, of
// its function, as Markdown, and returns its start, and where the rest
// starts, when there's more. It's ErrNotImplemented when the registry has no
// way to read Markdown, or the object can't be read as it.
func (self *Registry) Markdown(ctx context.Context, read gofiler.ReadObjectFunc, object *schema.Object) (*schema.Markdown, error) {
	if self.markdown == nil {
		return nil, gofiler.ErrNotImplemented.With("files can't be read as Markdown")
	}
	var result *schema.Markdown
	if err := read(ctx, func(r io.Reader) (err error) {
		result, err = self.markdown(ctx, r, object)
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// ListObjects lists the objects in the volume named by the request. The
// volume's backend isn't closed until the iterator is closed, or the volume
// is removed, when the iterator is closed, and returns fs.ErrClosed.
func (self *Registry) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	e, err := self.use(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	inner, err := e.ListObjects(ctx, req)
	if err != nil {
		e.wg.Done()
		return nil, err
	}

	// Only the objects of the type, which a backend can do itself, such as
	// an indexer of folders, and otherwise they're filtered
	if req.Type != "" {
		inner = &typeIterator{ListObjectsIterator: inner, t: req.Type}
	}

	// Close the iterator when the volume is removed
	iter := &listIterator{ListObjectsIterator: inner, ctx: e.ctx, name: req.Name, release: e.wg.Done}
	iter.Lock()
	iter.stop = context.AfterFunc(e.ctx, func() { iter.Close() })
	iter.Unlock()
	return iter, nil
}

// DeleteObjects deletes an object, or the objects beneath a directory, in the
// volume named by the request
func (self *Registry) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	var resp *schema.DeleteObjectsResponse
	if err := self.With(ctx, req.Name, func(ctx context.Context, b gofiler.Filer) (err error) {
		resp, err = b.DeleteObjects(ctx, req)
		return err
	}); err != nil {
		return nil, err
	}
	for _, obj := range resp.Body {
		obj.Name = req.Name
	}
	return resp, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// use returns the backend of the volume, which isn't closed until e.wg.Done
// is called, or, when it isn't mounted, why (see unmounted)
func (self *Registry) use(ctx context.Context, volume string) (*entry, error) {
	self.RLock()
	e, exists := self.backend[volume]
	if exists && e.state == stateReady {
		e.wg.Add(1)
		self.RUnlock()
		return e, nil
	}
	failed := self.failed[volume]
	self.RUnlock()
	return nil, self.unmounted(ctx, volume, failed)
}

// unmounted returns why a volume isn't mounted: ErrNotFound when it isn't
// stored, and otherwise ErrServiceUnavailable, as it's offline, with why it
// can't be mounted, which is tried again, or it isn't enabled, or is being
// mounted, or unmounted
func (self *Registry) unmounted(ctx context.Context, volume string, failed error) error {
	stored, err := self.store.GetVolume(ctx, volume)
	switch {
	case errors.Is(err, gofiler.ErrNotFound):
		return gofiler.ErrNotFound.Withf("volume %q doesn't exist", volume)
	case err != nil:
		return err
	case !stored.Enabled:
		return gofiler.ErrServiceUnavailable.Withf("volume %q is unmounted", volume)
	case failed != nil:
		return gofiler.ErrServiceUnavailable.Withf("volume %q is offline: %v", volume, failed)
	default:
		return gofiler.ErrServiceUnavailable.Withf("volume %q isn't mounted yet", volume)
	}
}

// Next returns the next object, with the name of its volume, or io.EOF when
// there are no more, or fs.ErrClosed once the iterator is closed. It's
// cancelled when the volume is removed, as well as when ctx is, so it
// doesn't hold up closing the iterator, which waits for it.
func (self *listIterator) Next(ctx context.Context) (*schema.Object, error) {
	self.Lock()
	defer self.Unlock()
	if self.closed {
		return nil, fs.ErrClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(self.ctx, cancel)
	defer stop()
	obj, err := self.ListObjectsIterator.Next(ctx)
	if err != nil {
		return nil, err
	}
	obj.Name = self.name
	return obj, nil
}

// Close the iterator, which lets the volume's backend be closed. It's safe
// to call more than once.
func (self *listIterator) Close() error {
	self.Lock()
	defer self.Unlock()
	if self.closed {
		return nil
	}
	self.closed = true
	self.stop()
	err := self.ListObjectsIterator.Close()
	self.release()
	return err
}

// typeIterator returns the objects of a listing which are of a type
type typeIterator struct {
	schema.ListObjectsIterator
	t string
}

// Count returns how many objects the listing has, when it can count them,
// which a backend which lists by a type counts of the type, or
// ErrNotImplemented
func (self *typeIterator) Count(ctx context.Context) (uint64, error) {
	return count(ctx, self.ListObjectsIterator)
}

// Count returns how many objects the listing has, when it can count them,
// or ErrNotImplemented
func (self *listIterator) Count(ctx context.Context) (uint64, error) {
	return count(ctx, self.ListObjectsIterator)
}

// count returns how many objects a listing has, when it can count them, or
// ErrNotImplemented
func count(ctx context.Context, iter schema.ListObjectsIterator) (uint64, error) {
	if c, ok := iter.(schema.Counter); ok {
		return c.Count(ctx)
	}
	return 0, gofiler.ErrNotImplemented.With("the listing can't be counted")
}

func (self *typeIterator) Next(ctx context.Context) (*schema.Object, error) {
	for {
		obj, err := self.ListObjectsIterator.Next(ctx)
		if err != nil || schema.MatchType(obj, self.t) {
			return obj, err
		}
	}
}
