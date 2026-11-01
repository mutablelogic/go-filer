package filer

import (
	"context"
	"io"
	"io/fs"
	"net/url"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
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

// ListObjects lists the objects in the volume named by the request. The
// volume's backend isn't closed until the iterator is closed, or the volume
// is removed, when the iterator is closed, and returns fs.ErrClosed.
func (self *Registry) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	e, err := self.use(req.Name)
	if err != nil {
		return nil, err
	}
	inner, err := e.ListObjects(ctx, req)
	if err != nil {
		e.wg.Done()
		return nil, err
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
// is called, or ErrNotFound when the volume isn't registered, or is being
// added or removed
func (self *Registry) use(volume string) (*entry, error) {
	self.RLock()
	defer self.RUnlock()
	e, exists := self.backend[volume]
	if !exists || !e.ready || e.closing {
		return nil, gofiler.ErrNotFound.Withf("volume %q is not mounted", volume)
	}
	e.wg.Add(1)
	return e, nil
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
