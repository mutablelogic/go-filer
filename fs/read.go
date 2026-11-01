package fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"path"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// reader reads a file, returning an error once the context is cancelled, or
// the filesystem is closed, before each read
type reader struct {
	ctx context.Context
	fs  *FS
	r   io.Reader
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Read object content from the filesystem. Returns the object, and a function
// which reads its content, which opens the file when it's called, so the
// content is read within it. Directories, and packages, can't be read. The
// file is checked when it's opened, as it could have changed since the
// object was returned: when it isn't the same size, or wasn't modified at the
// same time, it isn't read, and ErrPreconditionFailed is returned.
func (self *FS) ReadObject(ctx context.Context, req schema.GetObjectRequest) (filer.ReadObjectFunc, *schema.Object, error) {
	if err := self.checkRequest(ctx); err != nil {
		return nil, nil, err
	}

	// Check the file is an object before it's opened, as opening a named
	// pipe blocks until it is written to
	name := fsPath(req.Path)
	info, err := self.stat("read", name)
	if err != nil {
		return nil, nil, err
	} else if info.IsDir() && self.isPackage(path.Base(name)) {
		return nil, nil, filer.ErrNotImplemented.Withf("%q is a package, which can't be read", req.Path)
	} else if info.IsDir() {
		return nil, nil, &fs.PathError{Op: "read", Path: req.Path, Err: fs.ErrInvalid}
	}

	// Return the function which reads the file, and the object, unless it
	// hasn't changed, as GetObject does. The content type is from the
	// extension, never by sniffing the content, which would read the file
	// before the function is called.
	obj := self.objectFromInfo(name, info, objectType(name))
	if notModified(obj, req) {
		return nil, nil, filer.ErrNotModified.Withf("%q", obj.Path)
	}
	return func(ctx context.Context, fn func(io.Reader) error) error {
		return self.read(ctx, name, info, fn)
	}, obj, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// read opens the named file, checks it's the file which was returned, calls
// the function with a reader of it, and closes it
func (self *FS) read(ctx context.Context, name string, want fs.FileInfo, fn func(io.Reader) error) (err error) {
	if err := self.checkRequest(ctx); err != nil {
		return err
	}

	// Open the file, and close it when the function returns
	f, err := self.fs.Open(name)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, f.Close())
	}()

	// Check the open file is the one which was returned
	if info, err := f.Stat(); err != nil {
		return err
	} else if !info.Mode().IsRegular() {
		return &fs.PathError{Op: "read", Path: objectPath(name), Err: fs.ErrInvalid}
	} else if info.Size() != want.Size() || !info.ModTime().Equal(want.ModTime()) {
		return filer.ErrPreconditionFailed.Withf("%q changed since it was returned", objectPath(name))
	}

	// Read the file within the function
	return fn(&reader{ctx: ctx, fs: self, r: f})
}

// Read returns an error once the context is cancelled, or the filesystem is
// closed, rather than reading. The end of the content is checked too, as a
// read which returns the last of it, with io.EOF, isn't followed by another,
// so the error is returned with the data, in place of io.EOF.
func (r *reader) Read(data []byte) (int, error) {
	if err := r.fs.checkRequest(r.ctx); err != nil {
		return 0, err
	}
	n, err := r.r.Read(data)
	if errors.Is(err, io.EOF) {
		if err := r.fs.checkRequest(r.ctx); err != nil {
			return n, err
		}
	}
	return n, err
}
