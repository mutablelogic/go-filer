package fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"path"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// reader reads a file, returning an error once the context is cancelled, or
// the filesystem is closed, before each read
type reader struct {
	ctx  context.Context
	fs   *FS
	r    io.Reader
	name string // the file's name, which is empty for a request's body
}

// seekReader is a reader of a file which can seek, such as a file on disk,
// so its content can be read in ranges, which also returns an error once the
// context is cancelled, or the filesystem is closed, before each seek
type seekReader struct {
	reader
	s io.Seeker
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Read object content from the filesystem. Returns the object, and a function
// which reads its content, which opens the file when it's called, so the
// content is read within it. Directories, and packages, can't be read. The
// file is checked when it's opened, as it could have changed since the
// object was returned: when it isn't the same size, or wasn't modified at the
// same time, it isn't read, and ErrPreconditionFailed is returned.
func (self *FS) ReadObject(ctx context.Context, req schema.GetObjectRequest) (_ filer.ReadObjectFunc, _ *schema.Object, err error) {
	ctx, end := self.span(ctx, "ReadObject", req)
	defer func() { end(err) }()

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
	return func(ctx context.Context, fn func(io.Reader) error) (err error) {
		ctx, end := self.span(ctx, "ReadObject.Read", req)
		defer func() { end(err) }()
		return self.read(ctx, name, info, fn)
	}, obj, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// read opens the named file, checks it's the file which was returned, calls
// the function with a reader of it, and closes it. The errors opening,
// reading and closing the file are of its object, but not the function's,
// such as an error writing what's read, which are returned as they are.
func (self *FS) read(ctx context.Context, name string, want fs.FileInfo, fn func(io.Reader) error) (err error) {
	f, err := self.open(ctx, name, want)
	if err != nil {
		return objectError("read", name, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, objectError("read", name, closeErr))
		}
	}()

	// Read the file within the function
	return fn(newReader(ctx, self, f, name))
}

// open opens the named file, and checks it's the file which was returned
func (self *FS) open(ctx context.Context, name string, want fs.FileInfo) (fs.File, error) {
	if err := self.checkRequest(ctx); err != nil {
		return nil, err
	}
	f, err := self.fs.Open(name)
	if err != nil {
		return nil, err
	}
	if info, err := f.Stat(); err != nil {
		return nil, errors.Join(err, f.Close())
	} else if !info.Mode().IsRegular() {
		return nil, errors.Join(&fs.PathError{Op: "read", Path: objectPath(name), Err: fs.ErrInvalid}, f.Close())
	} else if info.Size() != want.Size() || !info.ModTime().Equal(want.ModTime()) {
		return nil, errors.Join(filer.ErrPreconditionFailed.Withf("%q changed since it was returned", objectPath(name)), f.Close())
	}
	return f, nil
}

// newReader returns a reader of the file, by its name, which can seek when
// the file can, so the function reading it can check if it's an
// io.ReadSeeker. Its errors are of the file's object, unless the name is
// empty, such as a request's body, which isn't a file.
func newReader(ctx context.Context, fs *FS, f io.Reader, name string) io.Reader {
	r := reader{ctx: ctx, fs: fs, r: f, name: name}
	if s, ok := f.(io.Seeker); ok {
		return &seekReader{reader: r, s: s}
	}
	return &r
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
	} else if err != nil && r.name != "" {
		err = objectError("read", r.name, err)
	}
	return n, err
}

// Seek returns an error once the context is cancelled, or the filesystem is
// closed, rather than seeking
func (r *seekReader) Seek(offset int64, whence int) (int64, error) {
	if err := r.fs.checkRequest(r.ctx); err != nil {
		return 0, err
	}
	n, err := r.s.Seek(offset, whence)
	if err != nil && r.name != "" {
		err = objectError("seek", r.name, err)
	}
	return n, err
}
