// Package fs provides the filesystem interface for managing objects in directories.
package fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type FS struct {
	name string
	fs   fs.FS
	path string // the directory of the filesystem, when it has one
	opt

	// done is closed when the filesystem is closed, which requests, and
	// reads in progress, check, so they stop
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

var _ filer.Filer = (*FS)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Create a new writable filesystem backend for the named volume, rooted at
// the directory path, with the options (see opt.go). Files can't be read or
// written outside the directory.
func New(name, path string, opts url.Values) (*FS, error) {
	wfs, err := newWriterFS(path)
	if err != nil {
		return nil, err
	}
	self, err := NewFS(name, wfs, opts)
	if err != nil {
		return nil, errors.Join(err, wfs.Close())
	}
	return self, nil
}

// Create a new filesystem backend for the named volume, with the options
// (see opt.go). The backend takes ownership of fsys: when it implements
// io.Closer, it's closed when the backend is closed, so the caller shouldn't
// close or reuse it afterwards.
func NewFS(name string, fsys fs.FS, opts url.Values) (*FS, error) {
	self := &FS{done: make(chan struct{})}

	// Check parameters
	if types.IsIdentifier(name) == false {
		return nil, filer.ErrBadParameter.Withf("Invalid identifier: %q", name)
	} else {
		self.name = name
	}

	// Set the filesystem
	if fsys == nil {
		return nil, filer.ErrBadParameter.With("Invalid fs")
	} else {
		self.fs = fsys
	}

	// Set the directory, when the filesystem is rooted at one, which is
	// kept for its URL once it's closed
	if fsys, ok := fsys.(pathFS); ok {
		self.path = fsys.Path()
	}

	// Set the options
	if opt, err := parseOpts(opts); err != nil {
		return nil, err
	} else {
		self.opt = opt
	}

	// A filesystem which doesn't implement the WriterFS interface is
	// read-only, whatever the readonly option is
	if _, ok := fsys.(filer.WriterFS); !ok {
		self.readonly = true
	}

	// Return success
	return self, nil
}

// Close the filesystem, and the filesystem it's backed by, when it can be
// closed. Requests, and reads in progress, then return fs.ErrClosed. It's
// safe to call more than once, and while requests are being made.
func (self *FS) Close() error {
	self.closeOnce.Do(func() {
		close(self.done)
		if closer, ok := self.fs.(io.Closer); ok {
			self.closeErr = closer.Close()
		}
	})
	return self.closeErr
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - PROPERTIES

// Readonly returns true when objects can't be created or deleted, by the
// readonly option, or as the filesystem can't be written
func (self *FS) Readonly() bool {
	return self.readonly
}

// URL returns the URL of the filesystem, which a filesystem can be created
// again from: file://<name>/<path> for a filesystem rooted at a directory,
// or fs://<name> for one which isn't, with the options which aren't the
// defaults as its query
func (self *FS) URL() *url.URL {
	url := &url.URL{
		Scheme: schema.SchemeFS,
		Host:   self.name,
	}
	if self.path != "" {
		url.Scheme, url.Path = schema.SchemeFile, urlPath(self.path)
	}

	// Set the options which aren't the defaults, including whether it's
	// read-only
	url.RawQuery = self.opt.values().Encode()

	// Return the URL
	return url
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// checkRequest returns an error if the context is cancelled or the backend is
// closed
func (self *FS) checkRequest(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	} else if self.isClosed() {
		return fs.ErrClosed
	}
	return nil
}

// isClosed returns true when the filesystem is closed
func (self *FS) isClosed() bool {
	select {
	case <-self.done:
		return true
	default:
		return false
	}
}

// isObject returns true for the file modes which are objects, which are
// regular files and directories
func isObject(mode fs.FileMode) bool {
	return mode.IsRegular() || mode.IsDir()
}

// urlPath converts the path of a directory to the path of a file URL, with
// forward slashes, and a leading "/", which a Windows path, such as
// C:\media, doesn't have, so its URL is file://<name>/C:/media
func urlPath(path string) string {
	if path = filepath.ToSlash(path); !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

// fsPath converts an object path to a path in the filesystem. Object paths
// start with "/", which is the root of the volume, and the filesystem uses
// paths without a leading "/", with "." as the root. A path without a leading
// "/" is accepted as relative to the root.
func fsPath(path string) string {
	if path = strings.TrimPrefix(path, "/"); path == "" {
		return "."
	}
	return path
}
