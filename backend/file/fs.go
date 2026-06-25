package file

import (
	"io/fs"
	"os"
	"path/filepath"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// WritableFS extends fs.FS with write operations against a rooted directory.
type WritableFS interface {
	fs.FS
	Root() string
	Create(name string) (*os.File, error)
	MkdirAll(name string, perm fs.FileMode) error
	Remove(name string) error
	RemoveAll(name string) error
}

// dirFS is the local-directory implementation of WritableFS.
type dirFS struct {
	root string
	fs   fs.FS
}

var _ WritableFS = (*dirFS)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newWritableFS(root string) (WritableFS, error) {
	fsys, err := fs.Sub(os.DirFS(root), ".")
	if err != nil {
		return nil, err
	}
	return &dirFS{root: root, fs: fsys}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Root returns the underlying directory path.
func (w *dirFS) Root() string {
	return w.root
}

// Open implements fs.FS.
func (w *dirFS) Open(name string) (fs.File, error) {
	return w.fs.Open(name)
}

// Create creates or truncates the named file for writing.
// Parent directories are created as needed.
func (w *dirFS) Create(name string) (*os.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "create", Path: name, Err: fs.ErrInvalid}
	}
	osPath := filepath.Join(w.root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(osPath), 0o755); err != nil {
		return nil, err
	}
	return os.Create(osPath)
}

// MkdirAll creates the named directory and any necessary parents.
func (w *dirFS) MkdirAll(name string, perm fs.FileMode) error {
	if name != "." && !fs.ValidPath(name) {
		return &fs.PathError{Op: "mkdir", Path: name, Err: fs.ErrInvalid}
	}
	return os.MkdirAll(filepath.Join(w.root, filepath.FromSlash(name)), perm)
}

// Remove removes the named file or empty directory.
func (w *dirFS) Remove(name string) error {
	if !fs.ValidPath(name) {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrInvalid}
	}
	return os.Remove(filepath.Join(w.root, filepath.FromSlash(name)))
}

// RemoveAll removes the named path and all its children.
func (w *dirFS) RemoveAll(name string) error {
	if !fs.ValidPath(name) {
		return &fs.PathError{Op: "removeall", Path: name, Err: fs.ErrInvalid}
	}
	return os.RemoveAll(filepath.Join(w.root, filepath.FromSlash(name)))
}
