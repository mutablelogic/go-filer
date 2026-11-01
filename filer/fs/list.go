// Package fs provides the filesystem interface for managing objects in directories.
package fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"path"
	"slices"
	"strings"
	"sync"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// listIterator walks a directory tree depth-first, returning entries in the
// same order as S3: by the byte order of the full path, where a directory
// sorts as its name followed by "/". Only one directory listing is held in
// memory at each level of the tree.
type listIterator struct {
	*FS
	mu        sync.Mutex
	recursive bool
	frames    []*listFrame
	closed    bool
}

// listFrame holds the remaining entries of one directory
type listFrame struct {
	dir     string
	entries []fs.DirEntry
}

var _ schema.ListObjectsIterator = (*listIterator)(nil)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// List objects in the filesystem
func (self *FS) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (_ schema.ListObjectsIterator, err error) {
	ctx, end := self.span(ctx, "ListObjects", req)
	defer func() { end(err) }()
	defer func() { err = objectError("list", req.Path, err) }()

	if err := self.checkRequest(ctx); err != nil {
		return nil, err
	} else {
		return newListIterator(self, req)
	}
}

// Next returns the next object, or io.EOF when there are no more objects
func (self *listIterator) Next(ctx context.Context) (*schema.Object, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	if err := self.checkRequest(ctx); err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		} else if self.closed || self.isClosed() {
			return nil, fs.ErrClosed
		} else if len(self.frames) == 0 {
			return nil, io.EOF
		}

		// Pop a finished directory
		frame := self.frames[len(self.frames)-1]
		if len(frame.entries) == 0 {
			self.frames = self.frames[:len(self.frames)-1]
			continue
		}

		// Take the next entry, skipping hidden files and others which
		// aren't content
		entry := frame.entries[0]
		frame.entries = frame.entries[1:]
		if self.skip(entry.Name()) {
			continue
		}
		name := path.Join(frame.dir, entry.Name())

		// Skip files which are not objects, such as sockets and devices.
		// Symbolic links are checked once resolved.
		if entry.Type()&fs.ModeSymlink == 0 && !isObject(entry.Type()) {
			continue
		}

		// A package is a file, which isn't descended into
		if entry.IsDir() && self.isPackage(entry.Name()) {
			if obj, err := self.object(name, entry.Info); err == nil {
				return obj, nil
			} else if !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
			continue
		}

		// Return directories, and descend into them when recursive.
		// Directories removed during iteration are skipped.
		if entry.IsDir() {
			if !self.recursive {
				return self.object(name, entry.Info)
			} else if err := self.push(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			} else if err == nil {
				return self.object(name, entry.Info)
			}
			continue
		}

		// Resolve symbolic links, skipping those which are broken or point
		// outside the filesystem. A link to a directory is returned, but isn't
		// descended into, as it could loop.
		if entry.Type()&fs.ModeSymlink != 0 {
			info, err := fs.Stat(self.fs, name)
			if err != nil || !isObject(info.Mode()) {
				continue
			}
			return self.object(name, func() (fs.FileInfo, error) { return info, nil })
		}

		// Return the file, skipping it if it was removed during iteration
		if obj, err := self.object(name, entry.Info); err == nil {
			return obj, nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newListIterator returns an iterator over the entries beneath the path. If it
// names a file rather than a directory, the iterator returns just that file.
func newListIterator(fsys *FS, req schema.ListObjectsRequest) (*listIterator, error) {
	self := &listIterator{
		FS:        fsys,
		recursive: req.Recursive,
	}

	// An empty path, or "/", lists from the root, and a path which ends with
	// "/" is a directory
	name := fsPath(strings.TrimSuffix(req.Path, "/"))
	dir := name != "." && strings.HasSuffix(req.Path, "/")

	// Stat the path, and either list the directory or return the single file,
	// which a package is, unless it must be a directory
	if info, err := fsys.stat("list", name); err != nil {
		return nil, err
	} else if isDir := info.IsDir() && !self.isPackage(path.Base(name)); dir && !isDir {
		return nil, &fs.PathError{Op: "list", Path: req.Path, Err: fs.ErrNotExist}
	} else if isDir {
		if err := self.push(name); err != nil {
			return nil, err
		}
	} else {
		self.frames = append(self.frames, &listFrame{
			dir:     path.Dir(name),
			entries: []fs.DirEntry{fs.FileInfoToDirEntry(info)},
		})
	}

	// Skip objects up to the start path
	if err := self.seek(req.StartAfter); err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

// Close the iterator. Any further calls to Next return fs.ErrClosed
func (self *listIterator) Close() error {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.frames = nil
	self.closed = true
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// push reads the directory and adds its entries to the top of the stack
//
// Entries are sorted so that directories compare as their name followed by
// "/". As a depth-first walk visits each directory's descendants together,
// this results in the full paths being returned in byte order, matching S3.
// For example, "a-b.txt" < "a.txt" < "a/b.txt" < "a0.txt".
func (self *listIterator) push(dir string) error {
	entries, err := fs.ReadDir(self.fs, dir)
	if err != nil {
		return err
	}

	// Compute the sort key for each entry
	keys := make(map[fs.DirEntry]string, len(entries))
	for _, entry := range entries {
		keys[entry] = self.sortKey(dir, entry)
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return strings.Compare(keys[a], keys[b])
	})

	self.frames = append(self.frames, &listFrame{dir: dir, entries: entries})
	return nil
}

// seek removes the entries which sort before or at the start path, so that
// Next returns only objects which sort after it. A directory is descended
// into only when the start path is beneath it, so entries before the start
// path are not read. Directories sort as their path followed by "/", as in
// S3, so a start path with a trailing "/" starts after the directory, with
// the objects beneath it.
func (self *listIterator) seek(startAfter string) error {
	start := strings.TrimPrefix(startAfter, "/")
	if start == "" {
		return nil
	}
	for len(self.frames) > 0 {
		frame := self.frames[len(self.frames)-1]
		if len(frame.entries) == 0 {
			return nil
		}
		// Remove skipped entries, which Next skips, so that a skipped
		// directory is not descended into
		entry := frame.entries[0]
		if self.skip(entry.Name()) {
			frame.entries = frame.entries[1:]
			continue
		}
		name := path.Join(frame.dir, entry.Name())
		key := self.sortKey(frame.dir, entry)
		if frame.dir != "." {
			key = frame.dir + "/" + key
		}

		// When the start path is beneath a directory which is listed
		// recursively, descend into it and seek within it. Every entry
		// after the directory sorts after the start path. A package is a
		// file, which isn't descended into.
		if self.recursive && entry.IsDir() && !self.isPackage(entry.Name()) && strings.HasPrefix(start, key) {
			frame.entries = frame.entries[1:]
			if err := self.push(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			continue
		}

		// Remove the entry when it sorts before or at the start path
		if key <= start {
			frame.entries = frame.entries[1:]
			continue
		}

		// Every remaining entry sorts after the start path
		return nil
	}
	return nil
}

// stat returns the file information for the named file, following symbolic
// links. A file which is not an object, such as a socket, named pipe or
// device, does not exist, nor does a file which is skipped, or is beneath a
// directory which is, such as a hidden file, or a file in a package, such as
// a band of a sparse disk image, as it's not listed. A path which isn't
// valid, such as one with "..", "." or empty elements, or a trailing "/", is
// invalid, whatever the filesystem would return for it.
func (self *FS) stat(op, name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: op, Path: objectPath(name), Err: fs.ErrInvalid}
	} else if self.skipPath(name) || self.inPackage(name) {
		return nil, &fs.PathError{Op: op, Path: objectPath(name), Err: fs.ErrNotExist}
	}
	// A file which doesn't exist, or can't be read, is reported by its object
	// path, rather than the filesystem's, whose errors are of its own
	// operations, such as statat
	info, err := fs.Stat(self.fs, name)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			err = pathErr.Err
		}
		return nil, &fs.PathError{Op: op, Path: objectPath(name), Err: err}
	} else if err != nil {
		return nil, err
	} else if !isObject(info.Mode()) {
		return nil, &fs.PathError{Op: op, Path: objectPath(name), Err: fs.ErrNotExist}
	}
	return info, nil
}

// sortKey returns the name of the entry, followed by "/" for a directory.
// Symbolic links are resolved, so that a link to a directory sorts as a
// directory. A package is a file, which sorts as its name.
func (self *listIterator) sortKey(dir string, entry fs.DirEntry) string {
	isDir := entry.IsDir()
	if entry.Type()&fs.ModeSymlink != 0 {
		if info, err := fs.Stat(self.fs, path.Join(dir, entry.Name())); err == nil {
			isDir = info.IsDir()
		}
	}
	if isDir && !self.isPackage(entry.Name()) {
		return entry.Name() + "/"
	}
	return entry.Name()
}

// object returns the object of the named entry, with its content type, from
// its information, which is only read when it's returned
func (self *listIterator) object(name string, infoFn func() (fs.FileInfo, error)) (*schema.Object, error) {
	info, err := infoFn()
	if err != nil {
		return nil, err
	}
	return self.objectFromInfo(name, info, objectType(name)), nil
}
