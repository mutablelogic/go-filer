package fs

import (
	"bufio"
	"crypto/rand"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// writeBufferSize is the size of the writes of a file's body, which are large,
// as each write to a filesystem such as a FUSE mount can be a round trip
const writeBufferSize = 4 << 20

type writerfs struct {
	root   *os.Root
	closed atomic.Bool // the root isn't set to nil when it's closed, as it can be used while it's closed
}

// fileInfo overrides the name of file information, so that information read
// from the temporary file reports the name of the target
type fileInfo struct {
	fs.FileInfo
	name string
}

const (
	// Temporary files are hidden, so they are not listed and cannot be
	// created or deleted through the WriterFS interface
	tempPrefix = ".filer-"
	tempSuffix = ".tmp"
)

var _ filer.WriterFS = (*writerfs)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newWriterFS returns a writable filesystem rooted at the directory path.
// Files can't be read or written outside the directory, even through
// symbolic links.
func newWriterFS(path string) (filer.WriterFS, error) {
	self := new(writerfs)
	if abspath, err := filepath.Abs(path); err != nil {
		return nil, filer.ErrBadParameter.Withf("Invalid path: %q", path)
	} else if stat, err := os.Stat(abspath); err != nil {
		return nil, filer.ErrBadParameter.Withf("Invalid path: %q", path)
	} else if stat.IsDir() == false {
		return nil, filer.ErrBadParameter.Withf("Path is not a directory: %q", path)
	} else if root, err := os.OpenRoot(abspath); err != nil {
		return nil, err
	} else {
		self.root = root
	}
	return self, nil
}

// Path returns the directory the filesystem is rooted at, or an empty string
// when it's closed
func (self *writerfs) Path() string {
	if self.closed.Load() {
		return ""
	}
	return self.root.Name()
}

// Close the filesystem. Any further operations will return fs.ErrClosed, as
// will those in progress which use the root after it's closed. It's safe to
// call more than once, and while operations are in progress.
func (self *writerfs) Close() error {
	if self.closed.Swap(true) {
		return nil
	}
	return self.root.Close()
}

////////////////////////////////////////////////////////////////////////////////
// WriterFS implementation

// Open the named file for reading. The path must be a valid fs.FS path:
// slash-separated, unrooted, and with no "." or ".." elements. Symbolic
// links which resolve outside the root directory cannot be opened.
func (self *writerfs) Open(path string) (fs.File, error) {
	if self.closed.Load() {
		return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrClosed}
	} else if !isValidPath(path) {
		return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrInvalid}
	}
	f, err := self.root.Open(filepath.FromSlash(path))
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Stat returns file information for the named file or directory, following
// symbolic links which resolve within the root directory. The path must be a
// valid fs.FS path.
func (self *writerfs) Stat(path string) (fs.FileInfo, error) {
	if self.closed.Load() {
		return nil, &fs.PathError{Op: "stat", Path: path, Err: fs.ErrClosed}
	} else if !isValidPath(path) {
		return nil, &fs.PathError{Op: "stat", Path: path, Err: fs.ErrInvalid}
	}
	return self.root.Stat(filepath.FromSlash(path))
}

// Create the named file from the body, creating any intermediate
// directories. The path must be a valid fs.FS path which names a file rather
// than the root, cannot contain hidden elements (starting with "."), and
// cannot resolve outside the root directory. A nil body creates an empty file.
//
// The body is written to a hidden temporary file in the same directory, and
// its modification time set, before it is renamed over the target. With
// filer.CreateOnly, the temporary file is hard linked to the target instead,
// which fails if the target exists. With filer.ReplaceOnly, the target must exist
// when the temporary file is renamed over it, which is checked immediately
// before, as there's no rename which only replaces a file. Either way the
// target only ever contains a complete file with the correct modification
// time, and on failure the temporary file and any empty intermediate
// directories are removed.
//
// The filesystem may store the modification time with less precision than
// requested, so the returned information reports the time actually stored.
func (self *writerfs) Create(path string, body io.Reader, mode filer.CreateMode, modtime time.Time) (fs.FileInfo, error) {
	if self.closed.Load() {
		return nil, &fs.PathError{Op: "create", Path: path, Err: fs.ErrClosed}
	} else if !isWritablePath(path) {
		return nil, &fs.PathError{Op: "create", Path: path, Err: fs.ErrInvalid}
	}

	// Create intermediate directories
	name := filepath.FromSlash(path)
	if dir := filepath.Dir(name); dir != "." {
		if err := self.root.MkdirAll(dir, 0o755); err != nil {
			self.removeEmptyParents(name)
			return nil, err
		}
	}

	// Write the body to a temporary file, then move it into place
	tmp := filepath.Join(filepath.Dir(name), tempPrefix+rand.Text()+tempSuffix)
	info, err := self.writeTemp(tmp, body)
	if err == nil && !modtime.IsZero() {
		info, err = self.setModTime(tmp, modtime)
	}
	if err == nil {
		err = self.commit(tmp, name, mode)
	}
	if err != nil {
		self.root.Remove(tmp)
		self.removeEmptyParents(name)
		return nil, err
	}

	// Return success
	return fileInfo{FileInfo: info, name: filepath.Base(name)}, nil
}

// Delete the named file, or the named directory and everything beneath it
// (including hidden files), and then remove any parent directories which are
// left empty. Hidden paths and the root directory cannot be deleted, and a
// symbolic link is removed rather than what it points to, including any
// symbolic links beneath a deleted directory.
func (self *writerfs) Delete(path string) error {
	if self.closed.Load() {
		return &fs.PathError{Op: "delete", Path: path, Err: fs.ErrClosed}
	} else if !isWritablePath(path) {
		return &fs.PathError{Op: "delete", Path: path, Err: fs.ErrInvalid}
	}

	// Check the path exists, and delete it
	name := filepath.FromSlash(path)
	if info, err := self.root.Lstat(name); err != nil {
		return err
	} else if info.IsDir() {
		if err := self.root.RemoveAll(name); err != nil {
			return err
		}
	} else if err := self.root.Remove(name); err != nil {
		return err
	}

	// Remove any empty parent directories
	self.removeEmptyParents(name)

	// Return success
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// writeTemp creates the temporary file, writes the body to it and flushes it
// to storage, returning the information for the file
func (self *writerfs) writeTemp(tmp string, body io.Reader) (fs.FileInfo, error) {
	f, err := self.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}

	// The body is written in large writes, as it can be read in small
	// reads, such as the 4 KiB reads of a multipart form's part, and each
	// write to a filesystem such as a FUSE mount, or a network share, can be
	// a round trip
	if body != nil {
		w := newLargeWriter(f)
		if _, err := io.Copy(w, body); err != nil {
			return nil, errors.Join(err, f.Close())
		} else if err := w.Flush(); err != nil {
			return nil, errors.Join(err, f.Close())
		}
	}
	if err := f.Sync(); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	info, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return info, f.Close()
}

// newLargeWriter returns a writer which writes to the writer in writes of
// writeBufferSize, except the last, which is flushed. Only the writer's Write
// method is visible to the buffer, as a buffer which is empty copies to a
// writer with a ReadFrom method, such as a file, directly, in the reader's
// reads.
func newLargeWriter(w io.Writer) *bufio.Writer {
	return bufio.NewWriterSize(struct{ io.Writer }{w}, writeBufferSize)
}

// setModTime sets the modification time of the temporary file, leaving the
// access time unchanged, and returns the updated file information
func (self *writerfs) setModTime(tmp string, modtime time.Time) (fs.FileInfo, error) {
	if err := self.root.Chtimes(tmp, time.Time{}, modtime); err != nil {
		return nil, err
	}
	return self.root.Lstat(tmp)
}

// commit moves the temporary file to the target. With filer.CreateOnly, the
// temporary file is hard linked to the target, which fails if the target
// already exists, and then removed. Otherwise it is renamed over the target,
// which with filer.ReplaceOnly must exist, which is checked immediately before.
func (self *writerfs) commit(tmp, name string, mode filer.CreateMode) error {
	switch mode {
	case filer.CreateOnly:
		if err := self.root.Link(tmp, name); err != nil {
			return err
		}

		// The target now exists, so failing to remove the temporary file is
		// not an error: at worst it leaves a hidden file behind
		self.root.Remove(tmp)
		return nil
	case filer.ReplaceOnly:
		if _, err := self.root.Lstat(name); err != nil {
			return err
		}
	}
	return self.root.Rename(tmp, name)
}

// isValidPath extends fs.ValidPath by also rejecting backslashes and colons
// on Windows, as os.DirFS does. There a backslash is a path separator (which
// would bypass the checks on each path element) and a colon names a drive or
// NTFS alternate data stream. Elsewhere they're ordinary characters in file
// names, such as a "/" in a name in the macOS Finder, which is a colon.
func isValidPath(path string) bool {
	return fs.ValidPath(path) && (runtime.GOOS != "windows" || !strings.ContainsAny(path, `\:`))
}

// isWritablePath extends isValidPath for paths which can be created or
// deleted: the path cannot be the root directory, and no element of the path
// can start with a "." (hidden files and directories)
func isWritablePath(path string) bool {
	if !isValidPath(path) || path == "." {
		return false
	}
	for elem := range strings.SplitSeq(path, "/") {
		if strings.HasPrefix(elem, ".") {
			return false
		}
	}
	return true
}

// removeEmptyParents removes the parent directories of the named file,
// working upwards towards the root, and stopping at the first one which is
// not empty or is not a directory (for example, a symbolic link). The root
// directory itself is never removed. Errors are ignored, as this is a
// best-effort tidy up.
func (self *writerfs) removeEmptyParents(name string) {
	for dir := filepath.Dir(name); dir != "."; dir = filepath.Dir(dir) {
		if info, err := self.root.Lstat(dir); err != nil || !info.IsDir() {
			return
		} else if err := self.root.Remove(dir); err != nil {
			return
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// fileInfo implementation

func (info fileInfo) Name() string {
	return info.name
}
