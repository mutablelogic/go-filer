package fs

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"syscall"

	// Packages
	filer "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// objectError returns an error of the filesystem, such as one opening a
// temporary file, by the path of the object, rather than of the file, with
// a reason which isn't of the operating system's calls, such as openat, so
// "openat .filer-123.tmp: operation not permitted" is "create /a.txt:
// permission denied". The filer's errors, and those which are already of an
// object's path, aren't changed.
func objectError(op, path string, err error) error {
	var filerErr filer.Err
	var pathErr *fs.PathError
	var linkErr *os.LinkError
	var syscallErr *os.SyscallError
	switch {
	case err == nil, errors.As(err, &filerErr):
		return err
	case errors.As(err, &pathErr) && strings.HasPrefix(pathErr.Path, "/"):
		return err
	case pathErr == nil && !errors.As(err, &linkErr) && !errors.As(err, &syscallErr):
		return err
	}

	// The reason, which is one of the errors of a filesystem, which are
	// mapped to HTTP statuses, or the operating system's error, without its
	// call
	path = objectPath(fsPath(path))
	for _, target := range []error{fs.ErrNotExist, fs.ErrPermission, fs.ErrExist, fs.ErrInvalid, fs.ErrClosed} {
		if errors.Is(err, target) {
			return &fs.PathError{Op: op, Path: path, Err: target}
		}
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return &fs.PathError{Op: op, Path: path, Err: errno}
	}
	return err
}
