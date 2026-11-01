package fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_objectError(t *testing.T) {
	// An error of the filesystem is of the object's path, with its reason
	err := objectError("create", "a.txt", &fs.PathError{Op: "openat", Path: ".filer-123.tmp", Err: syscall.EPERM})
	assert.EqualError(t, err, "create /a.txt: permission denied")
	assert.ErrorIs(t, err, fs.ErrPermission)
	err = objectError("create", "/a.txt", &os.LinkError{Op: "renameat", Old: ".filer-123.tmp", New: "a.txt", Err: syscall.ENOENT})
	assert.EqualError(t, err, "create /a.txt: file does not exist")
	err = objectError("create", "/a.txt", &fs.PathError{Op: "write", Path: ".filer-123.tmp", Err: syscall.ENOSPC})
	assert.EqualError(t, err, "create /a.txt: "+syscall.ENOSPC.Error())

	// The filer's errors, those of an object's path, and others, aren't
	for _, err := range []error{
		nil,
		filer.ErrConflict.With("a conflict"),
		&fs.PathError{Op: "stat", Path: "/a.txt", Err: fs.ErrNotExist},
		context.Canceled,
		errors.New("other"),
	} {
		assert.Equal(t, err, objectError("create", "/b.txt", err))
	}
}

func Test_objectErrorPermission(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a directory's permissions don't stop files being created in it on Windows")
	} else if os.Geteuid() == 0 {
		t.Skip("root can create files in any directory")
	}
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "locked"), 0o555))
	t.Cleanup(func() { os.Chmod(filepath.Join(dir, "locked"), 0o755) })
	self, err := New("test", dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { self.Close() })

	// A file which can't be created in a directory is reported by its path,
	// rather than of the temporary file it's written to
	_, err = self.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/locked/a.txt"}, Body: strings.NewReader("a")})
	assert.EqualError(t, err, "create /locked/a.txt: permission denied")
	assert.ErrorIs(t, err, fs.ErrPermission)
}

func Test_readError(t *testing.T) {
	self, err := New("test", t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { self.Close() })
	_, err = self.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}, Body: strings.NewReader("a")})
	require.NoError(t, err)
	read, _, err := self.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}})
	require.NoError(t, err)

	// An error of the function reading the file, such as one writing what's
	// read to a client which went away, is returned as it is, rather than as
	// the file's
	writeErr := &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}
	err = read(context.Background(), func(io.Reader) error { return writeErr })
	assert.Equal(t, writeErr, err)
}
