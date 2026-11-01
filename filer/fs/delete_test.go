package fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// newDeleteTestFS creates a temporary directory with some files, and returns
// a filesystem of it, which is either read-only (os.DirFS) or writable
func newDeleteTestFS(t *testing.T, writable bool) (*FS, string) {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello, world"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "subdir", "nested.txt"), []byte("nested"), 0o644))

	var self *FS
	var err error
	if writable {
		self, err = New("test", dir, nil, filer.Opts{})
	} else {
		self, err = NewFS("test", os.DirFS(dir), nil, filer.Opts{})
	}
	require.NoError(t, err)
	t.Cleanup(func() { self.Close() })

	return self, dir
}

// collect returns the paths of the objects of the iterator, with a "/" after
// those of directories
func collect(t *testing.T, iter schema.ListObjectsIterator) []string {
	t.Helper()
	paths := []string{}
	for {
		obj, err := iter.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return paths
		}
		require.NoError(t, err)
		if obj.IsDir {
			paths = append(paths, obj.Path+"/")
		} else {
			paths = append(paths, obj.Path)
		}
	}
}

func Test_DeleteObjects(t *testing.T) {
	backend, dir := newDeleteTestFS(t, true)
	ctx := context.Background()

	remove := func(path string, recursive bool) (*schema.DeleteObjectsResponse, error) {
		return backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			ObjectKey: schema.ObjectKey{Path: path},
			Recursive: recursive,
		})
	}
	exists := func(path string) bool {
		_, err := os.Lstat(filepath.Join(dir, filepath.FromSlash(path)))
		return err == nil
	}
	create := func(t *testing.T, paths ...string) {
		t.Helper()
		for _, path := range paths {
			require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, filepath.FromSlash(path))), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.FromSlash(path)), []byte("data"), 0o644))
		}
	}

	t.Run("Object", func(t *testing.T) {
		deleted, err := remove("hello.txt", false)
		require.NoError(t, err)
		assert.False(t, exists("hello.txt"))

		// The deleted object is returned
		require.Len(t, deleted.Body, 1)
		assert.Equal(t, "/hello.txt", deleted.Body[0].Path)
		assert.False(t, deleted.Body[0].IsDir)
		require.NotNil(t, deleted.Body[0].Size)
		assert.Equal(t, int64(len("hello, world")), *deleted.Body[0].Size)

		_, err = backend.GetObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "hello.txt"},
		})
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("ObjectRecursive", func(t *testing.T) {
		// Recursive makes no difference for an object
		create(t, "single.txt")
		deleted, err := remove("/single.txt", true)
		require.NoError(t, err)
		require.Len(t, deleted.Body, 1)
		assert.Equal(t, "/single.txt", deleted.Body[0].Path)
		assert.False(t, exists("single.txt"))
	})

	t.Run("DirectoryNotRecursive", func(t *testing.T) {
		// A directory is not deleted unless the request is recursive
		create(t, "keep/a.txt")
		_, err := remove("keep", false)
		assert.ErrorIs(t, err, filer.ErrConflict)
		assert.True(t, exists("keep/a.txt"))
	})

	t.Run("EmptyDirectoryNotRecursive", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "empty"), 0o755))
		_, err := remove("empty", false)
		assert.ErrorIs(t, err, filer.ErrConflict)
		assert.True(t, exists("empty"))
	})

	t.Run("DirectoryRecursive", func(t *testing.T) {
		// Every object beneath the directory is deleted and returned,
		// including hidden files, but not directories
		create(t, "tree/a.txt", "tree/sub/b.txt", "tree/sub/deeper/c.txt", "tree/.hidden")
		deleted, err := remove("tree", true)
		require.NoError(t, err)
		assert.False(t, exists("tree"))
		var paths []string
		for _, object := range deleted.Body {
			assert.False(t, object.IsDir)
			paths = append(paths, object.Path)
		}
		assert.ElementsMatch(t, []string{"/tree/a.txt", "/tree/sub/b.txt", "/tree/sub/deeper/c.txt", "/tree/.hidden"}, paths)

		// Nothing beneath the directory is listed
		iter, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Recursive: true,
		})
		require.NoError(t, err)
		defer iter.Close()
		for _, path := range collect(t, iter) {
			assert.NotContains(t, path, "tree")
		}
	})

	t.Run("EmptyDirectoryRecursive", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "empty2"), 0o755))
		deleted, err := remove("empty2", true)
		require.NoError(t, err)
		assert.Empty(t, deleted.Body)
		assert.False(t, exists("empty2"))
	})

	t.Run("RemovesEmptyParents", func(t *testing.T) {
		create(t, "parent/child/file.txt")
		_, err := remove("parent/child/file.txt", false)
		require.NoError(t, err)
		assert.False(t, exists("parent"))
	})

	t.Run("NotFound", func(t *testing.T) {
		for _, recursive := range []bool{false, true} {
			_, err := remove("missing.txt", recursive)
			assert.ErrorIs(t, err, fs.ErrNotExist)
		}
	})

	t.Run("Root", func(t *testing.T) {
		// The root cannot be deleted, even recursively
		for _, path := range []string{"", ".", "/"} {
			for _, recursive := range []bool{false, true} {
				_, err := remove(path, recursive)
				assert.ErrorIs(t, err, filer.ErrBadParameter, "path %q", path)
			}
		}
		assert.True(t, exists("subdir/nested.txt"))
	})

	t.Run("InvalidPath", func(t *testing.T) {
		create(t, ".hidden/file.txt")
		for _, path := range []string{"//abs", "../escape", ".hidden", ".hidden/file.txt"} {
			_, err := remove(path, true)
			assert.ErrorIs(t, err, fs.ErrInvalid, "path %q", path)
		}
		assert.True(t, exists(".hidden/file.txt"))
	})

	t.Run("Package", func(t *testing.T) {
		// A package is deleted as a file, without being recursive, with the
		// files in it
		create(t, "Disk.sparsebundle/bands/404d", "Disk.sparsebundle/Info.plist")
		deleted, err := remove("Disk.sparsebundle", false)
		require.NoError(t, err)
		require.Len(t, deleted.Body, 1)
		assert.Equal(t, "/Disk.sparsebundle", deleted.Body[0].Path)
		assert.False(t, deleted.Body[0].IsDir)
		assert.False(t, exists("Disk.sparsebundle"))

		// A package beneath a directory is returned as one object
		create(t, "disks/Disk.sparsebundle/bands/404d", "disks/a.txt")
		deleted, err = remove("disks", true)
		require.NoError(t, err)
		var paths []string
		for _, object := range deleted.Body {
			paths = append(paths, object.Path)
		}
		assert.ElementsMatch(t, []string{"/disks/Disk.sparsebundle", "/disks/a.txt"}, paths)

		// A file in a package doesn't exist
		create(t, "Other.sparsebundle/bands/404d")
		_, err = remove("Other.sparsebundle/bands/404d", false)
		assert.ErrorIs(t, err, fs.ErrNotExist)
		assert.True(t, exists("Other.sparsebundle/bands/404d"))
	})

	t.Run("Links", func(t *testing.T) {
		// A link to a file is returned as the file it's listed as, and links
		// to directories, broken links, and links out of the filesystem
		// aren't, and only the links are deleted, not what they lead to. Links
		// within the filesystem are relative, as absolute links can't be
		// resolved within its root.
		outside := t.TempDir()
		create(t, "targets/file.txt", "linked/a.txt")
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "targets", "dir"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o644))
		symlink(t, filepath.Join("..", "targets", "file.txt"), filepath.Join(dir, "linked", "file.txt"))
		symlink(t, filepath.Join("..", "targets", "dir"), filepath.Join(dir, "linked", "dir"))
		symlink(t, filepath.Join("..", "missing.txt"), filepath.Join(dir, "linked", "broken.txt"))
		symlink(t, filepath.Join(outside, "secret.txt"), filepath.Join(dir, "linked", "outside.txt"))

		// The objects deleted are the objects which are listed
		iter, err := backend.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/linked"}, Recursive: true})
		require.NoError(t, err)
		listed := []string{}
		for _, path := range collect(t, iter) {
			if !strings.HasSuffix(path, "/") {
				listed = append(listed, path)
			}
		}
		iter.Close()

		deleted, err := remove("linked", true)
		require.NoError(t, err)
		var paths []string
		for _, object := range deleted.Body {
			paths = append(paths, object.Path)
			if object.Path == "/linked/file.txt" {
				require.NotNil(t, object.Size)
				assert.Equal(t, int64(len("data")), *object.Size)
			}
		}
		assert.ElementsMatch(t, []string{"/linked/a.txt", "/linked/file.txt"}, paths)
		assert.ElementsMatch(t, listed, paths)
		assert.False(t, exists("linked"))
		assert.True(t, exists("targets/file.txt"))
		assert.True(t, exists("targets/dir"))
		_, err = os.Stat(filepath.Join(outside, "secret.txt"))
		assert.NoError(t, err)
	})

	t.Run("CancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			ObjectKey: schema.ObjectKey{Path: "subdir"},
			Recursive: true,
		})
		assert.ErrorIs(t, err, context.Canceled)
		assert.True(t, exists("subdir"))
	})

	t.Run("ReadOnly", func(t *testing.T) {
		backend, dir := newDeleteTestFS(t, false)
		_, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			ObjectKey: schema.ObjectKey{Path: "hello.txt"},
		})
		assert.ErrorIs(t, err, filer.ErrForbidden)
		_, err = os.Stat(filepath.Join(dir, "hello.txt"))
		assert.NoError(t, err)
	})

	t.Run("Closed", func(t *testing.T) {
		backend, _ := newDeleteTestFS(t, true)
		require.NoError(t, backend.Close())
		_, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			ObjectKey: schema.ObjectKey{Path: "hello.txt"},
		})
		assert.ErrorIs(t, err, fs.ErrClosed)
	})
}
