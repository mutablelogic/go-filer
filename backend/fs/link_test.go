package fs

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// Test_Links checks symbolic links aren't objects, as a bucket has none, so
// they aren't listed, counted, got or read, nor are the files beneath one,
// and an object isn't created over one, or beneath one
func Test_Links(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "target"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "target", "b.txt"), []byte("b"), 0o644))
	symlink(t, "a.txt", filepath.Join(dir, "file.txt"))
	symlink(t, "target", filepath.Join(dir, "folder"))
	self, err := New("test", dir, nil, filer.Opts{})
	require.NoError(t, err)
	defer self.Close()
	ctx := context.Background()
	key := func(path string) schema.GetObjectRequest {
		return schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}}
	}

	// They aren't listed, or counted
	iter, err := self.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a.txt", "/target/", "/target/b.txt"}, collect(t, iter))
	iter.Close()
	root, err := self.GetObject(ctx, key("/"))
	require.NoError(t, err)
	assert.Equal(t, schema.FolderMeta(2), root.Meta)

	// They, and the files beneath them, aren't got, or read
	for _, path := range []string{"/file.txt", "/folder", "/folder/b.txt"} {
		_, err := self.GetObject(ctx, key(path))
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
		_, _, err = self.ReadObject(ctx, key(path))
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
	}

	// An object isn't created over one, or beneath one
	for _, path := range []string{"/file.txt", "/folder/c.txt"} {
		_, err := self.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: path}, Body: strings.NewReader("c")})
		assert.ErrorIs(t, err, filer.ErrConflict, path)
	}
	_, err = os.Stat(filepath.Join(dir, "target", "c.txt"))
	assert.ErrorIs(t, err, fs.ErrNotExist)
}
