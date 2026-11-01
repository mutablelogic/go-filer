package s3

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"slices"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// deleteObjects deletes the object at the path, and returns the paths of
// the files which are deleted
func deleteObjects(b *S3, path string, recursive bool) ([]string, error) {
	response, err := b.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}, Recursive: recursive})
	if err != nil {
		return nil, err
	}
	paths := []string{}
	for _, object := range response.Body {
		paths = append(paths, object.Path)
	}
	return paths, nil
}

// bucketKeys returns the keys of the bucket, in order
func bucketKeys(bucket *fakeBucket) []string {
	bucket.Lock()
	defer bucket.Unlock()
	keys := make([]string, 0, len(bucket.keys))
	for key := range bucket.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func Test_S3_DeleteFile(t *testing.T) {
	bucket, b := newFakeBucket(t, "", "a.txt", "b.txt")

	// A file is deleted, and returned, without its type
	response, err := b.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}})
	require.NoError(t, err)
	require.Len(t, response.Body, 1)
	assert.Equal(t, "/a.txt", response.Body[0].Path)
	assert.Equal(t, int64(len("a.txt")), *response.Body[0].Size)
	assert.Empty(t, response.Body[0].ContentType)
	assert.Equal(t, []string{"b.txt"}, bucketKeys(bucket))

	// A file which doesn't exist isn't found, and the root, and a volume
	// which is read-only, can't be deleted
	_, err = deleteObjects(b, "/a.txt", false)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	assert.Equal(t, http.StatusNotFound, statusOf(gofiler.HTTPErr(err)))
	for _, path := range []string{"/", ""} {
		_, err := deleteObjects(b, path, true)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, path)
	}
	b.readonly = true
	_, err = deleteObjects(b, "/b.txt", false)
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	assert.Equal(t, []string{"b.txt"}, bucketKeys(bucket))
}

func Test_S3_DeleteDirectory(t *testing.T) {
	bucket, b := newFakeBucket(t, "", append(listKeys, "x", "x/y.txt")...)

	// A directory is only deleted when it's recursive
	for _, path := range []string{"/c", "/c/"} {
		_, err := deleteObjects(b, path, false)
		assert.ErrorIs(t, err, gofiler.ErrConflict, path)
		assert.Equal(t, http.StatusConflict, statusOf(gofiler.HTTPErr(err)), path)
	}

	// With every key beneath it, including its own, and those which aren't
	// paths of objects, but only files are returned
	paths, err := deleteObjects(b, "/c", true)
	require.NoError(t, err)
	assert.Equal(t, []string{"/c/d/e.txt", "/c/f.txt"}, paths)
	paths, err = deleteObjects(b, "/g", true)
	require.NoError(t, err)
	assert.Empty(t, paths)
	assert.Equal(t, []string{"a-b.txt", "a.txt", "a/b.txt", "a0.txt", "x", "x/y.txt"}, bucketKeys(bucket))

	// A file and a directory of the same name are deleted apart
	paths, err = deleteObjects(b, "/x/", true)
	require.NoError(t, err)
	assert.Equal(t, []string{"/x/y.txt"}, paths)
	paths, err = deleteObjects(b, "/x", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"/x"}, paths)
	assert.Equal(t, []string{"a-b.txt", "a.txt", "a/b.txt", "a0.txt"}, bucketKeys(bucket))
}

func Test_S3_DeletePages(t *testing.T) {
	defer func(size int32) { listPageSize = size }(listPageSize)
	listPageSize = 2
	keys := []string{"other.txt"}
	for i := range 5 {
		keys = append(keys, fmt.Sprintf("dir/%d.txt", i))
	}
	bucket, b := newFakeBucket(t, "", keys...)

	// A directory's keys are deleted a page at a time
	paths, err := deleteObjects(b, "/dir", true)
	require.NoError(t, err)
	assert.Len(t, paths, 5)
	assert.Equal(t, []string{"other.txt"}, bucketKeys(bucket))
	bucket.Lock()
	assert.Equal(t, 3, bucket.batches)
	bucket.Unlock()
}

func Test_S3_DeleteDenied(t *testing.T) {
	defer func(size int32) { listPageSize = size }(listPageSize)
	listPageSize = 2
	bucket, b := newFakeBucket(t, "", "dir/a.txt", "dir/b.txt", "dir/c.txt", "dir/d.txt")
	bucket.Lock()
	bucket.denied = map[string]bool{"dir/c.txt": true}
	bucket.Unlock()

	// A key which can't be deleted is an error, once the keys before it,
	// and the others of its page, are, and the pages after it aren't
	bucket.Lock()
	bucket.keys["dir/e.txt"] = "e"
	bucket.Unlock()
	_, err := deleteObjects(b, "/dir", true)
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	assert.Contains(t, err.Error(), "/dir/c.txt")
	assert.Equal(t, []string{"dir/c.txt", "dir/e.txt"}, bucketKeys(bucket))
}

func Test_S3_DeletePrefix(t *testing.T) {
	bucket, b := newFakeBucket(t, "photos/2026", "photos/2026/a.jpg", "photos/2026/b/c.jpg", "photos/2025/d.jpg", "photos/2026.txt")

	// The keys are those beneath the prefix
	paths, err := deleteObjects(b, "/b", true)
	require.NoError(t, err)
	assert.Equal(t, []string{"/b/c.jpg"}, paths)
	paths, err = deleteObjects(b, "/a.jpg", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"/a.jpg"}, paths)
	assert.Equal(t, []string{"photos/2025/d.jpg", "photos/2026.txt"}, bucketKeys(bucket))
}
