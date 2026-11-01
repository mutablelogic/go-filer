package s3

import (
	"context"
	"io/fs"
	"net/http"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// getObject returns the object of the path, with the request's conditions
func getObject(b *S3, path string, req schema.GetObjectRequest) (*schema.Object, error) {
	req.Path = path
	return b.GetObject(context.Background(), req)
}

func Test_S3_GetObject(t *testing.T) {
	_, b := newFakeBucket(t, "", append(listKeys, "x", "x/y.txt")...)

	// A file has its attributes, and type, by its extension, or the
	// object's, when it has none
	file, err := getObject(b, "/c/f.txt", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.Equal(t, "/c/f.txt", file.Path)
	assert.False(t, file.IsDir)
	assert.Equal(t, int64(len("c/f.txt")), *file.Size)
	assert.Equal(t, etagOf("c/f.txt"), *file.ETag)
	assert.Equal(t, time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC), file.ModTime.UTC())
	assert.Equal(t, "text/plain; charset=utf-8", file.ContentType)
	file, err = getObject(b, "/x", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.Equal(t, "application/x-fake", file.ContentType)

	// A directory is the prefix of a key, or a key which ends with "/", or
	// the root, and has no size or time
	for _, path := range []string{"/", "", "/a", "/a/", "/c", "/c/d", "/g", "/x/"} {
		object, err := getObject(b, path, schema.GetObjectRequest{})
		require.NoError(t, err, path)
		assert.True(t, object.IsDir, path)
		assert.Equal(t, schema.DirectoryType, object.ContentType, path)
		assert.Nil(t, object.Size, path)
		assert.True(t, object.ModTime.IsZero(), path)
	}
	object, err := getObject(b, "/c/d/", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.Equal(t, "/c/d", object.Path)

	// A file and a directory can have the same name, when the path of the
	// directory ends with "/"
	object, err = getObject(b, "/x", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.False(t, object.IsDir)
	object, err = getObject(b, "/x/", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.True(t, object.IsDir)

	// An object which doesn't exist, or a file whose path ends with "/",
	// isn't found
	for _, path := range []string{"/missing", "/missing/", "/a.txt/", "/c/f.txt/"} {
		_, err := getObject(b, path, schema.GetObjectRequest{})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
		assert.Equal(t, http.StatusNotFound, statusOf(gofiler.HTTPErr(err)), path)
	}
}

func Test_S3_GetNotModified(t *testing.T) {
	_, b := newFakeBucket(t, "", listKeys...)
	modTime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	// A file isn't returned when it matches the conditions
	for _, req := range []schema.GetObjectRequest{
		{IfNoneMatch: []string{"other", etagOf("a.txt")}},
		{IfNoneMatch: []string{"*"}},
		{IfModifiedSince: modTime},
		{IfModifiedSince: modTime.Add(time.Hour)},
	} {
		_, err := getObject(b, "/a.txt", req)
		assert.ErrorIs(t, err, gofiler.ErrNotModified, req)
	}

	// And otherwise it is, and so is a directory, which has no time
	for _, req := range []schema.GetObjectRequest{
		{IfNoneMatch: []string{"other"}},
		{IfModifiedSince: modTime.Add(-time.Second)},
		{IfNoneMatch: []string{"other"}, IfModifiedSince: modTime.Add(time.Hour)},
	} {
		_, err := getObject(b, "/a.txt", req)
		assert.NoError(t, err, req)
	}
	_, err := getObject(b, "/a", schema.GetObjectRequest{IfModifiedSince: modTime})
	assert.NoError(t, err)
}

func Test_S3_GetPrefix(t *testing.T) {
	_, b := newFakeBucket(t, "photos/2026", "photos/2026/a.jpg", "photos/2026/b/c.jpg", "photos/2025/d.jpg", "other.txt")

	// The objects are those beneath the prefix, whose paths are relative to it
	object, err := getObject(b, "/a.jpg", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.Equal(t, "/a.jpg", object.Path)
	assert.Equal(t, "image/jpeg", object.ContentType)
	object, err = getObject(b, "/b", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.True(t, object.IsDir)
	for _, path := range []string{"/d.jpg", "/other.txt", "/2025"} {
		_, err := getObject(b, path, schema.GetObjectRequest{})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
	}
}

func Test_S3_GetMeta(t *testing.T) {
	bucket, b := newFakeBucket(t, "", "a.txt", "b.txt", "c/d.txt")
	bucket.Lock()
	bucket.meta = map[string]map[string]string{"a.txt": {"rating": "5", "Camera": "X100V", "note": `"quoted"`, "photo.lens": `{"mm":35}`}}
	bucket.Unlock()

	// The user's metadata, with lowercase names, in their order, which are
	// the namespace and key, separated by a ".", or the key, and values which
	// are JSON, or strings, when they aren't, as another client wrote them
	object, err := getObject(b, "/a.txt", schema.GetObjectRequest{})
	require.NoError(t, err)
	require.Len(t, object.Meta, 4)
	for i, want := range []struct{ namespace, key, value string }{
		{"", "camera", `"X100V"`}, {"", "note", `"quoted"`}, {"photo", "lens", `{"mm":35}`}, {"", "rating", `5`},
	} {
		assert.Equal(t, want.namespace, object.Meta[i].Namespace)
		assert.Equal(t, want.key, object.Meta[i].Key)
		assert.JSONEq(t, want.value, string(object.Meta[i].Value))
	}

	// An object without metadata, a directory, and a listing, have none
	object, err = getObject(b, "/b.txt", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.Nil(t, object.Meta)
	object, err = getObject(b, "/c", schema.GetObjectRequest{})
	require.NoError(t, err)
	assert.Nil(t, object.Meta)
	iter, err := b.ListObjects(context.Background(), schema.ListObjectsRequest{})
	require.NoError(t, err)
	defer iter.Close()
	object, err = iter.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "/a.txt", object.Path)
	assert.Nil(t, object.Meta)
}
