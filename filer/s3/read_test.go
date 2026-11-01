package s3

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// readAll reads the file at the path, and returns its content
func readAll(t *testing.T, b *S3, path string) (string, error) {
	t.Helper()
	read, _, err := b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
	if err != nil {
		return "", err
	}
	var content []byte
	err = read(context.Background(), func(r io.Reader) error {
		content, err = io.ReadAll(r)
		return err
	})
	return string(content), err
}

func Test_S3_ReadObject(t *testing.T) {
	_, b := newFakeBucket(t, "", append(listKeys, "x", "x/y.txt", "empty")...)

	// A file's content is read, with the file, and an empty file is too
	read, object, err := b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c/f.txt"}})
	require.NoError(t, err)
	assert.Equal(t, "/c/f.txt", object.Path)
	assert.Equal(t, int64(len("c/f.txt")), *object.Size)
	require.NoError(t, read(context.Background(), func(r io.Reader) error {
		content, err := io.ReadAll(r)
		assert.Equal(t, "c/f.txt", string(content))
		return err
	}))
	content, err := readAll(t, b, "/x")
	require.NoError(t, err)
	assert.Equal(t, "x", content)

	// A directory can't be read, as it's a directory, and a path which
	// doesn't exist, or a file whose path ends with "/", isn't found
	for _, path := range []string{"/", "/a", "/x/", "/c/d"} {
		_, err := readAll(t, b, path)
		assert.ErrorIs(t, err, fs.ErrInvalid, path)
	}
	for _, path := range []string{"/missing", "/a.txt/"} {
		_, err := readAll(t, b, path)
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
	}

	// A file which isn't modified isn't read
	_, _, err = b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}, IfNoneMatch: []string{etagOf("a.txt")}})
	assert.ErrorIs(t, err, gofiler.ErrNotModified)
}

func Test_S3_ReadChanged(t *testing.T) {
	bucket, b := newFakeBucket(t, "", "a.txt")

	// A file which changes after it's returned isn't read
	read, _, err := b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}})
	require.NoError(t, err)
	bucket.Lock()
	bucket.keys["a.txt"] = "changed content"
	bucket.Unlock()
	err = read(context.Background(), func(r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	})
	assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed)
	assert.Equal(t, http.StatusPreconditionFailed, statusOf(gofiler.HTTPErr(err)))
}

func Test_S3_ReadSeek(t *testing.T) {
	bucket, b := newFakeBucket(t, "", "a.txt")
	bucket.Lock()
	bucket.keys["a.txt"] = "0123456789"
	bucket.Unlock()
	gets := func() int {
		bucket.Lock()
		defer bucket.Unlock()
		return bucket.gets
	}

	// The content is requested before it's read, and again when it seeks
	// within it, but not to its end, or beyond, or back to where it's
	// requested from
	read, _, err := b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}})
	require.NoError(t, err)
	require.NoError(t, read(context.Background(), func(r io.Reader) error {
		assert.Equal(t, 1, gets())
		seeker, ok := r.(io.ReadSeeker)
		require.True(t, ok)
		size, err := seeker.Seek(0, io.SeekEnd)
		require.NoError(t, err)
		assert.Equal(t, int64(10), size)
		data, err := io.ReadAll(seeker)
		require.NoError(t, err)
		assert.Empty(t, data)
		_, err = seeker.Seek(20, io.SeekStart)
		require.NoError(t, err)
		_, err = seeker.Seek(0, io.SeekStart)
		require.NoError(t, err)
		data = make([]byte, 2)
		_, err = io.ReadFull(seeker, data)
		require.NoError(t, err)
		assert.Equal(t, "01", string(data))
		assert.Equal(t, 1, gets())

		// From an offset, which is requested once
		_, err = seeker.Seek(4, io.SeekStart)
		require.NoError(t, err)
		data = make([]byte, 3)
		_, err = io.ReadFull(seeker, data)
		require.NoError(t, err)
		assert.Equal(t, "456", string(data))
		_, err = io.ReadFull(seeker, data)
		require.NoError(t, err)
		assert.Equal(t, "789", string(data))
		assert.Equal(t, 2, gets())

		// From the current offset, which doesn't change it, or back
		offset, err := seeker.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		assert.Equal(t, int64(10), offset)
		_, err = seeker.Seek(-10, io.SeekEnd)
		require.NoError(t, err)
		all, err := io.ReadAll(seeker)
		require.NoError(t, err)
		assert.Equal(t, "0123456789", string(all))
		assert.Equal(t, 3, gets())

		// Before the start is invalid
		_, err = seeker.Seek(-1, io.SeekStart)
		assert.ErrorIs(t, err, fs.ErrInvalid)
		return nil
	}))
}

func Test_S3_ReadEmpty(t *testing.T) {
	bucket, b := newFakeBucket(t, "", "empty.txt")
	bucket.Lock()
	bucket.keys["empty.txt"] = ""
	bucket.Unlock()

	// An empty file is requested, so it's checked, and is the end, wherever
	// it seeks to
	read, _, err := b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/empty.txt"}})
	require.NoError(t, err)
	require.NoError(t, read(context.Background(), func(r io.Reader) error {
		seeker := r.(io.ReadSeeker)
		for _, offset := range []int64{0, 1, 100} {
			_, err := seeker.Seek(offset, io.SeekStart)
			require.NoError(t, err)
			n, err := seeker.Read(make([]byte, 1))
			assert.Equal(t, 0, n)
			assert.ErrorIs(t, err, io.EOF, offset)
		}
		return nil
	}))
	bucket.Lock()
	assert.Equal(t, 1, bucket.gets)
	bucket.Unlock()
}

func Test_S3_ReadChangedResponse(t *testing.T) {
	// A file which changes after it's returned isn't written as a response,
	// whole, or in a range, as it's known before the response's headers are
	for _, rangeHeader := range []string{"", "bytes=2-4"} {
		bucket, b := newFakeBucket(t, "", "a.txt")
		read, object, err := b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}})
		require.NoError(t, err)
		bucket.Lock()
		bucket.keys["a.txt"] = "changed content"
		bucket.Unlock()

		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/a.txt", nil)
		if rangeHeader != "" {
			r.Header.Set("Range", rangeHeader)
		}
		binary := httpresponse.Binary{ContentType: object.ContentType, Size: object.Size, ModTime: object.ModTime, ETag: *object.ETag}
		err = read(context.Background(), func(body io.Reader) error {
			return binary.Write(w, r, body)
		})
		assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed, rangeHeader)
		assert.Empty(t, w.Header().Get("Content-Type"), rangeHeader)
		assert.Empty(t, w.Body.String(), rangeHeader)
	}

	// And otherwise, a range of it is
	bucket, b := newFakeBucket(t, "", "a.txt")
	bucket.Lock()
	bucket.keys["a.txt"] = "0123456789"
	bucket.Unlock()
	read, object, err := b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}})
	require.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/a.txt", nil)
	r.Header.Set("Range", "bytes=2-4")
	binary := httpresponse.Binary{ContentType: object.ContentType, Size: object.Size, ModTime: object.ModTime, ETag: *object.ETag}
	require.NoError(t, read(context.Background(), func(body io.Reader) error {
		return binary.Write(w, r, body)
	}))
	assert.Equal(t, http.StatusPartialContent, w.Code)
	assert.Equal(t, "234", w.Body.String())
}
