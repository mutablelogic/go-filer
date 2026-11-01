package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// fakeBucket is a bucket of a service which is compatible with S3, which
// has the keys, and answers HeadBucket, HeadObject and ListObjectsV2, which
// it counts
type fakeBucket struct {
	sync.Mutex
	name     string
	keys     map[string]string // the keys, and their content
	modTime  time.Time
	requests int // the number of lists
}

// listResult is the response of ListObjectsV2
type listResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	KeyCount              int            `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	Contents              []listContent  `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

type listContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// newFakeBucket returns the bucket, and a filer of it, beneath the prefix
func newFakeBucket(t *testing.T, prefix string, keys ...string) (*fakeBucket, *S3) {
	t.Helper()
	fakeCredentials(t)
	bucket := &fakeBucket{name: "photos", keys: map[string]string{}, modTime: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)}
	for _, key := range keys {
		bucket.keys[key] = key
	}
	server := httptest.NewServer(bucket)
	t.Cleanup(server.Close)
	b, err := New(context.Background(), bucketURL(t, server.URL, "photos/"+prefix, ""), gofiler.Opts{})
	require.NoError(t, err)
	return bucket, b
}

func (b *fakeBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.Lock()
	defer b.Unlock()
	key, ok := strings.CutPrefix(r.URL.Path, "/"+b.name)
	if !ok || (key != "" && !strings.HasPrefix(key, "/")) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key = strings.TrimPrefix(key, "/")
	switch {
	case r.Method == http.MethodHead && key == "":
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodHead:
		content, exists := b.keys[key]
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		w.Header().Set("Last-Modified", b.modTime.Format(http.TimeFormat))
		w.Header().Set("ETag", `"`+etagOf(content)+`"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && key == "" && r.URL.Query().Get("list-type") == "2":
		b.requests++
		b.list(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// list answers ListObjectsV2: the keys with the prefix, after where it
// starts, or continues, a page of them, where those with the delimiter after
// the prefix roll up into a common prefix, which is returned once
func (b *fakeBucket) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	prefix, delimiter := query.Get("prefix"), query.Get("delimiter")
	after := query.Get("start-after")
	if token := query.Get("continuation-token"); token != "" {
		after = token
	}
	maxKeys := 1000
	if value := query.Get("max-keys"); value != "" {
		maxKeys, _ = strconv.Atoi(value)
	}

	keys := make([]string, 0, len(b.keys))
	for key := range b.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := listResult{Name: b.name, Prefix: prefix, MaxKeys: maxKeys}
	last := ""
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) || key <= after {
			continue
		}

		// The key rolls up into a common prefix, which isn't returned again,
		// including when the page continues after it
		item := key
		if delimiter != "" {
			if i := strings.Index(key[len(prefix):], delimiter); i >= 0 {
				item = key[:len(prefix)+i+len(delimiter)]
			}
		}
		if item == last || (item != key && strings.HasPrefix(after, item)) {
			continue
		}
		if result.KeyCount == maxKeys {
			result.IsTruncated, result.NextContinuationToken = true, last
			break
		}
		if item == key {
			result.Contents = append(result.Contents, listContent{
				Key:          key,
				LastModified: b.modTime.Format(time.RFC3339),
				ETag:         `"` + etagOf(b.keys[key]) + `"`,
				Size:         len(b.keys[key]),
			})
		} else {
			result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{Prefix: item})
		}
		result.KeyCount++
		last = item
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(result)
}

// etagOf returns the entity tag of the content
func etagOf(content string) string {
	return fmt.Sprintf("%x", len(content))
}

// listPaths returns the paths of the objects which are listed, with "/" at
// the end of a directory's
func listPaths(t *testing.T, b *S3, req schema.ListObjectsRequest) []string {
	t.Helper()
	iter, err := b.ListObjects(context.Background(), req)
	require.NoError(t, err)
	defer iter.Close()
	paths := []string{}
	for {
		object, err := iter.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return paths
		}
		require.NoError(t, err)
		paths = append(paths, object.Key())
	}
}

// listKeys are the keys of the tests' bucket, which are those of the
// filesystem's tests, with a directory which is a key, a directory which
// is beneath another, and keys which aren't paths of objects
var listKeys = []string{
	"a.txt", "a-b.txt", "a/b.txt", "a0.txt",
	"c/", "c/d/e.txt", "c/f.txt",
	"g//h.txt", "g/./i.txt", "g/../j.txt",
}

func Test_S3_ListObjects(t *testing.T) {
	for _, pageSize := range []int32{1000, 1, 2, 3} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			defer func(size int32) { listPageSize = size }(listPageSize)
			listPageSize = pageSize
			_, b := newFakeBucket(t, "", listKeys...)
			list := func(path string, recursive bool, startAfter string) []string {
				return listPaths(t, b, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}, Recursive: recursive, StartAfter: startAfter})
			}

			// Files and directories are listed, in the filesystem's order, and
			// a directory is descended into when it's recursive, without keys
			// which aren't paths of objects. A directory which only has keys
			// which aren't paths, such as g, is a common prefix, so it's listed.
			assert.Equal(t, []string{"/a-b.txt", "/a.txt", "/a/", "/a0.txt", "/c/", "/g/"}, list("/", false, ""))
			assert.Equal(t, []string{"/a-b.txt", "/a.txt", "/a/", "/a/b.txt", "/a0.txt", "/c/", "/c/d/", "/c/d/e.txt", "/c/f.txt", "/g/"}, list("", true, ""))
			assert.Equal(t, []string{"/c/d/", "/c/f.txt"}, list("/c", false, ""))
			assert.Equal(t, []string{"/c/d/", "/c/d/e.txt", "/c/f.txt"}, list("/c/", true, ""))

			// Listing starts after the start path, within a directory too
			assert.Equal(t, []string{"/a/b.txt", "/a0.txt", "/c/", "/c/d/", "/c/d/e.txt", "/c/f.txt", "/g/"}, list("/", true, "/a/"))
			assert.Equal(t, []string{"/a/", "/a/b.txt", "/a0.txt", "/c/", "/c/d/", "/c/d/e.txt", "/c/f.txt", "/g/"}, list("/", true, "/a.txt"))
			assert.Equal(t, []string{"/a0.txt", "/c/", "/g/"}, list("/", false, "/a/b.txt"))
			assert.Equal(t, []string{"/c/f.txt"}, list("/c", true, "/c/d/e.txt"))
			assert.Equal(t, []string{}, list("/", true, "/z"))

			// Listing a file returns just it, unless it starts after it
			assert.Equal(t, []string{"/a/b.txt"}, list("/a/b.txt", false, ""))
			assert.Equal(t, []string{}, list("/a/b.txt", false, "/a/b.txt"))
		})
	}
}

func Test_S3_ListObject(t *testing.T) {
	_, b := newFakeBucket(t, "", listKeys...)

	// A file has its size, modification time, entity tag, without quotes,
	// and type, from its listing, or its metadata
	for _, recursive := range []bool{false, true} {
		iter, err := b.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/c"}, Recursive: recursive})
		require.NoError(t, err)
		var file *schema.Object
		for {
			object, err := iter.Next(context.Background())
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			if object.Path == "/c/f.txt" {
				file = object
			} else if object.Path == "/c/d" {
				assert.True(t, object.IsDir)
				assert.Equal(t, schema.DirectoryType, object.ContentType)
				assert.Nil(t, object.Size)
			}
		}
		require.NoError(t, iter.Close())
		require.NotNil(t, file)
		assert.Equal(t, int64(len("c/f.txt")), *file.Size)
		assert.Equal(t, etagOf("c/f.txt"), *file.ETag)
		assert.Equal(t, time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC), file.ModTime.UTC())
		assert.Equal(t, "text/plain; charset=utf-8", file.ContentType)
		assert.False(t, file.IsDir)
	}
	iter, err := b.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/c/f.txt"}})
	require.NoError(t, err)
	file, err := iter.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(len("c/f.txt")), *file.Size)
	assert.Equal(t, etagOf("c/f.txt"), *file.ETag)
	assert.Equal(t, time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC), file.ModTime.UTC())
	assert.Equal(t, "text/plain; charset=utf-8", file.ContentType)

	// Once it's closed, there are no more objects
	require.NoError(t, iter.Close())
	_, err = iter.Next(context.Background())
	assert.ErrorIs(t, err, fs.ErrClosed)
}

func Test_S3_ListPrefix(t *testing.T) {
	_, b := newFakeBucket(t, "photos/2026", "photos/2026/a.jpg", "photos/2026/b/c.jpg", "photos/2025/d.jpg", "photos/2026.txt", "other.txt")

	// The objects are those beneath the prefix, whose paths are relative to it
	assert.Equal(t, []string{"/a.jpg", "/b/"}, listPaths(t, b, schema.ListObjectsRequest{}))
	assert.Equal(t, []string{"/a.jpg", "/b/", "/b/c.jpg"}, listPaths(t, b, schema.ListObjectsRequest{Recursive: true}))
	assert.Equal(t, []string{"/b/c.jpg"}, listPaths(t, b, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/b"}}))
	assert.Equal(t, []string{"/b/c.jpg"}, listPaths(t, b, schema.ListObjectsRequest{Recursive: true, StartAfter: "/b/"}))
}

func Test_S3_ListNotExist(t *testing.T) {
	_, b := newFakeBucket(t, "")

	// The root of an empty bucket is empty, and a directory or file which
	// doesn't exist isn't found
	assert.Equal(t, []string{}, listPaths(t, b, schema.ListObjectsRequest{Recursive: true}))
	for _, path := range []string{"/a", "/a/", "/a/b.txt"} {
		_, err := b.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
		assert.Equal(t, http.StatusNotFound, statusOf(gofiler.HTTPErr(err)), path)
	}
}

func Test_S3_ListLazy(t *testing.T) {
	defer func(size int32) { listPageSize = size }(listPageSize)
	listPageSize = 2
	bucket, b := newFakeBucket(t, "", "a", "b", "c", "d", "e")

	// The pages are read as the objects are, so a page isn't read until
	// it's needed
	iter, err := b.ListObjects(context.Background(), schema.ListObjectsRequest{})
	require.NoError(t, err)
	defer iter.Close()
	bucket.Lock()
	assert.Equal(t, 0, bucket.requests)
	bucket.Unlock()
	for range 3 {
		_, err := iter.Next(context.Background())
		require.NoError(t, err)
	}
	bucket.Lock()
	assert.Equal(t, 2, bucket.requests)
	bucket.Unlock()

	// A request which is cancelled is its context's error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = iter.Next(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

// statusOf returns the HTTP status of the error
func statusOf(err error) int {
	var code httpresponse.Err
	if errors.As(err, &code) {
		return int(code)
	}
	return 0
}

func Test_S3_ListFileAndDir(t *testing.T) {
	_, b := newFakeBucket(t, "", "a", "a/b.txt")
	list := func(path string) []string {
		return listPaths(t, b, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}})
	}

	// A bucket can have an object, and a directory, of the same name, which
	// are both listed, and a path which ends with "/" is the directory
	assert.Equal(t, []string{"/a", "/a/"}, list("/"))
	assert.Equal(t, []string{"/a"}, list("/a"))
	assert.Equal(t, []string{"/a/b.txt"}, list("/a/"))
	_, err := b.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a/b.txt/"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
}
