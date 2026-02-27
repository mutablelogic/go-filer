package backend

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListObjects_Mem(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	// Create some test objects
	files := []struct {
		key     string
		content string
	}{
		{"file1.txt", "content1"},
		{"file2.txt", "content2"},
		{"subdir/file3.txt", "content3"},
		{"subdir/file4.txt", "content4"},
		{"subdir/nested/file5.txt", "content5"},
	}

	for _, f := range files {
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{

			Path:        "/" + f.key,
			Body:        bytes.NewReader([]byte(f.content)),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	t.Run("list root non-recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path:      "/",
			Recursive: false,
			Limit:     schema.MaxListLimit,
		})
		require.NoError(err)
		assert.Equal("testbucket", resp.Name)

		// Should have file1.txt, file2.txt, and subdir/ (as a prefix)
		assert.GreaterOrEqual(len(resp.Body), 2)

		var keys []string
		for _, obj := range resp.Body {
			keys = append(keys, obj.Path)
		}
		assert.Contains(keys, "/file1.txt")
		assert.Contains(keys, "/file2.txt")
	})

	t.Run("list root recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path:      "/",
			Recursive: true,
			Limit:     schema.MaxListLimit,
		})
		require.NoError(err)

		// Should have all 5 files
		assert.Equal(5, len(resp.Body))

		var keys []string
		for _, obj := range resp.Body {
			keys = append(keys, obj.Path)
		}
		assert.Contains(keys, "/file1.txt")
		assert.Contains(keys, "/subdir/nested/file5.txt")
	})

	t.Run("list subdir non-recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path:      "/subdir/",
			Recursive: false,
			Limit:     schema.MaxListLimit,
		})
		require.NoError(err)

		// Should have file3.txt, file4.txt, and nested/ prefix
		assert.GreaterOrEqual(len(resp.Body), 2)

		var keys []string
		for _, obj := range resp.Body {
			keys = append(keys, obj.Path)
		}
		assert.Contains(keys, "/subdir/file3.txt")
		assert.Contains(keys, "/subdir/file4.txt")
	})

	t.Run("list subdir recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path:      "/subdir/",
			Recursive: true,
			Limit:     schema.MaxListLimit,
		})
		require.NoError(err)

		// Should have file3.txt, file4.txt, nested/file5.txt
		assert.Equal(3, len(resp.Body))
	})

	t.Run("get single object", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path:  "/file1.txt",
			Limit: schema.MaxListLimit,
		})
		require.NoError(err)
		assert.Equal("testbucket", resp.Name)
		assert.Equal(1, len(resp.Body))
		assert.Equal("/file1.txt", resp.Body[0].Path)
		assert.Equal(int64(8), resp.Body[0].Size) // "content1" = 8 bytes
	})

	t.Run("get single object nested", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path:  "/subdir/nested/file5.txt",
			Limit: schema.MaxListLimit,
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
		assert.Equal("/subdir/nested/file5.txt", resp.Body[0].Path)
	})

	t.Run("get non-existent object", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Non-existent object returns empty list (treated as prefix with no matches)
		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path: "/nonexistent.txt",
		})
		require.NoError(err)
		assert.Empty(resp.Body)
	})

	t.Run("wrong bucket", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Path doesn't exist; ListObjects returns empty, not error
		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path: "/nonexistent-file.txt",
		})
		require.NoError(err)
		assert.Empty(resp.Body)
	})

	t.Run("empty directory", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path: "/emptydir/",
		})
		require.NoError(err)
		assert.Equal(0, len(resp.Body))
	})
}

func TestListObjects_WithPrefix(t *testing.T) {
	ctx := context.Background()

	// Backend with bucket prefix — callers use relative paths,
	// the prefix is an internal storage detail.
	backend, err := NewBlobBackend(ctx, "mem://testbucket/prefix")
	require.NoError(t, err)
	defer backend.Close()

	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        "/file.txt",
		Body:        bytes.NewReader([]byte("test")),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("list with prefix", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Path:  "/",
			Limit: schema.MaxListLimit,
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
		assert.Equal("/file.txt", resp.Body[0].Path)
	})

	t.Run("get single object with prefix", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Path:  "/file.txt",
			Limit: schema.MaxListLimit,
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
	})
}

// TestETagConsistency verifies that the ETag returned by CreateObject, GetObject,
// and ListObjects is the same value for the same object (fix: attrsToObject now
// uses MD5-as-hex to match the list iterator's format).
func TestETagConsistency(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	content := []byte("etag consistency check content")

	// CreateObject returns the ETag after a successful write.
	created, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        "/etag-test.txt",
		Body:        bytes.NewReader(content),
		ContentType: "text/plain",
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.ETag, "CreateObject must return a non-empty ETag")

	// GetObject must return the same ETag.
	got, err := backend.GetObject(ctx, schema.GetObjectRequest{Path: "/etag-test.txt"})
	require.NoError(t, err)
	assert.Equal(t, created.ETag, got.ETag, "GetObject ETag must match CreateObject ETag")

	// ListObjects (single-object path) must return the same ETag.
	listResp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{Path: "/etag-test.txt", Limit: schema.MaxListLimit})
	require.NoError(t, err)
	require.Len(t, listResp.Body, 1)
	assert.Equal(t, created.ETag, listResp.Body[0].ETag, "ListObjects ETag must match CreateObject ETag")

	// ListObjects (directory scan) must also return the same ETag.
	listAll, err := backend.ListObjects(ctx, schema.ListObjectsRequest{Path: "/", Recursive: true, Limit: schema.MaxListLimit})
	require.NoError(t, err)
	require.Len(t, listAll.Body, 1)
	assert.Equal(t, created.ETag, listAll.Body[0].ETag, "Recursive ListObjects ETag must match CreateObject ETag")
}

func TestListObjects_File(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	backend, err := NewBlobBackend(ctx, "file://testfiles"+tempDir, WithCreateDir())
	require.NoError(t, err)
	defer backend.Close()

	// Create test files
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{

		Path:        "/test.txt",
		Body:        bytes.NewReader([]byte("hello")),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("list directory", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path:  "/",
			Limit: schema.MaxListLimit,
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
	})

	t.Run("get single file", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{

			Path:  "/test.txt",
			Limit: schema.MaxListLimit,
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
		assert.Equal(int64(5), resp.Body[0].Size)
	})
}

func TestListObjects_S3(t *testing.T) {
	bucketURL, opts := s3TestConfig()
	if bucketURL == "" {
		t.Skip("Skipping S3 test: S3_BUCKET_URL not set")
	}

	ctx := context.Background()
	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	// Create a single test object with unique name for this test run
	s3bURL, _ := url.Parse(bucketURL)
	testKey := "listtest-" + time.Now().Format("20060102-150405") + ".txt"
	testPath := s3bURL.Path + "/" + testKey

	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        testPath,
		Body:        bytes.NewReader([]byte("test content")),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	// Wait for eventual consistency
	s3RetryWait()

	// Cleanup at end
	defer func() {
		backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: testPath})
	}()

	t.Run("get single object", func(t *testing.T) {
		assert := assert.New(t)

		var resp *schema.ListObjectsResponse
		err := s3Retry(t, 5, func() error {
			var err error
			resp, err = backend.ListObjects(ctx, schema.ListObjectsRequest{
				Path:  testPath,
				Limit: schema.MaxListLimit,
			})
			if err != nil {
				return err
			}
			if len(resp.Body) != 1 {
				return fmt.Errorf("expected 1 object, got %d", len(resp.Body))
			}
			return nil
		})
		assert.NoError(err)
		if len(resp.Body) == 1 {
			assert.Equal("/"+testKey, resp.Body[0].Path)
		}
	})

	t.Run("list root returns objects", func(t *testing.T) {
		assert := assert.New(t)

		var resp *schema.ListObjectsResponse
		err := s3Retry(t, 5, func() error {
			var err error
			resp, err = backend.ListObjects(ctx, schema.ListObjectsRequest{
				Path:      s3bURL.Path + "/",
				Recursive: true,
				Limit:     schema.MaxListLimit,
			})
			if err != nil {
				return err
			}
			if len(resp.Body) == 0 {
				return fmt.Errorf("expected at least 1 object, got 0")
			}
			return nil
		})
		assert.NoError(err)
		assert.GreaterOrEqual(len(resp.Body), 1)
	})
}
