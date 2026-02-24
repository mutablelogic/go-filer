package backend

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	// Packages
	"github.com/mutablelogic/go-filer/schema"
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
			Name: "testbucket",

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
			Name: "testbucket",

			Path:      "/",
			Recursive: false,
		})
		require.NoError(err)
		assert.Equal("testbucket", resp.Name)

		// Should have file1.txt, file2.txt, and subdir/ (as a prefix)
		assert.GreaterOrEqual(len(resp.Body), 2)

		var keys []string
		for _, obj := range resp.Body {
			keys = append(keys, obj.Name+obj.Path)
		}
		assert.Contains(keys, "testbucket/file1.txt")
		assert.Contains(keys, "testbucket/file2.txt")
	})

	t.Run("list root recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path:      "/",
			Recursive: true,
		})
		require.NoError(err)

		// Should have all 5 files
		assert.Equal(5, len(resp.Body))

		var keys []string
		for _, obj := range resp.Body {
			keys = append(keys, obj.Name+obj.Path)
		}
		assert.Contains(keys, "testbucket/file1.txt")
		assert.Contains(keys, "testbucket/subdir/nested/file5.txt")
	})

	t.Run("list subdir non-recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path:      "/subdir/",
			Recursive: false,
		})
		require.NoError(err)

		// Should have file3.txt, file4.txt, and nested/ prefix
		assert.GreaterOrEqual(len(resp.Body), 2)

		var keys []string
		for _, obj := range resp.Body {
			keys = append(keys, obj.Name+obj.Path)
		}
		assert.Contains(keys, "testbucket/subdir/file3.txt")
		assert.Contains(keys, "testbucket/subdir/file4.txt")
	})

	t.Run("list subdir recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path:      "/subdir/",
			Recursive: true,
		})
		require.NoError(err)

		// Should have file3.txt, file4.txt, nested/file5.txt
		assert.Equal(3, len(resp.Body))
	})

	t.Run("get single object", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path: "/file1.txt",
		})
		require.NoError(err)
		assert.Equal("testbucket", resp.Name)
		assert.Equal(1, len(resp.Body))
		assert.Equal("testbucket", resp.Body[0].Name)
		assert.Equal("/file1.txt", resp.Body[0].Path)
		assert.Equal(int64(8), resp.Body[0].Size) // "content1" = 8 bytes
	})

	t.Run("get single object nested", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path: "/subdir/nested/file5.txt",
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
		assert.Equal("testbucket", resp.Body[0].Name)
		assert.Equal("/subdir/nested/file5.txt", resp.Body[0].Path)
	})

	t.Run("get non-existent object", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Non-existent object returns empty list (treated as prefix with no matches)
		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path: "/nonexistent.txt",
		})
		require.NoError(err)
		assert.Empty(resp.Body)
	})

	t.Run("wrong bucket", func(t *testing.T) {
		assert := assert.New(t)

		_, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "otherbucket",

			Path: "/file.txt",
		})
		assert.Error(err)
	})

	t.Run("empty directory", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path: "/emptydir/",
		})
		require.NoError(err)
		assert.Equal(0, len(resp.Body))
	})
}

func TestListObjects_WithPrefix(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket/prefix")
	require.NoError(t, err)
	defer backend.Close()

	// Create test objects under the prefix
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Name: "testbucket",

		Path:        "/prefix/file.txt",
		Body:        bytes.NewReader([]byte("test")),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("list with prefix", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path: "/prefix/",
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
		assert.Equal("testbucket", resp.Body[0].Name)
		assert.Equal("/file.txt", resp.Body[0].Path) // prefix is stripped from the path
	})

	t.Run("get single object with prefix", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testbucket",

			Path: "/prefix/file.txt",
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
	})
}

func TestListObjects_File(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	backend, err := NewBlobBackend(ctx, "file://testfiles"+tempDir, WithCreateDir())
	require.NoError(t, err)
	defer backend.Close()

	// Create test files
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Name: "testfiles",

		Path:        "/test.txt",
		Body:        bytes.NewReader([]byte("hello")),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("list directory", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testfiles",

			Path: "/",
		})
		require.NoError(err)
		assert.Equal(1, len(resp.Body))
	})

	t.Run("get single file", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testfiles",

			Path: "/test.txt",
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
		Name:        s3bURL.Host,
		Path:        testPath,
		Body:        bytes.NewReader([]byte("test content")),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	// Wait for eventual consistency
	s3RetryWait()

	// Cleanup at end
	defer func() {
		backend.DeleteObject(ctx, schema.DeleteObjectRequest{Name: s3bURL.Host, Path: testPath})
	}()

	t.Run("get single object", func(t *testing.T) {
		assert := assert.New(t)

		var resp *schema.ListObjectsResponse
		err := s3Retry(t, 5, func() error {
			var err error
			resp, err = backend.ListObjects(ctx, schema.ListObjectsRequest{
				Name: s3bURL.Host,
				Path: testPath,
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
			assert.Equal(s3bURL.Host, resp.Body[0].Name)
			assert.Equal("/"+testKey, resp.Body[0].Path)
		}
	})

	t.Run("list root returns objects", func(t *testing.T) {
		assert := assert.New(t)

		var resp *schema.ListObjectsResponse
		err := s3Retry(t, 5, func() error {
			var err error
			resp, err = backend.ListObjects(ctx, schema.ListObjectsRequest{
				Name:      s3bURL.Host,
				Path:      s3bURL.Path + "/",
				Recursive: true,
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
