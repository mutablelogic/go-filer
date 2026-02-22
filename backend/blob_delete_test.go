package backend

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	// Packages
	"github.com/mutablelogic/go-filer/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteObject_Mem(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	// Helper to create test objects
	createTestObject := func(t *testing.T, key, content string) {
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         "mem://testbucket/" + key,
			Body:        strings.NewReader(content),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		setup       func(t *testing.T)
		url         string
		wantSize    int64
		wantErr     bool
		errContains string
	}{
		{
			name: "delete existing file",
			setup: func(t *testing.T) {
				createTestObject(t, "to-delete.txt", "delete me")
			},
			url:      "mem://testbucket/to-delete.txt",
			wantSize: 9,
		},
		{
			name: "delete nested file",
			setup: func(t *testing.T) {
				createTestObject(t, "subdir/nested-delete.txt", "nested content")
			},
			url:      "mem://testbucket/subdir/nested-delete.txt",
			wantSize: 14,
		},
		{
			name:        "delete non-existent file",
			setup:       func(t *testing.T) {},
			url:         "mem://testbucket/notfound.txt",
			wantErr:     true,
			errContains: "not found",
		},
		{
			name:        "wrong bucket",
			setup:       func(t *testing.T) {},
			url:         "mem://otherbucket/file.txt",
			wantErr:     true,
			errContains: "not handled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			tt.setup(t)

			obj, err := backend.DeleteObject(ctx, schema.DeleteObjectRequest{URL: tt.url})

			if tt.wantErr {
				assert.Error(err)
				if tt.errContains != "" {
					assert.Contains(err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(tt.url, obj.URL)
			assert.Equal(tt.wantSize, obj.Size)

			// Verify the object no longer exists
			_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: tt.url})
			assert.Error(err)
			assert.Contains(err.Error(), "not found")
		})
	}
}

func TestDeleteObject_File(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	backend, err := NewBlobBackend(ctx, "file://testfiles"+tempDir, WithCreateDir())
	require.NoError(t, err)
	defer backend.Close()

	// Helper to create test files
	createTestObject := func(t *testing.T, key, content string) {
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         "file://testfiles/" + key,
			Body:        strings.NewReader(content),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		setup       func(t *testing.T)
		key         string
		wantSize    int64
		wantErr     bool
		errContains string
	}{
		{
			name: "delete existing file",
			setup: func(t *testing.T) {
				createTestObject(t, "delete-me.txt", "hello world")
			},
			key:      "delete-me.txt",
			wantSize: 11,
		},
		{
			name: "delete nested file",
			setup: func(t *testing.T) {
				createTestObject(t, "subdir/nested.txt", "nested content")
			},
			key:      "subdir/nested.txt",
			wantSize: 14,
		},
		{
			name:        "delete non-existent file",
			setup:       func(t *testing.T) {},
			key:         "notfound.txt",
			wantErr:     true,
			errContains: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			tt.setup(t)

			reqURL := "file://testfiles/" + tt.key
			obj, err := backend.DeleteObject(ctx, schema.DeleteObjectRequest{URL: reqURL})

			if tt.wantErr {
				assert.Error(err)
				if tt.errContains != "" {
					assert.Contains(err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(reqURL, obj.URL)
			assert.Equal(tt.wantSize, obj.Size)

			// Verify the file no longer exists
			_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: reqURL})
			assert.Error(err)
			assert.Contains(err.Error(), "not found")
		})
	}
}

func TestDeleteObject_S3(t *testing.T) {
	bucketURL, opts := s3TestConfig()
	if bucketURL == "" {
		t.Skip("Skipping S3 test: S3_BUCKET_URL not set")
	}

	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	t.Run("delete existing object", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Generate unique key for this test run
		testKey := "delete-test-" + time.Now().Format("20060102-150405") + ".txt"
		reqURL := bucketURL + "/" + testKey

		// Create test object first
		content := "hello from S3 DeleteObject test"
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         reqURL,
			Body:        strings.NewReader(content),
			ContentType: "text/plain",
		})
		require.NoError(err)

		// Delete the object
		obj, err := backend.DeleteObject(ctx, schema.DeleteObjectRequest{URL: reqURL})
		require.NoError(err)

		assert.Equal(reqURL, obj.URL)
		assert.Equal(int64(len(content)), obj.Size)

		// Verify the object no longer exists
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: reqURL})
		assert.Error(err)
	})

	t.Run("delete non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		nonExistentURL := bucketURL + "/non-existent-delete-" + time.Now().Format("20060102-150405") + ".txt"
		_, err := backend.DeleteObject(ctx, schema.DeleteObjectRequest{URL: nonExistentURL})

		// S3 may or may not return an error for deleting non-existent objects
		// Some implementations are idempotent
		if err != nil {
			assert.Contains(err.Error(), "not found")
		}
	})
}

func TestDeleteObject_VerifyGone(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	t.Run("verify deleted object cannot be read", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Create object
		reqURL := "mem://testbucket/verify-gone.txt"
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         reqURL,
			Body:        strings.NewReader("test content"),
			ContentType: "text/plain",
		})
		require.NoError(err)

		// Verify it exists
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: reqURL})
		require.NoError(err)

		// Delete it
		_, err = backend.DeleteObject(ctx, schema.DeleteObjectRequest{URL: reqURL})
		require.NoError(err)

		// Verify GetObject fails
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: reqURL})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")

		// Verify ReadObject fails
		_, _, err = backend.ReadObject(ctx, schema.ReadObjectRequest{URL: reqURL})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})
}

func TestDeleteObject_ReturnsMetadata(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	t.Run("delete returns object metadata", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Create object with metadata
		reqURL := "mem://testbucket/with-meta.txt"
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         reqURL,
			Body:        strings.NewReader("content with metadata"),
			ContentType: "text/plain",
			Meta:        schema.ObjectMeta{"author": "test", "version": "1"},
		})
		require.NoError(err)

		// Delete and check returned metadata
		obj, err := backend.DeleteObject(ctx, schema.DeleteObjectRequest{URL: reqURL})
		require.NoError(err)

		assert.Equal(reqURL, obj.URL)
		assert.Equal(int64(21), obj.Size)
		assert.Equal("text/plain", obj.ContentType)
		assert.Equal("test", obj.Meta["author"])
		assert.Equal("1", obj.Meta["version"])
	})
}

func TestDeleteObjects_Mem(t *testing.T) {
	ctx := context.Background()

	// Helper to create a fresh backend with test objects
	setupBackend := func(t *testing.T) *blobbackend {
		backend, err := NewBlobBackend(ctx, "mem://testbucket")
		require.NoError(t, err)

		// Create test structure:
		// /file1.txt
		// /file2.txt
		// /subdir/nested1.txt
		// /subdir/nested2.txt
		// /subdir/deep/file.txt
		files := []string{
			"file1.txt",
			"file2.txt",
			"subdir/nested1.txt",
			"subdir/nested2.txt",
			"subdir/deep/file.txt",
		}
		for _, f := range files {
			_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
				URL:         "mem://testbucket/" + f,
				Body:        strings.NewReader("content of " + f),
				ContentType: "text/plain",
			})
			require.NoError(t, err)
		}
		return backend
	}

	t.Run("delete single object (no trailing slash)", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		backend := setupBackend(t)
		defer backend.Close()

		resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL: "mem://testbucket/file1.txt",
		})
		require.NoError(err)

		assert.Len(resp.Body, 1)
		assert.Equal("mem://testbucket/file1.txt", resp.Body[0].URL)

		// Verify deleted
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/file1.txt"})
		assert.Error(err)

		// Verify others still exist
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/file2.txt"})
		assert.NoError(err)
	})

	t.Run("delete prefix when no object exists (no trailing slash)", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		backend := setupBackend(t)
		defer backend.Close()

		// Delete "subdir" without trailing slash - no object named "subdir" exists,
		// so it should treat it as a prefix and delete subdir/* recursively
		resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL:       "mem://testbucket/subdir",
			Recursive: true,
		})
		require.NoError(err)

		// Should delete all 3 files in subdir/
		assert.Len(resp.Body, 3)

		// Verify all subdir files deleted
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/nested1.txt"})
		assert.Error(err)
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/deep/file.txt"})
		assert.Error(err)

		// Verify root files still exist
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/file1.txt"})
		assert.NoError(err)
	})

	t.Run("delete object not prefix when object exists (no trailing slash)", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		backend := setupBackend(t)
		defer backend.Close()

		// Create an object called "subdir" (no extension) alongside the subdir/ prefix
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         "mem://testbucket/subdir",
			Body:        strings.NewReader("i am a file named subdir"),
			ContentType: "text/plain",
		})
		require.NoError(err)

		// Delete "subdir" without trailing slash - object exists, so delete just that
		resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL: "mem://testbucket/subdir",
		})
		require.NoError(err)

		// Should delete just the one object
		assert.Len(resp.Body, 1)
		assert.Equal("mem://testbucket/subdir", resp.Body[0].URL)

		// Verify the object is deleted
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir"})
		assert.Error(err)

		// Verify subdir/* files still exist
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/nested1.txt"})
		assert.NoError(err)
	})

	t.Run("delete directory non-recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		backend := setupBackend(t)
		defer backend.Close()

		resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL:       "mem://testbucket/subdir/",
			Recursive: false,
		})
		require.NoError(err)

		// Should delete nested1.txt and nested2.txt but not deep/file.txt
		assert.Len(resp.Body, 2)

		// Verify nested files deleted
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/nested1.txt"})
		assert.Error(err)
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/nested2.txt"})
		assert.Error(err)

		// Verify deep file still exists
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/deep/file.txt"})
		assert.NoError(err)
	})

	t.Run("delete directory recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		backend := setupBackend(t)
		defer backend.Close()

		resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL:       "mem://testbucket/subdir/",
			Recursive: true,
		})
		require.NoError(err)

		// Should delete all 3 files in subdir
		assert.Len(resp.Body, 3)

		// Verify all subdir files deleted
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/nested1.txt"})
		assert.Error(err)
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/nested2.txt"})
		assert.Error(err)
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/subdir/deep/file.txt"})
		assert.Error(err)

		// Verify root files still exist
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/file1.txt"})
		assert.NoError(err)
	})

	t.Run("delete all from root", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		backend := setupBackend(t)
		defer backend.Close()

		resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL:       "mem://testbucket/",
			Recursive: true,
		})
		require.NoError(err)

		// Should delete all 5 files
		assert.Len(resp.Body, 5)

		// Verify all deleted
		listResp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			URL:       "mem://testbucket/",
			Recursive: true,
		})
		require.NoError(err)
		assert.Empty(listResp.Body)
	})

	t.Run("delete empty directory", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		backend, err := NewBlobBackend(ctx, "mem://emptybucket")
		require.NoError(err)
		defer backend.Close()

		resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL:       "mem://emptybucket/",
			Recursive: true,
		})
		require.NoError(err)

		assert.Empty(resp.Body)
	})

	t.Run("wrong bucket", func(t *testing.T) {
		assert := assert.New(t)

		backend := setupBackend(t)
		defer backend.Close()

		_, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL: "mem://otherbucket/file.txt",
		})
		assert.Error(err)
		assert.Contains(err.Error(), "not handled")
	})
}

func TestDeleteObjects_File(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	backend, err := NewBlobBackend(ctx, "file://testfiles"+tempDir, WithCreateDir())
	require.NoError(t, err)
	defer backend.Close()

	// Create test files
	files := []string{"a.txt", "b.txt", "dir/c.txt", "dir/d.txt"}
	for _, f := range files {
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         "file://testfiles/" + f,
			Body:        strings.NewReader("content"),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	t.Run("delete directory recursive", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			URL:       "file://testfiles/dir/",
			Recursive: true,
		})
		require.NoError(err)

		assert.Len(resp.Body, 2)

		// Verify deleted
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "file://testfiles/dir/c.txt"})
		assert.Error(err)

		// Verify root files still exist
		_, err = backend.GetObject(ctx, schema.GetObjectRequest{URL: "file://testfiles/a.txt"})
		assert.NoError(err)
	})
}

func TestDeleteObjects_S3(t *testing.T) {
	bucketURL, opts := s3TestConfig()
	if bucketURL == "" {
		t.Skip("Skipping S3 test: S3_BUCKET_URL not set")
	}

	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	t.Run("delete multiple objects", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Generate unique prefix for this test run
		prefix := "deleteobjects-test-" + time.Now().Format("20060102-150405")
		fileURLs := make([]string, 3)

		// Create test objects
		for i := 0; i < 3; i++ {
			fileURLs[i] = bucketURL + "/" + prefix + "/" + fmt.Sprintf("file%d.txt", i)
			_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
				URL:         fileURLs[i],
				Body:        strings.NewReader("test content"),
				ContentType: "text/plain",
			})
			require.NoError(err)
		}

		// Wait for eventual consistency
		s3RetryWait()

		// Delete each file individually (more reliable than prefix delete on some S3-compatible services)
		deletedCount := 0
		for _, fileURL := range fileURLs {
			_, err := backend.DeleteObject(ctx, schema.DeleteObjectRequest{URL: fileURL})
			if err == nil {
				deletedCount++
			}
		}
		assert.Equal(3, deletedCount)

		// Verify all deleted with retry for eventual consistency
		s3Retry(t, 5, func() error {
			listResp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
				URL:       bucketURL + "/" + prefix + "/",
				Recursive: true,
			})
			if err != nil {
				return err
			}
			if len(listResp.Body) > 0 {
				return fmt.Errorf("expected 0 objects, got %d", len(listResp.Body))
			}
			return nil
		})
	})
}
