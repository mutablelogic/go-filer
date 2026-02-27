package backend

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gcsTestConfig returns the bucket URL and options for GCS tests.
// Returns empty bucketURL if GCS_BUCKET_URL is not set.
//
// Required environment variables:
//   - GCS_BUCKET_URL: The GCS bucket URL, e.g. "gs://my-bucket" or "gs://my-bucket/prefix"
//
// Optional environment variables:
//   - GCS_CREDENTIALS_FILE: Path to a service-account JSON key file.
//     When absent, Application Default Credentials are used (GOOGLE_APPLICATION_CREDENTIALS,
//     gcloud auth, or GCE metadata server).
func gcsTestConfig() (bucketURL string, opts []Opt) {
	bucketURL = os.Getenv("GCS_BUCKET_URL")
	if bucketURL == "" {
		return "", nil
	}
	if f := os.Getenv("GCS_CREDENTIALS_FILE"); f != "" {
		opts = append(opts, WithGCSCredentialsFile(f))
	}
	return bucketURL, opts
}

func TestCreateObject_GCS(t *testing.T) {
	bucketURL, opts := gcsTestConfig()
	if bucketURL == "" {
		t.Skip("Skipping GCS test: GCS_BUCKET_URL not set")
	}

	ctx := context.Background()
	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	testKey := "gcs-create-test-" + time.Now().Format("20060102-150405") + ".txt"
	content := "hello from GCS create test"
	modTime := time.Date(2026, 1, 28, 12, 0, 0, 0, time.UTC)

	obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        "/" + testKey,
		Body:        bytes.NewReader([]byte(content)),
		ContentType: "text/plain",
		ModTime:     modTime,
		Meta:        schema.ObjectMeta{"test-key": "test-value"},
	})
	require.NoError(t, err)
	assert.Equal(t, backend.Name(), obj.Name)
	assert.Equal(t, "/"+testKey, obj.Path)
	assert.Equal(t, int64(len(content)), obj.Size)
	assert.Equal(t, "text/plain", obj.ContentType)
	assert.NotEmpty(t, obj.ETag)
	assert.Equal(t, "test-value", obj.Meta["test-key"])
	assert.Equal(t, modTime.Format(time.RFC3339), obj.Meta[schema.AttrLastModified])

	// Cleanup
	_, _ = backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: "/" + testKey})
}

func TestGetObject_GCS(t *testing.T) {
	bucketURL, opts := gcsTestConfig()
	if bucketURL == "" {
		t.Skip("Skipping GCS test: GCS_BUCKET_URL not set")
	}

	ctx := context.Background()
	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	testKey := "gcs-get-test-" + time.Now().Format("20060102-150405") + ".txt"
	content := "hello from GCS get test"

	// Create
	created, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        "/" + testKey,
		Body:        bytes.NewReader([]byte(content)),
		ContentType: "text/plain",
	})
	require.NoError(t, err)
	defer backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: "/" + testKey})

	// Get
	got, err := backend.GetObject(ctx, schema.GetObjectRequest{Path: "/" + testKey})
	require.NoError(t, err)
	assert.Equal(t, backend.Name(), got.Name)
	assert.Equal(t, "/"+testKey, got.Path)
	assert.Equal(t, int64(len(content)), got.Size)
	assert.Equal(t, created.ETag, got.ETag, "ETag must be consistent between Create and Get")

	// Non-existent
	_, err = backend.GetObject(ctx, schema.GetObjectRequest{Path: "/nonexistent-gcs-" + testKey})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestReadObject_GCS(t *testing.T) {
	bucketURL, opts := gcsTestConfig()
	if bucketURL == "" {
		t.Skip("Skipping GCS test: GCS_BUCKET_URL not set")
	}

	ctx := context.Background()
	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	testKey := "gcs-read-test-" + time.Now().Format("20060102-150405") + ".txt"
	content := "hello from GCS read test"

	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        "/" + testKey,
		Body:        bytes.NewReader([]byte(content)),
		ContentType: "text/plain",
	})
	require.NoError(t, err)
	defer backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: "/" + testKey})

	r, obj, err := backend.ReadObject(ctx, schema.ReadObjectRequest{
		GetObjectRequest: schema.GetObjectRequest{Path: "/" + testKey},
	})
	require.NoError(t, err)
	defer r.Close()

	assert.Equal(t, int64(len(content)), obj.Size)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	require.NoError(t, err)
	assert.Equal(t, content, buf.String())
}

func TestListObjects_GCS(t *testing.T) {
	bucketURL, opts := gcsTestConfig()
	if bucketURL == "" {
		t.Skip("Skipping GCS test: GCS_BUCKET_URL not set")
	}

	ctx := context.Background()
	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	prefix := "gcs-list-test-" + time.Now().Format("20060102-150405")
	keys := []string{
		prefix + "/file1.txt",
		prefix + "/file2.txt",
		prefix + "/sub/file3.txt",
	}

	for _, k := range keys {
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        "/" + k,
			Body:        bytes.NewReader([]byte("content")),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}
	defer func() {
		for _, k := range keys {
			backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: "/" + k})
		}
	}()

	t.Run("list prefix non-recursive", func(t *testing.T) {
		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Path:      "/" + prefix + "/",
			Recursive: false,
			Limit:     schema.MaxListLimit,
		})
		require.NoError(t, err)
		assert.Equal(t, backend.Name(), resp.Name)
		// Should see file1.txt, file2.txt, and the sub/ directory entry
		assert.GreaterOrEqual(t, len(resp.Body), 2)
	})

	t.Run("list prefix recursive", func(t *testing.T) {
		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Path:      "/" + prefix + "/",
			Recursive: true,
			Limit:     schema.MaxListLimit,
		})
		require.NoError(t, err)
		assert.Equal(t, 3, len(resp.Body))
		assert.Equal(t, 3, resp.Count)
	})

	t.Run("get single object", func(t *testing.T) {
		resp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Path:  "/" + keys[0],
			Limit: schema.MaxListLimit,
		})
		require.NoError(t, err)
		require.Len(t, resp.Body, 1)
		assert.Equal(t, "/"+keys[0], resp.Body[0].Path)
		assert.NotEmpty(t, resp.Body[0].ETag)
	})
}

func TestDeleteObject_GCS(t *testing.T) {
	bucketURL, opts := gcsTestConfig()
	if bucketURL == "" {
		t.Skip("Skipping GCS test: GCS_BUCKET_URL not set")
	}

	ctx := context.Background()
	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	testKey := "gcs-delete-test-" + time.Now().Format("20060102-150405") + ".txt"
	content := "hello from GCS delete test"

	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        "/" + testKey,
		Body:        bytes.NewReader([]byte(content)),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	obj, err := backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: "/" + testKey})
	require.NoError(t, err)
	assert.Equal(t, backend.Name(), obj.Name)
	assert.Equal(t, "/"+testKey, obj.Path)
	assert.Equal(t, int64(len(content)), obj.Size)

	_, err = backend.GetObject(ctx, schema.GetObjectRequest{Path: "/" + testKey})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestDeleteObjects_GCS(t *testing.T) {
	bucketURL, opts := gcsTestConfig()
	if bucketURL == "" {
		t.Skip("Skipping GCS test: GCS_BUCKET_URL not set")
	}

	ctx := context.Background()
	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	prefix := "gcs-deleteobjects-test-" + time.Now().Format("20060102-150405")
	keys := make([]string, 3)
	for i := range keys {
		keys[i] = prefix + "/" + fmt.Sprintf("file%d.txt", i)
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        "/" + keys[i],
			Body:        bytes.NewReader([]byte("test")),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	resp, err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
		Path:      "/" + prefix + "/",
		Recursive: true,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Body, 3)
	for _, o := range resp.Body {
		assert.Equal(t, backend.Name(), o.Name)
	}

	// Verify gone
	listResp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
		Path:      "/" + prefix + "/",
		Recursive: true,
		Limit:     schema.MaxListLimit,
	})
	require.NoError(t, err)
	assert.Empty(t, listResp.Body)
}

func TestCreateObject_GCS_IfNotExists(t *testing.T) {
	bucketURL, opts := gcsTestConfig()
	if bucketURL == "" {
		t.Skip("Skipping GCS test: GCS_BUCKET_URL not set")
	}

	ctx := context.Background()
	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	testKey := "gcs-ifnotexists-test-" + time.Now().Format("20060102-150405") + ".txt"
	defer backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: "/" + testKey})

	// First create succeeds
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        "/" + testKey,
		Body:        bytes.NewReader([]byte("original")),
		ContentType: "text/plain",
		IfNotExists: true,
	})
	require.NoError(t, err)

	// Second create with IfNotExists=true must fail
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        "/" + testKey,
		Body:        bytes.NewReader([]byte("new content")),
		ContentType: "text/plain",
		IfNotExists: true,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}
