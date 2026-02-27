package backend

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	// Packages
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/mutablelogic/go-filer/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// s3RetryWait waits for eventual consistency in S3-compatible services.
// Call this after creating objects before attempting to read/list them.
func s3RetryWait() {
	time.Sleep(100 * time.Millisecond)
}

// s3Retry retries a function until it succeeds or max attempts reached.
// Useful for eventual consistency issues with S3-compatible services.
func s3Retry(t *testing.T, maxAttempts int, fn func() error) error {
	var err error
	for i := 0; i < maxAttempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		t.Logf("Retry %d/%d: %v", i+1, maxAttempts, err)
		time.Sleep(200 * time.Millisecond)
	}
	return err
}

func TestCreateObject_Mem(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	tests := []struct {
		name        string
		key         string
		content     string
		contentType string
		modTime     time.Time
		meta        schema.ObjectMeta
		wantErr     bool
	}{
		{
			name:        "simple file",
			key:         "test.txt",
			content:     "hello world",
			contentType: "text/plain",
		},
		{
			name:        "file with metadata",
			key:         "data.json",
			content:     `{"foo":"bar"}`,
			contentType: "application/json",
			meta:        schema.ObjectMeta{"author": "test"},
		},
		{
			name:        "file with modtime",
			key:         "dated.txt",
			content:     "dated content",
			contentType: "text/plain",
			modTime:     time.Date(2026, 1, 28, 12, 0, 0, 0, time.UTC),
		},
		{
			name:    "nested path",
			key:     "subdir/nested/file.txt",
			content: "nested content",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
				Path:        "/" + tt.key,
				Body:        strings.NewReader(tt.content),
				ContentType: tt.contentType,
				ModTime:     tt.modTime,
				Meta:        tt.meta,
			})

			if tt.wantErr {
				assert.Error(err)
				return
			}

			require.NoError(err)
			assert.Equal(backend.Name(), obj.Name)
			assert.Equal("/"+tt.key, obj.Path)
			assert.Equal(int64(len(tt.content)), obj.Size)

			if tt.contentType != "" {
				assert.Equal(tt.contentType, obj.ContentType)
			}

			if tt.meta != nil {
				for k, v := range tt.meta {
					assert.Equal(v, obj.Meta[k])
				}
			}

			if !tt.modTime.IsZero() {
				assert.Equal(tt.modTime.Format(time.RFC3339), obj.Meta[schema.AttrLastModified])
			}
		})
	}
}

func TestCreateObject_File(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	backend, err := NewBlobBackend(ctx, "file://testfiles"+tempDir, WithCreateDir())
	require.NoError(t, err)
	defer backend.Close()

	tests := []struct {
		name        string
		key         string
		content     string
		contentType string
	}{
		{
			name:        "simple file",
			key:         "test.txt",
			content:     "hello world",
			contentType: "text/plain",
		},
		{
			name:    "binary content",
			key:     "data.bin",
			content: string([]byte{0x00, 0x01, 0x02, 0x03}),
		},
		{
			name:        "nested path",
			key:         "subdir/nested/file.txt",
			content:     "nested content",
			contentType: "text/plain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			reqURL := "file://testfiles/" + tt.key
			_ = reqURL

			obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
				Path:        "/" + tt.key,
				Body:        strings.NewReader(tt.content),
				ContentType: tt.contentType,
			})

			require.NoError(err)
			assert.Equal("testfiles", obj.Name)
			assert.Equal("/"+tt.key, obj.Path)
			assert.Equal(int64(len(tt.content)), obj.Size)

			// Verify file exists on disk
			filePath := tempDir + "/" + tt.key
			data, err := os.ReadFile(filePath)
			require.NoError(err)
			assert.Equal(tt.content, string(data))
		})
	}
}

// s3TestConfig returns the bucket URL and options for S3 tests.
// Returns empty bucketURL if S3_BUCKET_URL is not set.
//
// Required environment variables:
//   - S3_BUCKET_URL: The S3 bucket URL, e.g., "s3://my-bucket" or "s3://my-bucket/prefix"
//
// Optional environment variables:
//   - S3_ENDPOINT: Custom endpoint for S3-compatible services (e.g., MinIO, SeaweedFS)
//   - S3_ANONYMOUS: Set to "true" to use anonymous credentials (no authentication)
//   - AWS_REGION: AWS region
//   - AWS_PROFILE: AWS shared credentials profile name
//
// Credentials are read from the AWS SDK default credential chain:
//   - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
//   - AWS_SESSION_TOKEN for temporary credentials
//   - Shared credentials file (~/.aws/credentials)
//   - IAM role (when running on AWS)
func s3TestConfig() (bucketURL string, opts []Opt) {
	bucketURL = os.Getenv("S3_BUCKET_URL")
	if bucketURL == "" {
		return "", nil
	}

	// Optional endpoint for S3-compatible services
	if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
		opts = append(opts, WithEndpoint(endpoint))
	}

	// Anonymous access (for S3-compatible services without auth)
	if os.Getenv("S3_ANONYMOUS") == "true" {
		opts = append(opts, WithAnonymous())
	}

	// Load AWS config with optional region / profile so credentials and
	// region are resolved via the SDK chain (env, shared config, SSO, etc.).
	var cfgOpts []func(*config.LoadOptions) error
	if region := os.Getenv("AWS_REGION"); region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(region))
	}
	if profile := os.Getenv("AWS_PROFILE"); profile != "" {
		cfgOpts = append(cfgOpts, config.WithSharedConfigProfile(profile))
	}
	if len(cfgOpts) > 0 {
		if awsCfg, err := config.LoadDefaultConfig(context.Background(), cfgOpts...); err == nil {
			opts = append(opts, WithAWSConfig(awsCfg))
		}
	}

	return bucketURL, opts
}

// TestCreateObject_IfNotExists covers the conditional-create (fix: IfNotExists field).
func TestCreateObject_IfNotExists(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	t.Run("IfNotExists=true succeeds when object absent", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        "/new-object.txt",
			Body:        strings.NewReader("hello"),
			ContentType: "text/plain",
			IfNotExists: true,
		})
		require.NoError(err)
		assert.Equal("/new-object.txt", obj.Path)
		assert.Equal(int64(5), obj.Size)
	})

	t.Run("IfNotExists=true returns conflict when object exists", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Create the object first
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        "/existing.txt",
			Body:        strings.NewReader("original"),
			ContentType: "text/plain",
		})
		require.NoError(err)

		// Second create with IfNotExists=true must fail
		_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        "/existing.txt",
			Body:        strings.NewReader("new content"),
			ContentType: "text/plain",
			IfNotExists: true,
		})
		assert.Error(err)
		assert.Contains(err.Error(), "already exists")

		// Original content must be unchanged
		got, err := backend.GetObject(ctx, schema.GetObjectRequest{Path: "/existing.txt"})
		require.NoError(err)
		assert.Equal(int64(8), got.Size) // "original" = 8 bytes
	})

	t.Run("IfNotExists=false (default) overwrites existing object", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		// Create original
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        "/overwrite.txt",
			Body:        strings.NewReader("original content"),
			ContentType: "text/plain",
		})
		require.NoError(err)

		// Overwrite without IfNotExists (default=false)
		obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        "/overwrite.txt",
			Body:        strings.NewReader("new"),
			ContentType: "text/plain",
		})
		require.NoError(err)
		assert.Equal(int64(3), obj.Size) // "new" = 3 bytes
	})
}

func TestCreateObject_S3(t *testing.T) {
	bucketURL, opts := s3TestConfig()
	if bucketURL == "" {
		t.Skip("Skipping S3 test: S3_BUCKET_URL not set")
	}

	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer backend.Close()

	// Generate unique key for this test run
	bURL, _ := url.Parse(bucketURL)
	testKey := "test-" + time.Now().Format("20060102-150405") + ".txt"
	reqPath := bURL.Path + "/" + testKey
	reqURL := bucketURL + "/" + testKey
	_ = reqURL

	t.Run("create and verify", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		content := "hello from S3 test"
		modTime := time.Date(2026, 1, 28, 12, 0, 0, 0, time.UTC)

		obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        reqPath,
			Body:        bytes.NewReader([]byte(content)),
			ContentType: "text/plain",
			ModTime:     modTime,
			Meta:        schema.ObjectMeta{"test-key": "test-value"},
		})

		require.NoError(err)
		assert.Equal(bURL.Host, obj.Name)
		assert.Equal("/"+testKey, obj.Path)
		assert.Equal(int64(len(content)), obj.Size)
		assert.Equal("text/plain", obj.ContentType)
		assert.Equal("test-value", obj.Meta["test-key"])
		assert.Equal(modTime.Format(time.RFC3339), obj.Meta[schema.AttrLastModified])

		// Cleanup: delete the test object
		_, err = backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: reqPath})
		assert.NoError(err)
	})
}
