package backend

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	"github.com/mutablelogic/go-filer/schema"
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
		{
			name:    "wrong backend URL",
			key:     "file.txt",
			content: "test",
			wantErr: true, // Will use wrong URL
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)

			// Build the request URL
			var reqURL string
			if tt.name == "wrong backend URL" {
				reqURL = "mem://otherbucket/" + tt.key
			} else {
				reqURL = "mem://testbucket/" + tt.key
			}

			obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
				URL:         reqURL,
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
			assert.Equal(reqURL, obj.URL)
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
				assert.Equal(tt.modTime.Format(time.RFC3339), obj.Meta[filer.AttrLastModified])
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

			obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
				URL:         reqURL,
				Body:        strings.NewReader(tt.content),
				ContentType: tt.contentType,
			})

			require.NoError(err)
			assert.Equal(reqURL, obj.URL)
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
//   - AWS_REGION or AWS_DEFAULT_REGION: AWS region
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

	// Region (check both AWS_REGION and AWS_DEFAULT_REGION)
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region != "" {
		opts = append(opts, WithRegion(region))
	}

	// AWS profile
	if profile := os.Getenv("AWS_PROFILE"); profile != "" {
		opts = append(opts, WithProfile(profile))
	}

	return bucketURL, opts
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
	testKey := "test-" + time.Now().Format("20060102-150405") + ".txt"
	reqURL := bucketURL + "/" + testKey

	t.Run("create and verify", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		content := "hello from S3 test"
		modTime := time.Date(2026, 1, 28, 12, 0, 0, 0, time.UTC)

		obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         reqURL,
			Body:        bytes.NewReader([]byte(content)),
			ContentType: "text/plain",
			ModTime:     modTime,
			Meta:        schema.ObjectMeta{"test-key": "test-value"},
		})

		require.NoError(err)
		assert.Equal(reqURL, obj.URL)
		assert.Equal(int64(len(content)), obj.Size)
		assert.Equal("text/plain", obj.ContentType)
		assert.Equal("test-value", obj.Meta["test-key"])
		assert.Equal(modTime.Format(time.RFC3339), obj.Meta[filer.AttrLastModified])

		// Cleanup: delete the test object
		_, err = backend.DeleteObject(ctx, schema.DeleteObjectRequest{URL: reqURL})
		assert.NoError(err)
	})
}

func TestCreateObject_InvalidURL(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{
			name:    "wrong scheme",
			url:     "s3://bucket/file.txt",
			wantErr: true,
		},
		{
			name:    "wrong host",
			url:     "mem://otherbucket/file.txt",
			wantErr: true,
		},
		{
			name:    "invalid URL",
			url:     "://invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
				URL:  tt.url,
				Body: strings.NewReader("test"),
			})

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
