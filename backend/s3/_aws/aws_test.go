package aws

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	// Packages
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////
// UNIT TESTS

func TestBackendKey(t *testing.T) {
	tests := []struct {
		name       string
		backendURL string
		inputPath  string
		want       string
	}{
		// No bucket prefix
		{"root", "s3://mybucket", "/", ""},
		{"empty", "s3://mybucket", "", ""},
		{"file", "s3://mybucket", "/file.txt", "file.txt"},
		{"nested", "s3://mybucket", "/a/b/c.txt", "a/b/c.txt"},

		// With bucket prefix
		{"prefix root", "s3://mybucket/data", "/", "data/"},
		{"prefix file", "s3://mybucket/data", "/file.txt", "data/file.txt"},
		{"prefix nested", "s3://mybucket/data/sub", "/f.txt", "data/sub/f.txt"},

		// Path traversal prevention
		{"traversal", "s3://mybucket", "/../../etc/passwd", "etc/passwd"},
		{"traversal prefix", "s3://mybucket/data", "/../../etc/passwd", "data/etc/passwd"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.backendURL)
			require.NoError(t, err)
			b := &backend{
				opt:          &opt{url: u},
				bucket:       u.Host,
				bucketPrefix: strings.TrimPrefix(strings.TrimSuffix(u.Path, "/"), "/"),
			}
			assert.Equal(t, tt.want, b.key(tt.inputPath))
		})
	}
}

func TestCleanPath(t *testing.T) {
	tests := []struct{ in, want string }{
		{"/foo/bar.txt", "/foo/bar.txt"},
		{"", "/"},
		{"/", "/"},
		{"/a/b/../c", "/a/c"},
		{"/../../etc/passwd", "/etc/passwd"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, cleanPath(tt.in))
		})
	}
}

func Test_normaliseETag(t *testing.T) {
	tests := []struct{ in, want string }{
		{"a828f16f116f67e6ac37e409a9e7ced9", `"a828f16f116f67e6ac37e409a9e7ced9"`},
		{`"3d862beccbda836b05c87d3a79cfdc0a-40"`, `"3d862beccbda836b05c87d3a79cfdc0a-40"`},
		{`"abc123"`, `"abc123"`},
		{`W/"abc123"`, `W/"abc123"`},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			assert.Equal(t, tc.want, normaliseETag(tc.in))
		})
	}
}

////////////////////////////////////////////////////////////////////////////////
// S3 INTEGRATION TESTS

// s3TestConfig reads S3 connection settings from environment variables.
//
// Required:
//   - S3_BUCKET_URL: e.g. "s3://my-bucket" or "s3://my-bucket/prefix"
//
// Optional:
//   - S3_ENDPOINT:  custom endpoint for SeaweedFS / MinIO
//   - S3_ANONYMOUS: "true" for unauthenticated access
//   - AWS_REGION:   AWS region
//   - AWS_PROFILE:  shared credentials profile
func s3TestConfig() (bucketURL string, opts []Opt) {
	bucketURL = os.Getenv("S3_BUCKET_URL")
	if bucketURL == "" {
		return "", nil
	}
	if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
		opts = append(opts, WithEndpoint(endpoint))
	}
	if os.Getenv("S3_ANONYMOUS") == "true" {
		opts = append(opts, WithAnonymous())
	}
	var cfgOpts []func(*awsconfig.LoadOptions) error
	if region := os.Getenv("AWS_REGION"); region != "" {
		cfgOpts = append(cfgOpts, awsconfig.WithRegion(region))
	}
	if profile := os.Getenv("AWS_PROFILE"); profile != "" {
		cfgOpts = append(cfgOpts, awsconfig.WithSharedConfigProfile(profile))
	}
	if len(cfgOpts) > 0 {
		if awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), cfgOpts...); err == nil {
			opts = append(opts, WithAWSConfig(awsCfg))
		}
	}
	return bucketURL, opts
}

func TestS3_CreateGetDeleteObject(t *testing.T) {
	bucketURL, opts := s3TestConfig()
	if bucketURL == "" {
		t.Skip("S3_BUCKET_URL not set")
	}

	ctx := context.Background()
	b, err := New(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer b.Close()

	bURL, _ := url.Parse(bucketURL)
	testPath := bURL.Path + "/test-" + time.Now().Format("20060102-150405") + ".txt"
	content := "hello from aws backend test"
	modTime := time.Date(2026, 1, 28, 12, 0, 0, 0, time.UTC)

	t.Run("create", func(t *testing.T) {
		obj, err := b.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        testPath,
			Body:        bytes.NewReader([]byte(content)),
			ContentType: "text/plain",
			ModTime:     modTime,
			Meta:        schema.ObjectMeta{"test-key": "test-value"},
		})
		require.NoError(t, err)
		assert.Equal(t, bURL.Host, obj.Name)
		assert.Equal(t, int64(len(content)), obj.Size)
		assert.Equal(t, "text/plain", obj.ContentType)
		assert.Equal(t, "test-value", obj.Meta["test-key"])
		assert.Equal(t, modTime.Format(time.RFC3339), obj.Meta[schema.AttrLastModified])
	})

	t.Run("get", func(t *testing.T) {
		obj, err := b.GetObject(ctx, schema.GetObjectRequest{Path: testPath})
		require.NoError(t, err)
		assert.Equal(t, int64(len(content)), obj.Size)
		assert.Equal(t, "text/plain", obj.ContentType)
	})

	t.Run("read", func(t *testing.T) {
		rc, obj, err := b.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: testPath}})
		require.NoError(t, err)
		defer rc.Close()
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(rc)
		require.NoError(t, err)
		assert.Equal(t, content, buf.String())
		assert.Equal(t, int64(len(content)), obj.Size)
	})

	t.Run("list", func(t *testing.T) {
		resp, err := b.ListObjects(ctx, schema.ListObjectsRequest{Path: testPath})
		require.NoError(t, err)
		assert.Equal(t, 1, resp.Count)
	})

	t.Run("if-not-exists conflict", func(t *testing.T) {
		_, err := b.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        testPath,
			Body:        strings.NewReader("new"),
			ContentType: "text/plain",
			IfNotExists: true,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("delete", func(t *testing.T) {
		obj, err := b.DeleteObject(ctx, schema.DeleteObjectRequest{Path: testPath})
		require.NoError(t, err)
		assert.NotNil(t, obj)
	})

	t.Run("get after delete", func(t *testing.T) {
		_, err := b.GetObject(ctx, schema.GetObjectRequest{Path: testPath})
		assert.Error(t, err)
	})
}

func TestS3_DeleteObjects(t *testing.T) {
	bucketURL, opts := s3TestConfig()
	if bucketURL == "" {
		t.Skip("S3_BUCKET_URL not set")
	}

	ctx := context.Background()
	b, err := New(ctx, bucketURL, opts...)
	require.NoError(t, err)
	defer b.Close()

	bURL, _ := url.Parse(bucketURL)
	prefix := bURL.Path + "/bulk-" + time.Now().Format("20060102-150405")

	// Create several objects under the prefix.
	for i, name := range []string{"a.txt", "b.txt", "sub/c.txt"} {
		_, err := b.CreateObject(ctx, schema.CreateObjectRequest{
			Path:        prefix + "/" + name,
			Body:        strings.NewReader("content"),
			ContentType: "text/plain",
			Meta:        schema.ObjectMeta{"index": string(rune('0' + i))},
		})
		require.NoError(t, err)
	}

	resp, err := b.DeleteObjects(ctx, schema.DeleteObjectsRequest{
		Path:      prefix + "/",
		Recursive: true,
	})
	require.NoError(t, err)
	assert.Equal(t, 3, len(resp.Body))
}
