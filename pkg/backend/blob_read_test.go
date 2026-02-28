package backend

import (
	"context"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadObject_Mem(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	// Create test objects
	createTestObject := func(t *testing.T, key, content, contentType string, meta schema.ObjectMeta) {
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{

			Path:        "/" + key,
			Body:        strings.NewReader(content),
			ContentType: contentType,
			Meta:        meta,
		})
		require.NoError(t, err)
	}

	createTestObject(t, "simple.txt", "hello world", "text/plain", nil)
	createTestObject(t, "data.json", `{"foo":"bar"}`, "application/json", schema.ObjectMeta{"author": "test"})
	createTestObject(t, "subdir/nested.txt", "nested content", "text/plain", nil)

	tests := []struct {
		name        string
		reqName     string
		reqPath     string
		wantContent string
		wantSize    int64
		wantType    string
		wantMeta    schema.ObjectMeta
		wantErr     bool
		errContains string
	}{
		{
			name:        "simple file",
			reqName:     "testbucket",
			reqPath:     "/simple.txt",
			wantContent: "hello world",
			wantSize:    11,
			wantType:    "text/plain",
		},
		{
			name:        "file with metadata",
			reqName:     "testbucket",
			reqPath:     "/data.json",
			wantContent: `{"foo":"bar"}`,
			wantSize:    13,
			wantType:    "application/json",
			wantMeta:    schema.ObjectMeta{"author": "test"},
		},
		{
			name:        "nested file",
			reqName:     "testbucket",
			reqPath:     "/subdir/nested.txt",
			wantContent: "nested content",
			wantSize:    14,
			wantType:    "text/plain",
		},
		{
			name:        "non-existent file",
			reqName:     "testbucket",
			reqPath:     "/notfound.txt",
			wantErr:     true,
			errContains: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			reader, obj, err := backend.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: tt.reqPath}})

			if tt.wantErr {
				assert.Error(err)
				if tt.errContains != "" {
					assert.Contains(err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			defer reader.Close()

			// Read content
			content, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(tt.wantContent, string(content))

			// Check object metadata
			assert.Equal(tt.reqName, obj.Name)
			assert.Equal(tt.reqPath, obj.Path)
			assert.Equal(tt.wantSize, obj.Size)
			assert.Equal(tt.wantType, obj.ContentType)

			if tt.wantMeta != nil {
				for k, v := range tt.wantMeta {
					assert.Equal(v, obj.Meta[k])
				}
			}
		})
	}
}

func TestReadObject_File(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	backend, err := NewBlobBackend(ctx, "file://testfiles"+tempDir, WithCreateDir())
	require.NoError(t, err)
	defer backend.Close()

	// Create test files
	createTestObject := func(t *testing.T, key, content string) {
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{

			Path:        "/" + key,
			Body:        strings.NewReader(content),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	createTestObject(t, "test.txt", "hello world")
	createTestObject(t, "subdir/nested.txt", "nested content")

	tests := []struct {
		name        string
		key         string
		wantContent string
		wantSize    int64
		wantErr     bool
		errContains string
	}{
		{
			name:        "simple file",
			key:         "test.txt",
			wantContent: "hello world",
			wantSize:    11,
		},
		{
			name:        "nested file",
			key:         "subdir/nested.txt",
			wantContent: "nested content",
			wantSize:    14,
		},
		{
			name:        "non-existent file",
			key:         "notfound.txt",
			wantErr:     true,
			errContains: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			reqURL := "file://testfiles/" + tt.key
			_ = reqURL
			reader, obj, err := backend.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: "/" + tt.key}})

			if tt.wantErr {
				assert.Error(err)
				if tt.errContains != "" {
					assert.Contains(err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			defer reader.Close()

			// Read content
			content, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(tt.wantContent, string(content))

			// Check object metadata
			assert.Equal("testfiles", obj.Name)
			assert.Equal("/"+tt.key, obj.Path)
			assert.Equal(tt.wantSize, obj.Size)
		})
	}
}

func TestReadObject_S3(t *testing.T) {
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
	testKey := "read-test-" + time.Now().Format("20060102-150405") + ".txt"
	reqURL := bucketURL + "/" + testKey
	_ = reqURL
	reqPath := bURL.Path + "/" + testKey

	// Create test object first
	content := "hello from S3 ReadObject test"
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{
		Path:        reqPath,
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
		Meta:        schema.ObjectMeta{"test-key": "test-value"},
	})
	require.NoError(t, err)

	// Clean up after test
	defer func() {
		backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: reqPath})
	}()

	t.Run("read existing object", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		reader, obj, err := backend.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: reqPath}})
		require.NoError(err)
		defer reader.Close()

		// Read content
		data, err := io.ReadAll(reader)
		require.NoError(err)
		assert.Equal(content, string(data))

		// Check metadata
		assert.Equal(bURL.Host, obj.Name)
		assert.Equal("/"+testKey, obj.Path)
		assert.Equal(int64(len(content)), obj.Size)
		assert.Equal("text/plain", obj.ContentType)
		assert.Equal("test-value", obj.Meta["test-key"])
	})

	t.Run("read non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		nonExistentURL := bucketURL + "/non-existent-file.txt"
		_ = nonExistentURL
		_, _, err := backend.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: bURL.Path + "/non-existent-file.txt"}})

		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})
}

func TestReadObject_PartialRead(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	// Create a larger test object
	content := strings.Repeat("abcdefghij", 100) // 1000 bytes
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{

		Path:        "/large.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("partial read and close", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		reader, obj, err := backend.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: "/large.txt"}})
		require.NoError(err)

		// Only read first 10 bytes
		buf := make([]byte, 10)
		n, err := reader.Read(buf)
		require.NoError(err)
		assert.Equal(10, n)
		assert.Equal("abcdefghij", string(buf))

		// Close without reading the rest
		err = reader.Close()
		assert.NoError(err)

		// Verify object metadata is correct
		assert.Equal(int64(1000), obj.Size)
	})
}

func TestReadObject_EmptyFile(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	// Create an empty file
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{

		Path:        "/empty.txt",
		Body:        strings.NewReader(""),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("read empty file", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		reader, obj, err := backend.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: "/empty.txt"}})
		require.NoError(err)
		defer reader.Close()

		content, err := io.ReadAll(reader)
		require.NoError(err)
		assert.Empty(content)
		assert.Equal(int64(0), obj.Size)
	})
}

func TestReadObject_BinaryContent(t *testing.T) {
	ctx := context.Background()

	backend, err := NewBlobBackend(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	// Create binary content with null bytes
	binaryContent := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
	_, err = backend.CreateObject(ctx, schema.CreateObjectRequest{

		Path:        "/binary.bin",
		Body:        strings.NewReader(string(binaryContent)),
		ContentType: "application/octet-stream",
	})
	require.NoError(t, err)

	t.Run("read binary file", func(t *testing.T) {
		assert := assert.New(t)
		require := require.New(t)

		reader, obj, err := backend.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: "/binary.bin"}})
		require.NoError(err)
		defer reader.Close()

		content, err := io.ReadAll(reader)
		require.NoError(err)
		assert.Equal(binaryContent, content)
		assert.Equal(int64(6), obj.Size)
		assert.Equal("application/octet-stream", obj.ContentType)
	})
}
