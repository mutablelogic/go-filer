package backend

import (
	"context"
	"net/url"
	"strings"
	"testing"

	// Packages
	"github.com/stretchr/testify/assert"
)

func TestBlobBackendKey(t *testing.T) {
	tests := []struct {
		name       string
		backendURL string
		inputPath  string
		want       string
	}{
		// S3 tests (no prefix)
		{
			name:       "s3 path",
			backendURL: "s3://mybucket",
			inputPath:  "/path/to/file.txt",
			want:       "/path/to/file.txt",
		},
		{
			name:       "s3 root path",
			backendURL: "s3://mybucket",
			inputPath:  "/",
			want:       "/",
		},
		{
			name:       "s3 root empty path",
			backendURL: "s3://mybucket",
			inputPath:  "",
			want:       "/",
		},

		// S3 with prefix (prefix as discriminator, stripped in Key)
		{
			name:       "s3 with prefix match",
			backendURL: "s3://mybucket/data",
			inputPath:  "/data/file.txt",
			want:       "/file.txt",
		},
		{
			name:       "s3 with prefix root",
			backendURL: "s3://mybucket/data",
			inputPath:  "/data/",
			want:       "/",
		},
		{
			name:       "s3 with prefix exact",
			backendURL: "s3://mybucket/data",
			inputPath:  "/data",
			want:       "/",
		},
		{
			name:       "s3 with prefix mismatch",
			backendURL: "s3://mybucket/data",
			inputPath:  "/other/file.txt",
			want:       "",
		},
		{
			name:       "s3 with nested prefix",
			backendURL: "s3://mybucket/data/subdir",
			inputPath:  "/data/subdir/nested/file.txt",
			want:       "/nested/file.txt",
		},

		// File tests (path is returned as-is, cleaned)
		{
			name:       "file path",
			backendURL: "file://mystore/tmp/storage",
			inputPath:  "/test.txt",
			want:       "/test.txt",
		},
		{
			name:       "file nested path",
			backendURL: "file://mystore/data/dir",
			inputPath:  "/dir1/dir2/file.txt",
			want:       "/dir1/dir2/file.txt",
		},
		{
			name:       "file root",
			backendURL: "file://mystore/data",
			inputPath:  "/",
			want:       "/",
		},
		{
			name:       "file empty path",
			backendURL: "file://mystore/data",
			inputPath:  "",
			want:       "/",
		},

		// Mem tests
		{
			name:       "mem empty host path",
			backendURL: "mem://",
			inputPath:  "/path/to/file.txt",
			want:       "/path/to/file.txt",
		},
		{
			name:       "mem path",
			backendURL: "mem://testbucket",
			inputPath:  "/file.txt",
			want:       "/file.txt",
		},
		{
			name:       "mem with prefix match",
			backendURL: "mem://bucket/prefix",
			inputPath:  "/prefix/subdir/file.txt",
			want:       "/subdir/file.txt",
		},
		{
			name:       "mem with prefix mismatch",
			backendURL: "mem://bucket/prefix",
			inputPath:  "/other/file.txt",
			want:       "",
		},

		// Path traversal prevention
		{
			name:       "file traversal at root",
			backendURL: "file://mystore/data",
			inputPath:  "/../../etc/passwd",
			want:       "/etc/passwd",
		},
		{
			name:       "file traversal deep",
			backendURL: "file://mystore/data",
			inputPath:  "/valid/../../../etc/passwd",
			want:       "/etc/passwd",
		},
		{
			name:       "file traversal stays within path",
			backendURL: "file://mystore/data",
			inputPath:  "/a/b/../c",
			want:       "/a/c",
		},
		{
			name:       "s3 traversal within prefix",
			backendURL: "s3://mybucket/data",
			inputPath:  "/data/valid/../../other",
			want:       "/other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			// Parse backend URL to create a minimal blobbackend for testing
			backendURL, err := url.Parse(tt.backendURL)
			assert.NoError(err)

			b := &blobbackend{
				opt: &opt{
					url: backendURL,
				},
				prefix: strings.TrimSuffix(backendURL.Path, "/"),
			}

			got := b.Key(tt.inputPath)
			assert.Equal(tt.want, got)
		})
	}
}
func TestNewFileBackend(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		backend string
		dir     string
		wantErr bool
	}{
		{"valid name and dir", "mybackend", tmpDir, false},
		{"name with hyphen", "my-backend", tmpDir, false},
		{"name with digits", "backend2", tmpDir, false},
		{"empty name", "", tmpDir, true},
		{"name starts with digit", "2backend", tmpDir, true},
		{"name starts with hyphen", "-backend", tmpDir, true},
		{"name with underscore", "my_backend", tmpDir, false},
		{"name with slash", "my/backend", tmpDir, true},
		{"relative dir", "mybackend", "relative/path", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			b, err := NewFileBackend(ctx, tt.backend, tt.dir)
			if tt.wantErr {
				assert.Error(err)
			} else {
				assert.NoError(err)
				if b != nil {
					b.Close()
				}
			}
		})
	}
}
