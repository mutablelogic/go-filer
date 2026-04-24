package blob

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
		// S3 tests (no bucket prefix)
		{
			name:       "s3 path",
			backendURL: "s3://mybucket",
			inputPath:  "/path/to/file.txt",
			want:       "path/to/file.txt",
		},
		{
			name:       "s3 root path",
			backendURL: "s3://mybucket",
			inputPath:  "/",
			want:       "",
		},
		{
			name:       "s3 empty path",
			backendURL: "s3://mybucket",
			inputPath:  "",
			want:       "",
		},

		// S3 with bucket prefix
		{
			name:       "s3 with prefix",
			backendURL: "s3://mybucket/data",
			inputPath:  "/file.txt",
			want:       "data/file.txt",
		},
		{
			name:       "s3 with prefix root",
			backendURL: "s3://mybucket/data",
			inputPath:  "/",
			want:       "data/",
		},
		{
			name:       "s3 with nested prefix",
			backendURL: "s3://mybucket/data/subdir",
			inputPath:  "/nested/file.txt",
			want:       "data/subdir/nested/file.txt",
		},

		// File tests (no bucket prefix)
		{
			name:       "file path",
			backendURL: "file://mystore/tmp/storage",
			inputPath:  "/test.txt",
			want:       "test.txt",
		},
		{
			name:       "file nested path",
			backendURL: "file://mystore/data/dir",
			inputPath:  "/dir1/dir2/file.txt",
			want:       "dir1/dir2/file.txt",
		},
		{
			name:       "file root",
			backendURL: "file://mystore/data",
			inputPath:  "/",
			want:       "",
		},
		{
			name:       "file empty path",
			backendURL: "file://mystore/data",
			inputPath:  "",
			want:       "",
		},

		// Mem tests
		{
			name:       "mem no prefix",
			backendURL: "mem://testbucket",
			inputPath:  "/file.txt",
			want:       "file.txt",
		},
		{
			name:       "mem with prefix",
			backendURL: "mem://bucket/prefix",
			inputPath:  "/subdir/file.txt",
			want:       "prefix/subdir/file.txt",
		},

		// Path traversal prevention
		{
			name:       "file traversal at root",
			backendURL: "file://mystore/data",
			inputPath:  "/../../etc/passwd",
			want:       "etc/passwd",
		},
		{
			name:       "file traversal deep",
			backendURL: "file://mystore/data",
			inputPath:  "/valid/../../../etc/passwd",
			want:       "etc/passwd",
		},
		{
			name:       "file traversal stays within path",
			backendURL: "file://mystore/data",
			inputPath:  "/a/b/../c",
			want:       "a/c",
		},
		{
			name:       "s3 traversal with prefix",
			backendURL: "s3://mybucket/data",
			inputPath:  "/valid/../../other",
			want:       "data/other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			backendURL, err := url.Parse(tt.backendURL)
			assert.NoError(err)

			b := &backend{
				opt: &opt{
					url: backendURL,
				},
			}
			if backendURL.Scheme != "file" {
				b.bucketPrefix = strings.TrimPrefix(strings.TrimSuffix(backendURL.Path, "/"), "/")
			}

			got := b.key(tt.inputPath)
			assert.Equal(tt.want, got)
		})
	}
}

func TestCleanPath(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/foo/bar.txt", "/foo/bar.txt"},
		{"", "/"},
		{"/", "/"},
		{"/a/b/../c", "/a/c"},
		{"/../../etc/passwd", "/etc/passwd"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, cleanPath(tt.input))
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
func Test_normaliseETag(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		// Bare MD5 hex (single-part S3 upload) → must be quoted.
		{"a828f16f116f67e6ac37e409a9e7ced9", `"a828f16f116f67e6ac37e409a9e7ced9"`},
		// Already RFC 7232 quoted strong ETag → unchanged.
		{`"3d862beccbda836b05c87d3a79cfdc0a-40"`, `"3d862beccbda836b05c87d3a79cfdc0a-40"`},
		// Already RFC 7232 quoted simple ETag → unchanged.
		{`"abc123"`, `"abc123"`},
		// Weak ETag → unchanged.
		{`W/"abc123"`, `W/"abc123"`},
		// Empty → stays empty.
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := normaliseETag(tc.in)
			if got != tc.want {
				t.Errorf("normaliseETag(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
