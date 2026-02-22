package backend

import (
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
		inputURL   string
		want       string
	}{
		// S3 tests (no prefix)
		{
			name:       "s3 exact bucket match",
			backendURL: "s3://mybucket",
			inputURL:   "s3://mybucket/path/to/file.txt",
			want:       "/path/to/file.txt",
		},
		{
			name:       "s3 bucket mismatch",
			backendURL: "s3://mybucket",
			inputURL:   "s3://otherbucket/path/to/file.txt",
			want:       "",
		},
		{
			name:       "s3 scheme mismatch",
			backendURL: "s3://mybucket",
			inputURL:   "file:///path/to/file.txt",
			want:       "",
		},
		{
			name:       "s3 root path",
			backendURL: "s3://mybucket",
			inputURL:   "s3://mybucket/",
			want:       "/",
		},
		{
			name:       "s3 root no trailing slash",
			backendURL: "s3://mybucket",
			inputURL:   "s3://mybucket",
			want:       "/",
		},

		// S3 with prefix (prefix as discriminator, stripped in Key)
		{
			name:       "s3 with prefix match",
			backendURL: "s3://mybucket/data",
			inputURL:   "s3://mybucket/data/file.txt",
			want:       "/file.txt",
		},
		{
			name:       "s3 with prefix root",
			backendURL: "s3://mybucket/data",
			inputURL:   "s3://mybucket/data/",
			want:       "/",
		},
		{
			name:       "s3 with prefix exact",
			backendURL: "s3://mybucket/data",
			inputURL:   "s3://mybucket/data",
			want:       "/",
		},
		{
			name:       "s3 with prefix mismatch",
			backendURL: "s3://mybucket/data",
			inputURL:   "s3://mybucket/other/file.txt",
			want:       "",
		},
		{
			name:       "s3 with nested prefix",
			backendURL: "s3://mybucket/data/subdir",
			inputURL:   "s3://mybucket/data/subdir/nested/file.txt",
			want:       "/nested/file.txt",
		},

		// File tests (scheme+host matching only, path is bucket root dir)
		{
			name:       "file with name and path",
			backendURL: "file://mystore/tmp/storage",
			inputURL:   "file://mystore/test.txt",
			want:       "/test.txt",
		},
		{
			name:       "file name mismatch",
			backendURL: "file://mystore/tmp/storage",
			inputURL:   "file://other/test.txt",
			want:       "",
		},
		{
			name:       "file nested path",
			backendURL: "file://mystore/data/dir",
			inputURL:   "file://mystore/dir1/dir2/file.txt",
			want:       "/dir1/dir2/file.txt",
		},
		{
			name:       "file root",
			backendURL: "file://mystore/data",
			inputURL:   "file://mystore/",
			want:       "/",
		},
		{
			name:       "file root no trailing slash",
			backendURL: "file://mystore/data",
			inputURL:   "file://mystore",
			want:       "/",
		},

		// Mem tests
		{
			name:       "mem empty host",
			backendURL: "mem://",
			inputURL:   "mem:///path/to/file.txt",
			want:       "/path/to/file.txt",
		},
		{
			name:       "mem with host match",
			backendURL: "mem://testbucket",
			inputURL:   "mem://testbucket/file.txt",
			want:       "/file.txt",
		},
		{
			name:       "mem host mismatch",
			backendURL: "mem://bucket1",
			inputURL:   "mem://bucket2/file.txt",
			want:       "",
		},
		{
			name:       "mem with prefix match",
			backendURL: "mem://bucket/prefix",
			inputURL:   "mem://bucket/prefix/subdir/file.txt",
			want:       "/subdir/file.txt",
		},
		{
			name:       "mem with prefix mismatch",
			backendURL: "mem://bucket/prefix",
			inputURL:   "mem://bucket/other/file.txt",
			want:       "",
		},

		// Edge cases
		{
			name:       "nil URL returns empty",
			backendURL: "s3://mybucket",
			inputURL:   "",
			want:       "",
		},

		// NewFileBackend-style tests (file://name/dir)
		{
			name:       "file backend with name and dir",
			backendURL: "file://mybackend/var/data",
			inputURL:   "file://mybackend/doc.txt",
			want:       "/doc.txt",
		},
		{
			name:       "file backend root",
			backendURL: "file://mybackend/var/data",
			inputURL:   "file://mybackend/",
			want:       "/",
		},
		{
			name:       "file backend name mismatch",
			backendURL: "file://mybackend/var/data",
			inputURL:   "file://other/doc.txt",
			want:       "",
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

			// Parse input URL (handle empty string case)
			var inputURL *url.URL
			if tt.inputURL != "" {
				inputURL, err = url.Parse(tt.inputURL)
				assert.NoError(err)
			}

			got := b.Key(inputURL)
			assert.Equal(tt.want, got)
		})
	}
}
