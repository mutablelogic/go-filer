package backend

import (
	"net/url"
	"testing"

	// Packages
	"github.com/stretchr/testify/assert"
)

func TestBlobBackendPath(t *testing.T) {
	tests := []struct {
		name       string
		backendURL string
		inputURL   string
		want       string
	}{
		// S3 tests
		{
			name:       "s3 exact bucket match",
			backendURL: "s3://mybucket",
			inputURL:   "s3://mybucket/path/to/file.txt",
			want:       "path/to/file.txt",
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
			want:       "",
		},
		{
			name:       "s3 with prefix match",
			backendURL: "s3://mybucket/data",
			inputURL:   "s3://mybucket/data/file.txt",
			want:       "file.txt",
		},
		{
			name:       "s3 with prefix no match",
			backendURL: "s3://mybucket/data",
			inputURL:   "s3://mybucket/other/file.txt",
			want:       "",
		},
		{
			name:       "s3 with nested prefix",
			backendURL: "s3://mybucket/data/subdir",
			inputURL:   "s3://mybucket/data/subdir/nested/file.txt",
			want:       "nested/file.txt",
		},

		// File tests
		{
			name:       "file empty host match",
			backendURL: "file:///tmp/storage",
			inputURL:   "file:///tmp/storage/myfile.txt",
			want:       "myfile.txt",
		},
		{
			name:       "file with host match",
			backendURL: "file://myhost/tmp/storage",
			inputURL:   "file://myhost/tmp/storage/myfile.txt",
			want:       "myfile.txt",
		},
		{
			name:       "file host mismatch",
			backendURL: "file://host1/tmp/storage",
			inputURL:   "file://host2/tmp/storage/myfile.txt",
			want:       "",
		},
		{
			name:       "file path prefix mismatch",
			backendURL: "file:///tmp/storage",
			inputURL:   "file:///var/data/myfile.txt",
			want:       "",
		},
		{
			name:       "file nested path",
			backendURL: "file:///tmp/storage",
			inputURL:   "file:///tmp/storage/dir1/dir2/file.txt",
			want:       "dir1/dir2/file.txt",
		},

		// Mem tests
		{
			name:       "mem empty host",
			backendURL: "mem://",
			inputURL:   "mem:///path/to/file.txt",
			want:       "path/to/file.txt",
		},
		{
			name:       "mem with host match",
			backendURL: "mem://testbucket",
			inputURL:   "mem://testbucket/file.txt",
			want:       "file.txt",
		},
		{
			name:       "mem host mismatch",
			backendURL: "mem://bucket1",
			inputURL:   "mem://bucket2/file.txt",
			want:       "",
		},
		{
			name:       "mem with prefix",
			backendURL: "mem://bucket/prefix",
			inputURL:   "mem://bucket/prefix/subdir/file.txt",
			want:       "subdir/file.txt",
		},

		// Edge cases
		{
			name:       "nil URL returns empty",
			backendURL: "s3://mybucket",
			inputURL:   "",
			want:       "",
		},
		{
			name:       "exact prefix match returns empty path",
			backendURL: "s3://mybucket/data",
			inputURL:   "s3://mybucket/data",
			want:       "",
		},
		{
			name:       "prefix with trailing slash",
			backendURL: "s3://mybucket/data/",
			inputURL:   "s3://mybucket/data/file.txt",
			want:       "file.txt",
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
				prefix: trimPrefix(backendURL.Path),
			}

			// Parse input URL (handle empty string case)
			var inputURL *url.URL
			if tt.inputURL != "" {
				inputURL, err = url.Parse(tt.inputURL)
				assert.NoError(err)
			}

			got := b.Path(inputURL)
			assert.Equal(tt.want, got)
		})
	}
}

// trimPrefix mimics the prefix logic in NewBlobBackend
func trimPrefix(path string) string {
	if len(path) > 0 && path[0] == '/' {
		return path[1:]
	}
	return path
}
