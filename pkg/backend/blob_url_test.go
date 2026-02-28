package backend

import (
	"context"
	"testing"

	// Packages
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
)

// TestBlobBackendURL verifies that URL() returns a sanitized URL with
// query params only for s3:// backends, never for file:// or mem://.
func TestBlobBackendURL(t *testing.T) {
	ctx := context.Background()

	t.Run("file: no query params", func(t *testing.T) {
		dir := t.TempDir()
		b, err := NewFileBackend(ctx, "teststore", dir)
		assert.NoError(t, err)
		defer b.Close()
		u := b.URL()
		assert.Equal(t, "file", u.Scheme)
		assert.Empty(t, u.RawQuery, "file:// should have no query params")
	})

	t.Run("mem: no query params", func(t *testing.T) {
		b, err := NewBlobBackend(ctx, "mem://testbucket")
		assert.NoError(t, err)
		defer b.Close()
		u := b.URL()
		assert.Equal(t, "mem", u.Scheme)
		assert.Empty(t, u.RawQuery, "mem:// should have no query params")
	})

	t.Run("s3: region included", func(t *testing.T) {
		b, err := NewBlobBackend(ctx, "s3://mybucket",
			WithAWSConfig(aws.Config{Region: "eu-west-1"}),
		)
		assert.NoError(t, err)
		defer b.Close()
		u := b.URL()
		assert.Equal(t, "s3", u.Scheme)
		assert.Equal(t, "eu-west-1", u.Query().Get("region"))
		assert.Empty(t, u.Query().Get("endpoint"))
		assert.Empty(t, u.Query().Get("anonymous"))
	})

	t.Run("s3: endpoint sanitized (no userinfo)", func(t *testing.T) {
		b, err := NewBlobBackend(ctx, "s3://mybucket",
			WithEndpoint("http://user:pass@minio.local:9000"),
		)
		assert.NoError(t, err)
		defer b.Close()
		u := b.URL()
		ep := u.Query().Get("endpoint")
		assert.NotEmpty(t, ep, "endpoint should be present for s3://")
		assert.NotContains(t, ep, "user", "endpoint must not contain username")
		assert.NotContains(t, ep, "pass", "endpoint must not contain password")
		assert.Contains(t, ep, "minio.local:9000")
	})

	t.Run("s3: anonymous included", func(t *testing.T) {
		b, err := NewBlobBackend(ctx, "s3://mybucket", WithAnonymous())
		assert.NoError(t, err)
		defer b.Close()
		u := b.URL()
		assert.Equal(t, "true", u.Query().Get("anonymous"))
	})

	t.Run("s3: path (bucket prefix) preserved", func(t *testing.T) {
		b, err := NewBlobBackend(ctx, "s3://mybucket/myprefix")
		assert.NoError(t, err)
		defer b.Close()
		u := b.URL()
		assert.Equal(t, "mybucket", u.Host)
		assert.Equal(t, "/myprefix", u.Path)
	})
}
