package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// fakeS3 returns the endpoint of a service which is compatible with S3, with
// the credentials of the environment, which only has the bucket "photos",
// "private", which can't be accessed, and "moved", which is in eu-west-1, as
// a request signed for another region is told. An anonymous request can only
// access "photos".
func fakeS3(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_SESSION_TOKEN", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		switch {
		case r.Method != http.MethodHead:
			w.WriteHeader(http.StatusMethodNotAllowed)
		case r.URL.Path == "/photos":
			w.WriteHeader(http.StatusOK)
		case auth == "":
			w.WriteHeader(http.StatusForbidden)
		case r.URL.Path == "/private":
			w.WriteHeader(http.StatusForbidden)
		case r.URL.Path == "/moved" && strings.Contains(auth, "/eu-west-1/s3/"):
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/moved":
			w.Header().Set(headerBucketRegion, "eu-west-1")
			w.WriteHeader(http.StatusMovedPermanently)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// bucketURL returns the URL of the bucket, at the endpoint, with the options
func bucketURL(t *testing.T, endpoint, bucket, opts string) *url.URL {
	t.Helper()
	u, err := url.Parse("s3://" + bucket + "/?endpoint=" + url.QueryEscape(endpoint) + opts)
	require.NoError(t, err)
	return u
}

func Test_S3_Connect(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()

	// A bucket which can be accessed
	b, err := New(ctx, bucketURL(t, endpoint, "photos", ""))
	require.NoError(t, err)
	assert.Equal(t, defaultRegion, b.client.Options().Region)
	assert.NoError(t, b.Close())

	// Anonymously
	b, err = New(ctx, bucketURL(t, endpoint, "photos", "&anonymous"))
	require.NoError(t, err)
	assert.True(t, b.Readonly())
	_, err = New(ctx, bucketURL(t, endpoint, "private", "&anonymous"))
	assert.ErrorIs(t, err, gofiler.ErrForbidden)

	// A bucket in another region is in its region, which isn't in its URL,
	// unless the region is set, when it's an error
	b, err = New(ctx, bucketURL(t, endpoint, "moved", ""))
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", b.client.Options().Region)
	assert.NotContains(t, b.URL().RawQuery, "region")
	_, err = New(ctx, bucketURL(t, endpoint, "moved", "&region=us-west-2"))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	b, err = New(ctx, bucketURL(t, endpoint, "moved", "&region=eu-west-1"))
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", b.client.Options().Region)

	// A bucket which can't be accessed, or doesn't exist
	_, err = New(ctx, bucketURL(t, endpoint, "private", ""))
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = New(ctx, bucketURL(t, endpoint, "missing", ""))
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// A profile which doesn't exist
	_, err = New(ctx, bucketURL(t, endpoint, "photos", "&profile=missing"))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func Test_S3_NoCredentials(t *testing.T) {
	endpoint := fakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	// Without credentials, a bucket can't be accessed, unless it's anonymous
	_, err := New(context.Background(), bucketURL(t, endpoint, "photos", ""))
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	assert.ErrorContains(t, err, "no credentials")
	_, err = New(context.Background(), bucketURL(t, endpoint, "photos", "&anonymous"))
	assert.NoError(t, err)
}
