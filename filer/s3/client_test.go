package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// fakeS3 returns the endpoint of a service which is compatible with S3, with
// the credentials of the environment, which only has the bucket "photos",
// "private", which can't be accessed, and "moved", which is in eu-west-1, as
// a request signed for another region is told. An anonymous request can only
// access "photos".
func fakeS3(t *testing.T) string {
	t.Helper()
	fakeCredentials(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		switch {
		case r.Method != http.MethodHead:
			w.WriteHeader(http.StatusMethodNotAllowed)
		case r.URL.Path == "/photos":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/keys" && strings.Contains(auth, "Credential=AKIDTEST/") && r.Header.Get("X-Amz-Security-Token") == "TOKEN":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/keys":
			w.WriteHeader(http.StatusForbidden)
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

// fakeCredentials sets the credentials of the environment, without the
// configuration of the user, such as their profile and region
func fakeCredentials(t *testing.T) {
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
	b, err := New(ctx, bucketURL(t, endpoint, "photos", ""), gofiler.Opts{})
	require.NoError(t, err)
	assert.Equal(t, defaultRegion, b.client.Options().Region)
	assert.Equal(t, defaultRegion, b.URL().Query().Get("region"))
	assert.NoError(t, b.Close())

	// Anonymously
	b, err = New(ctx, bucketURL(t, endpoint, "photos", "&anonymous"), gofiler.Opts{})
	require.NoError(t, err)
	assert.True(t, b.Readonly())
	_, err = New(ctx, bucketURL(t, endpoint, "private", "&anonymous"), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)

	// A bucket in another region is in its region, which is in its URL,
	// unless the region is set, when it's an error
	b, err = New(ctx, bucketURL(t, endpoint, "moved", ""), gofiler.Opts{})
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", b.client.Options().Region)
	assert.Equal(t, "eu-west-1", b.URL().Query().Get("region"))

	// Its URL creates it again, in its region
	b, err = New(ctx, b.URL(), gofiler.Opts{})
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", b.client.Options().Region)
	_, err = New(ctx, bucketURL(t, endpoint, "moved", "&region=us-west-2"), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	b, err = New(ctx, bucketURL(t, endpoint, "moved", "&region=eu-west-1"), gofiler.Opts{})
	require.NoError(t, err)
	assert.Equal(t, "eu-west-1", b.client.Options().Region)

	// A bucket which can't be accessed, or doesn't exist
	_, err = New(ctx, bucketURL(t, endpoint, "private", ""), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = New(ctx, bucketURL(t, endpoint, "missing", ""), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// A profile which doesn't exist
	_, err = New(ctx, bucketURL(t, endpoint, "photos", "&profile=missing"), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func Test_S3_NoCredentials(t *testing.T) {
	endpoint := fakeS3(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	// Without credentials, a bucket can't be accessed, unless it's anonymous
	_, err := New(context.Background(), bucketURL(t, endpoint, "photos", ""), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	assert.ErrorContains(t, err, "no credentials")
	_, err = New(context.Background(), bucketURL(t, endpoint, "photos", "&anonymous"), gofiler.Opts{})
	assert.NoError(t, err)
}

// credentialProvider expands the placeholders of credentials with its
// values, and a placeholder which it doesn't have is an error
type credentialProvider map[string]string

func (p credentialProvider) Expand(_ context.Context, value string) (string, error) {
	var err error
	result := os.Expand(value, func(key string) string {
		if value, exists := p[key]; exists {
			return value
		}
		err = fmt.Errorf("no credential %q", key)
		return ""
	})
	return result, err
}

func (p credentialProvider) Get(context.Context, string) (json.RawMessage, error) {
	return nil, gofiler.ErrNotImplemented
}

// Set stores the credential, which is a JSON string, as its value
func (p credentialProvider) Set(_ context.Context, key string, value json.RawMessage) error {
	var credential string
	if err := json.Unmarshal(value, &credential); err != nil {
		return err
	}
	p[key] = credential
	return nil
}

func Test_S3_Credentials(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()
	provider := credentialProvider{"ID": "AKIDTEST", "SECRET": "secret", "TOKEN": "TOKEN"}
	opts := "&access-key-id=${ID}&secret-access-key=${SECRET}&session-token=${TOKEN}"

	// The credentials are the options', whose placeholders are expanded, and
	// which the URL with the credentials keeps, but the URL doesn't have
	b, err := New(ctx, bucketURL(t, endpoint, "keys", opts), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)
	assert.Equal(t, "${SECRET}", b.url(true).Query().Get("secret-access-key"))
	assert.NotContains(t, b.url(true).String(), "AKIDTEST")
	assert.False(t, b.URL().Query().Has("secret-access-key"))

	// Without a provider, or with a placeholder it doesn't have, they can't
	// be expanded, and other credentials can't access the bucket
	_, err = New(ctx, bucketURL(t, endpoint, "keys", opts), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = New(ctx, bucketURL(t, endpoint, "keys", opts), gofiler.Opts{Credentials: credentialProvider{"ID": "AKIDTEST"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=OTHER&secret-access-key=${SECRET}&session-token=${TOKEN}"), gofiler.Opts{Credentials: provider})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = New(ctx, bucketURL(t, endpoint, "keys", ""), gofiler.Opts{Credentials: provider})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
}

func Test_S3_Trace(t *testing.T) {
	endpoint := fakeS3(t)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())

	// Each request is a span, of its operation, with the bucket, region and
	// status of its response
	_, err := New(context.Background(), bucketURL(t, endpoint, "photos", ""), gofiler.Opts{Tracer: provider.Tracer("test")})
	require.NoError(t, err)
	_, err = New(context.Background(), bucketURL(t, endpoint, "missing", ""), gofiler.Opts{Tracer: provider.Tracer("test")})
	require.ErrorIs(t, err, gofiler.ErrNotFound)
	spans := recorder.Ended()
	require.Len(t, spans, 2)
	for i, status := range []int{http.StatusOK, http.StatusNotFound} {
		assert.Equal(t, "S3.HeadBucket", spans[i].Name())
		attrs := map[attribute.Key]attribute.Value{}
		for _, attr := range spans[i].Attributes() {
			attrs[attr.Key] = attr.Value
		}
		assert.Equal(t, int64(status), attrs["http.response.status_code"].AsInt64())
		assert.Equal(t, "us-east-1", attrs["cloud.region"].AsString())
		assert.Equal(t, "HeadBucket", attrs["rpc.method"].AsString())
		assert.NotEmpty(t, attrs["aws.s3.bucket"].AsString())
	}
	assert.Equal(t, codes.Unset, spans[0].Status().Code)
	assert.Equal(t, codes.Error, spans[1].Status().Code)
}

func Test_S3_StoreCredentials(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()
	opts := "&access-key-id=AKIDTEST&secret-access-key=secret&session-token=TOKEN"

	// Credentials which aren't placeholders are stored, once the bucket is
	// checked with them, by keys of the volume's name, and the URL with the
	// credentials has their placeholders, which create it again
	provider := credentialProvider{}
	b, err := New(ctx, bucketURL(t, endpoint, "keys", opts+"&name=photos"), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)
	assert.Equal(t, credentialProvider{"S3_PHOTOS_ACCESS_KEY_ID": "AKIDTEST", "S3_PHOTOS_SECRET_ACCESS_KEY": "secret", "S3_PHOTOS_SESSION_TOKEN": "TOKEN"}, provider)
	query := b.url(true).Query()
	assert.Equal(t, "${S3_PHOTOS_ACCESS_KEY_ID}", query.Get("access-key-id"))
	assert.Equal(t, "${S3_PHOTOS_SECRET_ACCESS_KEY}", query.Get("secret-access-key"))
	assert.Equal(t, "${S3_PHOTOS_SESSION_TOKEN}", query.Get("session-token"))
	assert.NotContains(t, b.url(true).String(), "secret&")
	assert.Equal(t, "s3://keys/?endpoint="+url.QueryEscape(endpoint)+"&name=photos&region=us-east-1", b.URL().String())
	_, err = New(ctx, b.url(true), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)

	// They can be mixed with placeholders, which are kept
	provider = credentialProvider{"TOKEN": "TOKEN"}
	b, err = New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=AKIDTEST&secret-access-key=secret&session-token=${TOKEN}"), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)
	assert.Equal(t, "${TOKEN}", b.url(true).Query().Get("session-token"))
	assert.Equal(t, "${S3_KEYS_SECRET_ACCESS_KEY}", b.url(true).Query().Get("secret-access-key"))

	// Without a provider, they can't be stored, and when the bucket can't be
	// accessed with them, they aren't
	_, err = New(ctx, bucketURL(t, endpoint, "keys", opts), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	provider = credentialProvider{}
	_, err = New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=OTHER&secret-access-key=secret"), gofiler.Opts{Credentials: provider})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	assert.Empty(t, provider)
}

func Test_S3_Clone(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()
	provider := credentialProvider{"ID": "AKIDTEST", "SECRET": "secret", "TOKEN": "TOKEN"}
	b, err := New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=${ID}&secret-access-key=${SECRET}&session-token=${TOKEN}"), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)

	// The clone has the credentials, which its URL doesn't, with the options
	// changed, and the filer isn't changed
	clone, err := b.Clone(ctx, url.Values{"readonly": {"true"}})
	require.NoError(t, err)
	assert.True(t, clone.(*S3).Readonly())
	assert.False(t, b.Readonly())
	assert.Equal(t, b.url(true).Query().Get("secret-access-key"), clone.(*S3).url(true).Query().Get("secret-access-key"))
	assert.False(t, clone.URL().Query().Has("secret-access-key"))

	// A patch can change the credentials, which are stored when they aren't
	// placeholders, or remove them, when the bucket can't be accessed
	clone, err = b.Clone(ctx, url.Values{"name": {"photos"}, "secret-access-key": {"secret"}})
	require.NoError(t, err)
	assert.Equal(t, "${S3_PHOTOS_SECRET_ACCESS_KEY}", clone.(*S3).url(true).Query().Get("secret-access-key"))
	assert.Equal(t, "secret", provider["S3_PHOTOS_SECRET_ACCESS_KEY"])
	_, err = b.Clone(ctx, url.Values{"access-key-id": nil, "secret-access-key": nil, "session-token": nil})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
}

func Test_S3_Placeholder(t *testing.T) {
	// A value is a placeholder when it's only a placeholder, and otherwise a
	// credential, which is stored
	for value, want := range map[string]bool{
		"${S3_SECRET}": true, "$S3_SECRET": true, "${_x1}": true,
		"secret": false, "secret${SUFFIX}": false, "${PREFIX}secret": false, "${A}${B}": false,
		"$": false, "${}": false, "$1": false, "${S3-SECRET}": false, "${S3_SECRET": false,
	} {
		assert.Equal(t, want, isPlaceholder(value), value)
	}
}

func Test_S3_MixedCredentials(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()

	// A credential with a placeholder, and other text, is stored whole, as it
	// isn't a placeholder
	provider := credentialProvider{"ID": "AKIDTEST", "TOKEN": "TOKEN"}
	b, err := New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=${ID}&secret-access-key=actual${SUFFIX}&session-token=${TOKEN}"), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)
	assert.Equal(t, "actual${SUFFIX}", provider["S3_KEYS_SECRET_ACCESS_KEY"])
	assert.Equal(t, "${S3_KEYS_SECRET_ACCESS_KEY}", b.url(true).Query().Get("secret-access-key"))

	// A credential which is expanded to nothing isn't used
	_, err = New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=${ID}&secret-access-key=${EMPTY}"), gofiler.Opts{Credentials: credentialProvider{"ID": "AKIDTEST", "EMPTY": ""}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func Test_S3_RotateCredentials(t *testing.T) {
	defer func(refresh time.Duration) { credentialRefresh = refresh }(credentialRefresh)
	credentialRefresh = 0
	endpoint := fakeS3(t)
	ctx := context.Background()

	// The credentials are expanded again, once they expire, so those which
	// are rotated are used by the client
	provider := credentialProvider{"ID": "AKIDTEST", "SECRET": "secret", "TOKEN": "TOKEN"}
	b, err := New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=${ID}&secret-access-key=${SECRET}&session-token=${TOKEN}"), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)
	provider["TOKEN"] = "ROTATED"
	_, err = b.ping(ctx, b.client)
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	provider["TOKEN"] = "TOKEN"
	_, err = b.ping(ctx, b.client)
	assert.NoError(t, err)
}
