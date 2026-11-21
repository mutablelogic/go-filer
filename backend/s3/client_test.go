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

func (p credentialProvider) Delete(_ context.Context, key string) error {
	delete(p, key)
	return nil
}

func Test_S3_Credentials(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()
	provider := credentialProvider{"ID": "AKIDTEST", "SECRET": "secret", "TOKEN": "TOKEN"}
	opts := "&access-key-credential=ID&secret-key-credential=SECRET&session-token-credential=TOKEN"

	// The credentials are of their names, which the provider looks up, and
	// which the URL has, but never the credentials
	b, err := New(ctx, bucketURL(t, endpoint, "keys", opts), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)
	assert.Equal(t, "SECRET", b.URL().Query().Get("secret-key-credential"))
	assert.Equal(t, b.URL(), b.URLWithCredentials())
	assert.NotContains(t, b.URL().String(), "AKIDTEST")
	assert.NotContains(t, b.URL().String(), "=secret")

	// Without a provider, or with a name it doesn't have, they can't be
	// looked up, and other credentials can't access the bucket
	_, err = New(ctx, bucketURL(t, endpoint, "keys", opts), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = New(ctx, bucketURL(t, endpoint, "keys", opts), gofiler.Opts{Credentials: credentialProvider{"ID": "AKIDTEST"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = New(ctx, bucketURL(t, endpoint, "keys", "&access-key-credential=OTHER&secret-key-credential=SECRET&session-token-credential=TOKEN"), gofiler.Opts{Credentials: credentialProvider{"OTHER": "AKIDOTHER", "SECRET": "secret", "TOKEN": "TOKEN"}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = New(ctx, bucketURL(t, endpoint, "keys", ""), gofiler.Opts{Credentials: provider})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)

	// A credential which is empty isn't used
	_, err = New(ctx, bucketURL(t, endpoint, "keys", "&access-key-credential=ID&secret-key-credential=EMPTY"), gofiler.Opts{Credentials: credentialProvider{"ID": "AKIDTEST", "EMPTY": ""}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func Test_S3_EarlierCredentials(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()
	provider := credentialProvider{"ID": "AKIDTEST", "SECRET": "secret", "TOKEN": "TOKEN"}

	// A stored volume's URL, of an earlier version, whose options are
	// placeholders of the credentials' names, is of their names, which its
	// URL has, of their options
	b, err := New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=${ID}&secret-access-key=$SECRET&session-token=${TOKEN}"), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)
	query := b.URL().Query()
	assert.Equal(t, "ID", query.Get("access-key-credential"))
	assert.Equal(t, "SECRET", query.Get("secret-key-credential"))
	assert.Equal(t, "TOKEN", query.Get("session-token-credential"))
	assert.False(t, query.Has("access-key-id"))

	// Its options of the credentials themselves aren't, so they're never in
	// a URL
	_, err = New(ctx, bucketURL(t, endpoint, "keys", "&access-key-id=AKIDTEST&secret-access-key=secret"), gofiler.Opts{Credentials: provider})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
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

func Test_S3_Description(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()

	// A bucket's description is of the bucket, its prefix, and its
	// endpoint, when it isn't set, and is its option otherwise
	b, err := New(ctx, bucketURL(t, endpoint, "photos/2026", ""), gofiler.Opts{})
	require.NoError(t, err)
	assert.Equal(t, "S3 bucket photos/2026 at "+strings.TrimPrefix(endpoint, "http://"), b.URL().Query().Get("description"))
	described, err := New(ctx, bucketURL(t, endpoint, "photos", "&description=Holidays"), gofiler.Opts{})
	require.NoError(t, err)
	assert.Equal(t, "Holidays", described.URL().Query().Get("description"))
	clone, err := described.Clone(ctx, url.Values{"readonly": {"true"}})
	require.NoError(t, err)
	assert.Equal(t, "Holidays", clone.URL().Query().Get("description"), "a clone keeps it")

	// Of a bucket of AWS, it's of its region
	aws := &S3{opt: opt{region: "eu-west-1"}, bucket: "photos"}
	assert.Equal(t, "S3 bucket photos in eu-west-1", aws.describe())
}

func Test_S3_Clone(t *testing.T) {
	endpoint := fakeS3(t)
	ctx := context.Background()
	provider := credentialProvider{"ID": "AKIDTEST", "SECRET": "secret", "TOKEN": "TOKEN", "OTHER": "secret"}
	b, err := New(ctx, bucketURL(t, endpoint, "keys", "&access-key-credential=ID&secret-key-credential=SECRET&session-token-credential=TOKEN"), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)

	// The clone has the credentials' names, with the options changed, and
	// the filer isn't changed
	clone, err := b.Clone(ctx, url.Values{"readonly": {"true"}})
	require.NoError(t, err)
	assert.True(t, clone.(*S3).Readonly())
	assert.False(t, b.Readonly())
	assert.Equal(t, "SECRET", clone.URL().Query().Get("secret-key-credential"))

	// A patch can change their names, of their options, or an earlier
	// version's, or remove them, when the bucket can't be accessed
	clone, err = b.Clone(ctx, url.Values{"secret-key-credential": {"OTHER"}})
	require.NoError(t, err)
	assert.Equal(t, "OTHER", clone.URL().Query().Get("secret-key-credential"))
	clone, err = b.Clone(ctx, url.Values{"secret-access-key": {"${OTHER}"}})
	require.NoError(t, err)
	assert.Equal(t, "OTHER", clone.URL().Query().Get("secret-key-credential"))
	_, err = b.Clone(ctx, url.Values{"secret-access-key": {"secret"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = b.Clone(ctx, url.Values{"access-key-credential": nil, "secret-key-credential": nil, "session-token-credential": nil})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
}

func Test_S3_RotateCredentials(t *testing.T) {
	defer func(refresh time.Duration) { credentialRefresh = refresh }(credentialRefresh)
	credentialRefresh = 0
	endpoint := fakeS3(t)
	ctx := context.Background()

	// The credentials are looked up again, once they expire, so those which
	// are rotated are used by the client
	provider := credentialProvider{"ID": "AKIDTEST", "SECRET": "secret", "TOKEN": "TOKEN"}
	b, err := New(ctx, bucketURL(t, endpoint, "keys", "&access-key-credential=ID&secret-key-credential=SECRET&session-token-credential=TOKEN"), gofiler.Opts{Credentials: provider})
	require.NoError(t, err)
	provider["TOKEN"] = "ROTATED"
	_, err = b.ping(ctx, b.client)
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	provider["TOKEN"] = "TOKEN"
	_, err = b.ping(ctx, b.client)
	assert.NoError(t, err)
}
