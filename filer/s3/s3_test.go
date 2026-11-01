package s3

import (
	"context"
	"net/url"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_S3_Parse(t *testing.T) {
	// The URL is the bucket, the prefix, which is a directory, and the
	// options which are set, which it's created again from. The name is the
	// bucket's, unless it's set.
	for _, test := range []struct {
		url, want string
		readonly  bool
	}{
		{"s3://photos", "s3://photos/", false},
		{"s3://my-bucket/a?name=", "s3://my-bucket/a/", false},
		{"s3://my-bucket/?name=my-bucket", "s3://my-bucket/", false},
		{"s3://my-bucket/?readonly", "s3://my-bucket/?readonly=true", true},
		{"s3://my.bucket/?name=photos", "s3://my.bucket/?name=photos", false},
		{"s3://192.168.0.1a/?name=photos", "s3://192.168.0.1a/?name=photos", false},
		{"s3://my-xn--bucket", "s3://my-xn--bucket/", false},
		{"s3://my-bucket?name=photos", "s3://my-bucket/?name=photos", false},
		{"s3://my-bucket/?name=photos", "s3://my-bucket/?name=photos", false},
		{"s3://my-bucket/a/b?name=photos", "s3://my-bucket/a/b/?name=photos", false},
		{"s3://my-bucket//a/b//?name=photos", "s3://my-bucket/a/b/?name=photos", false},
		{"s3://my.bucket-1/?name=photos&region=eu-west-1", "s3://my.bucket-1/?name=photos&region=eu-west-1", false},
		{"s3://my-bucket/?name=photos&endpoint=http://localhost:9000", "s3://my-bucket/?endpoint=http%3A%2F%2Flocalhost%3A9000&name=photos", false},
		{"s3://my-bucket/?name=photos&readonly", "s3://my-bucket/?name=photos&readonly=true", true},
		{"s3://my-bucket/?profile=work", "s3://my-bucket/?profile=work", false},
		{"s3://my-bucket/?access-key-id=AKID&secret-access-key=${SECRET}", "s3://my-bucket/?access-key-id=AKID&secret-access-key=%24%7BSECRET%7D", false},
		{"s3://my-bucket/?access-key-id=${ID}&secret-access-key=$SECRET&session-token=${TOKEN}", "s3://my-bucket/?access-key-id=%24%7BID%7D&secret-access-key=%24SECRET&session-token=%24%7BTOKEN%7D", false},
		{"s3://my-bucket/?name=photos&readonly=false", "s3://my-bucket/?name=photos", false},
		{"s3://my-bucket/?name=photos&anonymous=true", "s3://my-bucket/?anonymous=true&name=photos", true},
	} {
		u, err := url.Parse(test.url)
		require.NoError(t, err)
		b, err := parse(u)
		require.NoError(t, err, test.url)
		assert.Equal(t, test.want, b.url(true).String(), test.url)
		assert.Equal(t, test.readonly, b.Readonly(), test.url)
		assert.NoError(t, b.Close())

		// The URL with the credentials creates the same filer again, and the
		// URL doesn't have them
		again, err := parse(b.url(true))
		require.NoError(t, err, test.url)
		assert.Equal(t, b.url(true), again.url(true), test.url)
		for _, key := range []string{"access-key-id", "secret-access-key", "session-token"} {
			assert.False(t, b.URL().Query().Has(key), test.url)
		}
	}
}

func Test_S3_ParseInvalid(t *testing.T) {
	// A URL without a name, when the bucket's isn't an identifier, or
	// without a bucket, or with an invalid option, or another scheme, is
	// invalid
	for _, test := range []string{
		"s3://my.bucket/",
		"s3://1bucket/",
		"s3://1bucket/?name=",
		"s3://my-bucket/?name=1photos",
		"s3:///photos?name=photos",
		"s3://ab/?name=photos",
		"s3://My-Bucket/?name=photos",
		"s3://-bucket/?name=photos",
		"s3://my_bucket/?name=photos",
		"s3://my..bucket/?name=photos",
		"s3://192.168.0.1/?name=photos",
		"s3://xn--bucket/?name=photos",
		"s3://sthree-bucket/?name=photos",
		"s3://amzn-s3-demo-bucket/?name=photos",
		"s3://bucket-s3alias/?name=photos",
		"s3://bucket--ol-s3/?name=photos",
		"s3://bucket.mrap/?name=photos",
		"s3://bucket--x-s3/?name=photos",
		"s3://bucket--table-s3/?name=photos",
		"s3://my-bucket:9000/?name=photos",
		"s3://my-bucket/?name=photos&readonly=maybe",
		"s3://my-bucket/?name=photos&anonymous=maybe",
		"s3://my-bucket/?name=photos&endpoint=localhost:9000",
		"s3://my-bucket/?name=photos&endpoint=ftp://localhost",
		"s3://my-bucket/?name=photos&skip=x",
		"s3://my-bucket/?access-key-id=AKID",
		"s3://my-bucket/?secret-access-key=${SECRET}",
		"s3://my-bucket/?session-token=${TOKEN}",
		"s3://my-bucket/?access-key-id=AKID&secret-access-key=${SECRET}&profile=work",
		"s3://my-bucket/?access-key-id=AKID&secret-access-key=${SECRET}&anonymous",
		"s3://my-bucket/?name=photos&x=%ZZ",
		"file://my-bucket/?name=photos",
	} {
		u, err := url.Parse(test)
		require.NoError(t, err)
		_, err = parse(u)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, test)
	}
	_, err := parse(nil)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func Test_S3_NotImplemented(t *testing.T) {
	// The objects aren't implemented yet
	u, err := url.Parse("s3://my-bucket/?name=photos")
	require.NoError(t, err)
	b, err := parse(u)
	require.NoError(t, err)
	ctx := context.Background()

	_, err = b.CreateObject(ctx, schema.CreateObjectRequest{})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	_, _, err = b.ReadObject(ctx, schema.GetObjectRequest{})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	_, err = b.DeleteObjects(ctx, schema.DeleteObjectsRequest{})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	assert.ErrorIs(t, b.WatchObjects(ctx, make(chan schema.Event)), gofiler.ErrNotImplemented)
}
