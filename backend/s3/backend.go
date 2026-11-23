package s3

import (
	"context"
	_ "embed"
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	icon "github.com/mutablelogic/go-filer/backend/internal/icon"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// backend is the backend of buckets, of s3:// URLs
type backend struct{}

var _ gofiler.Backend = backend{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed icon.svg
var iconSVG []byte

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Backend returns the backend of S3 buckets, and services which are
// compatible with S3, of URLs of s3://<bucket>/<prefix>
func Backend() gofiler.Backend {
	return backend{}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (backend) Scheme() string      { return schema.SchemeS3 }
func (backend) Description() string { return "S3 bucket" }
func (backend) Icon() gofiler.Icon  { return icon.SVG(iconSVG) }

// Defaults returns the options of a bucket which aren't set: its region,
// profile, endpoint, SeaweedFS filer, which are none, and that it isn't
// anonymous, and is writable
func (backend) Defaults() url.Values {
	o, err := parseOpts("bucket", url.Values{})
	if err != nil {
		return url.Values{}
	}
	values := o.values()
	values.Del(optName)
	values.Del(schema.OptDescription)
	return values
}

// New opens the bucket of s3://<bucket>/<prefix>, which is checked
func (backend) New(ctx context.Context, u *url.URL, _ gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
	return New(ctx, u, opts)
}
