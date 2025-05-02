package filer

import (
	"net/url"

	"github.com/mutablelogic/go-server/pkg/httpresponse"
	"github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type opt struct {
	s3endpoint    *string
	region        *string
	prefix        *string
	contenttype   *string
	contentlength *int64
	meta          url.Values
}

// Opt represents a function that modifies the options
type Opt func(*opt) error

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// ApplyOpts applies the given options to the opt struct
func ApplyOpts(opts ...Opt) (*opt, error) {
	var o opt

	// Apply the options
	for _, fn := range opts {
		if err := fn(&o); err != nil {
			return nil, err
		}
	}

	// Return success
	return &o, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - GET

func (o *opt) S3Endpoint() *string {
	return o.s3endpoint
}

func (o *opt) Region() string {
	return types.PtrString(o.region)
}

func (o *opt) Prefix() *string {
	return o.prefix
}

func (o *opt) ContentType() *string {
	return o.contenttype
}

func (o *opt) ContentLength() *int64 {
	return o.contentlength
}

func (o *opt) Meta() map[string]string {
	result := make(map[string]string, len(o.meta))
	if o.meta == nil {
		return result
	}
	for k := range o.meta {
		result[k] = o.meta.Get(k)
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - SET

// Set the S3 endpoint URL
func WithS3Endpoint(endpoint string) Opt {
	return func(o *opt) error {
		if endpoint == "" {
			o.s3endpoint = nil
		} else if url, err := url.Parse(endpoint); err != nil {
			return httpresponse.ErrBadRequest.Withf("Invalid S3 endpoint: %s", err)
		} else if url.Scheme != "http" && url.Scheme != "https" {
			return httpresponse.ErrBadRequest.Withf("Invalid S3 endpoint scheme: %q", url.Scheme)
		} else {
			o.s3endpoint = types.StringPtr(url.String())
		}
		return nil
	}
}

// Set the AWS region
func WithRegion(region string) Opt {
	return func(o *opt) error {
		o.region = &region
		return nil
	}
}

// Set the prefix for listing objects
func WithPrefix(v string) Opt {
	return func(o *opt) error {
		o.prefix = &v
		return nil
	}
}

// Apply content type to the PutObject request
func WithContentType(v string) Opt {
	return func(o *opt) error {
		o.contenttype = &v
		return nil
	}
}

// Apply content length to the PutObject request
func WithContentLength(v int64) Opt {
	return func(o *opt) error {
		o.contentlength = &v
		return nil
	}
}

// Apply additional metadata to the PutObject request
func WithMeta(v url.Values) Opt {
	return func(o *opt) error {
		o.meta = v
		return nil
	}
}
