package opt

import (
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	types "github.com/mutablelogic/go-server/pkg/types"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Opts interface {
	Scheme() string
	Host() string
}

type opt struct {
	url    url.URL
	tracer trace.Tracer
}

type Opt func(*opt) error

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func Apply(u *url.URL, opts ...Opt) (Opts, error) {
	o := new(opt)
	o.url = types.Value(u)

	for _, fn := range opts {
		if err := fn(o); err != nil {
			return nil, err
		}
	}

	return o, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (o *opt) Scheme() string {
	return o.url.Scheme
}

func (o *opt) Host() string {
	return o.url.Host
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - Opts

func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.tracer = tracer
		return nil
	}
}

// WithEndpoint sets the S3 endpoint for S3-compatible services.
// For http:// endpoints, HTTPS is automatically disabled.
func WithEndpoint(endpoint string) Opt {
	return func(o *opt) error {
		// Ignore empty endpoints
		if endpoint == "" {
			return nil
		}
		// Set endpoint parameter
		if endpoint, err := url.Parse(endpoint); err != nil {
			return err
		} else if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
			return gofiler.ErrBadParameter.Withf("endpoint must be http:// or https://, got %s://", endpoint.Scheme)
		} else {
			o.set("endpoint", endpoint.String())
			o.set("s3ForcePathStyle", "true") // Always set s3ForcePathStyle=true for custom endpoints
			if endpoint.Scheme == "http" {
				o.set("disable_https", "true")
			}
		}
		return nil
	}
}

// WithAnonymous forces use of anonymous credentials.
// Use this for S3-compatible services that don't require authentication.
func WithAnonymous() Opt {
	return func(o *opt) error {
		o.set("anonymous", "true")
		return nil
	}
}

// WithCreateDir sets create_dir=true for file:// URLs to create the directory if it doesn't exist
func WithCreateDir() Opt {
	return func(o *opt) error {
		o.set("create_dir", "true")
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (o *opt) set(key, value string) {
	q := o.url.Query()
	if value == "" {
		q.Del(key)
	} else {
		q.Set(key, value)
	}
	o.url.RawQuery = q.Encode()
}
