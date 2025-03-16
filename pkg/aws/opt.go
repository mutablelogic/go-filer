package aws

import "net/url"

////////////////////////////////////////////////////////////////////////////////
// TYPES

type opt struct {
	s3endpoint    *string
	region        *string
	prefix        *string
	contentType   *string
	contentLength *int64
	metadata      map[string]string
}

// Opt represents a function that modifies the options
type Opt func(*opt) error

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func applyOpts(opts ...Opt) (*opt, error) {
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
// PUBLIC METHODS

func WithS3Endpoint(endpoint string) Opt {
	return func(o *opt) error {
		o.s3endpoint = &endpoint
		return nil
	}
}

func WithRegion(region string) Opt {
	return func(o *opt) error {
		o.region = &region
		return nil
	}
}

func WithPrefix(v string) Opt {
	return func(o *opt) error {
		o.prefix = &v
		return nil
	}
}

func WithContentType(v string) Opt {
	return func(o *opt) error {
		o.contentType = &v
		return nil
	}
}
func WithContentLength(v int64) Opt {
	return func(o *opt) error {
		o.contentLength = &v
		return nil
	}
}

func WithMeta(v url.Values) Opt {
	return func(o *opt) error {
		o.metadata = make(map[string]string, len(v))
		for k := range v {
			o.metadata[k] = v.Get(k)
		}
		return nil
	}
}
