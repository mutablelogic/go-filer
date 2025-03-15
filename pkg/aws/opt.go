package aws

////////////////////////////////////////////////////////////////////////////////
// TYPES

type opt struct {
	s3endpoint *string
	region     *string
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
