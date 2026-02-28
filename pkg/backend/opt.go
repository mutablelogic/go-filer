package backend

import (
	"fmt"
	"net/url"

	// Packages
	"github.com/aws/aws-sdk-go-v2/aws"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type opt struct {
	url          *url.URL
	awsConfig    *aws.Config
	endpoint     string // raw endpoint URL set via WithEndpoint; wired into awsConfig when both are present
	gcsCredsFile string // path to GCS service-account JSON key file; empty = use ADC
}

type Opt func(*opt) error

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func apply(url *url.URL, opts ...Opt) (*opt, error) {
	// Apply options
	o := opt{url: url}
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	// Return success
	return &o, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithEndpoint sets the S3 endpoint for S3-compatible services.
// For http:// endpoints, HTTPS is automatically disabled.
func WithEndpoint(endpoint string) Opt {
	return func(o *opt) error {
		// Set endpoint parameter
		if endpoint, err := url.Parse(endpoint); err != nil {
			return err
		} else if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
			return fmt.Errorf("endpoint must be http:// or https://, got %s://", endpoint.Scheme)
		} else {
			o.endpoint = endpoint.String() // stored for use with awsConfig path
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

// WithAWSConfig provides an AWS SDK v2 Config directly.
// When provided for s3:// URLs, this config is used instead of the URL-based configuration.
// This allows full control over AWS configuration including custom credentials providers,
// HTTP clients, retry settings, etc.
func WithAWSConfig(cfg aws.Config) Opt {
	return func(o *opt) error {
		o.awsConfig = &cfg
		return nil
	}
}

// WithGCSCredentialsFile sets an explicit service-account JSON key file for gs:// URLs.
// When omitted, Application Default Credentials (ADC) are used — i.e. the
// GOOGLE_APPLICATION_CREDENTIALS environment variable, gcloud auth, or the GCE metadata
// server, in that order.
func WithGCSCredentialsFile(path string) Opt {
	return func(o *opt) error {
		o.gcsCredsFile = path
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (o *opt) set(key, value string) {
	if o.url == nil {
		return
	}
	q := o.url.Query()
	if value == "" {
		q.Del(key)
	} else {
		q.Set(key, value)
	}
	o.url.RawQuery = q.Encode()
}
