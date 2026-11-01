package filer

import (
	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	metric "go.opentelemetry.io/otel/metric"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option of the registry
type Opt func(*opt) error

type opt struct {
	tracer      trace.Tracer
	meter       metric.Meter
	clientopts  []client.ClientOpt
	credentials gofiler.CredentialProvider
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (o *opt) apply(opt []Opt) error {
	o.defaults()

	// Apply options
	for _, fn := range opt {
		if err := fn(o); err != nil {
			return err
		}
	}

	// Return success
	return nil
}

func (o *opt) defaults() {
	// TODO
}

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithTracer sets the tracer used for tracing operations.
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.tracer = tracer
		return nil
	}
}

// WithMeter sets the OpenTelemetry meter used for metrics.
func WithMeter(meter metric.Meter) Opt {
	return func(o *opt) error {
		o.meter = meter
		return nil
	}
}

// WithCredentials sets the provider of the credentials of the backends,
// which expands the placeholders of the credentials in their URLs, such as
// ${S3_SECRET}, so the URLs don't have the credentials
func WithCredentials(credentials gofiler.CredentialProvider) Opt {
	return func(o *opt) error {
		o.credentials = credentials
		return nil
	}
}

// WithClientOpts sets the client options of the backends which call other
// services over HTTP
func WithClientOpts(clientopts ...client.ClientOpt) Opt {
	return func(o *opt) error {
		o.clientopts = clientopts
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// backendOpts returns the options of a backend, which are the registry's
// tracer, and the provider of credentials
func (o *opt) backendOpts() gofiler.Opts {
	return gofiler.Opts{Tracer: o.tracer, Credentials: o.credentials}
}
