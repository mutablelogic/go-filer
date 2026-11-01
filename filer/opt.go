package filer

import (
	// Packages
	client "github.com/mutablelogic/go-client"
	metric "go.opentelemetry.io/otel/metric"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option of the registry
type Opt func(*opt) error

type opt struct {
	tracer     trace.Tracer
	meter      metric.Meter
	clientopts []client.ClientOpt
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

// WithClientOpts sets the client options of the backends which call other
// services over HTTP
func WithClientOpts(clientopts ...client.ClientOpt) Opt {
	return func(o *opt) error {
		o.clientopts = clientopts
		return nil
	}
}
