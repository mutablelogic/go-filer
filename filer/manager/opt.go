package manager

import (
	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metric "go.opentelemetry.io/otel/metric"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option for filer manager configuration.
type Opt func(*opt) error

type opt struct {
	tracer  trace.Tracer
	metrics metric.Meter
	schema  string
	indexer bool
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
	o.schema = schema.DefaultSchema
	o.indexer = false
}

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithIndexer uses this instance as an indexer of content
func WithIndexer(indexer bool) Opt {
	return func(o *opt) error {
		o.indexer = indexer
		return nil
	}
}

// WithTracer sets the tracer used for tracing operations.
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.tracer = tracer
		return nil
	}
}

// WithMeter sets the OpenTelemetry meter used for manager metrics.
func WithMeter(meter metric.Meter) Opt {
	return func(o *opt) error {
		o.metrics = meter
		return nil
	}
}
