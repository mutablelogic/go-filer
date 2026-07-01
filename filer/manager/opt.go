package manager

import (
	// Packages

	client "github.com/mutablelogic/go-client"
	credential "github.com/mutablelogic/go-filer/credential/manager"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metric "go.opentelemetry.io/otel/metric"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option for filer manager configuration.
type Opt func(*opt) error

type opt struct {
	tracer         trace.Tracer
	metrics        metric.Meter
	schema         string
	indexer        bool
	credentialopts []credential.Opt
	clientopts     []client.ClientOpt
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
	o.credentialopts = []credential.Opt{}
	o.clientopts = []client.ClientOpt{}
}

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithLLMClientOpts sets options for the LLM provider registry's HTTP clients.
func WithLLMClientOpts(opts ...client.ClientOpt) Opt {
	return func(o *opt) error {
		o.clientopts = opts
		return nil
	}
}

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
		o.credentialopts = append(o.credentialopts, credential.WithTracer(tracer))
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

// WithSchema sets the database schema used for storing filer objects.
func WithSchema(schema string) Opt {
	return func(o *opt) error {
		o.schema = schema
		o.credentialopts = append(o.credentialopts, credential.WithSchema(schema))
		return nil
	}
}

// WithPassphrase registers an in-memory storage passphrase for a certificate
// passphrase version. Versions are uint64 and passphrases must be non-empty.
func WithPassphrase(version uint64, passphrase string) Opt {
	return func(o *opt) error {
		o.credentialopts = append(o.credentialopts, credential.WithPassphrase(version, passphrase))
		return nil
	}
}
