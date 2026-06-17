package manager

import (
	"context"
	"errors"
	"fmt"

	// Packages
	backend "github.com/mutablelogic/go-filer/backend"
	blob "github.com/mutablelogic/go-filer/backend/blob"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option for filer manager configuration.
type Opt func(*opt) error

type opt struct {
	tracer   trace.Tracer
	schema   string
	backends map[string]backend.Backend
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
	o.backends = make(map[string]backend.Backend)
	o.schema = schema.DefaultSchema
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

// WithBackend adds a blob backend (mem://, file://, s3://) to the filer.
// The url should be in the format "scheme://bucket" (e.g., "mem://mybucket", "s3://mybucket").
// Returns an error if a backend with the same name already exists.
func WithBackend(ctx context.Context, url string, blobOpts ...blob.Opt) Opt {
	return func(o *opt) error {
		// Thread the tracer down into the backend so S3 SDK calls are instrumented
		// only when OTel is actually configured.
		if o.tracer != nil {
			blobOpts = append(blobOpts, blob.WithTracer(o.tracer))
		}
		b, err := blob.New(ctx, url, blobOpts...)
		if err != nil {
			return err
		}
		if _, exists := o.backends[b.Name()]; exists {
			return errors.Join(fmt.Errorf("backend with name %q already registered", b.Name()), b.Close())
		} else {
			o.backends[b.Name()] = b
		}
		return nil
	}
}

// WithFileBackend is a convenience option that adds a file:// backend.
// name must be a valid identifier; dir must be an absolute path.
func WithFileBackend(ctx context.Context, name, dir string, blobOpts ...blob.Opt) Opt {
	return func(o *opt) error {
		if o.tracer != nil {
			blobOpts = append(blobOpts, blob.WithTracer(o.tracer))
		}
		b, err := blob.NewFileBackend(ctx, name, dir, blobOpts...)
		if err != nil {
			return err
		}
		if _, exists := o.backends[b.Name()]; exists {
			_ = b.Close()
			return fmt.Errorf("backend with name %q already registered", b.Name())
		}
		o.backends[b.Name()] = b
		return nil
	}
}
