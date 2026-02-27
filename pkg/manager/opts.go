package manager

import (
	"context"
	"fmt"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/pkg/backend"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option for filer manager configuration.
type Opt func(*opts) error

type opts struct {
	tracer   trace.Tracer
	backends []filer.Filer
}

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithTracer sets the tracer used for tracing operations.
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opts) error {
		o.tracer = tracer
		return nil
	}
}

// WithBackend adds a blob backend (mem://, file://, s3://) to the filer.
// The url should be in the format "scheme://bucket" (e.g., "mem://mybucket", "s3://mybucket").
// Returns an error if a backend with the same name already exists.
func WithBackend(ctx context.Context, url string, backendOpts ...backend.Opt) Opt {
	return func(o *opts) error {
		b, err := backend.NewBlobBackend(ctx, url, backendOpts...)
		if err != nil {
			return err
		}
		for _, existing := range o.backends {
			if existing.Name() == b.Name() {
				return fmt.Errorf("backend with name %q already registered", b.Name())
			}
		}
		o.backends = append(o.backends, b)
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func applyOpts(opt []Opt) (opts, error) {
	// Set defaults
	o := opts{}

	// Apply options
	for _, fn := range opt {
		if err := fn(&o); err != nil {
			return opts{}, err
		}
	}

	// Return success
	return o, nil
}
