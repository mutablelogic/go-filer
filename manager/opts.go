package manager

import (
	"context"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
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
func WithBackend(ctx context.Context, url string, backendOpts ...backend.Opt) Opt {
	return func(o *opts) error {
		b, err := backend.NewBlobBackend(ctx, url, backendOpts...)
		if err != nil {
			return err
		}
		o.backends = append(o.backends, b)
		return nil
	}
}

// WithFileBackend adds a file backend with a logical name.
// The name is used as the backend identifier (file://name),
// while dir specifies the actual filesystem directory for storage.
func WithFileBackend(name, dir string, backendOpts ...backend.Opt) Opt {
	return func(o *opts) error {
		b, err := backend.NewFileBackend(context.Background(), name, dir, backendOpts...)
		if err != nil {
			return err
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
