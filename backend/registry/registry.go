package registry

import (
	"context"
	"net/url"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	file "github.com/mutablelogic/go-filer/backend/file"
	google "github.com/mutablelogic/go-filer/backend/google"
	s3 "github.com/mutablelogic/go-filer/backend/s3"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Registry struct {
	sync.RWMutex
	tracer    trace.Tracer
	decryptfn backend.DecryptCredentailFunc
	backends  map[string]backend.Backend
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new registry with no backends.
func New(tracer trace.Tracer, decryptfn backend.DecryptCredentailFunc) *Registry {
	return &Registry{
		tracer:    tracer,
		decryptfn: decryptfn,
		backends:  make(map[string]backend.Backend),
	}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Validate checks that the provided URL is valid for a supported backend type, and returns the unique name
// for that backend if valid.
func (r *Registry) Validate(ctx context.Context, url *url.URL) (string, error) {
	// Add cases for different backend types here
	switch url.Scheme {
	case "file":
		f, err := file.New(ctx, r.tracer, r.decryptfn, url)
		if err != nil {
			return "", err
		}
		defer f.Close()
		return f.Name(), nil
	case "s3":
		f, err := s3.New(ctx, r.tracer, r.decryptfn, url)
		if err != nil {
			return "", err
		}
		defer f.Close()
		return f.Name(), nil
	case "googledrive":
		f, err := google.NewDriveBackend(ctx, r.tracer, r.decryptfn, url)
		if err != nil {
			return "", err
		}
		defer f.Close()
		return f.Name(), nil
	default:
		return "", gofiler.ErrBadParameter.Withf("unsupported backend scheme: %q", url.Scheme)
	}
}

// Names returns a list of all backend names in the registry.
func (r *Registry) Names() []string {
	r.RLock()
	defer r.RUnlock()

	names := make([]string, 0, len(r.backends))
	for name := range r.backends {
		names = append(names, name)
	}
	return names
}

// Get returns the backend with the specified name, or an error if it does not exist.
func (r *Registry) Get(name string) backend.Backend {
	r.RLock()
	defer r.RUnlock()
	if backend, ok := r.backends[name]; ok {
		return backend
	} else {
		return nil
	}
}

// New creates a new backend based on the provided URL, and adds it to the registry.
// The URL must be valid for the backend type, and the backend name must be unique within the registry.
func (r *Registry) New(ctx context.Context, path string) (backend.Backend, error) {
	parsedURL, err := url.Parse(path)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid URL: %v", err)
	}
	if parsedURL.Scheme == "" {
		return nil, gofiler.ErrBadParameter.With("url with scheme is required")
	}

	// Lock the registry while we validate and add the backend, to ensure that the backend name is unique.
	r.Lock()
	defer r.Unlock()

	// Add cases for different backend types here
	switch parsedURL.Scheme {
	case "file":
		backend, err := file.New(ctx, r.tracer, r.decryptfn, parsedURL)
		if err != nil {
			return nil, err
		}

		// Check for unique name
		name := backend.Name()
		if _, ok := r.backends[name]; ok {
			return nil, gofiler.ErrConflict.Withf("backend with name %q already exists", name)
		} else {
			r.backends[name] = backend
		}

		// Return success
		return backend, nil
	case "s3":
		backend, err := s3.New(ctx, r.tracer, r.decryptfn, parsedURL)
		if err != nil {
			return nil, err
		}

		// Check for unique name
		name := backend.Name()
		if _, ok := r.backends[name]; ok {
			return nil, gofiler.ErrConflict.Withf("backend with name %q already exists", name)
		} else {
			r.backends[name] = backend
		}

		// Return success
		return backend, nil
	case "googledrive":
		backend, err := google.NewDriveBackend(ctx, r.tracer, r.decryptfn, parsedURL)
		if err != nil {
			return nil, err
		}

		// Check for unique name
		name := backend.Name()
		if _, ok := r.backends[name]; ok {
			return nil, gofiler.ErrConflict.Withf("backend with name %q already exists", name)
		} else {
			r.backends[name] = backend
		}

		// Return success
		return backend, nil
	default:
		return nil, gofiler.ErrBadParameter.Withf("unsupported backend scheme: %q", parsedURL.Scheme)
	}
}

func (r *Registry) Delete(name string) error {
	r.Lock()
	defer r.Unlock()

	defer delete(r.backends, name)
	if backend, ok := r.backends[name]; !ok {
		return gofiler.ErrNotFound.Withf("backend with name %q does not exist", name)
	} else if err := backend.Close(); err != nil {
		return err
	}

	// Return success
	return nil
}
