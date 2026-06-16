package registry

import (
	"net/url"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	file "github.com/mutablelogic/go-filer/backend/file"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Registry struct {
	sync.RWMutex
	backends map[string]backend.Backend
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Validate checks that the provided URL is valid for a supported backend type, and returns the unique name for that backend if valid.
func (*Registry) Validate(url *url.URL) (string, error) {
	// Add cases for different backend types here
	switch url.Scheme {
	case "file":
		return file.Validate(url)
	default:
		return "", gofiler.ErrBadParameter.Withf("unsupported backend scheme: %q", url.Scheme)
	}
}

// New creates a new registry with no backends.
func New() *Registry {
	return &Registry{
		backends: make(map[string]backend.Backend),
	}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

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

// New creates a new backend based on the provided URL, and adds it to the registry.
//
//	The URL must be valid for the backend type, and the backend name must be unique within the registry.
func (r *Registry) New(path string) (backend.Backend, error) {
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
		backend, err := file.New(parsedURL)
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

	if _, ok := r.backends[name]; !ok {
		return gofiler.ErrNotFound.Withf("backend with name %q does not exist", name)
	}

	delete(r.backends, name)
	return nil
}
