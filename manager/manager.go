package manager

import (
	"context"
	"errors"
	"io"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opts
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new filer manager.
func New(ctx context.Context, opts ...Opt) (*Manager, error) {
	self := new(Manager)

	// Apply options
	if opt, err := applyOpts(opts); err != nil {
		return nil, err
	} else {
		self.opts = opt
	}

	// Return success
	return self, nil
}

// Close all backends
func (manager *Manager) Close() error {
	var result error
	for _, backend := range manager.backends {
		if err := backend.Close(); err != nil {
			result = errors.Join(result, err)
		}
	}

	// Return any errors
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Backends returns the list of backend names
func (manager *Manager) Backends() []string {
	result := make([]string, 0, len(manager.backends))
	for _, b := range manager.backends {
		result = append(result, b.Name())
	}
	return result
}

// Key returns the storage key for a named backend and path, or empty string if
// the backend does not exist or the path is not handled (e.g., prefix mismatch).
func (manager *Manager) Key(name, path string) string {
	for _, backend := range manager.backends {
		if backend.Name() == name {
			return backend.Key(path)
		}
	}
	return ""
}

func (manager *Manager) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForName(req.Name)
	if err != nil {
		return nil, err
	}

	// OTEL span
	var result error
	child, endFunc := otel.StartSpan(manager.tracer, ctx, spanManagerName("CreateObject"))
	defer func() { endFunc(result) }()

	// Run the backend
	return backend.CreateObject(child, req)
}

func (manager *Manager) ReadObject(ctx context.Context, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForName(req.Name)
	if err != nil {
		return nil, nil, err
	}

	// OTEL span
	var result error
	child, endFunc := otel.StartSpan(manager.tracer, ctx, spanManagerName("ReadObject"))
	defer func() { endFunc(result) }()

	// Run the backend
	return backend.ReadObject(child, req)
}

func (manager *Manager) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	// Find the right backend
	backend, err := manager.backendForName(req.Name)
	if err != nil {
		return nil, err
	}

	// OTEL span
	var result error
	child, endFunc := otel.StartSpan(manager.tracer, ctx, spanManagerName("ListObjects"))
	defer func() { endFunc(result) }()

	// Run the backend
	return backend.ListObjects(child, req)
}

func (manager *Manager) DeleteObject(ctx context.Context, req schema.DeleteObjectRequest) (*schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForName(req.Name)
	if err != nil {
		return nil, err
	}

	// OTEL span
	var result error
	child, endFunc := otel.StartSpan(manager.tracer, ctx, spanManagerName("DeleteObject"))
	defer func() { endFunc(result) }()

	// Run the backend
	return backend.DeleteObject(child, req)
}

func (manager *Manager) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	// Find the right backend
	backend, err := manager.backendForName(req.Name)
	if err != nil {
		return nil, err
	}

	// OTEL span
	var result error
	child, endFunc := otel.StartSpan(manager.tracer, ctx, spanManagerName("DeleteObjects"))
	defer func() { endFunc(result) }()

	// Run the backend
	return backend.DeleteObjects(child, req)
}

func (manager *Manager) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForName(req.Name)
	if err != nil {
		return nil, err
	}

	// OTEL span
	var result error
	child, endFunc := otel.StartSpan(manager.tracer, ctx, spanManagerName("GetObject"))
	defer func() { endFunc(result) }()

	// Run the backend
	return backend.GetObject(child, req)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) backendForName(name string) (filer.Filer, error) {
	for _, backend := range manager.backends {
		if backend.Name() == name {
			return backend, nil
		}
	}
	return nil, httpresponse.ErrNotFound.Withf("no backend found for name %q", name)
}

func spanManagerName(op string) string {
	return schema.SchemaName + ".manager." + op
}
