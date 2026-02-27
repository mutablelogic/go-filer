package manager

import (
	"context"
	"errors"
	"io"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
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

func (manager *Manager) CreateObject(ctx context.Context, name string, req schema.CreateObjectRequest) (*schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForName(name)
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

func (manager *Manager) ReadObject(ctx context.Context, name string, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForName(name)
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

func (manager *Manager) ListObjects(ctx context.Context, name string, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	// Find the right backend
	backend, err := manager.backendForName(name)
	if err != nil {
		return nil, err
	}

	// OTEL span
	var result error
	child, endFunc := otel.StartSpan(manager.tracer, ctx, spanManagerName("ListObjects"))
	defer func() { endFunc(result) }()

	// Run the backend (always returns the full set; pagination is applied here)
	resp, err := backend.ListObjects(child, req)
	if err != nil {
		return nil, err
	}

	// Record total count before slicing
	resp.Count = len(resp.Body)

	// Limit==0 means count-only: return the total with no body
	if req.Limit == 0 {
		resp.Body = nil
		return resp, nil
	}

	// Apply offset
	offset := req.Offset
	if offset > resp.Count {
		offset = resp.Count
	}
	resp.Body = resp.Body[offset:]

	// Clamp limit to MaxListLimit and apply
	limit := req.Limit
	if limit > schema.MaxListLimit {
		limit = schema.MaxListLimit
	}
	if limit < len(resp.Body) {
		resp.Body = resp.Body[:limit]
	}

	return resp, nil
}

func (manager *Manager) DeleteObject(ctx context.Context, name string, req schema.DeleteObjectRequest) (*schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForName(name)
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

func (manager *Manager) DeleteObjects(ctx context.Context, name string, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	// Find the right backend
	backend, err := manager.backendForName(name)
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

func (manager *Manager) GetObject(ctx context.Context, name string, req schema.GetObjectRequest) (*schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForName(name)
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
