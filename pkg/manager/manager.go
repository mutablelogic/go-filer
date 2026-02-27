package manager

import (
	"context"
	"errors"
	"io"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	backend "github.com/mutablelogic/go-filer/pkg/backend"
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
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Backends returns the list of backend names
func (manager *Manager) Backends() []string {
	result := make([]string, 0, len(manager.backends))
	for name := range manager.backends {
		result = append(result, name)
	}
	return result
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
	obj, result := backend.CreateObject(child, req)
	return obj, result
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
	r, obj, result := backend.ReadObject(child, req)
	return r, obj, result
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

	// Clamp Limit to MaxListLimit when set
	if req.Limit > schema.MaxListLimit {
		req.Limit = schema.MaxListLimit
	}

	// Delegate to the backend; it owns Count, Offset, and Limit.
	resp, result := backend.ListObjects(child, req)
	return resp, result
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
	obj, result := backend.DeleteObject(child, req)
	return obj, result
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
	resp, result := backend.DeleteObjects(child, req)
	return resp, result
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
	obj, result := backend.GetObject(child, req)
	return obj, result
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) backendForName(name string) (backend.Backend, error) {
	if b, ok := manager.backends[name]; ok {
		return b, nil
	}
	return nil, httpresponse.ErrNotFound.Withf("no backend found for name %q", name)
}

func spanManagerName(op string) string {
	return schema.SchemaName + ".manager." + op
}
