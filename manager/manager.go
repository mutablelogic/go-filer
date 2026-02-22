package manager

import (
	"context"
	"errors"
	"io"
	"net/url"

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

// Backends returns the list of backend URLs
func (manager *Manager) Backends() []string {
	result := make([]string, 0, len(manager.backends))
	for _, b := range manager.backends {
		result = append(result, b.URL().String())
	}
	return result
}

// Key returns the storage key for a URL if any backend handles it, or empty string otherwise.
func (manager *Manager) Key(url *url.URL) string {
	for _, backend := range manager.backends {
		if key := backend.Key(url); key != "" {
			return key
		}
	}
	return ""
}

func (manager *Manager) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForURL(req.URL)
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
	backend, err := manager.backendForURL(req.URL)
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
	backend, err := manager.backendForURL(req.URL)
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
	backend, err := manager.backendForURL(req.URL)
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

func (manager *Manager) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	// Find the right backend
	backend, err := manager.backendForURL(req.URL)
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

func (manager *Manager) backendForURL(urlStr string) (filer.Filer, error) {
	url, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	for _, backend := range manager.backends {
		if backend.Handles(url) {
			return backend, nil
		}
	}
	return nil, httpresponse.ErrNotFound.Withf("no backend found for URL: %q", urlStr)
}

func spanManagerName(op string) string {
	return schema.SchemaName + ".manager." + op
}
