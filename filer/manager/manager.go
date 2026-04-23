package manager

import (
	"context"
	"errors"
	"io"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	backend "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	queuemanager "github.com/mutablelogic/go-filer/queue/manager"
	queueschema "github.com/mutablelogic/go-filer/queue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opts
	queue *queuemanager.Manager
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new filer manager.
func New(ctx context.Context, queue *queuemanager.Manager, opts ...Opt) (*Manager, error) {
	self := new(Manager)

	// Set queue manager
	if queue == nil {
		return nil, errors.New("queue manager is required")
	} else {
		self.queue = queue
	}

	// Apply options
	if opt, err := applyOpts(opts); err != nil {
		return nil, err
	} else {
		self.opts = opt
	}

	// Register a queue for indexing tasks. This is used by the manager to schedule background tasks
	// for indexing objects.
	if _, err := self.queue.RegisterQueue(ctx, schema.IndexingQueueName, queueschema.QueueMeta{
		TTL: types.Ptr(schema.IndexingTTL),
	}, self.RunIndexer); err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Tracer returns the tracer used by this manager, or nil if not set.
func (manager *Manager) Tracer() trace.Tracer {
	return manager.tracer
}

// Backends returns the list of backend names
func (manager *Manager) Backends() []string {
	result := make([]string, 0, len(manager.backends))
	for name := range manager.backends {
		result = append(result, name)
	}
	return result
}

// Backend returns the named backend, or nil if it does not exist.
func (manager *Manager) Backend(name string) backend.Backend {
	return manager.backends[name]
}

func (manager *Manager) CreateObject(ctx context.Context, name string, req schema.CreateObjectRequest) (obj *schema.Object, err error) {
	// OTEL span
	child, endSpan := otel.StartSpan(manager.tracer, ctx, "CreateObject",
		attribute.String("name", name),
		attribute.String("request", req.String()),
	)
	defer func() {
		if errors.Is(err, schema.ErrAlreadyExists) {
			endSpan(nil)
		} else {
			endSpan(err)
		}
	}()

	// Find the right backend
	backend, err := manager.backendForName(name)
	if err != nil {
		return nil, err
	} else if obj, err := backend.CreateObject(child, req); err != nil {
		return nil, err
	} else {
		return obj, manager.QueueIndexTask(ctx, types.Value(obj))
	}
}

func (manager *Manager) ReadObject(ctx context.Context, name string, req schema.ReadObjectRequest) (r io.ReadCloser, obj *schema.Object, err error) {
	// OTEL span
	child, endSpan := otel.StartSpan(manager.tracer, ctx, "ReadObject",
		attribute.String("name", name),
		attribute.String("request", req.String()),
	)
	defer func() { endSpan(err) }()

	// Find the right backend
	backend, err := manager.backendForName(name)
	if err != nil {
		return nil, nil, err
	} else {
		return backend.ReadObject(child, req)
	}
}

func (manager *Manager) ListObjects(ctx context.Context, name string, req schema.ListObjectsRequest) (resp *schema.ListObjectsResponse, err error) {
	// OTEL span
	child, endSpan := otel.StartSpan(manager.tracer, ctx, "ListObjects",
		attribute.String("name", name),
		attribute.String("request", req.String()),
	)
	defer func() { endSpan(err) }()

	// Find the right backend
	backend, err := manager.backendForName(name)
	if err != nil {
		return nil, err
	}

	// Clamp Limit to MaxListLimit when set
	if req.Limit > schema.MaxListLimit {
		req.Limit = schema.MaxListLimit
	}

	// Delegate to the backend; it owns Count, Offset, and Limit.
	return backend.ListObjects(child, req)
}

func (manager *Manager) DeleteObject(ctx context.Context, name string, req schema.DeleteObjectRequest) (obj *schema.Object, err error) {
	// OTEL span
	child, endSpan := otel.StartSpan(manager.tracer, ctx, "DeleteObject",
		attribute.String("name", name),
		attribute.String("request", req.String()),
	)
	defer func() { endSpan(err) }()

	// Find the right backend
	backend, err := manager.backendForName(name)
	if err != nil {
		return nil, err
	} else if obj, err := backend.DeleteObject(child, req); err != nil {
		return nil, err
	} else {
		return obj, manager.QueueIndexTask(ctx, types.Value(obj))
	}
}

func (manager *Manager) DeleteObjects(ctx context.Context, name string, req schema.DeleteObjectsRequest) (resp *schema.DeleteObjectsResponse, err error) {
	// OTEL span
	child, endSpan := otel.StartSpan(manager.tracer, ctx, "DeleteObjects",
		attribute.String("name", name),
		attribute.String("request", req.String()),
	)
	defer func() { endSpan(err) }()

	// Find the right backend
	backend, err := manager.backendForName(name)
	if err != nil {
		return nil, err
	} else if resp, err := backend.DeleteObjects(child, req); err != nil {
		return nil, err
	} else {
		return resp, manager.QueueIndexTask(ctx, resp.Body...)
	}
}

func (manager *Manager) GetObject(ctx context.Context, name string, req schema.GetObjectRequest) (obj *schema.Object, err error) {
	// OTEL span
	child, endSpan := otel.StartSpan(manager.tracer, ctx, "GetObject",
		attribute.String("name", name),
		attribute.String("request", req.String()),
	)
	defer func() { endSpan(err) }()

	// Find the right backend
	backend, err := manager.backendForName(name)
	if err != nil {
		return nil, err
	} else {
		return backend.GetObject(child, req)
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) backendForName(name string) (backend.Backend, error) {
	if b, ok := manager.backends[name]; ok {
		return b, nil
	} else {
		return nil, httpresponse.ErrNotFound.Withf("no backend found for name %q", name)
	}
}
