package filer

import (
	"context"
	"io"
	"strings"
	"time"

	// Packages
	pg "github.com/djthorpe/go-pg"
	filer "github.com/mutablelogic/go-filer"
	handler "github.com/mutablelogic/go-filer/pkg/filer/handler"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	task "github.com/mutablelogic/go-filer/pkg/filer/task"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	queue_schema "github.com/mutablelogic/go-server/pkg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	aws   filer.AWS
	conn  pg.PoolConn
	queue server.PGQueue
}

var _ filer.Filer = (*Manager)(nil)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, prefix string, router server.HTTPRouter, aws filer.AWS, queue server.PGQueue) (*Manager, error) {
	self := new(Manager)

	// AWS
	if aws == nil {
		return nil, httpresponse.ErrInternalError.With("Invalid filer.AWS")
	} else {
		self.aws = aws
	}

	// Router
	if router == nil {
		return nil, httpresponse.ErrInternalError.With("Invalid server.HTTPRouter")
	} else {
		handler.RegisterHandlers(ctx, prefix, router, self)
	}

	// Task runner
	taskrunner, err := task.NewTaskRunner()
	if err != nil {
		return nil, err
	}

	// Queue - optional
	if queue != nil {
		self.queue = queue
		self.conn = queue.Conn()

		// Register a queue for objects
		if _, err := self.queue.RegisterQueue(ctx, queue_schema.QueueMeta{
			Queue:      task.TaskNameRegisterObject,
			TTL:        types.DurationPtr(time.Hour),
			Retries:    types.Uint64Ptr(3),
			RetryDelay: types.DurationPtr(time.Minute),
		}, func(ctx context.Context, in any) error {
			var object schema.Object
			if err := self.queue.UnmarshalPayload(&object, in); err != nil {
				return err
			}
			return taskrunner.RegisterObject(ctx, &object)
		}); err != nil {
			return nil, err
		}
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - BUCKETS

// ListBuckets returns a list of buckets, with optional offset and limit
func (manager *Manager) ListBuckets(ctx context.Context, req schema.BucketListRequest) (*schema.BucketList, error) {
	var resp schema.BucketList

	// Set limit, allocate body
	limit := uint64(schema.BucketListLimit)
	if req.Limit != nil {
		limit = min(types.PtrUint64(req.Limit), schema.BucketListLimit)
	}
	resp.Body = make([]*schema.Bucket, 0, limit)

	// Get buckets
	buckets, err := manager.aws.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	// Adjust limit
	limit = min(limit, uint64(len(buckets)))

	// Page the results
	resp.Count = uint64(len(buckets))
	for i := req.Offset; i < resp.Count; i++ {
		if uint64(len(resp.Body)) >= limit {
			break
		}
		resp.Body = append(resp.Body, schema.BucketFromAWS(&buckets[i]))
	}

	// Return success
	return &resp, nil
}

// CreateBucket creates a new bucket with the specified metadata
func (manager *Manager) CreateBucket(ctx context.Context, meta schema.BucketMeta) (*schema.Bucket, error) {
	opts := []filer.Opt{}
	if meta.Region != nil {
		opts = append(opts, filer.WithRegion(*meta.Region))
	}
	bucket, err := manager.aws.CreateBucket(ctx, meta.Name, opts...)
	if err != nil {
		return nil, err
	}

	// Return success
	return schema.BucketFromAWS(bucket), nil
}

// GetBucket returns the bucket metadata for the specified bucket name
func (manager *Manager) GetBucket(ctx context.Context, name string) (*schema.Bucket, error) {
	bucket, err := manager.aws.GetBucket(ctx, name)
	if err != nil {
		return nil, err
	}

	// Return success
	return schema.BucketFromAWS(bucket), nil
}

// DeleteBucket deletes the specified bucket and returns it
func (manager *Manager) DeleteBucket(ctx context.Context, name string, opt ...filer.Opt) (*schema.Bucket, error) {
	opts, err := filer.ApplyOpts(opt...)
	if err != nil {
		return nil, err
	}

	bucket, err := manager.aws.GetBucket(ctx, name)
	if err != nil {
		return nil, err
	}

	// If force is set then delete all objects in the bucket
	if opts.Force() {
		if err := manager.aws.DeleteObjects(ctx, name, nil); err != nil {
			return nil, err
		}
	}

	// Delete the bucket
	if err := manager.aws.DeleteBucket(ctx, name); err != nil {
		return nil, err
	}

	// Return success
	return schema.BucketFromAWS(bucket), nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - OBJECT

// PutObject creates or updates an object in the specified bucket with the specified
// key. The object is created from the specified reader.
func (manager *Manager) PutObject(ctx context.Context, bucket, key string, r io.Reader, opt ...filer.Opt) (*schema.Object, error) {
	key, err := normalizeKey(key)
	if err != nil {
		return nil, err
	}

	// Put the object
	if _, err := manager.aws.PutObject(ctx, bucket, key, r, opt...); err != nil {
		return nil, err
	}

	// Retrieve the object with metadata
	object, meta, err := manager.aws.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	// Translate the object to the schema
	object_ := schema.ObjectFromAWS(object, bucket, meta)

	// Enqueue the object for processing
	if manager.queue != nil {
		manager.queue.CreateTask(ctx, task.TaskNameRegisterObject, object_, 0)
	}

	// Return success
	return object_, nil
}

func (manager *Manager) ListObjects(ctx context.Context, bucket string, req schema.ObjectListRequest) (*schema.ObjectList, error) {
	var opts []filer.Opt
	var resp schema.ObjectList

	// Normalize prefix
	if req.Prefix != nil {
		if prefix, err := normalizeKey(types.PtrString(req.Prefix)); err != nil {
			return nil, err
		} else {
			opts = append(opts, filer.WithPrefix(prefix))
		}
	}

	// Enumerate objects
	objects, err := manager.aws.ListObjects(ctx, bucket, opts...)
	if err != nil {
		return nil, err
	}

	// Allocate body
	resp.Body = make([]*schema.Object, 0, len(objects))
	for _, object := range objects {
		resp.Body = append(resp.Body, schema.ObjectFromAWS(&object, bucket, nil))
	}

	// Return success
	return &resp, nil
}

func (manager *Manager) GetObject(ctx context.Context, bucket, key string) (*schema.Object, error) {
	key, err := normalizeKey(key)
	if err != nil {
		return nil, err
	}

	// Get object
	object, meta, err := manager.aws.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	// Return success
	return schema.ObjectFromAWS(object, bucket, meta), nil
}

func (manager *Manager) DeleteObject(ctx context.Context, bucket, key string) (*schema.Object, error) {
	key, err := normalizeKey(key)
	if err != nil {
		return nil, err
	}

	// Get object
	object, meta, err := manager.aws.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	// Delete object
	if err := manager.aws.DeleteObject(ctx, bucket, key); err != nil {
		return nil, err
	}

	// Return success
	return schema.ObjectFromAWS(object, bucket, meta), nil
}

func (manager *Manager) WriteObject(ctx context.Context, w io.Writer, bucket, key string, opt ...filer.Opt) (int64, error) {
	key, err := normalizeKey(key)
	if err != nil {
		return -1, err
	}

	// Write the object
	return manager.aws.WriteObject(ctx, w, bucket, key, opt...)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func normalizeKey(key string) (string, error) {
	if strings.HasPrefix(key, schema.PathSeparator) {
		key = key[1:]
	}
	if strings.HasSuffix(key, schema.PathSeparator) {
		return key, httpresponse.ErrBadRequest.With("object key cannot end with a separator")
	}
	return key, nil
}
