package filer

import (
	"context"
	"errors"
	"io"
	"strings"

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

	// Queue - optional
	if queue == nil {
		return self, nil
	} else {
		self.queue = queue
		self.conn = queue.Conn().With(
			"schema", schema.SchemaName,
			"pgqueue_schema", queue_schema.SchemaName,
			"queue_registerobject", task.TaskNameRegisterObject,
		).(pg.PoolConn)
	}

	// Create the schema
	if exists, err := pg.SchemaExists(ctx, self.conn, schema.SchemaName); err != nil {
		return nil, err
	} else if !exists {
		if err := pg.SchemaCreate(ctx, self.conn, schema.SchemaName); err != nil {
			return nil, err
		}
	}

	// Bootstrap the schema
	if err := self.conn.Tx(ctx, func(conn pg.Conn) error {
		return schema.Bootstrap(ctx, conn)
	}); err != nil {
		return nil, err
	}

	// Task runner
	_, err := task.NewTaskRunner(ctx, self, self.queue)
	if err != nil {
		return nil, err
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

	// Insert the object into the database and create a task
	if manager.queue != nil {
		if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
			// Insert the object into the database
			if err := conn.With("queue", task.TaskNameRegisterObject).Insert(ctx, object_, object_); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, errors.Join(err, manager.aws.DeleteObject(ctx, bucket, key))
		}
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

	// Delete object from the database
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Delete the object from the database
		if err := manager.conn.Delete(ctx, nil, schema.ObjectKey{Bucket: bucket, Key: types.PtrString(object.Key)}); err != nil {
			return err
		}
		// Delete object
		if err := manager.aws.DeleteObject(ctx, bucket, key); err != nil {
			return err
		}

		// Success
		return nil
	}); err != nil {
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
// PUBLIC METHODS - MEDIA

func (manager *Manager) CreateMedia(ctx context.Context, bucket, key string, meta schema.MediaMeta) (*schema.Media, error) {
	var media schema.Media

	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Insert the media into the database
		if err := manager.conn.With("bucket", bucket, "key", key).Insert(ctx, &media, meta); err != nil {
			return err
		}

		// Delete existing image mapppings
		if err := conn.Delete(ctx, nil, schema.MediaImageMap{
			Bucket: media.Bucket,
			Key:    media.Key,
		}); err != nil {
			return err
		}

		// Insert the images into the database
		for _, image := range meta.Images {
			if err := conn.Insert(ctx, nil, image); err != nil {
				return err
			}

			// Insert the mappings
			if err := conn.Insert(ctx, nil, schema.MediaImageMap{
				Bucket: media.Bucket,
				Key:    media.Key,
				Hash:   image.Hash,
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Return success
	return &media, nil
}

func (manager *Manager) CreateMediaFragments(ctx context.Context, bucket, key string, meta []schema.MediaFragmentMeta) (*schema.MediaFragmentList, error) {
	var typ string
	var list schema.MediaFragmentList

	// We need at least one fragment
	if len(meta) == 0 {
		return nil, httpresponse.ErrBadRequest.With("missing media fragment metadata")
	}

	// All types need to be the same
	for i, meta := range meta {
		if i == 0 {
			typ = meta.Type
		} else if meta.Type != typ {
			return nil, httpresponse.ErrBadRequest.With("media fragment types do not match")
		}
	}

	// Replace fragments in the database
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Delete fragments into the database
		if err := manager.conn.With("bucket", bucket, "key", key).Delete(ctx, nil, schema.MediaFragmentMeta{
			Type: typ,
		}); err != nil {
			return err
		}

		// Insert the fragments into the database
		for _, meta := range meta {
			var fragment schema.MediaFragment
			if err := manager.conn.With("bucket", bucket, "key", key).Insert(ctx, &fragment, meta); err != nil {
				return err
			} else {
				list = append(list, fragment)
			}
		}

		// Return success
		return nil
	}); err != nil {
		return nil, err
	}

	// Return success
	return &list, nil
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
