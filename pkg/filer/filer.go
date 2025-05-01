package filer

import (
	"context"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	"github.com/mutablelogic/go-filer/pkg/aws"
	handler "github.com/mutablelogic/go-filer/pkg/filer/handler"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	"github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	aws filer.AWS
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, prefix string, router server.HTTPRouter, aws filer.AWS) (*Manager, error) {
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
	opts := []aws.Opt{}
	if meta.Region != nil {
		opts = append(opts, aws.WithRegion(*meta.Region))
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
func (manager *Manager) DeleteBucket(ctx context.Context, name string) (*schema.Bucket, error) {
	bucket, err := manager.aws.GetBucket(ctx, name)
	if err != nil {
		return nil, err
	} else if err := manager.aws.DeleteBucket(ctx, name); err != nil {
		return nil, err
	}

	// Return success
	return schema.BucketFromAWS(bucket), nil
}
