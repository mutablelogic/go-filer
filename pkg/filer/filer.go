package filer

import (
	"context"

	// Packages
	filer "github.com/mutablelogic/go-filer"
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

func (manager *Manager) ListBuckets(ctx context.Context, req schema.BucketListRequest) (*schema.BucketList, error) {
	var resp schema.BucketList

	// Set limit, allocate body
	var limit uint64
	if req.Limit != nil {
		limit = min(types.PtrUint64(req.Limit), schema.BucketListLimit)
	}
	resp.Body = make([]*schema.Bucket, 0, limit)

	// Get buckets
	buckets, err := manager.aws.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	// Page the results
	resp.Count = uint64(len(buckets))
	for i := req.Offset; i < resp.Count; i++ {
		resp.Body = append(resp.Body, schema.BucketFromAWS(&buckets[i]))
	}

	// Return success
	return &resp, nil
}

func (self *Manager) CreateBucket(context.Context, schema.BucketMeta) (*schema.Bucket, error) {
	return nil, httpresponse.ErrNotImplemented.With("CreateBucket")
}

func (self *Manager) GetBucket(context.Context, string) (*schema.Bucket, error) {
	return nil, httpresponse.ErrNotImplemented.With("GetBucket")
}

func (self *Manager) DeleteBucket(context.Context, string) error {
	return httpresponse.ErrNotImplemented.With("DeleteBucket")
}
