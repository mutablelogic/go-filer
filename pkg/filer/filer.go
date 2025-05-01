package filer

import (
	"context"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	handler "github.com/mutablelogic/go-filer/pkg/filer/handler"
	"github.com/mutablelogic/go-filer/pkg/filer/schema"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
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

	// Check arguments
	if aws == nil {
		return nil, httpresponse.ErrInternalError.With("Invalid filer.AWS")
	} else {
		self.aws = aws
	}
	if router == nil {
		return nil, httpresponse.ErrInternalError.With("Invalid plugin.HTTPRouter")
	} else {
		// Register HTTP handlers
		handler.RegisterHandlers(ctx, prefix, router, self.aws)
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - BUCKETS

func (self *Manager) ListBuckets(ctx context.Context, req schema.BucketListRequest) (*schema.BucketList, error) {
	return nil, httpresponse.ErrNotImplemented.With("ListBuckets")
}
