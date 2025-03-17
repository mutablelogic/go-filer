package filer

import (
	"context"

	// Packages
	plugin "github.com/mutablelogic/go-filer"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type filer struct {
	aws plugin.AWS
}

var _ server.Task = (*filer)(nil)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, prefix string, router server.HTTPRouter, aws plugin.AWS) (*filer, error) {
	self := new(filer)

	// Check arguments
	if router == nil {
		return nil, httpresponse.ErrInternalError.With("Invalid plugin.HTTPRouter")
	}
	if aws == nil {
		return nil, httpresponse.ErrInternalError.With("Invalid plugin.AWS")
	} else {
		self.aws = aws
	}

	// Register HTTP handlers
	self.RegisterBucketHandlers(ctx, prefix, router)
	self.RegisterObjectHandlers(ctx, prefix, router)

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// TASK

func (*filer) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
