package aws

import (
	"context"

	// Packages
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	server "github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type awstask struct {
	*aws.Client
}

var _ server.Task = (*awstask)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func taskWithClient(client *aws.Client) *awstask {
	return &awstask{client}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (*awstask) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
