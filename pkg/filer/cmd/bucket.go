package cmd

import (
	// Packages
	"context"
	"fmt"

	client "github.com/mutablelogic/go-filer/pkg/filer/client"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type BucketCommands struct {
	Buckets BucketListCommand `cmd:"" group:"FILER" help:"List buckets"`
}

type BucketListCommand struct {
	schema.BucketListRequest
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *BucketListCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		buckets, err := filer.ListBuckets(ctx, client.WithOffsetLimit(cmd.Offset, cmd.Limit))
		if err != nil {
			return err
		}
		fmt.Println(buckets)
		return nil
	})
}
