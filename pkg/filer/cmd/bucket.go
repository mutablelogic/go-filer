package cmd

import (
	"context"
	"fmt"

	// Packages
	client "github.com/mutablelogic/go-filer/pkg/filer/client"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type BucketCommands struct {
	Buckets      BucketListCommand   `cmd:"" group:"FILER" help:"List buckets"`
	Bucket       BucketGetCommand    `cmd:"get" group:"FILER" help:"Get bucket"`
	CreateBucket BucketCreateCommand `cmd:"" group:"FILER" help:"Create a new bucket"`
	DeleteBucket BucketDeleteCommand `cmd:"delete" group:"FILER" help:"Delete a bucket"`
}

type BucketListCommand struct {
	schema.BucketListRequest
}

type BucketCreateCommand struct {
	schema.BucketMeta
}

type BucketGetCommand struct {
	Name string `arg:"" name:"name" help:"Name of the bucket"`
}

type BucketDeleteCommand struct {
	BucketGetCommand
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

func (cmd *BucketCreateCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		bucket, err := filer.CreateBucket(ctx, cmd.BucketMeta)
		if err != nil {
			return err
		}
		fmt.Println(bucket)
		return nil
	})
}

func (cmd *BucketGetCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		bucket, err := filer.GetBucket(ctx, cmd.Name)
		if err != nil {
			return err
		}
		fmt.Println(bucket)
		return nil
	})
}

func (cmd *BucketDeleteCommand) Run(app server.Cmd) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		return filer.DeleteBucket(ctx, cmd.Name)
	})
}
