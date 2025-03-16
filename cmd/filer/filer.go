package main

import (
	"context"
	"fmt"
	"time"

	// Packages

	"github.com/mutablelogic/go-filer/pkg/filer/client"
	"github.com/mutablelogic/go-filer/pkg/filer/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type FilerCommands struct {
	Buckets      ListBucketsCommand   `cmd:"" group:"BUCKETS" help:"List buckets"`
	Bucket       GetBucketCommand     `cmd:"" group:"BUCKETS" help:"Get bucket"`
	BucketCreate CreateBucketCommand  `cmd:"" group:"BUCKETS" help:"Create a new bucket"`
	BucketDelete DeleteBucketCommand  `cmd:"" group:"BUCKETS" help:"Delete bucket"`
	Objects      ListObjectsCommand   `cmd:"" group:"OBJECTS" help:"List objects"`
	Upload       UploadObjectsCommand `cmd:"" group:"OBJECTS" help:"Upload files as objects to a bucket"`
	Object       GetObjectCommand     `cmd:"" group:"OBJECTS" help:"Get object"`
	ObjectDelete DeleteObjectCommand  `cmd:"" group:"OBJECTS" help:"Delete object"`
}

type ListBucketsCommand struct {
}

type ListObjectsCommand struct {
	GetBucketCommand
	Prefix *string `name:"prefix" help:"Prefix for the object key"`
}

type GetBucketCommand struct {
	Bucket string `arg:"" help:"Bucket name"`
}

type GetObjectCommand struct {
	GetBucketCommand
	Key string `arg:"" help:"Object key"`
}

type DeleteObjectCommand struct {
	GetObjectCommand
}

type DeleteBucketCommand struct {
	GetBucketCommand
}

type CreateBucketCommand struct {
	GetBucketCommand
	Region *string `name:"region" help:"Region of the bucket"`
}

type UploadObjectsCommand struct {
	GetBucketCommand
	Path string `arg:"" type:"path" help:"File or path of files to upload"`
}

// /////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *CreateBucketCommand) Run(app App) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		bucket, err := filer.CreateBucket(ctx, schema.BucketMeta{
			Name: cmd.Bucket,
		})
		if err != nil {
			return err
		}
		fmt.Println(bucket)
		return nil
	})
}

func (cmd *GetBucketCommand) Run(app App) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		bucket, err := filer.GetBucket(ctx, cmd.Bucket)
		if err != nil {
			return err
		}
		fmt.Println(bucket)
		return nil
	})
}

func (cmd *ListBucketsCommand) Run(app App) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		buckets, err := filer.ListBuckets(ctx)
		if err != nil {
			return err
		}
		fmt.Println(buckets)
		return nil
	})
}

func (cmd *ListObjectsCommand) Run(app App) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		buckets, err := filer.ListObjects(ctx, cmd.Bucket, client.WithPrefix(cmd.Prefix))
		if err != nil {
			return err
		}
		fmt.Println(buckets)
		return nil
	})
}

func (cmd *GetObjectCommand) Run(app App) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		object, err := filer.GetObject(ctx, cmd.Bucket, cmd.Key)
		if err != nil {
			return err
		}
		fmt.Println(object)
		return nil
	})
}

func (cmd *DeleteObjectCommand) Run(app App) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		return filer.DeleteObject(ctx, cmd.Bucket, cmd.Key)
	})
}

func (cmd *DeleteBucketCommand) Run(app App) error {
	return run(app, func(ctx context.Context, filer *client.Client) error {
		return filer.DeleteBucket(ctx, cmd.Bucket)
	})
}

func (cmd *UploadObjectsCommand) Run(app App) error {
	now := time.Now()
	return run(app, func(ctx context.Context, filer *client.Client) error {
		response, err := filer.CreateObjects(ctx, cmd.Bucket, cmd.Path, client.WithProgress(func(cur, total uint64) {
			if time.Since(now) > time.Second {
				fmt.Printf("Uploaded %v/%v bytes (%.1f%%)\r", cur, total, float64(cur)/float64(total)*100)
				now = time.Now()
			}
		}))
		if err != nil {
			return err
		}

		if len(response.Body) == 0 {
			fmt.Print("No objects created")
		} else {
			fmt.Println(response.Body)
		}
		if response.Count > 0 {
			fmt.Println("Total objects created:", response.Count)
		} else {
			fmt.Println("")
		}
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func run(app App, fn func(context.Context, *client.Client) error) error {
	// Create a client
	provider, err := client.New(app.GetEndpoint("${FILERPREFIX}").String(), app.GetClientOpts()...)
	if err != nil {
		return err
	}
	// Run the function
	return fn(app.Context(), provider)
}
