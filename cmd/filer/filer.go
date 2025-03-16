package main

import (
	"fmt"

	"github.com/mutablelogic/go-filer/pkg/filer/client"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type FilerCommands struct {
	Buckets      ListBucketsCommand  `cmd:"" group:"BUCKETS" help:"List buckets"`
	Bucket       GetBucketCommand    `cmd:"" group:"BUCKETS" help:"Get bucket"`
	BucketCreate BucketCreateCommand `cmd:"" group:"BUCKETS" help:"Create a new bucket"`
	BucketDelete DeleteBucketCommand `cmd:"" group:"BUCKETS" help:"Delete bucket"`
	Objects      ListObjectsCommand  `cmd:"" group:"OBJECTS" help:"List objects"`
}

type ListBucketsCommand struct {
}

type ListObjectsCommand struct {
	GetBucketCommand
	Prefix *string `name:"prefix" help:"Prefix for the object key"`
}

type GetBucketCommand struct {
	Name string `arg:"" help:"Name of the bucket"`
}

type DeleteBucketCommand struct {
	GetBucketCommand
}

type BucketCreateCommand struct {
	GetBucketCommand
	Region *string `name:"region" help:"Region of the bucket"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *ListBucketsCommand) Run(app App) error {
	buckets, err := app.GetClient().ListBuckets(app.Context())
	if err != nil {
		return err
	}
	fmt.Println(buckets)
	return nil
}

func (cmd *ListObjectsCommand) Run(app App) error {
	buckets, err := app.GetClient().ListObjects(app.Context(), cmd.Name, client.WithPrefix(cmd.Prefix))
	if err != nil {
		return err
	}
	fmt.Println(buckets)
	return nil
}
