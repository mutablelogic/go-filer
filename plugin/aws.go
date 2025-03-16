package plugin

import (
	"context"

	// Packages
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	server "github.com/mutablelogic/go-server"
)

////////////////////////////////////////////////////////////////////////////////
// INTERFACES

type AWS interface {
	server.Task

	// Return region
	Region() string

	// S3
	S3() *s3.Client
	ListBuckets(context.Context) ([]s3types.Bucket, error)
	CreateBucket(context.Context, string, ...aws.Opt) (*s3types.Bucket, error)
	GetBucket(context.Context, string) (*s3types.Bucket, error)
	DeleteBucket(context.Context, string) error
	ListObjects(context.Context, string, ...aws.Opt) ([]s3types.Object, error)
}
