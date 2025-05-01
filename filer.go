package plugin

import (
	"context"
	"io"
	"net/url"

	// Packages
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// INTERFACES

// AWS is the interface for the AWS plugin
type AWS interface {
	// Return region
	Region() string

	// S3
	S3() *s3.Client

	// Buckets
	ListBuckets(context.Context) ([]s3types.Bucket, error)
	CreateBucket(context.Context, string, ...aws.Opt) (*s3types.Bucket, error)
	GetBucket(context.Context, string) (*s3types.Bucket, error)
	DeleteBucket(context.Context, string) error

	// Objects
	ListObjects(context.Context, string, ...aws.Opt) ([]s3types.Object, error)
	GetObjectMeta(context.Context, string, string) (*s3types.Object, url.Values, error)
	GetObject(context.Context, io.Writer, func(url.Values) error, string, string) (*s3types.Object, error)
	DeleteObject(context.Context, string, string) error
	PutObject(context.Context, string, string, io.Reader, ...aws.Opt) (*s3types.Object, error)
}

// Filer is the interface for the Filer plugin
type Filer interface {
	// Buckets
	ListBuckets(context.Context, schema.BucketListRequest) (*schema.BucketList, error)
	CreateBucket(context.Context, schema.BucketMeta) (*schema.Bucket, error)
	GetBucket(context.Context, string) (*schema.Bucket, error)
	DeleteBucket(context.Context, string) (*schema.Bucket, error)
}
