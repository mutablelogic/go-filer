package filer

import (
	"context"
	"io"
	"net/url"

	// Packages
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	feed "github.com/mutablelogic/go-filer/pkg/feed/schema"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// INTERFACES

// AWS is the interface for the AWS plugin
type AWS interface {
	S3 // Implements S3

	// Return region
	Region() string
}

// S3 is the interface for S3 object-storage
type S3 interface {
	// Buckets
	ListBuckets(context.Context) ([]s3types.Bucket, error)
	CreateBucket(context.Context, string, ...Opt) (*s3types.Bucket, error)
	GetBucket(context.Context, string) (*s3types.Bucket, error)
	DeleteBucket(context.Context, string) error

	// Objects
	ListObjects(context.Context, string, ...Opt) ([]s3types.Object, error)
	GetObjectMeta(context.Context, string, string) (*s3types.Object, url.Values, error)
	GetObject(context.Context, io.Writer, func(url.Values) error, string, string) (*s3types.Object, error)
	DeleteObject(context.Context, string, string) error
	DeleteObjects(context.Context, string, func(*s3types.Object) bool, ...Opt) error
	PutObject(context.Context, string, string, io.Reader, ...Opt) (*s3types.Object, error)
	WriteObject(context.Context, io.Writer, string, string, ...Opt) (int64, error)
}

// Filer is the higher-level interface for the Filer plugin
type Filer interface {
	// Buckets
	ListBuckets(context.Context, schema.BucketListRequest) (*schema.BucketList, error)
	CreateBucket(context.Context, schema.BucketMeta) (*schema.Bucket, error)
	GetBucket(context.Context, string) (*schema.Bucket, error)
	DeleteBucket(context.Context, string, ...Opt) (*schema.Bucket, error)

	// Objects
	ListObjects(context.Context, string, schema.ObjectListRequest) (*schema.ObjectList, error)
	PutObject(context.Context, string, string, io.Reader, ...Opt) (*schema.Object, error)
	DeleteObject(context.Context, string, string) (*schema.Object, error)
	GetObject(context.Context, string, string) (*schema.Object, error)
	WriteObject(context.Context, io.Writer, string, string, ...Opt) (int64, error)

	// Media
	CreateMedia(context.Context, string, string, schema.MediaMeta) (*schema.Media, error)
	CreateMediaFragments(context.Context, string, string, []schema.MediaFragmentMeta) (*schema.MediaFragmentList, error)
}

// Feed is the higher-level interface for Feeds
type Feed interface {
	// Urls
	CreateUrl(context.Context, feed.UrlMeta) (*feed.Url, error)
	ListUrls(context.Context, feed.UrlListRequest) (*feed.UrlList, error)
	GetUrl(context.Context, uint64) (*feed.Url, error)
	DeleteUrl(context.Context, uint64) (*feed.Url, error)
}
