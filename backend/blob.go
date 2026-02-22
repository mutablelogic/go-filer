package backend

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	// Packages

	"github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	blob "gocloud.dev/blob"
	s3blob "gocloud.dev/blob/s3blob"
	gcerrors "gocloud.dev/gcerrors"

	//Drivers
	_ "gocloud.dev/blob/fileblob" // file:// URLs
	_ "gocloud.dev/blob/memblob"  // mem:// URLs
	_ "gocloud.dev/blob/s3blob"   // s3:// URLs
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type blobbackend struct {
	*opt
	bucket       *blob.Bucket
	prefix       string // URL path used for matching/stripping in Key()
	bucketPrefix string // key prefix for bucket operations (empty for file://)
}

var _ filer.Filer = (*blobbackend)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewBlobBackend creates a new blob backend using Go CDK.
// Supported URL schemes: s3://, file://, mem://
// Examples:
//   - "s3://my-bucket?region=us-east-1"
//   - "file:///path/to/directory"
//   - "mem://"
//
// For S3 URLs, you can optionally provide an aws.Config via WithAWSConfig()
// for full control over AWS SDK configuration.
func NewBlobBackend(ctx context.Context, u string, opts ...Opt) (*blobbackend, error) {
	self := new(blobbackend)

	// Set the options
	if url, err := url.Parse(u); err != nil {
		return nil, err
	} else if opt, err := apply(url, opts...); err != nil {
		return nil, err
	} else {
		self.opt = opt
	}

	// Store the prefix from the URL path.
	// For s3/mem: prefix is used as a key discriminator in Key() and
	// as a bucket prefix in storageKey() (bucket opens at host level).
	// For file://: path is the bucket root directory, NOT a key discriminator.
	// Key() matches on scheme+host only for file://.
	self.prefix = strings.TrimSuffix(self.url.Path, "/")
	if self.url.Scheme != "file" {
		self.bucketPrefix = strings.TrimPrefix(self.prefix, "/")
	}

	// Open the bucket
	var bucket *blob.Bucket
	var err error

	if self.url.Scheme == "s3" && self.awsConfig != nil {
		// Use the provided AWS config to open S3 bucket directly
		client := s3blob.Dial(*self.awsConfig)
		bucket, err = s3blob.OpenBucket(ctx, client, self.url.Host, nil)
	} else if self.url.Scheme == "file" {
		// For file:// the path is the bucket root dir - open using just the path
		openURL := &url.URL{Scheme: "file", Path: self.url.Path}
		bucket, err = blob.OpenBucket(ctx, openURL.String())
	} else {
		// For s3, mem, etc.: open at root (strip path) to avoid PrefixedBucket
		openURL := *self.url
		openURL.Path = ""
		openURL.RawPath = ""
		bucket, err = blob.OpenBucket(ctx, openURL.String())
	}

	if err != nil {
		return nil, fmt.Errorf("failed to open bucket: %w", err)
	}
	self.bucket = bucket

	return self, nil
}

// Close the backend
func (b *blobbackend) Close() error {
	var result error
	if b.bucket != nil {
		result = errors.Join(result, b.bucket.Close())
		b.bucket = nil
	}

	// Return any errors
	return result
}

// NewFileBackend creates a file-based backend with a logical name.
// This is equivalent to NewBlobBackend(ctx, "file://name/dir").
// The name is used as the host component of the backend URL (file://name/dir),
// while dir specifies the actual filesystem directory for storage.
func NewFileBackend(ctx context.Context, name, dir string, opts ...Opt) (*blobbackend, error) {
	return NewBlobBackend(ctx, "file://"+name+dir, opts...)
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// URL returns the URL for the backend
func (b *blobbackend) URL() *url.URL {
	return b.url
}

// Key returns the storage key for a URL within this backend.
// Returns empty string if the URL is not handled by this backend.
// Returns "/" for the root of the backend.
// For s3/mem: matches on scheme+host+prefix, returns key relative to prefix.
// For file://: matches on scheme+host only, returns the input path directly.
func (b *blobbackend) Key(u *url.URL) string {
	if u == nil || u.Scheme != b.url.Scheme || u.Host != b.url.Host {
		return ""
	}

	// Get the path from the input URL
	path := u.Path
	if path == "" {
		path = "/"
	}

	// For file://: no prefix matching — the path IS the key
	if b.url.Scheme == "file" {
		return path
	}

	// For s3/mem: require prefix in the path and strip it
	if b.prefix != "" {
		if !strings.HasPrefix(path, b.prefix) {
			return ""
		}
		path = strings.TrimPrefix(path, b.prefix)
		if path == "" {
			path = "/"
		}
	}

	return path
}

// Handles returns true if this backend handles the given URL
func (b *blobbackend) Handles(u *url.URL) bool {
	return b.Key(u) != ""
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// storageKey returns the blob storage key for a given key (as returned by Key()).
// It converts the relative key to the actual blob storage key by prepending
// the bucket prefix (for s3/mem where the bucket opens at the host level).
func (b *blobbackend) storageKey(key string) string {
	sk := strings.TrimPrefix(key, "/")
	if b.bucketPrefix != "" {
		if sk == "" {
			return b.bucketPrefix + "/"
		}
		return b.bucketPrefix + "/" + sk
	}
	return sk
}

func (b *blobbackend) attrsToObject(objectURL string, attrs *blob.Attributes) *schema.Object {
	obj := &schema.Object{
		URL:         objectURL,
		Size:        attrs.Size,
		ModTime:     attrs.ModTime,
		ContentType: attrs.ContentType,
	}
	if attrs.ETag != "" {
		obj.ETag = attrs.ETag
	}
	if len(attrs.Metadata) > 0 {
		obj.Meta = attrs.Metadata
	}
	return obj
}

// blobErr wraps a go-cloud blob error with the appropriate httpresponse error
func blobErr(err error, url string) error {
	if err == nil {
		return nil
	}
	switch gcerrors.Code(err) {
	case gcerrors.NotFound:
		return httpresponse.ErrNotFound.Withf("object %q not found", url)
	case gcerrors.PermissionDenied:
		return httpresponse.ErrForbidden.Withf("permission denied for %q", url)
	case gcerrors.InvalidArgument:
		return httpresponse.ErrBadRequest.Withf("invalid argument for %q: %v", url, err)
	case gcerrors.FailedPrecondition:
		return httpresponse.ErrConflict.Withf("precondition failed for %q: %v", url, err)
	default:
		return httpresponse.ErrInternalError.Withf("blob operation failed: %v", err)
	}
}
