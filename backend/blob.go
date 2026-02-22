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
	bucket *blob.Bucket
	prefix string
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

	// Set the prefix (path within the bucket)
	self.prefix = strings.TrimPrefix(self.url.Path, "/")

	// Open the bucket
	var bucket *blob.Bucket
	var err error

	if self.url.Scheme == "s3" && self.awsConfig != nil {
		// Use the provided AWS config to open S3 bucket directly
		client := s3blob.Dial(*self.awsConfig)
		bucket, err = s3blob.OpenBucket(ctx, client, self.url.Host, nil)
	} else {
		// Use Go CDK's URL opener for all other cases
		bucket, err = blob.OpenBucket(ctx, self.url.String())
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

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// URL returns the URL for the backend
func (b *blobbackend) URL() *url.URL {
	return b.url
}

// Path returns the relative path for a URL, or empty string if not handled.
// The URL must have the same scheme and host as the backend, and the path
// must start with the backend's prefix.
func (b *blobbackend) Path(u *url.URL) string {
	if u == nil || u.Scheme != b.url.Scheme || u.Host != b.url.Host {
		return ""
	}

	// Get the path from the URL, trim leading slash
	path := strings.TrimPrefix(u.Path, "/")

	// If we have a prefix, the path must start with it
	if b.prefix != "" {
		if !strings.HasPrefix(path, b.prefix) {
			return ""
		}
		// Remove the prefix to get the relative path
		path = strings.TrimPrefix(path, b.prefix)
		// Also trim any leading slash after removing prefix
		path = strings.TrimPrefix(path, "/")
	}

	// Return the path (empty string means root of this backend)
	// This is a valid path - it just means the root
	return path
}

// Handles returns true if this backend handles the given URL
func (b *blobbackend) Handles(u *url.URL) bool {
	if u == nil || u.Scheme != b.url.Scheme || u.Host != b.url.Host {
		return false
	}

	// Check prefix if we have one
	if b.prefix != "" {
		path := strings.TrimPrefix(u.Path, "/")
		if !strings.HasPrefix(path, b.prefix) {
			return false
		}
	}

	return true
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

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
