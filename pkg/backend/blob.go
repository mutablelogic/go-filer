package backend

import (
	"context"
	"errors"
	"io"
	"fmt"
	"net/url"
	"path"
	"strings"
	"syscall"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	blob "gocloud.dev/blob"
	s3blob "gocloud.dev/blob/s3blob"
	gcerrors "gocloud.dev/gcerrors"

	// Drivers
	_ "gocloud.dev/blob/fileblob" // file:// URLs
	_ "gocloud.dev/blob/memblob"  // mem:// URLs
	_ "gocloud.dev/blob/s3blob"   // s3:// URLs
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type blobbackend struct {
	*opt
	bucket       *blob.Bucket
	bucketPrefix string // key prefix for bucket operations (empty for file://)
}

var _ Backend = (*blobbackend)(nil)

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
func NewBlobBackend(ctx context.Context, u string, opts ...Opt) (Backend, error) {
	self := new(blobbackend)

	// Set the options
	if url, err := url.Parse(u); err != nil {
		return nil, err
	} else if opt, err := apply(url, opts...); err != nil {
		return nil, err
	} else {
		self.opt = opt
	}

	// Validate the backend name (URL host) is a valid identifier
	if !types.IsIdentifier(self.url.Host) {
		return nil, fmt.Errorf("backend name %q must be a valid identifier (letter, digits, underscores, hyphens; max 64 chars)", self.url.Host)
	}
	// For s3/mem: bucketPrefix is prepended to paths to form storage keys
	// (bucket opens at host level). For file://: no prefix needed.
	if self.url.Scheme != "file" {
		self.bucketPrefix = strings.TrimPrefix(strings.TrimSuffix(self.url.Path, "/"), "/")
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
// name must be a valid identifier (see types.IsIdentifier): starts with a
// letter, contains only letters, digits, underscores, or hyphens, max 64 chars.
// dir must be an absolute path; if it doesn't start with "/" an error is returned.
func NewFileBackend(ctx context.Context, name, dir string, opts ...Opt) (Backend, error) {
	if !path.IsAbs(dir) {
		return nil, fmt.Errorf("backend dir %q must be an absolute path", dir)
	}
	return NewBlobBackend(ctx, "file://"+name+path.Clean(dir), opts...)
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Name returns the name of the backend (the host component of the URL)
func (b *blobbackend) Name() string {
	return b.url.Host
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// key returns the blob storage key for a given request path.
// Cleans the path, strips the leading slash, and prepends the bucket prefix.
func (b *blobbackend) key(p string) string {
	sk := strings.TrimPrefix(cleanPath(p), "/")
	if b.bucketPrefix != "" {
		if sk == "" {
			return b.bucketPrefix + "/"
		}
		return b.bucketPrefix + "/" + sk
	}
	return sk
}

// cleanPath normalises a request path for use as Object.Path.
func cleanPath(p string) string {
	if p == "" {
		p = "/"
	}
	return path.Clean(p)
}

func (b *blobbackend) attrsToObject(objPath string, attrs *blob.Attributes) *schema.Object {
	obj := &schema.Object{
		Path:        objPath,
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

// pathFromStorageKey converts a blob storage key back to a logical path
// by stripping the bucket prefix (for s3/mem with bucket prefix).
func (b *blobbackend) pathFromStorageKey(sk string) string {
	if b.bucketPrefix != "" {
		sk = strings.TrimPrefix(sk, b.bucketPrefix+"/")
	}
	if !strings.HasPrefix(sk, "/") {
		sk = "/" + sk
	}
	return path.Clean(sk)
}

// isRealObject checks whether the storage key refers to a single real object
// (as opposed to a phantom directory — a size-0 pseudo-object with children).
// Returns the object's attributes if real, nil otherwise.
func (b *blobbackend) isRealObject(ctx context.Context, sk string) *blob.Attributes {
	if sk == "" || strings.HasSuffix(sk, "/") {
		return nil
	}
	attrs, err := b.bucket.Attributes(ctx, sk)
	if err != nil {
		return nil
	}
	if attrs.Size > 0 {
		return attrs
	}
	// Size is 0 — check if there are objects with this key as a prefix.
	// If children exist, this is a phantom directory.
	iter := b.bucket.List(&blob.ListOptions{Prefix: sk + "/"})
	if _, err := iter.Next(ctx); err == io.EOF {
		return attrs // no children → real (empty) object
	}
	return nil // has children → phantom directory
}

// blobErr wraps a go-cloud blob error with the appropriate httpresponse error
func blobErr(err error, ref string) error {
	if err == nil {
		return nil
	}
	// Check for OS-level errors before go-cloud classification, since the
	// gcerrors default path wraps with %v and breaks the chain.
	if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EEXIST) {
		return httpresponse.ErrBadRequest.Withf("cannot overwrite directory with file: %q", ref)
	}
	switch gcerrors.Code(err) {
	case gcerrors.NotFound:
		return httpresponse.ErrNotFound.Withf("object %q not found", ref)
	case gcerrors.PermissionDenied:
		return httpresponse.ErrForbidden.Withf("permission denied for %q", ref)
	case gcerrors.InvalidArgument:
		return httpresponse.ErrBadRequest.Withf("invalid argument for %q: %v", ref, err)
	case gcerrors.FailedPrecondition:
		return httpresponse.ErrConflict.Withf("precondition failed for %q: %v", ref, err)
	default:
		return httpresponse.ErrInternalError.Withf("blob operation failed: %v", err)
	}
}
