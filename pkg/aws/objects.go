package aws

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"

	// Packages
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListObjects lists all objects in an S3 bucket. Adding WithPrefix() to the
// options will limit the objects to those with a key that starts with the
// specified prefix.
// TODO: up to the specified limit.
func (aws *Client) ListObjects(ctx context.Context, bucket string, opts ...Opt) ([]s3types.Object, error) {
	var result []s3types.Object

	// Parse options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Iterate through the objects
	if err := listObjects(ctx, aws.S3(), bucket, opt.prefix, func(objects []s3types.Object) error {
		result = append(result, objects...)
		return nil
	}); err != nil {
		return nil, err
	}

	// Return the list of objects
	return result, nil
}

// GetObject returns the object metadata with the specified key in the
// specified bucket, and the metadata. The object is not downloaded.
func (aws *Client) GetObject(ctx context.Context, bucket, key string) (*s3types.Object, url.Values, error) {
	// Get the object metadata
	result, err := aws.S3().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: types.StringPtr(bucket),
		Key:    types.StringPtr(key),
	})
	if err != nil {
		return nil, nil, Err(err)
	}

	// Convert the metadata to a url.Values
	metadata := make(url.Values)
	for k, v := range result.Metadata {
		metadata.Set(k, v)
	}

	// Return the object metadata
	return &s3types.Object{
		Key:          types.StringPtr(key),
		ETag:         result.ETag,
		LastModified: result.LastModified,
		Size:         result.ContentLength,
	}, metadata, nil
}

// DeleteObject deletes the object with the specified key in the specified
// bucket.
func (aws *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := aws.S3().DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: types.StringPtr(bucket),
		Key:    types.StringPtr(key),
	})
	if err != nil {
		return Err(err)
	}

	// Return success
	return nil
}

// CreateObject creates an object in the specified bucket with the specified
// key. The object is created from the specified reader. Content type, length
// and additional metadata can be specified in the options.
func (aws *Client) CreateObject(ctx context.Context, bucket, key string, r io.Reader, opts ...Opt) (*s3types.Object, error) {
	// Parse options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Insert the object into S3
	response, err := aws.S3().PutObject(ctx, &s3.PutObjectInput{
		Bucket:             types.StringPtr(bucket),
		Key:                types.StringPtr(key),
		Body:               r,
		ContentType:        opt.contentType,
		ContentDisposition: types.StringPtr(fmt.Sprintf("inline; filename=%q", filepath.Base(key))),
		ContentLength:      opt.contentLength,
		Metadata:           opt.metadata,
	}, s3.WithAPIOptions(
		v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
	))
	if err != nil {
		return nil, Err(err)
	}

	// Return the object
	return &s3types.Object{
		Key:  types.StringPtr(key),
		ETag: response.ETag,
		Size: response.Size,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func listObjects(ctx context.Context, client *s3.Client, bucket string, prefix *string, fn func(objects []s3types.Object) error) error {
	var token *string
	for {
		// List objects
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            types.StringPtr(bucket),
			Prefix:            prefix,
			ContinuationToken: token,
		})
		if err != nil {
			return Err(err)
		}

		// Return objects
		if err := fn(objects.Contents); err != nil {
			return err
		}

		// Check if there are more objects to list
		if objects.NextContinuationToken == nil {
			break
		} else {
			token = objects.NextContinuationToken
		}
	}

	// Return success
	return nil
}
