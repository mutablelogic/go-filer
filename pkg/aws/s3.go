package aws

import (
	"context"
	"errors"
	"io"

	// Packages
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

// ListBuckets lists all S3 buckets in the account
// TODO: up to the specified limit.
func ListBuckets(ctx context.Context, client *s3.Client) ([]s3types.Bucket, error) {
	if client == nil {
		return nil, httpresponse.ErrInternalError.Withf("S3 client is nil")
	}

	var result []s3types.Bucket
	if err := listBuckets(ctx, client, func(buckets []s3types.Bucket) error {
		result = append(result, buckets...)
		return nil
	}); err != nil {
		return nil, err
	}

	// Return the list of buckets
	return result, nil
}

// CreateBucket creates a new S3 bucket
func CreateBucket(ctx context.Context, client *s3.Client, name string, opt ...Opt) (*s3types.Bucket, error) {
	if client == nil {
		return nil, httpresponse.ErrInternalError.Withf("S3 client is nil")
	}

	opts, err := applyOpts(opt...)
	if err != nil {
		return nil, httpresponse.ErrBadRequest.With(err.Error())
	}

	// The name must be an identifier
	if !types.IsIdentifier(name) {
		return nil, httpresponse.ErrBadRequest.Withf("Invalid bucket name: %q", name)
	}

	// Create the bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: types.StringPtr(name),
		CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(types.PtrString(opts.region)),
		},
	})
	if err != nil {
		return nil, Err(err)
	}

	// Return the bucket
	return GetBucket(ctx, client, name)
}

// GetBucket returns an S3 bucket
func GetBucket(ctx context.Context, client *s3.Client, name string) (*s3types.Bucket, error) {
	if client == nil {
		return nil, httpresponse.ErrInternalError.Withf("S3 client is nil")
	}

	// The name must be an identifier
	if !types.IsIdentifier(name) {
		return nil, httpresponse.ErrBadRequest.Withf("Invalid bucket name: %q", name)
	}

	// Match the bucket by name
	var result s3types.Bucket
	if err := listBuckets(ctx, client, func(buckets []s3types.Bucket) error {
		for _, bucket := range buckets {
			if *bucket.Name == name {
				result = bucket
				return io.EOF
			}
		}
		return nil
	}); errors.Is(err, io.EOF) {
		// We found the bucket
		return &result, nil
	} else if err != nil {
		// An error occurred
		return nil, err
	}

	// No bucket found
	return nil, httpresponse.ErrNotFound.Withf("Bucket %q not found", name)
}

// DeleteBucket returns an S3 bucket
func DeleteBucket(ctx context.Context, client *s3.Client, name string) error {
	if client == nil {
		return httpresponse.ErrInternalError.Withf("S3 client is nil")
	}

	// The name must be an identifier
	if !types.IsIdentifier(name) {
		return httpresponse.ErrBadRequest.Withf("Invalid bucket name: %q", name)
	}

	// Perform the delete
	_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: types.StringPtr(name),
	})

	// Return any errors
	return Err(err)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func listBuckets(ctx context.Context, client *s3.Client, fn func(buckets []s3types.Bucket) error) error {
	var token *string
	for {
		// List buckets
		buckets, err := client.ListBuckets(ctx, &s3.ListBucketsInput{
			ContinuationToken: token,
		})
		if err != nil {
			return Err(err)
		}

		// Return buckets
		if err := fn(buckets.Buckets); err != nil {
			return err
		}

		// Check if there are more buckets to list
		if buckets.ContinuationToken == nil {
			break
		} else {
			token = buckets.ContinuationToken
		}

	}

	// Return success
	return nil
}
