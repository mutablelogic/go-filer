package aws

import (
	"context"

	// Packages
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

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func listObjects(ctx context.Context, client *s3.Client, bucket string, prefix *string, fn func(objects []s3types.Object) error) error {
	var token *string
	for {
		// List objects
		objects, err := client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket: types.StringPtr(bucket),
			Prefix: prefix,
			Marker: token,
		})
		if err != nil {
			return Err(err)
		}

		// Return objects
		if err := fn(objects.Contents); err != nil {
			return err
		}

		// Check if there are more objects to list
		if objects.NextMarker == nil {
			break
		} else {
			token = objects.NextMarker
		}
	}

	// Return success
	return nil
}
