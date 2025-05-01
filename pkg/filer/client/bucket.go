package client

import (
	"context"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) CreateBucket(ctx context.Context, bucket schema.BucketMeta) (*schema.Bucket, error) {
	// Make request
	req, err := client.NewJSONRequest(bucket)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Bucket
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("bucket")); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) ListBuckets(ctx context.Context, opts ...Opt) (*schema.BucketList, error) {
	// Make request
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.BucketList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("bucket"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) DeleteBucket(ctx context.Context, bucket string) error {
	// Make request
	req := client.NewRequestEx(http.MethodDelete, "")

	// Perform request
	if err := c.DoWithContext(ctx, req, nil, client.OptPath("bucket", bucket)); err != nil {
		return err
	}

	// Return the responses
	return nil
}

func (c *Client) GetBucket(ctx context.Context, bucket string) (*schema.Bucket, error) {
	// Make request
	req := client.NewRequest()

	// Perform request
	var response schema.Bucket
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("bucket", bucket)); err != nil {
		return nil, err
	}

	// Return the response
	return &response, nil
}
