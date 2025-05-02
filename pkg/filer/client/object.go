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

func (c *Client) ListObjects(ctx context.Context, bucket string, opts ...Opt) (*schema.ObjectList, error) {
	// Make request
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("object", bucket), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) GetObject(ctx context.Context, bucket, key string) (*schema.Object, error) {
	// Make request
	req := client.NewRequestEx(http.MethodHead, "")

	// Perform request
	var response schema.Object
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("object", bucket, key)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	// Make request
	req := client.NewRequestEx(http.MethodDelete, "")

	// Perform request
	return c.DoWithContext(ctx, req, nil, client.OptPath("object", bucket, key))
}

// Create objects from a file or directory
func (c *Client) CreateObjects(ctx context.Context, bucket string, path []string, opts ...Opt) (*schema.ObjectList, error) {
	// Make the uploader request
	uploader := NewUploader(opts...)

	// Add paths
	for _, path := range path {
		if err := uploader.Add("file", path); err != nil {
			return nil, err
		}
	}

	// Indicate ready to upload
	if err := uploader.Close(); err != nil {
		return nil, err
	}

	// Perform request - with no timeout
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, uploader, &response, client.OptPath("object", bucket), client.OptNoTimeout()); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}
