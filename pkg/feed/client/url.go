package client

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Enumerate the objects in a bucket, optionally with a prefix
func (c *Client) ListUrls(ctx context.Context, opts ...Opt) (*schema.UrlList, error) {
	// Make request
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.UrlList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("url"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// Create objects from a file or directory
func (c *Client) CreateUrl(ctx context.Context, meta schema.UrlMeta) (*schema.Url, error) {
	req, err := client.NewJSONRequest(meta)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Url
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("url")); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}
