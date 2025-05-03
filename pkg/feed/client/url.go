package client

import (
	"context"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Enumerate the urls
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

	// Return the response
	return &response, nil
}

// Create url
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

	// Return the response
	return &response, nil
}

// Delete url
func (c *Client) DeleteUrl(ctx context.Context, id string) error {
	// Make request
	req := client.NewRequestEx(http.MethodDelete, "")

	// Perform request
	return c.DoWithContext(ctx, req, nil, client.OptPath("url", id))
}

// Get url
func (c *Client) GetUrl(ctx context.Context, id string) (*schema.Url, error) {
	var url schema.Url

	// Perform request
	if err := c.DoWithContext(ctx, nil, &url, client.OptPath("url", id)); err != nil {
		return nil, err
	}

	// Return the response
	return &url, nil
}
