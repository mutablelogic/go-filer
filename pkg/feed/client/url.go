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
func (c *Client) CreateUrl(ctx context.Context, url string, meta schema.UrlMeta) (*schema.Url, error) {
	type createUrl struct {
		Url string `json:"url"`
		schema.UrlMeta
	}
	req, err := client.NewJSONRequest(createUrl{
		Url:     url,
		UrlMeta: meta,
	})
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

// Update url
func (c *Client) UpdateUrl(ctx context.Context, id string, meta schema.UrlMeta) (*schema.Url, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var url schema.Url
	if err := c.DoWithContext(ctx, req, &url, client.OptPath("url", id)); err != nil {
		return nil, err
	}

	// Return the response
	return &url, nil
}
