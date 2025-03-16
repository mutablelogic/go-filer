package client

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListObjects(ctx context.Context, bucket string, opts ...Opt) ([]schema.Object, error) {
	// Make request
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response []schema.Object
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("object", bucket), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return response, nil
}
