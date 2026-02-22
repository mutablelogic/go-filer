package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListBackends returns a list of backend URLs from the filer API.
func (c *Client) ListBackends(ctx context.Context) (*schema.ListResponse, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.ListResponse
	if err := c.DoWithContext(ctx, req, &response); err != nil {
		return nil, err
	}

	// Return the response
	return &response, nil
}
