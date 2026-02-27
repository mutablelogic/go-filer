package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListBackends returns a list of backend URLs from the filer API.
func (c *Client) ListBackends(ctx context.Context) (*schema.BackendListResponse, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.BackendListResponse
	if err := c.DoWithContext(ctx, req, &response); err != nil {
		return nil, err
	}

	// Return the response
	return &response, nil
}
