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
	var response schema.BackendListResponse
	if err := c.DoWithContext(ctx, client.NewRequest(), &response); err != nil {
		return nil, err
	}
	return &response, nil
}
