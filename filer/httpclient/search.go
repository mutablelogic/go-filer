package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) Search(ctx context.Context, req schema.SearchListRequest) (*schema.SearchList, error) {
	var response schema.SearchList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("search"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
