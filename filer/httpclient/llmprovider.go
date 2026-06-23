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

func (c *Client) CreateLLMProvider(ctx context.Context, meta schema.LLMProviderCreate) (*schema.LLMProvider, error) {
	req, err := client.NewJSONRequest(meta)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.LLMProvider
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("llmprovider")); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
