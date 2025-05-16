package client

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/llm/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListModels(ctx context.Context, opts ...Opt) (*schema.ModelList, error) {
	// Make request
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.ModelList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("model"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) GetModel(ctx context.Context, name string) (*schema.Model, error) {
	// Return error if name is empty
	if name == "" {
		return nil, httpresponse.ErrBadRequest.With("model name is required")
	}

	// Perform request
	var response schema.Model
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("model", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}
