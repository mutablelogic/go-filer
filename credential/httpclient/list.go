package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListCredentials(ctx context.Context, req schema.CredentialListRequest) (*schema.CredentialList, error) {
	var response schema.CredentialList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("credential"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
