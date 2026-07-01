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

func (c *Client) RotateCredential(ctx context.Context, key schema.CredentialKey) (*schema.Credential, error) {
	var response schema.Credential
	if err := c.DoWithContext(ctx, client.MethodPost, &response, client.OptPath("credential", key.Key, "rotate")); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
