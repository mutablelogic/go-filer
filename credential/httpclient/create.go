package httpclient

import (
	"context"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) CreateCredential(ctx context.Context, credential schema.CredentialCreate) (*schema.Credential, error) {
	req, err := client.NewJSONRequestEx(http.MethodPut, credential, types.ContentTypeAny)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Credential
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("credential")); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
