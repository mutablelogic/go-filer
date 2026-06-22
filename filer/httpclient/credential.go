package httpclient

import (
	"context"
	"io"
	"net/http"
	"strings"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type textPayload struct {
	io.Reader
}

func (textPayload) Method() string { return http.MethodGet }
func (textPayload) Accept() string { return types.ContentTypeJSON }
func (textPayload) Type() string   { return types.ContentTypeTextPlain }

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

func (c *Client) GetCredential(ctx context.Context, key schema.CredentialKey, passphrase string, result any) error {
	return c.DoWithContext(ctx, textPayload{Reader: strings.NewReader(passphrase)}, result, client.OptPath("credential", key.Key))
}

func (c *Client) DeleteCredential(ctx context.Context, key schema.CredentialKey) (*schema.Credential, error) {
	var response schema.Credential
	if err := c.DoWithContext(ctx, client.MethodDelete, &response, client.OptPath("credential", key.Key)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
