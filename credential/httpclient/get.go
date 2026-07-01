package httpclient

import (
	"context"
	"strings"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/credential/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) GetCredential(ctx context.Context, key schema.CredentialKey, passphrase string, result any) error {
	return c.DoWithContext(ctx, textPayload{Reader: strings.NewReader(passphrase)}, result, client.OptPath("credential", key.Key))
}
