package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// SignIn completes a sign-in, which was authorized, at a loopback address,
// with PKCE: the server exchanges the code, with the client's secret, and
// stores the sign-in, as the request's credential
func (c *Client) SignIn(ctx context.Context, signIn authclient.SignInCode) (*schema.SignInResponse, error) {
	req, err := client.NewJSONRequest(signIn)
	if err != nil {
		return nil, err
	}
	var response schema.SignInResponse
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("signin")); err != nil {
		return nil, err
	}
	return types.Ptr(response), nil
}

// SignInOf returns what's needed to sign in, of an error of a request which
// is 401, as a volume's backend needs its user to sign in, or nil
func SignInOf(err error) *authclient.SignIn {
	var response httpresponse.ErrResponse
	if !errors.As(err, &response) || response.Code != http.StatusUnauthorized || response.Detail == nil {
		return nil
	}
	data, err := json.Marshal(response.Detail)
	if err != nil {
		return nil
	}
	var signIn authclient.SignIn
	if err := json.Unmarshal(data, &signIn); err != nil || signIn.ClientID == "" || signIn.Server.AuthorizationEndpoint == "" {
		return nil
	}
	return &signIn
}
