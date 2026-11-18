package httpclient

import (
	"encoding/json"
	"errors"
	"net/http"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

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
