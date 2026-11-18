package google

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strings"

	// Packages
	auth "github.com/mutablelogic/go-auth"
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	types "github.com/mutablelogic/go-server/pkg/types"
	oauth2 "golang.org/x/oauth2"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// SignInOptions are the options of the URL to sign in with Google at: it
// asks for offline access, and consent, so Google issues a refresh token,
// and includes the scopes the person has granted already
var SignInOptions = []oauth2.AuthCodeOption{
	oauth2.AccessTypeOffline,
	oauth2.SetAuthURLParam("prompt", "consent"),
	oauth2.SetAuthURLParam("include_granted_scopes", "true"),
}

// Exchange completes a sign-in with the server, which is Google, unless it's
// another, for tests: the code is exchanged, with its PKCE verifier, and the
// secret of the client, of ClientCredential, with go-auth's ExchangeCode,
// for a Grant, which is stored, without the secret, as the code's
// credential. The credential must be a sign-in's, GOOGLE_*, other than ClientCredential, so
// a request can't replace another credential, and the redirect URI a
// loopback address's, as a desktop client's is.
func Exchange(ctx context.Context, credentials gofiler.CredentialProvider, server authclient.ServerMetadata, code authclient.SignInCode) (*authclient.Grant, error) {
	switch {
	case credentials == nil:
		return nil, gofiler.ErrServiceUnavailable.With("can't sign in with Google, without a provider of credentials")
	case !types.IsIdentifier(code.Credential) || !strings.HasPrefix(code.Credential, SignInPrefix) || code.Credential == ClientCredential:
		return nil, gofiler.ErrBadParameter.Withf("credential %q isn't a sign-in's, which is GOOGLE_*, other than %s", code.Credential, ClientCredential)
	case !isLoopback(code.RedirectURI):
		return nil, gofiler.ErrBadParameter.Withf("redirect URI %q isn't a loopback address", code.RedirectURI)
	}
	client, err := readClient(ctx, credentials)
	if err != nil {
		return nil, err
	}

	// Exchange the code, with the client's secret, and store the grant,
	// without it
	// A client which Google refuses, such as one whose secret is wrong, isn't
	// mended by signing in again
	grant, err := authclient.ExchangeCode(ctx, server, *client, code)
	var retrieve *oauth2.RetrieveError
	switch {
	case errors.As(err, &retrieve) && (retrieve.ErrorCode == "invalid_client" || retrieve.ErrorCode == "unauthorized_client"):
		return nil, gofiler.ErrServiceUnavailable.Withf("Google refused the client of %q, so check its client_id and client_secret: %v", ClientCredential, err)
	case errors.Is(err, auth.ErrBadParameter):
		return nil, gofiler.ErrNotAuthorized.Withf("%v", err)
	case err != nil:
		return nil, gofiler.ErrServiceUnavailable.Withf("%v", err)
	}
	// Store it under the key's lock, which every volume of the key shares, so
	// a refresh in progress doesn't replace it
	data, err := json.Marshal(grant)
	if err != nil {
		return nil, err
	}
	unlock := lockCredential(code.Credential)
	defer unlock()
	if err := credentials.Set(ctx, code.Credential, data); err != nil {
		return nil, err
	}
	return grant, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// isLoopback returns true when the URI is http, of a loopback address, such
// as http://127.0.0.1:8085/callback
func isLoopback(uri string) bool {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "http" {
		return false
	}
	if u.Hostname() == "localhost" {
		return true
	}
	ip := net.ParseIP(u.Hostname())
	return ip != nil && ip.IsLoopback()
}
