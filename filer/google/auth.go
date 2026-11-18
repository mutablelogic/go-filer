package google

import (
	"context"
	"errors"
	"strings"

	// Packages
	auth "github.com/mutablelogic/go-auth"
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	grant "github.com/mutablelogic/go-auth/credential/grant"
	gofiler "github.com/mutablelogic/go-filer"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// tokens is the source of a filer's access token, of the user's sign-in,
// which go-auth's grant source refreshes, and stores, with the client of
// ClientCredential, with go-filer's errors
type tokens struct {
	source *grant.Source
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// ClientCredential is the key of the credential of the OAuth client, a
	// desktop app's, which can redirect to a loopback address, as the JSON
	// of its client_id and client_secret, or the file Google's console
	// downloads, and DefaultCredential the key of a user's sign-in, a go-auth
	// Grant, which the credential parameter sets another of
	ClientCredential  = "GOOGLE_CLIENT"
	DefaultCredential = "GOOGLE_TOKEN"
)

// Google is the authorization server of Google accounts
var Google = authclient.ServerMetadata{
	Issuer:                        "https://accounts.google.com",
	AuthorizationEndpoint:         "https://accounts.google.com/o/oauth2/v2/auth",
	TokenEndpoint:                 "https://oauth2.googleapis.com/token",
	CodeChallengeMethodsSupported: []string{"S256"},
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newTokens returns the source of the access token of the sign-in with the
// key, which must be for the client, of ClientCredential, and have one of
// the required scopes. The scopes are asked for when the user signs in, with
// those the sign-in has already. Returns a SignInError, which is
// ErrNotAuthorized, with how to sign in, when the user hasn't, or their
// sign-in is for another client, or doesn't have the scopes.
func newTokens(ctx context.Context, credentials gofiler.CredentialProvider, key string, required, scopes []string) (*tokens, error) {
	if credentials == nil {
		return nil, gofiler.ErrServiceUnavailable.With("can't sign in with Google, without a provider of credentials")
	}
	source, err := grant.New(ctx, credentials, grant.Config{Server: Google, Client: ClientCredential, Key: key, Required: required, Scopes: scopes})
	if err != nil {
		return nil, authError(err)
	}
	return &tokens{source: source}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// token returns the access token, which is refreshed, and stored, once it
// has expired
func (self *tokens) token(ctx context.Context) (string, error) {
	token, err := self.source.Token(ctx)
	if err != nil {
		return "", authError(err)
	}
	return token.AccessToken, nil
}

// signInAgain returns a SignInError, of the current sign-in, which wraps the
// error, such as when the API doesn't accept its token
func (self *tokens) signInAgain(err error) error {
	return authError(self.source.SignIn(err))
}

// authError returns go-filer's error of go-auth's, so a sign-in which is
// needed is ErrNotAuthorized, and keeps how to sign in
func authError(err error) error {
	var signIn *authclient.SignInError
	if errors.As(err, &signIn) {
		return authclient.NewSignInError(signIn.SignIn, authError(signIn.Err))
	}
	var authErr auth.Err
	if !errors.As(err, &authErr) {
		return err
	}
	reason := strings.TrimPrefix(err.Error(), authErr.Error()+": ")
	switch authErr {
	case auth.ErrInvalidProvider:
		return gofiler.ErrNotAuthorized.With(reason)
	case auth.ErrBadParameter:
		return gofiler.ErrBadParameter.With(reason)
	case auth.ErrNotFound:
		return gofiler.ErrNotFound.With(reason)
	case auth.ErrForbidden:
		return gofiler.ErrForbidden.With(reason)
	case auth.ErrConflict:
		return gofiler.ErrConflict.With(reason)
	case auth.ErrNotImplemented:
		return gofiler.ErrNotImplemented.With(reason)
	default:
		return gofiler.ErrServiceUnavailable.With(reason)
	}
}

// credentialKey returns an error when the key of a credential isn't an
// identifier
func credentialKey(key string) error {
	if !types.IsIdentifier(key) {
		return gofiler.ErrBadParameter.Withf("%s: %q isn't the key of a credential", paramCredential, key)
	}
	return nil
}
