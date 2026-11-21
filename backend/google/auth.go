package google

import (
	"context"
	"strings"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	credential "github.com/mutablelogic/go-auth/credential"
	grant "github.com/mutablelogic/go-auth/credential/grant"
	gofiler "github.com/mutablelogic/go-filer"
	types "github.com/mutablelogic/go-server/pkg/types"
	oauth2 "golang.org/x/oauth2"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// tokens is the source of a filer's access token, of the user's sign-in,
// which go-auth's grant source refreshes, and stores, with the client of
// ClientCredential
type tokens struct {
	source *grant.Source
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// SignInPrefix is the prefix of the keys of Google's credentials: the
	// client's, and the sign-ins, which are those other than the client's
	SignInPrefix = "GOOGLE_"

	// ClientCredential is the key of the credential of the OAuth client, a
	// desktop app's, which can redirect to a loopback address, as the JSON
	// of its client_id and client_secret, or the file Google's console
	// downloads, and DefaultCredential the key of a user's sign-in, a go-auth
	// Grant, which the credential parameter sets another of
	ClientCredential  = SignInPrefix + "CLIENT"
	DefaultCredential = SignInPrefix + "TOKEN"

	// WebClientCredential is the key of the credential of the OAuth client
	// of a web application, whose redirect URI is the server's callback,
	// credential/callback, with which the web UI signs in, in a browser,
	// whose sign-ins are refreshed with it, as a refresh token is the
	// client's which issued it
	WebClientCredential = SignInPrefix + "WEB_CLIENT"
)

var (
	// Google is the authorization server of Google accounts
	Google = authclient.ServerMetadata{
		Issuer:                        "https://accounts.google.com",
		AuthorizationEndpoint:         "https://accounts.google.com/o/oauth2/v2/auth",
		TokenEndpoint:                 "https://oauth2.googleapis.com/token",
		CodeChallengeMethodsSupported: []string{"S256"},
	}

	// SignInOptions are the options of the URL to sign in with Google at:
	// it asks for offline access, and consent, so Google issues a refresh
	// token, and includes the scopes the person has granted already
	SignInOptions = []oauth2.AuthCodeOption{
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("prompt", "consent"),
		oauth2.SetAuthURLParam("include_granted_scopes", "true"),
	}
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newTokens returns the source of the access token of the sign-in with the
// key, which must be for the client, of ClientCredential, or the web
// client, of WebClientCredential, and have one of the required scopes. The scopes are asked for when the user signs in, with
// those the sign-in has already. Returns go-auth's errors, which the
// root's HTTPErr maps: a SignInError, which is 401, with how to sign in, when the user hasn't, or their
// sign-in is for another client, or doesn't have the scopes.
func newTokens(ctx context.Context, credentials credential.CredentialProvider, key string, required, scopes []string) (*tokens, error) {
	source, err := grant.New(ctx, credentials, grant.Config{Server: Google, Client: ClientCredential, Clients: []string{WebClientCredential}, Key: key, Required: required, Scopes: scopes})
	if err != nil {
		return nil, err
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
		return "", err
	}
	return token.AccessToken, nil
}

// signInAgain returns a SignInError, of the current sign-in, which wraps the
// error, such as when the API doesn't accept its token
func (self *tokens) signInAgain(err error) error {
	return self.source.SignIn(err)
}

// credentialKey returns an error when the key of a credential isn't that
// of a sign-in, which is an identifier, with SignInPrefix, other than
// ClientCredential, as only those can be signed in to
func credentialKey(key string) error {
	if !types.IsIdentifier(key) || !strings.HasPrefix(key, SignInPrefix) || key == ClientCredential || strings.HasSuffix(key, calendarSyncSuffix) {
		return gofiler.ErrBadParameter.Withf("%s: %q isn't the key of a sign-in, which is %s*, other than %s, or *%s, of calendars' sync tokens", paramCredential, key, SignInPrefix, ClientCredential, calendarSyncSuffix)
	}
	return nil
}
