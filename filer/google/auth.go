package google

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strings"
	"sync"

	// Packages
	auth "github.com/mutablelogic/go-auth"
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	types "github.com/mutablelogic/go-server/pkg/types"
	oauth2 "golang.org/x/oauth2"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// tokens is the source of a filer's access token, of the user's sign-in,
// which is a go-auth Grant, whose token is refreshed, and stored again, when
// it has expired, with the client of ClientCredential
type tokens struct {
	mu          sync.Mutex
	credentials gofiler.CredentialProvider
	key         string   // The key of the sign-in
	required    []string // A sign-in has one of the scopes
	scopes      []string // The scopes which are asked for, when signing in
	client      authclient.Registration
	current     authclient.Grant
	refresher   *authclient.Client // Refreshes tokens, without the API's options, so a trace doesn't log their secrets
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

	// SignInPrefix is the prefix of the keys of sign-ins, which are those
	// which start with it, other than ClientCredential
	SignInPrefix = "GOOGLE_"
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
// those the sign-in has already. Returns a SignInError, with how to sign
// in, when the user hasn't, or their sign-in is for another client, or
// doesn't have the scopes.
func newTokens(ctx context.Context, credentials gofiler.CredentialProvider, key string, required, scopes []string) (*tokens, error) {
	if credentials == nil {
		return nil, gofiler.ErrServiceUnavailable.With("can't sign in with Google, without a provider of credentials")
	}
	refresher, err := authclient.New(Google.Issuer)
	if err != nil {
		return nil, err
	}
	self := &tokens{credentials: credentials, key: key, required: required, scopes: scopes, refresher: refresher}
	client, err := readClient(ctx, credentials)
	if err != nil {
		return nil, err
	}
	self.client = *client
	current, err := self.read(ctx)
	if err != nil {
		return nil, err
	}
	self.current = *current
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ParseClient returns the client of the JSON, which has its client_id, and
// client_secret, or has them in "installed" or "web", as the file Google's
// console downloads for a client does
func ParseClient(data []byte) (*authclient.Registration, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return nil, gofiler.ErrBadParameter.Withf("credential %q is not a JSON object, of the client_id and client_secret of a Google OAuth client", ClientCredential)
	}
	for _, kind := range []string{"installed", "web"} {
		if nested, exists := object[kind]; exists {
			data = nested
			break
		}
	}
	var client authclient.Registration
	if err := json.Unmarshal(data, &client); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("credential %q: %v", ClientCredential, err)
	} else if strings.TrimSpace(client.ClientID) == "" {
		return nil, gofiler.ErrBadParameter.Withf("credential %q has no client_id", ClientCredential)
	}
	return &authclient.Registration{ClientID: client.ClientID, ClientSecret: client.ClientSecret}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// token returns the access token, which is refreshed, and stored, once it
// has expired. A sign-in which was replaced while it was refreshed, such as
// when the user signed in again, is used, rather than overwritten.
func (self *tokens) token(ctx context.Context) (string, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	// The sign-in, as it's stored, which the user can replace, by signing in
	// again
	current, err := self.read(ctx)
	if err != nil {
		return "", err
	}
	self.current = *current
	if current.Token.Valid() {
		return current.Token.AccessToken, nil
	}

	// Refresh it, with the client's secret, which isn't stored with it. A
	// client which Google refuses, such as one whose secret is wrong, isn't
	// mended by signing in again.
	grant := *current
	grant.Client = self.client
	token, err := self.refresher.TokenSource(ctx, &grant, nil).Token()
	var retrieve *oauth2.RetrieveError
	switch {
	case errors.As(err, &retrieve) && (retrieve.ErrorCode == "invalid_client" || retrieve.ErrorCode == "unauthorized_client"):
		return "", gofiler.ErrServiceUnavailable.Withf("Google refused the client of %q, so check its client_id and client_secret: %v", ClientCredential, err)
	case errors.Is(err, auth.ErrBadParameter):
		// Refused, as it's revoked, or expired, or can't be refreshed
		return "", self.signIn(current, gofiler.ErrNotAuthorized.Withf("sign-in %q is revoked, or expired, so sign in with Google again: %v", self.key, err))
	case err != nil:
		return "", gofiler.ErrServiceUnavailable.Withf("refreshing sign-in %q: %v", self.key, err)
	}

	// Store it, unless it was replaced meanwhile, under the key's lock,
	// which every volume, and sign-in, of the key shares
	unlock := lockCredential(self.key)
	defer unlock()
	if latest, err := self.read(ctx); err != nil {
		return "", err
	} else if !sameToken(latest.Token, current.Token) {
		self.current = *latest
		return latest.Token.AccessToken, nil
	}
	refreshed := *current
	refreshed.Token = token
	if refreshed.Token.RefreshToken == "" {
		refreshed.Token.RefreshToken = current.Token.RefreshToken
	}
	data, err := json.Marshal(refreshed)
	if err != nil {
		return "", err
	} else if err := self.credentials.Set(ctx, self.key, data); err != nil {
		return "", err
	}
	self.current = refreshed
	return refreshed.Token.AccessToken, nil
}

// read returns the sign-in, which must be Google's, for the client, and
// have one of the required scopes, or a SignInError
func (self *tokens) read(ctx context.Context) (*authclient.Grant, error) {
	data, err := self.credentials.Get(ctx, self.key)
	if isNotFound(err) {
		return nil, self.signIn(nil, gofiler.ErrNotAuthorized.Withf("no %q sign-in, so sign in with Google", self.key))
	} else if err != nil {
		return nil, err
	}
	var grant authclient.Grant
	if err := json.Unmarshal(data, &grant); err != nil {
		return nil, self.signIn(nil, gofiler.ErrNotAuthorized.Withf("credential %q isn't a sign-in: %v", self.key, err))
	}
	switch {
	case grant.Server.Issuer != Google.Issuer:
		return nil, self.signIn(nil, gofiler.ErrNotAuthorized.Withf("sign-in %q isn't Google's, so sign in with Google", self.key))
	case grant.Client.ClientID != self.client.ClientID:
		return nil, self.signIn(&grant, gofiler.ErrNotAuthorized.Withf("sign-in %q is for another client than %q's, so sign in with Google again", self.key, ClientCredential))
	case grant.Token == nil || (grant.Token.AccessToken == "" && grant.Token.RefreshToken == ""):
		return nil, self.signIn(&grant, gofiler.ErrNotAuthorized.Withf("sign-in %q has no token, so sign in with Google again", self.key))
	case !hasScope(grant.Scopes, self.required):
		return nil, self.signIn(&grant, gofiler.ErrNotAuthorized.Withf("sign-in %q doesn't have the access the volume needs, %s, so sign in with Google again", self.key, strings.Join(self.required, " or ")))
	}
	if u, err := url.Parse(grant.Server.TokenEndpoint); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, self.signIn(&grant, gofiler.ErrNotAuthorized.Withf("sign-in %q has no token endpoint, so sign in with Google again", self.key))
	}
	return &grant, nil
}

// signInAgain returns a SignInError, of the current sign-in, which is read
// under the lock, as a request which changes it can be in progress
func (self *tokens) signInAgain(err error) error {
	self.mu.Lock()
	current := self.current
	self.mu.Unlock()
	return self.signIn(&current, err)
}

// signIn returns a SignInError, with how the user signs in: Google, the
// client, and the scopes, with those the sign-in has, when it isn't nil, so
// signing in again doesn't take away what others need
func (self *tokens) signIn(grant *authclient.Grant, err error) error {
	scopes := slices.Clone(self.scopes)
	if grant != nil {
		for _, scope := range grant.Scopes {
			if !slices.Contains(scopes, scope) {
				scopes = append(scopes, scope)
			}
		}
	}
	return authclient.NewSignInError(authclient.SignIn{
		Server:     Google,
		ClientID:   self.client.ClientID,
		Scopes:     scopes,
		Credential: self.key,
	}, err)
}

// readClient returns the client, of ClientCredential, which is
// ErrServiceUnavailable when there's none, as no one can sign in
func readClient(ctx context.Context, credentials gofiler.CredentialProvider) (*authclient.Registration, error) {
	data, err := credentials.Get(ctx, ClientCredential)
	if isNotFound(err) {
		return nil, gofiler.ErrServiceUnavailable.Withf("can't sign in with Google: no %q credential, with the client_id and client_secret of a Google OAuth desktop client", ClientCredential)
	} else if err != nil {
		return nil, err
	}
	var value string
	if json.Unmarshal(data, &value) == nil {
		// A credential which is a string of JSON, such as one which is
		// stored from a file, or the environment
		data = []byte(value)
	}
	return ParseClient(data)
}

// credentialLocks are the locks of the keys of sign-ins, which every volume,
// and sign-in, of a key, in the process, shares, so a refresh doesn't
// replace a sign-in which was stored after it was read. A credential which
// is stored by another process, or the credentials API, isn't locked, as
// the provider can't store one only when it hasn't changed.
var credentialLocks sync.Map

// lockCredential locks the key, and returns the function which unlocks it
func lockCredential(key string) func() {
	value, _ := credentialLocks.LoadOrStore(key, new(sync.Mutex))
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// sameToken returns true when two tokens are the same
func sameToken(a, b *oauth2.Token) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.AccessToken == b.AccessToken && a.RefreshToken == b.RefreshToken && a.Expiry.Equal(b.Expiry)
}

// hasScope returns true when the scopes have one of those which are required
func hasScope(scopes, required []string) bool {
	for _, scope := range required {
		if slices.Contains(scopes, scope) {
			return true
		}
	}
	return false
}

// isNotFound returns true when the error of a provider of credentials is
// that it doesn't have the credential, which is go-auth's ErrNotFound, of
// its providers, or go-filer's
func isNotFound(err error) bool {
	return errors.Is(err, auth.ErrNotFound) || errors.Is(err, gofiler.ErrNotFound)
}

// credentialKey returns an error when the key of a credential isn't that
// of a sign-in, which is an identifier, with SignInPrefix, other than
// ClientCredential, as only those can be signed in to
func credentialKey(key string) error {
	if !types.IsIdentifier(key) || !strings.HasPrefix(key, SignInPrefix) || key == ClientCredential {
		return gofiler.ErrBadParameter.Withf("%s: %q isn't the key of a sign-in, which is %s*, other than %s", paramCredential, key, SignInPrefix, ClientCredential)
	}
	return nil
}
