package google

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	// Packages
	auth "github.com/mutablelogic/go-auth"
	gofiler "github.com/mutablelogic/go-filer"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Client is the identifier and secret of the OAuth client which users sign
// in to, which is the ClientCredential, and isn't returned to users. It's
// JSON with its client_id and client_secret, or the file Google's console
// downloads for a client, which has them in "installed", of a desktop
// client, or "web".
type Client struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret,omitempty"`
}

// Credential is a user's sign-in, which is stored once they've signed in,
// as DefaultCredential, or another key: the issuer, the endpoint which
// refreshes its token, the client it was issued to, the scopes it has, and
// its token.
type Credential struct {
	Issuer        string   `json:"issuer"`
	TokenEndpoint string   `json:"token_endpoint"`
	ClientID      string   `json:"client_id"`
	Scopes        []string `json:"scopes,omitempty"`
	Token         Token    `json:"token"`
}

// Token is the access token of a credential, which is refreshed with its
// refresh token, once it expires
type Token struct {
	AccessToken  string    `json:"access_token,omitempty"`
	TokenType    string    `json:"token_type,omitempty"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	Expiry       time.Time `json:"expiry,omitzero"`
}

// tokens is the source of a filer's access token, of its credential, which
// is refreshed, and stored again, when it expires
type tokens struct {
	mu          sync.Mutex
	credentials gofiler.CredentialProvider
	key         string   // The key of the credential
	required    []string // A credential has one of the scopes
	scopes      []string // The scopes which are asked for, when signing in
	client      Client
	current     Credential
	http        *http.Client // Refreshes the token, without the API's options, so a trace doesn't log its secrets
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Google's issuer, the endpoint which users are sent to, to sign in, and
	// the endpoint which issues and refreshes tokens
	GoogleIssuer                = "https://accounts.google.com"
	GoogleAuthorizationEndpoint = "https://accounts.google.com/o/oauth2/v2/auth"
	GoogleTokenEndpoint         = "https://oauth2.googleapis.com/token"

	// ClientCredential is the key of the credential of the OAuth client, a
	// desktop app's, which can redirect to a loopback address, and
	// DefaultCredential the key of a user's sign-in, which the credential
	// parameter sets another of
	ClientCredential  = "GOOGLE_CLIENT"
	DefaultCredential = "GOOGLE_TOKEN"

	// expiryMargin is how long before it expires a token is refreshed
	expiryMargin = time.Minute
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newTokens returns the source of the access token of the credential with
// the key, which must be for the client, of ClientCredential, and have one
// of the required scopes. The scopes are asked for when the user signs in,
// with those the credential has already. Returns an AuthorizationError,
// with how to sign in, when the user hasn't, or their credential is for
// another client, or doesn't have the scopes.
func newTokens(ctx context.Context, credentials gofiler.CredentialProvider, key string, required, scopes []string) (*tokens, error) {
	if credentials == nil {
		return nil, gofiler.ErrServiceUnavailable.With("can't sign in with Google, without a provider of credentials")
	}
	self := &tokens{credentials: credentials, key: key, required: required, scopes: scopes, http: &http.Client{Timeout: 30 * time.Second}}
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
func ParseClient(data []byte) (*Client, error) {
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
	var client Client
	if err := json.Unmarshal(data, &client); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("credential %q: %v", ClientCredential, err)
	} else if strings.TrimSpace(client.ClientID) == "" {
		return nil, gofiler.ErrBadParameter.Withf("credential %q has no client_id", ClientCredential)
	}
	return &client, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// token returns the access token, which is refreshed, and stored, once it
// has expired. A credential which was replaced while it was refreshed, such
// as when the user signed in again, is used, rather than overwritten.
func (self *tokens) token(ctx context.Context) (string, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	// The credential, as it is stored, which the user can replace, by
	// signing in again
	current, err := self.read(ctx)
	if err != nil {
		return "", err
	}
	self.current = *current
	if !current.Token.expired(time.Now()) {
		return current.Token.AccessToken, nil
	}

	// Refresh it, and store it, unless it was replaced meanwhile
	refreshed, err := self.refresh(ctx, *current)
	if err != nil {
		return "", err
	}
	if latest, err := self.read(ctx); err != nil {
		return "", err
	} else if latest.Token != current.Token {
		self.current = *latest
		return latest.Token.AccessToken, nil
	}
	data, err := json.Marshal(refreshed)
	if err != nil {
		return "", err
	} else if err := self.credentials.Set(ctx, self.key, data); err != nil {
		return "", err
	}
	self.current = *refreshed
	return refreshed.Token.AccessToken, nil
}

// read returns the credential, which must be for the client, and have one
// of the required scopes, or an AuthorizationError
func (self *tokens) read(ctx context.Context) (*Credential, error) {
	data, err := self.credentials.Get(ctx, self.key)
	if isNotFound(err) {
		return nil, self.notAuthorized(nil, gofiler.ErrNotAuthorized.Withf("no %q credential, so sign in with Google", self.key))
	} else if err != nil {
		return nil, err
	}
	var credential Credential
	if err := json.Unmarshal(data, &credential); err != nil {
		return nil, self.notAuthorized(nil, gofiler.ErrNotAuthorized.Withf("credential %q isn't a sign-in: %v", self.key, err))
	}
	switch {
	case credential.Issuer != GoogleIssuer:
		return nil, self.notAuthorized(nil, gofiler.ErrNotAuthorized.Withf("credential %q isn't Google's, so sign in with Google", self.key))
	case credential.ClientID != self.client.ClientID:
		return nil, self.notAuthorized(&credential, gofiler.ErrNotAuthorized.Withf("credential %q is for another client than %q's, so sign in with Google again", self.key, ClientCredential))
	case credential.Token.AccessToken == "" && credential.Token.RefreshToken == "":
		return nil, self.notAuthorized(&credential, gofiler.ErrNotAuthorized.Withf("credential %q has no token, so sign in with Google again", self.key))
	case !hasScope(credential.Scopes, self.required):
		return nil, self.notAuthorized(&credential, gofiler.ErrNotAuthorized.Withf("credential %q doesn't have the access the volume needs, %s, so sign in with Google again", self.key, strings.Join(self.required, " or ")))
	}
	if u, err := url.Parse(credential.TokenEndpoint); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, self.notAuthorized(&credential, gofiler.ErrNotAuthorized.Withf("credential %q has no token endpoint, so sign in with Google again", self.key))
	}
	return &credential, nil
}

// refresh returns the credential, with its token refreshed, with its
// refresh token, and the client's secret. A refresh token which is revoked,
// or expired, is an AuthorizationError.
func (self *tokens) refresh(ctx context.Context, credential Credential) (*Credential, error) {
	if credential.Token.RefreshToken == "" {
		return nil, self.notAuthorized(&credential, gofiler.ErrNotAuthorized.Withf("credential %q has expired, and can't be refreshed, so sign in with Google again", self.key))
	}
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {credential.Token.RefreshToken},
		"client_id":     {self.client.ClientID},
	}
	if self.client.ClientSecret != "" {
		form.Set("client_secret", self.client.ClientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, credential.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := self.http.Do(req)
	if err != nil {
		return nil, gofiler.ErrServiceUnavailable.Withf("refreshing credential %q: %v", self.key, err)
	}
	defer resp.Body.Close()
	var response struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		Scope        string `json:"scope"`
		Error        string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, gofiler.ErrServiceUnavailable.Withf("refreshing credential %q: %s", self.key, resp.Status)
	}
	switch {
	case response.Error == "invalid_grant", response.Error == "unauthorized_client":
		return nil, self.notAuthorized(&credential, gofiler.ErrNotAuthorized.Withf("credential %q is revoked, or expired, so sign in with Google again", self.key))
	case resp.StatusCode != http.StatusOK || response.AccessToken == "":
		return nil, gofiler.ErrServiceUnavailable.Withf("refreshing credential %q: %s %s", self.key, resp.Status, response.Error)
	}

	// The refreshed credential keeps its refresh token, unless it's sent
	// another, and its scopes, unless they're said
	credential.Token.AccessToken, credential.Token.TokenType = response.AccessToken, response.TokenType
	if response.RefreshToken != "" {
		credential.Token.RefreshToken = response.RefreshToken
	}
	credential.Token.Expiry = time.Time{}
	if response.ExpiresIn > 0 {
		credential.Token.Expiry = time.Now().Add(time.Duration(response.ExpiresIn) * time.Second).UTC()
	}
	if scopes := strings.Fields(response.Scope); len(scopes) > 0 {
		credential.Scopes = scopes
	}
	return &credential, nil
}

// notAuthorized returns an AuthorizationError, with how the user signs in:
// Google, the client, and the scopes, with those the credential has, when
// it isn't nil, so signing in again doesn't take away what others need
func (self *tokens) notAuthorized(credential *Credential, err error) error {
	scopes := slices.Clone(self.scopes)
	if credential != nil {
		for _, scope := range credential.Scopes {
			if !slices.Contains(scopes, scope) {
				scopes = append(scopes, scope)
			}
		}
	}
	return &gofiler.AuthorizationError{
		Authorization: gofiler.Authorization{
			Issuer:                GoogleIssuer,
			AuthorizationEndpoint: GoogleAuthorizationEndpoint,
			ClientID:              self.client.ClientID,
			Scopes:                scopes,
			Credential:            self.key,
		},
		Err: err,
	}
}

// expired returns true when the token has no access token, or it expires
// within the margin
func (t Token) expired(now time.Time) bool {
	return t.AccessToken == "" || (!t.Expiry.IsZero() && now.Add(expiryMargin).After(t.Expiry))
}

// readClient returns the client, of ClientCredential, which is
// ErrServiceUnavailable when there's none, as no one can sign in
func readClient(ctx context.Context, credentials gofiler.CredentialProvider) (*Client, error) {
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

// credentialKey returns an error when the key of a credential isn't an
// identifier
func credentialKey(key string) error {
	if !types.IsIdentifier(key) {
		return gofiler.ErrBadParameter.Withf("%s: %q isn't the key of a credential", paramCredential, key)
	}
	return nil
}
