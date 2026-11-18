package filer

import (
	"errors"
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Authorization is what's needed for a user to sign in, so a backend can be
// used: the issuer, its endpoint, which the user is sent to, the client which
// is signed in to, the scopes which are asked for, and the key of the
// credential which is stored once they have signed in. It has no secrets.
type Authorization struct {
	Issuer                string   `json:"issuer"`
	AuthorizationEndpoint string   `json:"authorization_endpoint"`
	ClientID              string   `json:"client_id"`
	Scopes                []string `json:"scopes,omitempty"`
	Credential            string   `json:"credential"`
}

// AuthorizationError is the error of a backend whose user needs to sign in,
// such as when they haven't, or their credential is for another client, or
// it doesn't have the access the backend needs, or it's revoked, with how
// they sign in. It's ErrNotAuthorized.
type AuthorizationError struct {
	Authorization
	Err error
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *AuthorizationError) Error() string {
	return fmt.Sprintf("%v: sign in with %s", e.Err, e.Issuer)
}

// Unwrap returns the error, and ErrNotAuthorized
func (e *AuthorizationError) Unwrap() []error {
	return []error{ErrNotAuthorized, e.Err}
}

// AuthorizationOf returns what's needed to sign in, of an error which is an
// AuthorizationError, or nil
func AuthorizationOf(err error) *Authorization {
	var authErr *AuthorizationError
	if errors.As(err, &authErr) {
		return &authErr.Authorization
	}
	return nil
}
