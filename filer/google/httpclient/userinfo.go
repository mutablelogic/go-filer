package httpclient

import (
	"context"
	"strings"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// UserinfoClient is a client of Google's OpenID Connect userinfo endpoint,
// of the user who signed in
type UserinfoClient struct {
	*authclient.Client
}

// Userinfo is the user who signed in: their email address, when the
// sign-in has EmailScope, and their name, when it has the profile scope
type Userinfo struct {
	Email string `json:"email,omitempty"`
	Name  string `json:"name,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// UserinfoEndpoint is the endpoint of the user who signed in
	UserinfoEndpoint = "https://openidconnect.googleapis.com/v1/userinfo"

	// EmailScope is the scope of reading the email address of the user who
	// signed in
	EmailScope = "https://www.googleapis.com/auth/userinfo.email"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewUserinfoClient returns a client of the userinfo endpoint, or
// UserinfoEndpoint, when it's empty, with go-client's options, such as a
// token
func NewUserinfoClient(endpoint string, opts ...client.ClientOpt) (*UserinfoClient, error) {
	if endpoint = strings.TrimSpace(endpoint); endpoint == "" {
		endpoint = UserinfoEndpoint
	}
	c, err := authclient.New(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return &UserinfoClient{Client: c}, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Userinfo returns the user who signed in, or an AuthError, when the
// sign-in can't read it
func (c *UserinfoClient) Userinfo(ctx context.Context) (*Userinfo, error) {
	var result Userinfo
	if err := c.DoAuthWithContext(ctx, client.NewRequest(), &result); err != nil {
		return nil, err
	}
	return &result, nil
}
