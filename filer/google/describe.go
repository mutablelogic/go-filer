package google

import (
	"context"
	"net/http"
	"net/url"
	"slices"

	// Packages
	client "github.com/mutablelogic/go-client"
	httpclient "github.com/mutablelogic/go-filer/filer/google/httpclient"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// userinfoEndpoint is the endpoint of the user who signed in, which is
// Google's, unless a test sets it
var userinfoEndpoint = httpclient.UserinfoEndpoint

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// describe returns what a volume of the kind is, such as "Google Tasks for
// ada@example.com", of the email address of the user who signed in, or the
// kind, when it can't be read, such as when the sign-in hasn't EmailScope,
// as it's from before it was asked for
func describe(ctx context.Context, kind string, tokens *tokens, opts []client.ClientOpt) string {
	if email := userEmail(ctx, tokens, opts); email != "" {
		return kind + " for " + email
	}
	return kind
}

// userEmail returns the email address of the user who signed in, or empty,
// when it can't be read
func userEmail(ctx context.Context, tokens *tokens, opts []client.ClientOpt) string {
	endpoint, err := url.Parse(userinfoEndpoint)
	if err != nil {
		return ""
	}
	opts = append(slices.Clone(opts), client.OptTransport(func(parent http.RoundTripper) http.RoundTripper {
		return &tokenTransport{parent: parent, tokens: tokens, scheme: endpoint.Scheme, host: endpoint.Host}
	}))
	c, err := httpclient.NewUserinfoClient(userinfoEndpoint, opts...)
	if err != nil {
		return ""
	}
	user, err := c.Userinfo(ctx)
	if err != nil {
		return ""
	}
	return user.Email
}
