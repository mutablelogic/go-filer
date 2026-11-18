package google

import (
	// Packages
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
