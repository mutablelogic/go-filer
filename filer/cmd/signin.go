package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	google "github.com/mutablelogic/go-filer/filer/google"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	server "github.com/mutablelogic/go-server"
	oauth2 "golang.org/x/oauth2"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// signInWithBrowser signs in, of what the server needs: the person signs in
// with Google, in the browser, at the URL, which is printed, and opened,
// unless noBrowser, which redirects to a loopback address, with the code,
// which the server exchanges, as it has the client's secret, and stores
func signInWithBrowser(ctx context.Context, api *httpclient.Client, signIn authclient.SignIn, noBrowser bool) error {
	if signIn.Server.Issuer != google.Google.Issuer {
		return gofiler.ErrNotImplemented.Withf("signing in with %q", signIn.Server.Issuer)
	}
	loopback, err := authclient.NewLoopback()
	if err != nil {
		return err
	}
	defer loopback.Close()

	// The URL to sign in at, with the state, and the PKCE challenge
	state, verifier := oauth2.GenerateVerifier(), oauth2.GenerateVerifier()
	u := signIn.AuthCodeURL(loopback.RedirectURI(), state, verifier, google.SignInOptions...)
	fmt.Fprintln(os.Stderr, "Sign in with Google, in your browser, at:")
	fmt.Fprintln(os.Stderr, u)
	if !noBrowser {
		_ = openBrowser(u)
	}

	// Wait for the code, which is for the state
	query, err := loopback.Wait(ctx)
	if err != nil {
		return err
	} else if query.Get("state") != state {
		return gofiler.ErrBadParameter.With("the sign-in's state isn't the one which was sent")
	} else if query.Get("code") == "" {
		return gofiler.ErrNotAuthorized.Withf("signing in with Google: %s", query.Get("error"))
	}

	// The server exchanges it, and stores the sign-in
	response, err := api.SignIn(ctx, authclient.SignInCode{
		Credential:   signIn.Credential,
		Code:         query.Get("code"),
		CodeVerifier: verifier,
		RedirectURI:  loopback.RedirectURI(),
	})
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "Signed in, as", response.Credential)
	return nil
}

// newSignInClient returns a client of the API, which sends the code of a
// sign-in, and its verifier: it has the command's tracer, which records
// the URL, but not the trace to stderr, of --debug or --verbose, which
// writes the request's body
func newSignInClient(ctx server.Cmd) (*httpclient.Client, error) {
	endpoint, _, err := ctx.ClientEndpoint()
	if err != nil {
		return nil, err
	}
	var opts []client.ClientOpt
	if tracer := ctx.Tracer(); tracer != nil {
		opts = append(opts, client.OptTracer(tracer))
	}
	return httpclient.New(endpoint, opts...)
}

// openBrowser opens the URL in the person's browser
func openBrowser(u string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", u).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", u).Start()
	default:
		return exec.Command("xdg-open", u).Start()
	}
}
