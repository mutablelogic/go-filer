package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	credentialclient "github.com/mutablelogic/go-auth/credential/httpclient"
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	google "github.com/mutablelogic/go-filer/backend/google"
	server "github.com/mutablelogic/go-server"
	oauth2 "golang.org/x/oauth2"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// SignInFlags are the flags of signing in, of a command which signs in, when
// the server needs it: whether the URL to sign in at is opened, and the
// loopback address it redirects to, which is fixed, so it can be one a web
// application's client registers, as well as any a desktop app's can
type SignInFlags struct {
	NoBrowser  bool   `name:"no-browser" help:"Print the URL to sign in at, when its user needs to sign in, rather than opening it in the browser."`
	SignInAddr string `name:"signin-addr" default:"localhost:8085" placeholder:"HOST:PORT" help:"Loopback address the sign-in redirects to, http://<addr>/callback, which a web application's client registers as a redirect URI."`
}

// signInWithBrowser signs in, of what the server needs: the person signs in
// with Google, in the browser, at the URL, which is printed, and opened,
// unless NoBrowser, which redirects to the loopback address, with the code,
// which the server's credentials API exchanges, as it has the client's
// secret, and stores
func signInWithBrowser(ctx context.Context, api *credentialclient.Client, signIn authclient.SignIn, flags SignInFlags) error {
	if signIn.Server.Issuer != google.Google.Issuer {
		return gofiler.ErrNotImplemented.Withf("signing in with %q", signIn.Server.Issuer)
	}
	loopback, err := authclient.NewLoopbackAt(flags.SignInAddr)
	if err != nil {
		return err
	}
	defer loopback.Close()

	// The URL to sign in at, with the state, and the PKCE challenge
	state, verifier := oauth2.GenerateVerifier(), oauth2.GenerateVerifier()
	u := signIn.AuthCodeURL(loopback.RedirectURI(), state, verifier, google.SignInOptions...)
	fmt.Fprintln(os.Stderr, "Sign in with Google, in your browser, at:")
	fmt.Fprintln(os.Stderr, u)
	if !flags.NoBrowser {
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
	fmt.Fprintln(os.Stderr, "Signed in, as", response.Key)
	return nil
}

// newSignInClient returns a client of the server's credentials API, which
// sends the code of a
// sign-in, and its verifier: it has the command's tracer, which records
// the URL, but not the trace to stderr, of --debug or --verbose, which
// writes the request's body
func newSignInClient(ctx server.Cmd) (*credentialclient.Client, error) {
	endpoint, _, err := ctx.ClientEndpoint()
	if err != nil {
		return nil, err
	}
	var opts []client.ClientOpt
	if tracer := ctx.Tracer(); tracer != nil {
		opts = append(opts, client.OptTracer(tracer))
	}
	return credentialclient.New(endpoint, opts...)
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
