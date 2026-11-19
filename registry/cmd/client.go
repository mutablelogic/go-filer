package cmd

import (
	"context"

	// Packages
	credentialcmd "github.com/mutablelogic/go-auth/credential/cmd"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	httpclient "github.com/mutablelogic/go-filer/registry/httpclient"
	server "github.com/mutablelogic/go-server"
	changesclient "github.com/mutablelogic/go-server/broadcaster/httpclient"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// ClientCommands are the commands which call the filer API of a server
type ClientCommands struct {
	VolumeCommands
	ObjectCommands

	// The server's credentials, such as GOOGLE_CLIENT, and the sign-ins
	// which are stored, of go-auth's
	credentialcmd.CredentialProviderCommands
	credentialcmd.CredentialManagerCommands
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// withClient calls the function with a client of the API, in a span which
// records the function's error
func withClient(ctx server.Cmd, span string, fn func(context.Context, *httpclient.Client) error) (err error) {
	endpoint, opts, err := ctx.ClientEndpoint()
	if err != nil {
		return err
	}
	client, err := httpclient.New(endpoint, opts...)
	if err != nil {
		return err
	}

	// The span ends with the named error, which is the function's
	spanCtx, endfn := otel.StartSpan(ctx.Tracer(), ctx.Context(), span)
	defer func() { endfn(err) }()
	return fn(spanCtx, client)
}

// withChangesClient calls the function with a client of go-server's
// /changes, of the server, within a span of the name
func withChangesClient(ctx server.Cmd, span string, fn func(context.Context, *changesclient.Client) error) (err error) {
	endpoint, opts, err := ctx.ClientEndpoint()
	if err != nil {
		return err
	}
	client, err := changesclient.New(endpoint, opts...)
	if err != nil {
		return err
	}

	// The span ends with the named error, which is the function's
	spanCtx, endfn := otel.StartSpan(ctx.Tracer(), ctx.Context(), span)
	defer func() { endfn(err) }()
	return fn(spanCtx, client)
}
