package cmd

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// ClientCommands are the commands which call the filer API of a server
type ClientCommands struct {
	VolumeCommands
	ObjectCommands
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
