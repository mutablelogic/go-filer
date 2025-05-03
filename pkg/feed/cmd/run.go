package cmd

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-filer/pkg/feed/client"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func run(ctx server.Cmd, fn func(context.Context, *client.Client) error) error {
	// Create a client
	provider, err := client.New(ctx.GetEndpoint(schema.APIPrefix).String(), ctx.GetClientOpts()...)
	if err != nil {
		return err
	}
	// Run the function
	return fn(ctx.Context(), provider)
}
