package cmd

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-filer/pkg/llm/client"
	schema "github.com/mutablelogic/go-filer/pkg/llm/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func run(ctx server.Cmd, fn func(context.Context, *client.Client) error) error {
	// Create a client
	if endpoint, err := ctx.GetEndpoint(schema.APIPrefix); err != nil {
		return err
	} else if provider, err := client.New(endpoint.String(), ctx.GetClientOpts()...); err != nil {
		return err
	} else {
		// Run the function
		return fn(ctx.Context(), provider)
	}
}
