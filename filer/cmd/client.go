package cmd

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	server "github.com/mutablelogic/go-server"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	Backends BackendsCommand `cmd:"" name:"backends" help:"Return list of backends." group:"SERVER"`
	List     ListCommand     `cmd:"" name:"ls" help:"List objects at the backend root." group:"OBJECTS"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Client returns an httpclient.Client configured from the global HTTP flags.
func Client(ctx server.Cmd) (*httpclient.Client, error) {
	url, opts, err := ctx.ClientEndpoint()
	if err != nil {
		return nil, err
	}
	return httpclient.New(url, opts...)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func withClient(globals server.Cmd, command string, attrs any, fn func(context.Context, *httpclient.Client) error) (err error) {
	// Create the client
	client, err := Client(globals)
	if err != nil {
		return err
	}

	// Create a span
	ctx, endFunc := otel.StartSpan(globals.Tracer(), globals.Context(), command,
		attribute.String("cmd", types.Stringify(attrs)),
	)
	defer func() { endFunc(err) }()

	// Run the command
	return fn(ctx, client)
}
