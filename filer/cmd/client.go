package cmd

import (
	// Packages
	"context"

	"github.com/mutablelogic/go-client/pkg/otel"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	server "github.com/mutablelogic/go-server"
	"github.com/mutablelogic/go-server/pkg/types"
	"go.opentelemetry.io/otel/attribute"
)

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
