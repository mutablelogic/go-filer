//go:build !client

package main

import (
	// Packages
	filer "github.com/mutablelogic/go-filer/filer"
	filercmd "github.com/mutablelogic/go-filer/filer/cmd"
	server "github.com/mutablelogic/go-server"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	Server ServerCommands          `embed:""`
	Filer  filercmd.ClientCommands `embed:""`
}

type ServerCommands struct {
	RunServer RunServer `cmd:"" name:"run" help:"Run the server." group:"SERVER"`
	servercmd.OpenAPICommands
}

type RunServer struct {
	Filer filercmd.Flags `embed:"" prefix:"filer."`
	servercmd.RunServer
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (runner *RunServer) Run(ctx server.Cmd) error {
	// Log the server configuration
	ctx.Logger().InfoContext(ctx.Context(), "starting server", "name", ctx.Name(), "version", ctx.Version())

	// Create the registry of volumes, which registers its HTTP methods, and
	// run the HTTP server to serve requests, until the server is stopped,
	// when the registry, and its volumes, are closed
	return runner.Filer.WithRegistry(ctx, &runner.RunServer, func(*filer.Registry) error {
		return runner.RunServer.Run(ctx)
	})
}
