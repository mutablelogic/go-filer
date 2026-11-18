//go:build !client

package main

import (
	// Packages
	credentialhandler "github.com/mutablelogic/go-auth/credential/httphandler"
	filer "github.com/mutablelogic/go-filer/filer"
	filercmd "github.com/mutablelogic/go-filer/filer/cmd"
	google "github.com/mutablelogic/go-filer/filer/google"
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
	Filer       filercmd.Flags  `embed:""`
	Credentials CredentialFlags `embed:""`
	Indexer     IndexerFlags    `embed:""`
	servercmd.RunServer
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (runner *RunServer) Run(ctx server.Cmd) error {
	// Log the server configuration
	ctx.Logger().InfoContext(ctx.Context(), "starting server", "name", ctx.Name(), "version", ctx.Version())

	// The provider of the volumes' credentials, which are kept, encrypted,
	// or are the environment's
	credentials, err := runner.Credentials.provider(ctx)
	if err != nil {
		return err
	}

	// Serve the credentials, so they're stored, such as GOOGLE_CLIENT, with
	// the filer command's credential-set, and listed, without their values
	runner.RunServer.Register(credentialhandler.RegisterManagerHandlers(credentials, false, credentialhandler.WithSignIn(google.SignInPrefix, google.Google, google.ClientCredential)))

	// The store of the indexes of volumes with the index option, which make's
	// INDEXER_BACKEND chooses: sqlite, by default, memory or pg
	store, closeStore, err := runner.Indexer.store(ctx)
	if err != nil {
		return err
	}
	defer closeStore()

	// Create the registry of volumes, which registers its HTTP methods, and
	// run the HTTP server to serve requests, until the server is stopped,
	// when the registry, and its volumes, are closed
	return runner.Filer.WithRegistry(ctx, &runner.RunServer, nil, func(*filer.Registry) error {
		return runner.RunServer.Run(ctx)
	}, filer.WithCredentials(credentials), filer.WithIndexerStore(store))
}
