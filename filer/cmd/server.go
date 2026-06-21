package cmd

import (
	"errors"
	"fmt"

	// Packages
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	manager "github.com/mutablelogic/go-filer/filer/manager"
	pg "github.com/mutablelogic/go-pg"
	pgcmd "github.com/mutablelogic/go-pg/pkg/cmd"
	server "github.com/mutablelogic/go-server"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	errgroup "golang.org/x/sync/errgroup"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServerCommands struct {
	RunServer RunServer `cmd:"" name:"run" help:"Run the filer server." group:"SERVER"`
	servercmd.OpenAPICommands
}

type RunServer struct {
	pgcmd.PostgresFlags
	servercmd.RunServer

	// Other flags
	Indexer     bool     `long:"indexer" help:"Run this instance as an indexer of content" default:"false" negatable:""`
	Passphrases []string `name:"passphrase" env:"${ENV_NAME}_PASSPHRASES" help:"One or more passphrases used to encrypt credentials."`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (runner *RunServer) Run(ctx server.Cmd) error {
	// Connect to the database, if configured
	conn, err := runner.PostgresFlags.Connect(ctx)
	if err != nil {
		return err
	} else if conn == nil {
		return fmt.Errorf("database connection is required")
	}

	// Log the server configuration
	ctx.Logger().InfoContext(ctx.Context(), "starting filer server", "name", ctx.Name(), "version", ctx.Version(), "indexer", runner.Indexer)

	// Create the manager, run the server, and return any error
	return runner.WithManager(ctx, conn, func(manager *manager.Manager) error {
		// Create an error context - which will cancel any other goroutine on exit
		errgroup, errctx := errgroup.WithContext(ctx.Context())

		// Register http handlers for the manager
		runner.Register(func(router *httprouter.Router) error {
			ctx.Logger().DebugContext(ctx.Context(), "registering http handlers")
			return errors.Join(
				httphandler.RegisterVolumeHandlers(manager, router),
				httphandler.RegisterObjectHandlers(manager, router),
				httphandler.RegisterMetadataHandlers(manager, router),
			)
		})

		// Run the manager
		errgroup.Go(func() error {
			return manager.Run(errctx, ctx.Logger())
		})

		// Run the server - if any co-routine in the error group returns an error, the server will be shutdown
		errgroup.Go(func() error {
			return runner.RunServer.Run(ctx.WithContext(errctx))
		})

		// Wait for the server and manager to exit, and return any error
		return errgroup.Wait()
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (runner *RunServer) WithManager(ctx server.Cmd, conn pg.PoolConn, fn func(*manager.Manager) error) error {
	// Set basic mamager options
	opts := []manager.Opt{
		manager.WithMeter(ctx.Meter()),
		manager.WithTracer(ctx.Tracer()),
		manager.WithIndexer(runner.Indexer),
	}

	// Set passphrases for credential encryption
	for i, passphrase := range runner.Passphrases {
		opts = append(opts, manager.WithPassphrase(uint64(i+1), passphrase))
	}

	// Create a manager and then call the function with the manager, returning any error
	if manager, err := manager.New(ctx.Context(), conn, opts...); err != nil {
		return err
	} else {
		return fn(manager)
	}
}
