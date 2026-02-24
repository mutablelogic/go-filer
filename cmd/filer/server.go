//go:build !client

package main

import (
	"context"
	"fmt"
	"net/http"

	// Packages
	httphandler "github.com/mutablelogic/go-filer/httphandler"
	manager "github.com/mutablelogic/go-filer/manager"
	version "github.com/mutablelogic/go-filer/pkg/version"
	server "github.com/mutablelogic/go-server"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	httpserver "github.com/mutablelogic/go-server/pkg/httpserver"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServerCommands struct {
	Server RunServerCommand `cmd:"" name:"server" help:"Run HTTP server." group:"SERVER"`
}

type RunServerCommand struct {
	Backend []string `name:"backend" help:"Backend URL (e.g. mem://name, file://name/path). May be repeated." optional:""`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *RunServerCommand) Run(ctx *Globals) error {
	// Create manager with backends
	opts := []manager.Opt{}
	for _, url := range cmd.Backend {
		opts = append(opts, manager.WithBackend(ctx.ctx, url))
	}
	mgr, err := manager.New(ctx.ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}
	defer mgr.Close()

	return serve(ctx, mgr)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// serve registers HTTP handlers and runs the server until context is done.
func serve(ctx *Globals, mgr *manager.Manager) error {
	// Build middleware
	middleware := []httprouter.HTTPMiddlewareFunc{}
	if mw, ok := ctx.logger.(server.HTTPMiddleware); ok {
		middleware = append(middleware, mw.WrapFunc)
	}

	// Create the router
	router, err := httprouter.NewRouter(ctx.ctx, ctx.HTTP.Prefix, ctx.HTTP.Origin, "filer", version.Version(), middleware...)
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
	}

	// Register filer HTTP handlers
	if err := httphandler.RegisterHandlers(mgr, router); err != nil {
		return fmt.Errorf("failed to register handlers: %w", err)
	}

	// Create and run the HTTP server
	srv, err := httpserver.New(ctx.HTTP.Addr, http.Handler(router), nil)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	ctx.logger.Printf(ctx.ctx, "filer@%s started on %s", version.Version(), ctx.HTTP.Addr)
	if err := srv.Run(ctx.ctx); err != nil {
		return err
	}
	ctx.logger.Printf(context.Background(), "filer stopped")
	return nil
}
