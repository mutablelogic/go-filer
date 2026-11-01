//go:build !client

package cmd

import (
	"errors"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer"
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	server "github.com/mutablelogic/go-server"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Flags struct{}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithRegistry creates a registry of volumes, with the server's tracer and
// meter, and then the options, registers its HTTP handlers with the server,
// when there is one, and then calls the function with the registry. The
// registry, and its volumes, are closed when the function returns. Returns
// any error.
func (flags Flags) WithRegistry(ctx server.Cmd, srv *servercmd.RunServer, fn func(*filer.Registry) error, opts ...filer.Opt) (err error) {
	opts = append([]filer.Opt{filer.WithTracer(ctx.Tracer()), filer.WithMeter(ctx.Meter())}, opts...)
	registry, err := filer.New(ctx.Context(), opts...)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, registry.Close())
	}()

	// Register the HTTP handlers
	if srv != nil {
		srv.Register(func(router *httprouter.Router) error {
			ctx.Logger().DebugContext(ctx.Context(), "registering filer http handlers")
			return httphandler.RegisterHandlers(registry, router)
		})
	}

	// Call the function with the registry
	return fn(registry)
}
