//go:build !client

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer"
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	server "github.com/mutablelogic/go-server"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Flags struct {
	Volume []string `name:"volume" sep:"none" placeholder:"URL" help:"Volume to add when the server starts, as the URL of its backend, named by the host of a file URL, such as file://media/mnt/media?readonly=true, or otherwise by its name option, such as s3://bucket/photos?name=photos, which can be repeated."`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithRegistry creates a registry of volumes, with the server's tracer and
// meter, and then the options, adds the volumes of the flags, registers its
// HTTP handlers with the server, when there is one, and then calls the
// function with the registry. A volume which can't be added is an error. The
// registry, and its volumes, are closed when the function returns. Returns
// any error.
func (flags Flags) WithRegistry(ctx server.Cmd, srv *servercmd.RunServer, fn func(*filer.Registry) error, opts ...filer.Opt) (err error) {
	opts = append([]filer.Opt{filer.WithTracer(ctx.Tracer()), filer.WithMeter(ctx.Meter())}, opts...)
	registry, err := filer.New(ctx.Context(), opts...)
	if err != nil {
		return err
	}

	// Close the registry as soon as the server is stopping, which ends the
	// streams of changes, and interrupts transfers to clients which are slow,
	// so the server isn't kept waiting for them. Its error is returned, as
	// it's only returned by the call which closes the volumes.
	closed := make(chan error, 1)
	stop := context.AfterFunc(ctx.Context(), func() { closed <- registry.Close() })
	defer func() {
		if !stop() {
			err = errors.Join(err, <-closed)
		}
		err = errors.Join(err, registry.Close())
	}()

	// Add the volumes, whose names are the hosts of their file URLs, or the
	// name options of the others
	for _, volume := range flags.Volume {
		u, err := url.Parse(volume)
		if err != nil {
			return gofiler.ErrBadParameter.Withf("invalid volume %q: %v", volume, err)
		}
		name := filer.VolumeName(u)
		if name == "" {
			return gofiler.ErrBadParameter.Withf("volume %q has no name, which is the host of a file url, or the name option", volume)
		} else if _, err := registry.Add(ctx.Context(), name, u); err != nil {
			return fmt.Errorf("volume %q: %w", name, err)
		}
		ctx.Logger().InfoContext(ctx.Context(), "added volume", "name", name, "url", u.String())
	}

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
