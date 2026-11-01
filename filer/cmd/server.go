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
	Volume []string `name:"volume" sep:"none" placeholder:"URL" help:"Volume to add when the server starts, as the URL of its backend, named by its name option, such as s3://my.bucket/photos?name=photos, or its host, such as file://media/mnt/media?readonly=true, which can be repeated."`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithRegistry creates a registry of volumes, with the server's tracer and
// meter, and credentials from its environment, such as S3_SECRET, which are
// the placeholders of a volume's credentials, such as ${S3_SECRET}, and
// those which are stored while it runs, and then the options, runs it, so
// the changes to its volumes are sent to its subscriptions, adds the volumes
// of the flags, registers its HTTP handlers with the server, when there is
// one, and then calls the function with the registry. A volume which can't be added is an error. The
// registry, and its volumes, are closed when the function returns. Returns
// any error.
func (flags Flags) WithRegistry(ctx server.Cmd, srv *servercmd.RunServer, fn func(*filer.Registry) error, opts ...filer.Opt) (err error) {
	opts = append([]filer.Opt{filer.WithTracer(ctx.Tracer()), filer.WithMeter(ctx.Meter()), filer.WithCredentials(newEnvCredentials())}, opts...)
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

	// Run the registry, which sends the changes to the volumes to the
	// subscriptions, until it's closed, which is waited for
	running := make(chan error, 1)
	go func() {
		running <- registry.Run(ctx.Context(), ctx.Logger())
	}()
	defer func() {
		registry.Close()
		err = errors.Join(err, <-running)
	}()

	// Add the volumes, whose names are their name options, or the hosts of
	// their URLs. A volume which can't be is named by its position, without
	// its URL, or why it can't be parsed, as they can have credentials.
	for i, volume := range flags.Volume {
		u, err := url.Parse(volume)
		if err != nil {
			return gofiler.ErrBadParameter.Withf("volume %d: invalid url", i+1)
		}
		name := filer.VolumeName(u)
		if name == "" {
			return gofiler.ErrBadParameter.Withf("volume %d has no name, which is its name option, or the host of its url", i+1)
		}

		// The backend's URL is logged, as it has the placeholders of the
		// credentials, which are stored, not the credentials
		b, err := registry.Add(ctx.Context(), name, u)
		if err != nil {
			return fmt.Errorf("volume %q: %w", name, err)
		}
		ctx.Logger().InfoContext(ctx.Context(), "added volume", "name", name, "url", b.URL().String())
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
