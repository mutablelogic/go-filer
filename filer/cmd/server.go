//go:build !client

package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer"
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	server "github.com/mutablelogic/go-server"
	broadcaster "github.com/mutablelogic/go-server/broadcaster"
	changes "github.com/mutablelogic/go-server/broadcaster/httphandler"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Flags struct {
	Volume []string `name:"volume" sep:"none" placeholder:"URL" help:"Volume to add when the server starts, as the URL of its backend, named by its name option, such as s3://my.bucket/photos?name=photos, or its host, such as file://media/mnt/media?readonly=true, or as a directory, named by its name option, or its base name, such as /mnt/media, which can be repeated."`
	Worker string   `name:"worker" env:"FILER_WORKER,WORKER" placeholder:"NAME" help:"Name of the worker, which is in the metrics, so those of the servers are told apart. Defaults to the hostname."`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithRegistry creates a registry of volumes, with the server's tracer and
// meter, and the flags' worker, when it's set, and then the options, such as
// the provider of the volumes' credentials (filer.WithCredentials), whose
// placeholders, such as ${S3_SECRET}, it expands, and which stores those
// which aren't placeholders, and runs it, so the changes to its volumes are
// published to the broadcaster, and it's closed when the function returns, adds the volumes of the flags, registers its
// HTTP handlers with the server, when there is one, and then calls the
// function with the registry. The broadcaster is one a server shares with
// its other modules, which it runs and serves, or, when it's nil, one which
// is created, served as go-server's /changes, and run until the function
// returns. A volume which can't be added is an error. The registry, and its
// volumes, are closed when the function returns. Returns any error.
func (flags Flags) WithRegistry(ctx server.Cmd, srv *servercmd.RunServer, b *broadcaster.Broadcaster, fn func(*filer.Registry) error, opts ...filer.Opt) (err error) {
	if b == nil {
		b = broadcaster.New()
		if srv != nil {
			srv.Register(func(router *httprouter.Router) error {
				return changes.RegisterHandlers(b, router)
			})
		}
		runCtx, cancel := context.WithCancel(ctx.Context())
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			_ = b.Run(runCtx)
		}()
		defer func() {
			cancel()
			<-stopped
		}()
	}
	defaults := []filer.Opt{filer.WithBroadcaster(b), filer.WithTracer(ctx.Tracer()), filer.WithMeter(ctx.Meter())}

	// The backends' clients, such as of Google's APIs, have the command's
	// client options: its trace, with --debug, or --verbose, which redacts
	// headers' secrets, its tracer, and its timeout, and its user agent, of
	// its name and version
	_, clientopts, err := ctx.ClientEndpoint()
	if err != nil {
		return err
	}
	clientopts = append(clientopts, client.OptUserAgent(ctx.Name()+"/"+ctx.Version()))
	defaults = append(defaults, filer.WithClientOpts(clientopts...))
	if flags.Worker != "" {
		defaults = append(defaults, filer.WithWorker(flags.Worker))
	}
	opts = append(defaults, opts...)
	registry, err := filer.New(ctx.Context(), opts...)
	if err != nil {
		return err
	}

	// Run the registry, which publishes the changes to the volumes, until
	// the function returns, or the server is stopping, which cancels it, when
	// it closes the volumes, so streams end, and transfers to clients which
	// are slow are interrupted, so the server isn't kept waiting for them.
	runCtx, cancel := context.WithCancel(ctx.Context())
	running := make(chan error, 1)
	go func() {
		running <- registry.Run(runCtx, ctx.Logger())
	}()
	defer func() {
		cancel()
		err = errors.Join(err, <-running)
	}()

	// Add the volumes, whose names are their name options, or the hosts of
	// their URLs, or the base names of their directories. A volume which
	// can't be is named by its position, without its URL, or why it can't be
	// parsed, as they can have credentials.
	for i, volume := range flags.Volume {
		u, err := volumeURL(volume)
		if err != nil {
			return gofiler.ErrBadParameter.Withf("volume %d: %v", i+1, err)
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

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// volumeURL returns the URL of a volume's backend, which is a URL, or a
// directory, as gofiler.ParseURL returns it, whose name, as a file URL's
// host, is its name option, which is moved to its host, or its base name,
// such as "media" for /mnt/media, when it has no host. Returns an error,
// without the URL, which can have credentials, when it can't be parsed, or
// the name isn't valid.
func volumeURL(volume string) (*url.URL, error) {
	u, err := gofiler.ParseURL(volume)
	if err != nil {
		return nil, errors.New("invalid url")
	}
	if u.Scheme == schema.SchemeFile && u.Host == "" {
		// The options are parsed strictly, as a malformed option, which
		// would be left out, could be one which makes it read-only
		query, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return nil, errors.New("invalid options")
		}
		name := query.Get("name")
		if name == "" {
			name = path.Base(u.Path)
		} else {
			query.Del("name")
			u.RawQuery = query.Encode()
		}

		// The name isn't in the error, as it's from the volume, which can
		// have credentials
		if !types.IsIdentifier(name) {
			return nil, errors.New("the base name of the directory isn't a valid name, so give it with the name option, or as file://NAME/path")
		}
		u.Host = name
	}
	return u, nil
}
