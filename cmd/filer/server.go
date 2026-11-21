//go:build !client

package main

import (
	"context"

	// Packages
	credentialhandler "github.com/mutablelogic/go-auth/credential/httphandler"
	google "github.com/mutablelogic/go-filer/backend/google"
	rss "github.com/mutablelogic/go-filer/backend/rss"
	frontend "github.com/mutablelogic/go-filer/frontend/cmd"
	metadatacmd "github.com/mutablelogic/go-filer/metadata/cmd"
	registry "github.com/mutablelogic/go-filer/registry"
	filercmd "github.com/mutablelogic/go-filer/registry/cmd"
	store "github.com/mutablelogic/go-filer/store"
	server "github.com/mutablelogic/go-server"
	broadcaster "github.com/mutablelogic/go-server/broadcaster"
	changes "github.com/mutablelogic/go-server/broadcaster/httphandler"
	config "github.com/mutablelogic/go-server/config"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
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
	Filer       filercmd.Flags    `embed:""`
	Credentials CredentialFlags   `embed:""`
	Indexer     IndexerFlags      `embed:""`
	Enrichment  metadatacmd.Flags `embed:""`
	Frontend    frontend.Flags    `embed:""`
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
	// the filer command's credential-set, and listed, without their values,
	// and Google's sign-ins, of a loopback address, of the CLI, or of the
	// server's callback, of the web UI, when GOOGLE_CLIENT is a web
	// application's client
	runner.RunServer.Register(credentialhandler.RegisterManagerHandlers(credentials, false,
		credentialhandler.WithSignIn(google.SignInPrefix, google.Google, google.ClientCredential),
		credentialhandler.WithWebSignIn(google.SignInPrefix, google.SignInOptions...),
	))

	// The broadcaster, of the changes of the volumes' objects, and of the
	// store's volumes, which is served as /changes, and runs until the
	// registry, and the store, are closed
	b := broadcaster.New()
	runner.RunServer.Register(func(router *httprouter.Router) error {
		// The configuration, at /config.json, and the web UI, at /, when
		// it's built in, or else a handler of / which isn't found
		if ui, err := runner.Frontend.RegisterUI(ctx, ctx.Logger(), router, runner.RunServer.OpenAPI); err != nil {
			return err
		} else if !ui {
			if err := router.RegisterCatchAll("/", true); err != nil {
				return err
			}
		}

		// What a UI uses, apart from the filer: its configuration, watching
		// for changes, and the OpenAPI spec
		router.Spec().AddTagGroup("UI", config.Tag, changes.Tag, "OpenAPI")
		return changes.RegisterHandlers(b, router)
	})
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

	// The store of the volumes, and their indexes, which make's
	// INDEXER_BACKEND chooses: sqlite, by default, memory or pg, which
	// publishes the changes of its volumes
	s, closeStore, err := runner.Indexer.store(ctx, store.WithTracer(ctx.Tracer()), store.WithBroadcaster(b))
	if err != nil {
		return err
	}
	defer closeStore()

	// The volumes' caches, in the command's folder of the user's cache,
	// which can be cleared, so they're kept after a restart, unless it is
	cacheDir, err := ctx.CacheDir()
	if err != nil {
		return err
	}

	// The task which enriches the files of volumes with index=metadata, of
	// go-metadata, which reads media, too, when it's built with cgo
	enrichment, err := runner.Enrichment.WithMetadataEnrichment(ctx)
	if err != nil {
		return err
	}
	opts := append([]registry.Opt{registry.WithCredentials(credentials), registry.WithStore(s), registry.WithCacheDir(cacheDir)}, enrichment...)

	// Create the registry of volumes, which registers its HTTP methods, and
	// run the HTTP server to serve requests, until the server is stopped,
	// when the registry, and its volumes, are closed
	return runner.Filer.WithRegistry(ctx, &runner.RunServer, b, func(*registry.Registry) error {
		return runner.RunServer.Run(ctx)
	}, append(opts,
		// RSS feeds are a backend which isn't built into the registry, which
		// is added, as another program's would be
		registry.WithBackend(rss.Scheme, rss.Description, rss.Open),
	)...)
}
