package main

import (
	"context"
	"errors"
	"net/http"
	"os"

	// Packages
	task "github.com/mutablelogic/go-filer/pkg/filer/task"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	provider "github.com/mutablelogic/go-server/pkg/provider"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	types "github.com/mutablelogic/go-server/pkg/types"

	// Plugins
	plugin "github.com/mutablelogic/go-filer"
	feed "github.com/mutablelogic/go-filer/pkg/feed/config"
	filer "github.com/mutablelogic/go-filer/pkg/filer/config"
	version "github.com/mutablelogic/go-filer/pkg/version"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter/config"
	httpserver "github.com/mutablelogic/go-server/pkg/httpserver/config"
	logger "github.com/mutablelogic/go-server/pkg/logger/config"
	pgpool "github.com/mutablelogic/go-server/pkg/pgmanager/config"
	pgqueue "github.com/mutablelogic/go-server/pkg/pgqueue/config"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServiceCommands struct {
	Run    ServiceRunCommand    `cmd:"" group:"SERVICE" help:"Run the service with plugins"`
	Config ServiceConfigCommand `cmd:"" group:"SERVICE" help:"Output the plugin configuration"`
}

type ServiceRunCommand struct {
	Plugins []string `help:"Plugin paths" env:"PLUGIN_PATH"`
}

type ServiceConfigCommand struct {
	ServiceRunCommand
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *ServiceConfigCommand) Run(app server.Cmd) error {
	// Create a provider by loading the plugins
	provider, err := provider.NewWithPlugins(cmd.Plugins...)
	if err != nil {
		return err
	}
	return provider.WriteConfig(os.Stdout)
}

func (cmd *ServiceRunCommand) Run(app server.Cmd) error {
	// Create a provider by loading the plugins
	provider, err := provider.NewWithPlugins(cmd.Plugins...)
	if err != nil {
		return err
	}

	// Get endpoint
	endpoint, err := app.GetEndpoint()
	if err != nil {
		return err
	}

	// Set the configuration
	err = errors.Join(err, provider.Load("log", "main", func(ctx context.Context, label string, config server.Plugin) error {
		logger := config.(*logger.Config)
		logger.Debug = app.GetDebug() >= server.Debug
		return nil
	}))

	err = errors.Join(err, provider.Load("httprouter", "main", func(ctx context.Context, label string, config server.Plugin) error {
		httprouter := config.(*httprouter.Config)
		httprouter.Prefix = types.NormalisePath(endpoint.Path)
		httprouter.Origin = "*"
		httprouter.Middleware = []string{"log.main"}
		return nil
	}))

	err = errors.Join(err, provider.Load("httpserver", "main", func(ctx context.Context, label string, config server.Plugin) error {
		httpserver := config.(*httpserver.Config)
		httpserver.Listen = endpoint

		// Set router
		if router, ok := provider.Task(ctx, "httprouter.main").(http.Handler); !ok || router == nil {
			return httpresponse.ErrInternalError.With("Invalid router")
		} else {
			httpserver.Router = router
		}

		// Return success
		return nil
	}))

	err = errors.Join(err, provider.Load("pgpool", "main", func(ctx context.Context, label string, config server.Plugin) error {
		pgpool := config.(*pgpool.Config)

		// Set trace
		if app.GetDebug() == server.Trace {
			pgpool.Trace = func(ctx context.Context, query string, args any, err error) {
				if err != nil {
					ref.Log(ctx).With("args", args).Print(ctx, err, " ON ", query)
				} else {
					ref.Log(ctx).With("args", args).Debug(ctx, query)
				}
			}
		}

		return nil
	}))

	err = errors.Join(err, provider.Load("pgqueue", "main", func(ctx context.Context, label string, config server.Plugin) error {
		pgqueue := config.(*pgqueue.Config)

		// Set connection pool
		if conn, ok := provider.Task(ctx, "pgpool.main").(server.PG); !ok || conn == nil {
			return httpresponse.ErrInternalError.With("Invalid connection pool")
		} else {
			pgqueue.Pool = conn
		}

		// Set namespace
		pgqueue.Namespace = types.StringPtr(task.TaskNamespace)

		return nil
	}))

	err = errors.Join(err, provider.Load("aws", "main", func(ctx context.Context, label string, config server.Plugin) error {
		return nil
	}))

	err = errors.Join(err, provider.Load("filer", "main", func(ctx context.Context, label string, config server.Plugin) error {
		filer := config.(*filer.Config)

		// Set AWS
		filer.AWS = provider.Task(ctx, "aws.main").(plugin.AWS)

		// Set router
		if router, ok := provider.Task(ctx, "httprouter.main").(server.HTTPRouter); !ok || router == nil {
			return httpresponse.ErrInternalError.With("Invalid router")
		} else {
			filer.Router = router
		}

		// Set queue
		if queue, ok := provider.Task(ctx, "pgqueue.main").(server.PGQueue); !ok || queue == nil {
			return httpresponse.ErrInternalError.With("Invalid or missing task queue")
		} else {
			filer.Queue = queue
		}

		// Return success
		return nil
	}))

	err = errors.Join(err, provider.Load("feed", "main", func(ctx context.Context, label string, config server.Plugin) error {
		feed := config.(*feed.Config)

		// Set router
		if router, ok := provider.Task(ctx, "httprouter.main").(server.HTTPRouter); !ok || router == nil {
			return httpresponse.ErrInternalError.With("Invalid router")
		} else {
			feed.Router = router
		}

		// Set queue
		if queue, ok := provider.Task(ctx, "pgqueue.main").(server.PGQueue); !ok || queue == nil {
			return httpresponse.ErrInternalError.With("Invalid or missing task queue")
		} else {
			feed.Queue = queue
		}

		// Return success
		return nil
	}))

	err = errors.Join(err, provider.Load("llm", "main", func(ctx context.Context, label string, config server.Plugin) error {
		// Return success
		return nil
	}))

	if err != nil {
		return err
	}

	provider.(server.Logger).Print(context.TODO(), "go-filer ", version.Version())

	// Run the provider
	return provider.Run(app.Context())
}
