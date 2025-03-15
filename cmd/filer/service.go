package main

import (
	"context"
	"net/http"

	// Packages
	filerplugins "github.com/mutablelogic/go-filer/plugin"
	"github.com/mutablelogic/go-filer/plugin/aws"
	filer "github.com/mutablelogic/go-filer/plugin/filer"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	provider "github.com/mutablelogic/go-server/pkg/provider"
	httprouter "github.com/mutablelogic/go-server/plugin/httprouter"
	httpserver "github.com/mutablelogic/go-server/plugin/httpserver"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServiceCommands struct {
	Run ServiceRunCommand `cmd:"" group:"SERVICE" help:"Run the service"`
}

type ServiceRunCommand struct {
	Router struct {
		httprouter.Config `embed:"" prefix:"router."` // Router configuration
	} `embed:""`
	Server struct {
		httpserver.Config `embed:"" prefix:"server."` // Server configuration
	} `embed:""`
	Filer struct {
		filer.Config `embed:"" prefix:"filer."` // Filer configuration
	} `embed:""`
	AWS struct {
		aws.Config `embed:"" prefix:"aws."` // AWS configuration
	} `embed:""`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *ServiceRunCommand) Run(app App) error {
	// Set the server listener
	cmd.Server.Listen = app.GetEndpoint()

	// Create a provider
	provider, err := provider.New(func(ctx context.Context, label string, plugin server.Plugin) (server.Plugin, error) {
		switch label {
		case "httpserver":
			config := plugin.(httpserver.Config)

			// Set the router
			if router, ok := provider.Provider(ctx).Task(ctx, "httprouter").(http.Handler); !ok || router == nil {
				return nil, httpresponse.ErrInternalError.Withf("Invalid router %q", label)
			} else {
				config.Router = router
			}

			// Return the new configuration with the router
			return config, nil
		case "filer":
			config := plugin.(filer.Config)

			// AWS
			aws, ok := provider.Provider(ctx).Task(ctx, "aws").(filerplugins.AWS)
			if !ok || aws == nil {
				return nil, httpresponse.ErrInternalError.Withf("Invalid AWS %q", label)
			} else {
				config.AWS = aws
			}

			// Router
			router, ok := provider.Provider(ctx).Task(ctx, "httprouter").(server.HTTPRouter)
			if !ok || router == nil {
				return nil, httpresponse.ErrInternalError.Withf("Invalid router %q", label)
			} else {
				config.Router = router
			}

			// Return the new configuration with the router
			return config, nil
		}

		// No-op
		return plugin, nil
	}, cmd.Router.Config, cmd.Server.Config, cmd.Filer.Config, cmd.AWS.Config)
	if err != nil {
		return err
	}

	// Run the provider
	return provider.Run(app.Context())
}
