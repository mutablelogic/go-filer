//go:build !client

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	config "github.com/aws/aws-sdk-go-v2/config"
	credentials "github.com/aws/aws-sdk-go-v2/credentials"
	backend "github.com/mutablelogic/go-filer/pkg/backend"
	httphandler "github.com/mutablelogic/go-filer/pkg/httphandler"
	manager "github.com/mutablelogic/go-filer/pkg/manager"
	version "github.com/mutablelogic/go-filer/pkg/version"
	server "github.com/mutablelogic/go-server"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	httpserver "github.com/mutablelogic/go-server/pkg/httpserver"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServerCommands struct {
	Run RunServerCommand `cmd:"" name:"run" help:"Run HTTP server." group:"SERVER"`
}

type AWSConfig struct {
	AccessKey    string `name:"access-key"    env:"AWS_ACCESS_KEY_ID"     help:"AWS access key ID (s3://)."                                                           optional:""`
	SecretKey    string `name:"secret-key"    env:"AWS_SECRET_ACCESS_KEY" help:"AWS secret access key (s3://)."                                                       optional:""`
	SessionToken string `name:"session-token" env:"AWS_SESSION_TOKEN"     help:"AWS session token for temporary credentials (s3://, optional)."                      optional:""`
	Region       string `name:"region"   env:"AWS_REGION,AWS_DEFAULT_REGION" help:"AWS region."                                                                 optional:""`
	Profile      string `name:"profile"  env:"AWS_PROFILE"                    help:"AWS credentials profile (s3://, ignored when access-key is set)."  optional:""`
	Endpoint     string `name:"endpoint"                                      help:"S3-compatible endpoint URL, e.g. http://localhost:9000 (s3://)."   optional:""`
}

type RunServerCommand struct {
	Backend []string  `arg:"" name:"backend" help:"Backend URL (e.g. mem://name, file://name/path, s3://bucket)." optional:""`
	AWS     AWSConfig `embed:"" prefix:"aws."`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *RunServerCommand) Run(ctx *Globals) error {
	bOpts, err := cmd.backendOpts(ctx.ctx)
	if err != nil {
		return err
	}

	backends := cmd.Backend
	if len(backends) == 0 {
		def, err := defaultBackendURL()
		if err != nil {
			return err
		}
		backends = []string{def}
	}

	opts := []manager.Opt{}
	for _, url := range backends {
		opts = append(opts, manager.WithBackend(ctx.ctx, url, bOpts...))
	}
	mgr, err := manager.New(ctx.ctx, opts...)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}
	defer mgr.Close()

	for i, url := range backends {
		ctx.logger.Printf(ctx.ctx, "backend[%d] %s", i, url)
	}

	return serve(ctx, mgr)
}

// backendOpts builds a []backend.Opt from the server flags.
//
// Credential priority: --aws.profile / AWS_PROFILE > --aws.access-key / AWS_ACCESS_KEY_ID > anonymous.
// When a profile is given, config.LoadDefaultConfig is called with
// config.WithSharedConfigProfile so SSO and assume-role profiles work correctly.
// When --aws.access-key is set (and no profile), static credentials are used.
// Otherwise anonymous credentials are used (suitable for public buckets or
// S3-compatible services that don't require authentication).
func (cmd *RunServerCommand) backendOpts(ctx context.Context) ([]backend.Opt, error) {
	var opts []backend.Opt

	if cmd.AWS.Profile != "" {
		// Profile takes priority: load via the SDK's config chain so SSO profiles work.
		cfgOpts := []func(*config.LoadOptions) error{
			config.WithSharedConfigProfile(cmd.AWS.Profile),
		}
		if cmd.AWS.Region != "" {
			cfgOpts = append(cfgOpts, config.WithRegion(cmd.AWS.Region))
		}
		awsCfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config for profile %q: %w", cmd.AWS.Profile, err)
		}
		opts = append(opts, backend.WithAWSConfig(awsCfg))
		if cmd.AWS.Endpoint != "" {
			opts = append(opts, backend.WithEndpoint(cmd.AWS.Endpoint))
		}
	} else if cmd.AWS.AccessKey != "" {
		// Static credentials (no profile set).
		if cmd.AWS.SecretKey == "" {
			return nil, fmt.Errorf("--aws.secret-key is required when --aws.access-key is set")
		}
		cfg := aws.Config{
			Credentials: credentials.NewStaticCredentialsProvider(
				cmd.AWS.AccessKey, cmd.AWS.SecretKey, cmd.AWS.SessionToken,
			),
		}
		if cmd.AWS.Region != "" {
			cfg.Region = cmd.AWS.Region
		}
		opts = append(opts, backend.WithAWSConfig(cfg))
		if cmd.AWS.Endpoint != "" {
			opts = append(opts, backend.WithEndpoint(cmd.AWS.Endpoint))
		}
	} else {
		// No explicit credentials — use anonymous access.
		cfg := aws.Config{
			Credentials: aws.AnonymousCredentials{},
		}
		if cmd.AWS.Region != "" {
			cfg.Region = cmd.AWS.Region
		}
		opts = append(opts, backend.WithAWSConfig(cfg))
		if cmd.AWS.Endpoint != "" {
			opts = append(opts, backend.WithEndpoint(cmd.AWS.Endpoint))
		}
	}

	return opts, nil
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

// defaultBackendURL returns a file:// backend URL rooted at
// os.UserCacheDir()/<execName>, creating the directory if needed.
func defaultBackendURL() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cannot determine executable name: %w", err)
	}
	name := filepath.Base(exe)
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine user cache dir: %w", err)
	}
	dir := filepath.Join(cacheDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("cannot create cache dir %s: %w", dir, err)
	}
	// file://<name><absolute-path>
	return "file://" + name + dir, nil
}
