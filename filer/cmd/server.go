//go:build !client

package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/mutablelogic/go-filer/backend/blob"
	"github.com/mutablelogic/go-filer/filer/manager"
	server "github.com/mutablelogic/go-server"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServerCommands struct {
	Run RunServerCommand `cmd:"" name:"run" help:"Run HTTP server." group:"SERVER"`
}

type AWSConfig struct {
	AccessKey    string `name:"access-key" env:"AWS_ACCESS_KEY_ID" help:"AWS access key ID (s3://)."  optional:""`
	SecretKey    string `name:"secret-key" env:"AWS_SECRET_ACCESS_KEY" help:"AWS secret access key (s3://)." optional:""`
	SessionToken string `name:"session-token" env:"AWS_SESSION_TOKEN"     help:"AWS session token for temporary credentials (s3://)." optional:""`
	Region       string `name:"region"   env:"AWS_REGION,AWS_DEFAULT_REGION" help:"AWS region." optional:""`
	Profile      string `name:"profile"  env:"AWS_PROFILE" help:"AWS credentials profile (s3://)."  optional:""`
	Endpoint     string `name:"endpoint" help:"S3-compatible endpoint URL, e.g. http://localhost:9000 (s3://)."   optional:""`
}

type RunServerCommand struct {
	Backend []string  `arg:"" name:"backend" help:"Backend URL (e.g. mem://name, file://name/path, s3://bucket)." optional:""`
	AWS     AWSConfig `embed:"" prefix:"aws."`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *RunServerCommand) Run(globals server.Cmd) error {
	// Gather backend options
	opts := []manager.Opt{
		manager.WithTracer(globals.Tracer()),
	}

	// If there are no backends, then register one with a default file:// URL rooted in the user cache dir.
	if len(cmd.Backend) == 0 {
		backend, err := defaultBackendURL(globals)
		if err != nil {
			return err
		} else {
			cmd.Backend = append(cmd.Backend, backend)
		}
	}

	// Create the backends
	for _, backend := range cmd.Backend {
		backend_opts, err := cmd.OptsForBackend(globals.Context(), backend)
		if err != nil {
			return fmt.Errorf("failed to create backend for %q: %w", backend, err)
		} else {
			opts = append(opts, manager.WithBackend(globals.Context(), backend, backend_opts...))
		}
	}

	return cmd.withManager(globals, opts, func(ctx context.Context, mgr *manager.Manager) error {
		// Log the backends
		globals.Logger().InfoContext(globals.Context(), "filer manager started", "name", globals.Name(), "version", globals.Version())
		for _, name := range mgr.Backends() {
			globals.Logger().InfoContext(globals.Context(), "registered backend", "name", name, "url", mgr.Backend(name).URL().String())
		}

		<-globals.Context().Done()
		return nil
	})
}

func (cmd *RunServerCommand) OptsForBackend(ctx context.Context, backend string) ([]blob.Opt, error) {
	opts := []blob.Opt{}
	cfg := aws.Config{}

	switch {
	// Profile takes priority: load via the SDK's config chain so SSO profiles work.
	case cmd.AWS.Profile != "":
		cfgOpts := []func(*config.LoadOptions) error{
			config.WithSharedConfigProfile(cmd.AWS.Profile),
		}
		if cmd.AWS.Region != "" {
			cfgOpts = append(cfgOpts, config.WithRegion(cmd.AWS.Region))
		}
		if awsCfg, err := config.LoadDefaultConfig(ctx, cfgOpts...); err != nil {
			return nil, fmt.Errorf("failed to load AWS config for profile %q: %w", cmd.AWS.Profile, err)
		} else {
			cfg = awsCfg
		}
	case cmd.AWS.AccessKey != "":
		// Static credentials (no profile set).
		if cmd.AWS.SecretKey == "" {
			return nil, fmt.Errorf("--aws.secret-key is required when --aws.access-key is set")
		} else {
			cfg.Credentials = credentials.NewStaticCredentialsProvider(cmd.AWS.AccessKey, cmd.AWS.SecretKey, cmd.AWS.SessionToken)
		}
	default:
		// No explicit credentials — use anonymous access.
		cfg.Credentials = aws.AnonymousCredentials{}
	}

	// Set region
	if cmd.AWS.Region != "" {
		cfg.Region = cmd.AWS.Region
	}

	// Set endpoint for S3-compatible services, if given.
	opts = append(opts, blob.WithAWSConfig(cfg), blob.WithEndpoint(cmd.AWS.Endpoint))

	// Return the blob options
	return opts, nil
}

func (cmd *RunServerCommand) withManager(globals server.Cmd, opts []manager.Opt, fn func(context.Context, *manager.Manager) error) (err error) {
	// Create manager
	manager, err := manager.New(globals.Context(), opts...)
	if err != nil {
		return err
	}
	defer manager.Close()

	// Run the function
	return fn(globals.Context(), manager)
}

// defaultBackendURL returns a file:// backend URL rooted at
// os.UserCacheDir()/<execName>, creating the directory if needed.
func defaultBackendURL(globals server.Cmd) (string, error) {
	name := globals.Name()
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine user cache dir: %w", err)
	}

	dir := filepath.Join(cacheDir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("cannot create cache dir %s: %w", dir, err)
	} else {
		return types.Ptr(url.URL{Scheme: "file", Host: name, Path: filepath.ToSlash(dir)}).String(), nil
	}
}

/*
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
	}

	return opts, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// serve registers HTTP handlers and runs the server until context is done.
func serve(ctx *Globals, mgr *manager.Manager) error {
	// Ensure TMPDIR exists so Go's os.TempDir() is usable for fileblob temp files.
	if tmpdir := os.Getenv("TMPDIR"); tmpdir != "" {
		if err := os.MkdirAll(tmpdir, 0o755); err != nil {
			ctx.logger.Printf(ctx.ctx, "warning: could not create TMPDIR %s: %v", tmpdir, err)
		}
	}
	// Build middleware
	middleware := []httprouter.HTTPMiddlewareFunc{}
	if mw, ok := ctx.logger.(server.HTTPMiddleware); ok {
		middleware = append(middleware, mw.WrapFunc)
	}
	if ctx.tracer != nil {
		middleware = append(middleware, otel.HTTPHandlerFunc(ctx.tracer))
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
	srv, err := httpserver.New(ctx.HTTP.Addr, http.Handler(router), nil,
		httpserver.WithReadTimeout(ctx.HTTP.Timeout),
		httpserver.WithWriteTimeout(ctx.HTTP.Timeout),
	)
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
*/
