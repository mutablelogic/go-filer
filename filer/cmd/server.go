//go:build !client

package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	config "github.com/aws/aws-sdk-go-v2/config"
	credentials "github.com/aws/aws-sdk-go-v2/credentials"
	blob "github.com/mutablelogic/go-filer/backend/blob"
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	manager "github.com/mutablelogic/go-filer/filer/manager"
	queue "github.com/mutablelogic/go-filer/queue/cmd"
	queuemanager "github.com/mutablelogic/go-filer/queue/manager"
	server "github.com/mutablelogic/go-server"
	cmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	types "github.com/mutablelogic/go-server/pkg/types"
	errgroup "golang.org/x/sync/errgroup"
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
	Backend             []string  `arg:"" name:"backend" help:"Backend URL (e.g. mem://name, file://name/path, s3://bucket)." optional:""`
	AWS                 AWSConfig `embed:"" prefix:"aws."`
	queue.PostgresFlags `embed:"" prefix:"pg."`
	queue.Queue         `embed:"" prefix:"queue."`
	cmd.RunServer
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *RunServerCommand) Run(globals server.Cmd) error {
	conn, err := cmd.PostgresFlags.Connect(globals)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	} else if conn == nil {
		return fmt.Errorf("PostgreSQL connection URL is required")
	} else {
		defer conn.Close()
	}

	// Wrap in a queue manager
	return cmd.Queue.WithQueueManager(globals, conn, func(queue *queuemanager.Manager) error {
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

		return cmd.withManager(globals, opts, func(ctx context.Context, manager *manager.Manager) error {

			// Register the handlers for the manager
			cmd.RunServer.Register(func(router *httprouter.Router) error {
				return httphandler.RegisterHandlers(router, manager)
			})

			// Log the backends
			globals.Logger().InfoContext(globals.Context(), "filer manager started", "name", globals.Name(), "version", globals.Version())
			for _, name := range manager.Backends() {
				globals.Logger().InfoContext(globals.Context(), "registered backend", "name", name, "url", manager.Backend(name).URL().String())
			}

			// Create an error group for the different components
			errgroup, errctx := errgroup.WithContext(globals.Context())

			// Run the filer manager
			errgroup.Go(func() error {
				return manager.Run(errctx)
			})

			// Run the queue manager
			errgroup.Go(func() error {
				return queue.Run(errctx, globals.Logger())
			})

			// Run the server
			errgroup.Go(func() error {
				return cmd.RunServer.Run(globals)
			})
			return errgroup.Wait()
		})
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
