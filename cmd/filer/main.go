package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	// Packages
	kong "github.com/alecthomas/kong"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	version "github.com/mutablelogic/go-filer/pkg/version"
	server "github.com/mutablelogic/go-server"
	logger "github.com/mutablelogic/go-server/pkg/logger"
	gootel "go.opentelemetry.io/otel"
	trace "go.opentelemetry.io/otel/trace"
	terminal "golang.org/x/term"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Globals struct {
	Debug   bool             `name:"debug" help:"Enable debug logging"`
	Verbose bool             `name:"verbose" help:"Enable verbose logging"`
	Version kong.VersionFlag `name:"version" help:"Print version and exit"`

	// HTTP server/client options
	HTTP struct {
		Prefix  string        `name:"prefix" help:"HTTP path prefix" default:"/api/filer"`
		Addr    string        `name:"addr" env:"FILER_ADDR" help:"HTTP listen address" default:"localhost:8087"`
		Timeout time.Duration `name:"timeout" help:"HTTP request timeout" default:"30m"`
		Origin  string        `name:"origin" help:"CORS origin ('*' to allow all, empty for same-origin only)" default:""`
	} `embed:"" prefix:"http."`

	// Open Telemetry options
	OTel struct {
		Endpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT" help:"OpenTelemetry endpoint" default:""`
		Header   string `env:"OTEL_EXPORTER_OTLP_HEADERS" help:"OpenTelemetry collector headers"`
		Name     string `env:"OTEL_SERVICE_NAME" help:"OpenTelemetry service name" default:"${EXECUTABLE_NAME}"`
	} `embed:"" prefix:"otel."`

	// Private fields
	ctx      context.Context
	cancel   context.CancelFunc
	logger   server.Logger
	tracer   trace.Tracer
	execName string
}

type CLI struct {
	Globals
	Backends BackendsCommand `cmd:"" name:"backends" help:"List registered backends."            group:"CLIENT"`
	List     ListCommand     `cmd:"" name:"list"     help:"List objects in a backend."           group:"CLIENT"`
	Get      GetCommand      `cmd:"" name:"get"      help:"Download an object to stdout."        group:"CLIENT"`
	Head     HeadCommand     `cmd:"" name:"head"     help:"Print object metadata."               group:"CLIENT"`
	Upload   UploadCommand   `cmd:"" name:"upload"   help:"Upload a file or directory."              group:"CLIENT"`
	Download DownloadCommand `cmd:"" name:"download" help:"Download objects to a local directory."   group:"CLIENT"`
	Delete   DeleteCommand   `cmd:"" name:"delete"   help:"Delete an object, or recursively under a path." group:"CLIENT"`
	ServerCommands
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	// Get executable name
	var execName string
	if exe, err := os.Executable(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	} else {
		execName = filepath.Base(exe)
	}

	// Parse command-line arguments
	cli := new(CLI)
	ctx := kong.Parse(cli,
		kong.Name(execName),
		kong.Description(execName+" command line interface"),
		kong.Vars{
			"version":         string(version.JSON(execName)),
			"EXECUTABLE_NAME": execName,
		},
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
	)

	// Set the executable name
	cli.Globals.execName = execName

	// Run the command
	os.Exit(run(ctx, &cli.Globals))
}

func run(ctx *kong.Context, globals *Globals) int {
	parent := context.Background()

	// Create logger
	if isTerminal(os.Stderr) {
		globals.logger = logger.New(os.Stderr, logger.Term, globals.Debug)
	} else {
		globals.logger = logger.New(os.Stderr, logger.JSON, globals.Debug)
	}

	// Create the context and cancel function
	globals.ctx, globals.cancel = signal.NotifyContext(parent, os.Interrupt)
	defer globals.cancel()

	// Open Telemetry
	if globals.OTel.Endpoint != "" {
		provider, err := otel.NewProvider(globals.OTel.Endpoint, globals.OTel.Header, globals.OTel.Name)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
			return 2
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			provider.Shutdown(shutdownCtx)
		}()

		// Set as global so instrumentation libraries (e.g. otelaws) pick it up.
		gootel.SetTracerProvider(provider)

		// Store tracer for creating spans
		globals.tracer = provider.Tracer(globals.OTel.Name)
	}

	// Call the Run() method of the selected parsed command.
	if err := ctx.Run(globals); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		return 1
	}

	// Return success
	return 0
}

func isTerminal(w io.Writer) bool {
	if f, ok := w.(*os.File); ok {
		return terminal.IsTerminal(int(f.Fd()))
	}
	return false
}
