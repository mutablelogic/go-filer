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
	version "github.com/mutablelogic/go-filer/pkg/version"
	server "github.com/mutablelogic/go-server"
	logger "github.com/mutablelogic/go-server/pkg/logger"
	terminal "golang.org/x/term"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Globals struct {
	Debug   bool             `name:"debug" help:"Enable debug logging"`
	Version kong.VersionFlag `name:"version" help:"Print version and exit"`

	HTTP struct {
		Prefix  string        `name:"prefix" help:"HTTP path prefix" default:"/api/filer"`
		Addr    string        `name:"addr" env:"FILER_ADDR" help:"HTTP listen address" default:"localhost:8080"`
		Timeout time.Duration `name:"timeout" help:"HTTP request timeout" default:"30s"`
		Origin  string        `name:"origin" help:"CORS origin ('*' to allow all, empty for same-origin only)" default:""`
	} `embed:"" prefix:"http."`

	// Private fields
	ctx    context.Context
	cancel context.CancelFunc
	logger server.Logger
}

type CLI struct {
	Globals
	Backends BackendsCommand `cmd:"" name:"backends" help:"List registered backends." group:"CLIENT"`
	List     ListCommand     `cmd:"" name:"list"     help:"List objects in a backend."      group:"CLIENT"`
	Get      GetCommand      `cmd:"" name:"get"      help:"Download an object to stdout."   group:"CLIENT"`
	Head     HeadCommand     `cmd:"" name:"head"     help:"Print object metadata."           group:"CLIENT"`
	Create   CreateCommand   `cmd:"" name:"create"   help:"Upload an object."               group:"CLIENT"`
	Delete   DeleteCommand   `cmd:"" name:"delete"   help:"Delete an object or prefix."     group:"CLIENT"`
	ServerCommands
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	exe, _ := os.Executable()
	execName := filepath.Base(exe)

	cli := new(CLI)
	ctx := kong.Parse(cli,
		kong.Name(execName),
		kong.Description("filer file storage command-line interface"),
		kong.Vars{"version": string(version.JSON(execName))},
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{Compact: true}),
	)
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

	// Create context with signal handling
	globals.ctx, globals.cancel = signal.NotifyContext(parent, os.Interrupt)
	defer globals.cancel()

	if err := ctx.Run(globals); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		return -1
	}
	return 0
}

func isTerminal(w io.Writer) bool {
	if f, ok := w.(*os.File); ok {
		return terminal.IsTerminal(int(f.Fd()))
	}
	return false
}
