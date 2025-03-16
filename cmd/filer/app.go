package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	// Packages
	kong "github.com/alecthomas/kong"
	client "github.com/mutablelogic/go-client"
	filer "github.com/mutablelogic/go-filer/pkg/filer/client"
	version "github.com/mutablelogic/go-llm/pkg/version"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Globals struct {
	Endpoint string `env:"FILER_ENDPOINT" default:"http://localhost/" help:"Service endpoint"`
	Debug    bool   `help:"Enable debug output"`
	Trace    bool   `help:"Enable trace output"`

	vars   kong.Vars `kong:"-"` // Variables for kong
	ctx    context.Context
	cancel context.CancelFunc
	client *filer.Client
}

type App interface {
	Context() context.Context
	GetEndpoint(paths ...string) *url.URL
	GetDebug() bool
	GetClient() *filer.Client
}

var _ App = (*Globals)(nil)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewApp(app Globals, vars kong.Vars) (*Globals, error) {
	// Set the vars
	app.vars = vars

	// Create the context
	// This context is cancelled when the process receives a SIGINT or SIGTERM
	app.ctx, app.cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// Create the client
	opts := []client.ClientOpt{}
	if app.Debug {
		opts = append(opts, client.OptTrace(os.Stderr, app.Trace))
	}
	if client, err := filer.New(app.Endpoint, opts...); err != nil {
		return nil, err
	} else {
		app.client = client
	}

	// Return the app
	return &app, nil
}

func (app *Globals) Close() error {
	app.cancel()
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (app *Globals) Context() context.Context {
	return app.ctx
}

func (app *Globals) GetDebug() bool {
	return app.Debug || app.Trace
}

func (app *Globals) GetClient() *filer.Client {
	return app.client
}

func (app *Globals) GetEndpoint(paths ...string) *url.URL {
	url, err := url.Parse(app.Endpoint)
	if err != nil {
		return nil
	}
	for _, path := range paths {
		url.Path = types.JoinPath(url.Path, os.Expand(path, func(key string) string {
			return app.vars[key]
		}))
	}
	return url
}

func (app *Globals) ClientOpts() []client.ClientOpt {
	opts := []client.ClientOpt{}

	// Trace mode
	if app.Debug || app.Trace {
		opts = append(opts, client.OptTrace(os.Stderr, app.Trace))
	}

	// Append user agent
	source := version.GitSource
	version := version.GitTag
	if source == "" {
		source = "go-service"
	}
	if version == "" {
		version = "v0.0.0"
	}
	opts = append(opts, client.OptUserAgent(fmt.Sprintf("%v/%v", filepath.Base(source), version)))

	// Return options
	return opts
}
