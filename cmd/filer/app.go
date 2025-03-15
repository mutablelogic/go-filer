package main

import (
	"context"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	// Packages
	kong "github.com/alecthomas/kong"
	"github.com/mutablelogic/go-client"
	filer "github.com/mutablelogic/go-filer/pkg/filer/client"
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
	GetEndpoint() *url.URL
	GetDebug() bool
	GetClient() *filer.Client
}

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

func (app *Globals) GetEndpoint() *url.URL {
	if url, err := url.Parse(app.Endpoint); err == nil {
		return url
	}
	return nil
}

func (app *Globals) GetDebug() bool {
	return app.Debug || app.Trace
}

func (app *Globals) GetClient() *filer.Client {
	return app.client
}
