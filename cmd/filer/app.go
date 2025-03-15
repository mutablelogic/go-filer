package main

import (
	"context"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	// Packages
	"github.com/alecthomas/kong"
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
}

type App interface {
	Context() context.Context
	GetEndpoint() *url.URL
	GetDebug() bool
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewApp(app Globals, vars kong.Vars) *Globals {
	// Set the vars
	app.vars = vars

	// Create the context
	// This context is cancelled when the process receives a SIGINT or SIGTERM
	app.ctx, app.cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// Return the app
	return &app
}

func (app *Globals) Close() error {
	app.cancel()
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// METHODS

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
