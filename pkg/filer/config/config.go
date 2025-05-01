package filer

import (
	"context"

	// Packages
	plugin "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/pkg/filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////

type Config struct {
	AWS    plugin.AWS        `kong:"-"` // AWS configuration
	Router server.HTTPRouter `kong:"-"` // HTTP Router
}

type task struct {
	*filer.Manager
}

var _ server.Plugin = Config{}
var _ server.Task = task{}

///////////////////////////////////////////////////////////////////////////////
// MODULE

func (c Config) New(ctx context.Context) (server.Task, error) {
	manager, err := filer.New(ctx, schema.APIPrefix, c.Router, c.AWS)
	if err != nil {
		return nil, err
	}
	return &task{manager}, nil
}

func (Config) Name() string {
	return "filer"
}

func (Config) Description() string {
	return "Data filer"
}

///////////////////////////////////////////////////////////////////////////////
// TASK

func (task) Run(ctx context.Context) error {
	// Wait for context to be done
	<-ctx.Done()

	// Return success
	return nil
}
