package filer

import (
	"context"

	// Packages
	plugin "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/pkg/filer"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////

type Config struct {
	AWS    plugin.AWS        `kong:"-"`                                    // AWS configuration
	Router server.HTTPRouter `kong:"-"`                                    // HTTP Router
	Prefix string            `default:"${FILERPREFIX}" help:"Path prefix"` // HTTP Path Prefix
}

var _ server.Plugin = Config{}

///////////////////////////////////////////////////////////////////////////////
// MODULE

func (c Config) New(ctx context.Context) (server.Task, error) {
	return filer.New(ctx, c.Prefix, c.Router, c.AWS)
}

func (Config) Name() string {
	return "filer"
}

func (Config) Description() string {
	return "Data filer"
}
