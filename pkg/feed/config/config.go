package filer

import (
	"context"

	// Packages
	feed "github.com/mutablelogic/go-filer/pkg/feed"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////

type Config struct {
	Router server.HTTPRouter `kong:"-"` // HTTP Router
	Queue  server.PGQueue    `kong:"-"` // Task Queue
}

type task struct {
	*feed.Manager
}

var _ server.Plugin = Config{}
var _ server.Task = task{}

///////////////////////////////////////////////////////////////////////////////
// MODULE

func (c Config) New(ctx context.Context) (server.Task, error) {
	manager, err := feed.NewManager(ctx, c.Queue, c.Router)
	if err != nil {
		return nil, err
	}
	return &task{manager}, nil
}

func (Config) Name() string {
	return "feed"
}

func (Config) Description() string {
	return "RSS Feed manager"
}

///////////////////////////////////////////////////////////////////////////////
// TASK

func (task) Run(ctx context.Context) error {
	// Wait for context to be done
	<-ctx.Done()

	// Return success
	return nil
}
