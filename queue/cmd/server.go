package cmd

import (
	queue "github.com/mutablelogic/go-filer/queue/manager"
	pg "github.com/mutablelogic/go-pg"
	server "github.com/mutablelogic/go-server"
)

type Queue struct {
	Schema string `name:"schema" env:"QUEUE_SCHEMA" help:"Database schema to use for queue tables"`
}

func (cmd Queue) WithQueueManager(globals server.Cmd, conn pg.PoolConn, fn func(manager *queue.Manager) error) error {
	opts := []queue.Opt{
		queue.WithMeter(globals.Meter()),
		queue.WithTracer(globals.Tracer()),
	}
	if cmd.Schema != "" {
		opts = append(opts, queue.WithSchema(cmd.Schema))
	}

	// Create a queue
	manager, err := queue.New(globals.Context(), conn, globals.Name(), globals.Version(), opts...)
	if err != nil {
		return err
	}

	// Call the function with the manager
	return fn(manager)
}
