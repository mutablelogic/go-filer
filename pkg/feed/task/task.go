package task

import (
	"context"
	"time"

	// Packages
	"github.com/djthorpe/go-pg"
	client "github.com/mutablelogic/go-client"
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	server "github.com/mutablelogic/go-server"
	queue_schema "github.com/mutablelogic/go-server/pkg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

const (
	TaskNameRegisterUrl = "register_url"
)

type taskrunner struct {
	queue  server.PGQueue
	conn   pg.PoolConn
	feed   filer.Feed
	client *client.Client
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewTaskRunner(ctx context.Context, feed filer.Feed, queue server.PGQueue) (*taskrunner, error) {
	self := new(taskrunner)
	self.feed = feed
	self.queue = queue

	// Make a client
	if client, err := client.New(client.OptEndpoint("http://localhost/")); err != nil {
		return nil, err
	} else {
		self.client = client
	}

	// Register tasks
	taskMap := map[string]func(context.Context, *schema.Url) error{
		TaskNameRegisterUrl: self.RegisterUrl,
	}

	for task, fn := range taskMap {
		if _, err := self.queue.RegisterQueue(ctx, queue_schema.QueueMeta{
			Queue:      task,
			TTL:        types.DurationPtr(time.Hour),
			Retries:    types.Uint64Ptr(3),
			RetryDelay: types.DurationPtr(5 * time.Minute),
		}, func(ctx context.Context, in any) error {
			var object schema.Url
			if err := self.queue.UnmarshalPayload(&object, in); err != nil {
				return err
			}
			return fn(ctx, &object)
		}); err != nil {
			return nil, err
		}
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (t *taskrunner) queueTask(ctx context.Context, task string, url *schema.Url) error {
	if _, err := t.queue.CreateTask(ctx, task, url, 0); err != nil {
		return err
	}
	return nil
}
