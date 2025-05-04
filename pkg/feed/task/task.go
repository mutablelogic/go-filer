package task

import (
	"context"
	"time"

	// Packages
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
	TaskNameRegisterUrl   = "register_url"
	TaskNameFetchFeed     = "fetch_feed"
	TimerNameRefreshFeeds = "refresh_feeds"
	TaskNameFetchItem     = "fetch_item"
)

type taskrunner struct {
	queue  server.PGQueue
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
	taskMap := map[string]func(context.Context, any) error{
		TaskNameRegisterUrl: self.RegisterUrl,
		TaskNameFetchFeed:   self.FetchFeed,
		TaskNameFetchItem:   self.FetchItem,
	}

	for task, fn := range taskMap {
		if _, err := self.queue.RegisterQueue(ctx, queue_schema.QueueMeta{
			Queue:      task,
			TTL:        types.DurationPtr(time.Hour),
			Retries:    types.Uint64Ptr(3),
			RetryDelay: types.DurationPtr(5 * time.Minute),
		}, fn); err != nil {
			return nil, err
		}
	}

	// Register timers
	timerMap := map[string]func(context.Context, any) error{
		TimerNameRefreshFeeds: self.RefreshFeeds,
	}

	for task, fn := range timerMap {
		if _, err := self.queue.RegisterTicker(ctx, queue_schema.TickerMeta{
			Ticker:   task,
			Interval: types.DurationPtr(15 * time.Second),
		}, fn); err != nil {
			return nil, err
		}
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (t *taskrunner) queueFetchFeed(ctx context.Context, url *schema.Url) error {
	if _, err := t.queue.CreateTask(ctx, TaskNameFetchFeed, url, 0); err != nil {
		return err
	}
	return nil
}
