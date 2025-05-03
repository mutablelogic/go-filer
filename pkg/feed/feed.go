package feed

import (
	"context"
	"errors"

	// Packages
	pg "github.com/djthorpe/go-pg"
	filer "github.com/mutablelogic/go-filer"
	handler "github.com/mutablelogic/go-filer/pkg/feed/handler"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	task "github.com/mutablelogic/go-filer/pkg/feed/task"
	"github.com/mutablelogic/go-filer/pkg/rss"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	queue_schema "github.com/mutablelogic/go-server/pkg/pgqueue/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	queue server.PGQueue
	conn  pg.PoolConn
}

var _ filer.Feed = (*Manager)(nil)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewManager(ctx context.Context, queue server.PGQueue, router server.HTTPRouter) (*Manager, error) {
	self := new(Manager)

	// Queue
	if queue == nil {
		return nil, httpresponse.ErrInternalError.With("Queue is nil")
	} else {
		self.queue = queue
		self.conn = queue.Conn().With(
			"schema", schema.SchemaName,
			"pgqueue_schema", queue_schema.SchemaName,
			"taskname_create_url", task.TaskNameRegisterUrl,
		).(pg.PoolConn)
	}
	// Create the schema
	if exists, err := pg.SchemaExists(ctx, self.conn, schema.SchemaName); err != nil {
		return nil, err
	} else if !exists {
		if err := pg.SchemaCreate(ctx, self.conn, schema.SchemaName); err != nil {
			return nil, err
		}
	}

	// Router
	if router != nil {
		handler.RegisterHandlers(ctx, router, self)
	}

	// Bootstrap the schema
	if err := self.conn.Tx(ctx, func(conn pg.Conn) error {
		return schema.Bootstrap(ctx, conn)
	}); err != nil {
		return nil, err
	}

	// Task runner
	_, err := task.NewTaskRunner(ctx, self, self.queue)
	if err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - URL

func (manager *Manager) CreateUrl(ctx context.Context, meta schema.UrlMeta) (*schema.Url, error) {
	var url schema.Url
	if err := manager.conn.Insert(ctx, &url, meta); err != nil {
		return nil, httperr(err)
	}
	// Return success
	return &url, nil
}

func (manager *Manager) ListUrls(ctx context.Context, req schema.UrlListRequest) (*schema.UrlList, error) {
	var list schema.UrlList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, httperr(err)
	} else {
		return &list, nil
	}
}

func (manager *Manager) GetUrl(ctx context.Context, id uint64) (*schema.Url, error) {
	var url schema.Url
	if err := manager.conn.Get(ctx, &url, schema.UrlId(id)); err != nil {
		return nil, httperr(err)
	}
	// Return success
	return &url, nil
}

func (manager *Manager) DeleteUrl(ctx context.Context, id uint64) (*schema.Url, error) {
	var url schema.Url
	if err := manager.conn.Delete(ctx, &url, schema.UrlId(id)); err != nil {
		return nil, httperr(err)
	}
	// Return success
	return &url, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - FEED

func (manager *Manager) CreateFeed(ctx context.Context, id uint64, rss rss.Feed) (*schema.Feed, error) {
	var feed schema.Feed
	var hash schema.FeedHash

	// Set the schedule TTL based on the feed TTL
	hash.Period = schema.DefaultFeedTTL
	if ttl := rss.Channel.TTL; ttl != nil {
		if period, err := ttl.Seconds(); err == nil && period > 0 {
			hash.Period = period
		}
	}

	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Insert the hash
		if err := manager.conn.With("meta", rss).Insert(ctx, &hash, schema.FeedHash{
			Id:     id,
			Period: hash.Period,
		}); err != nil {
			return httperr(err)
		}

		// Remove the items from the feed so we don't insert them into the database
		rss.Channel.Items = nil

		// Insert the feed into the database
		if err := manager.conn.With("id", id).Insert(ctx, &feed, schema.RSSFeed(rss)); err != nil {
			return httperr(err)
		}

		// Return success
		return nil
	}); err != nil {
		return nil, httperr(err)
	}

	// Return success
	return &feed, nil
}

func (manager *Manager) ListFeeds(ctx context.Context, req schema.FeedListRequest) (*schema.FeedList, error) {
	var response schema.FeedList

	// List the feeds
	if err := manager.conn.List(ctx, &response, req); err != nil {
		return nil, httperr(err)
	}

	// Return success
	return &response, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func httperr(err error) error {
	if errors.Is(err, pg.ErrNotFound) {
		return httpresponse.ErrNotFound.With(err)
	}
	return err
}
