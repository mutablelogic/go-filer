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
	rss "github.com/mutablelogic/go-filer/pkg/rss"
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
			"taskname_fetch_item", task.TaskNameFetchItem,
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

func (manager *Manager) CreateUrl(ctx context.Context, url string, meta schema.UrlMeta) (*schema.Url, error) {
	var resp schema.Url
	if err := manager.conn.With("url", url).Insert(ctx, &resp, meta); err != nil {
		return nil, httperr(err)
	}
	// Return success
	return &resp, nil
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

func (manager *Manager) UpdateUrl(ctx context.Context, id uint64, meta schema.UrlMeta) (*schema.Url, error) {
	var url schema.Url
	if err := manager.conn.Update(ctx, &url, schema.UrlId(id), meta); err != nil {
		return nil, httperr(err)
	}
	// Return success
	return &url, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - FEED

// Create a new feed or replace it. The feed is created with the given id and RSS feed.
// The changed flag is set to true if the feed was updated and changed.
func (manager *Manager) UpsertFeed(ctx context.Context, id uint64, rss rss.Feed, changed *bool) (*schema.Feed, error) {
	var feed schema.Feed

	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		var insert bool
		// Get the feed
		if err := conn.Get(ctx, &feed, schema.FeedId(id)); errors.Is(err, pg.ErrNotFound) {
			// Insert the feed
			if err := manager.conn.With("id", id).Insert(ctx, &feed, schema.RSSFeed(rss)); err != nil {
				return err
			}
			// Set changed flag
			insert = true
			if changed != nil {
				*changed = true
			}
		} else if err != nil {
			return err
		} else if err := conn.Update(ctx, &feed, schema.FeedId(id), schema.RSSFeed(rss)); errors.Is(err, pg.ErrNotFound) {
			// Feed was not updated
			if changed != nil {
				*changed = false
			}
		} else if err != nil {
			return err
		} else {
			// Feed was updated
			if changed != nil {
				*changed = true
			}
		}

		// When inserting, we set the period to the channel TTL,
		// and when updating, we update to the current timestamp
		switch insert {
		case true:
			hashPeriod := schema.DefaultFeedTTL
			if ttl := rss.Channel.TTL; ttl != nil {
				if period, err := ttl.Seconds(); err == nil && period > 0 {
					hashPeriod = period
				}
			}
			if err := manager.conn.Update(ctx, nil, schema.UrlId(id), schema.UrlMeta{
				Period: hashPeriod,
			}); err != nil {
				return err
			}
		case false:
			if err := manager.conn.With("update", true).Update(ctx, nil, schema.UrlId(id), schema.UrlMeta{}); err != nil {
				return err
			}
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

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - ITEM

// Insert or replace items in the database from an RSS feed. The items which
// have been changed are returned.
func (manager *Manager) UpsertItems(ctx context.Context, feed uint64, rss rss.Feed) (*schema.ItemList, error) {
	var list schema.ItemList

	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// TODO: Update all the items in the feed to set 'live' as false for this feed
		var errs error
		for _, rssitem := range rss.Channel.Items {
			var item schema.Item
			if err := manager.conn.With("feed", feed).Insert(ctx, &item, schema.RSSItem(*rssitem)); errors.Is(err, pg.ErrNotFound) {
				// Skip if not found, this means the item was not changed
			} else if err != nil {
				errs = errors.Join(errs, err)
			} else {
				list.Body = append(list.Body, item)
			}
		}

		// Return success
		return errs
	}); err != nil {
		return nil, httperr(err)
	}

	// Return success
	return &list, nil
}

func (manager *Manager) GetItem(ctx context.Context, feed uint64, guid string) (*schema.Item, error) {
	var item schema.Item
	if err := manager.conn.Get(ctx, &item, schema.ItemId{Guid: guid, Feed: feed}); err != nil {
		return nil, httperr(err)
	}
	// Return success
	return &item, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func httperr(err error) error {
	if errors.Is(err, pg.ErrNotFound) {
		return httpresponse.ErrNotFound.With(err)
	}
	return err
}
