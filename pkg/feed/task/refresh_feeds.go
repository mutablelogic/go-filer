package task

import (
	"context"

	// Packages
	pg "github.com/djthorpe/go-pg"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Get list of stale feeds and queue them for refresh
func (t *taskrunner) RefreshFeeds(ctx context.Context, _ any) error {
	feeds, err := t.feed.ListFeeds(ctx, schema.FeedListRequest{
		Stale: true,
		OffsetLimit: pg.OffsetLimit{
			Offset: 0,
			Limit:  types.Uint64Ptr(1),
		},
	})
	if err != nil {
		return err
	}

	// For each feed, fetch the feed
	for _, feed := range feeds.Body {
		url, err := t.feed.GetUrl(ctx, feed.Id)
		if err != nil {
			return err
		} else if err := t.queueFetchFeed(ctx, url); err != nil {
			return err
		}
		// TODO: Update the timestamp so we consider other feeds
		// in preference order
	}

	// Return sucess
	return nil
}
