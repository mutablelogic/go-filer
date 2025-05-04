package task

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	rss "github.com/mutablelogic/go-filer/pkg/rss"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) FetchFeed(ctx context.Context, payload any) error {
	var rss rss.Feed

	// Decode the payload
	var url schema.Url
	if err := t.queue.UnmarshalPayload(&url, payload); err != nil {
		return err
	}

	// Refetch the url in case it has changed
	if url_, err := t.feed.GetUrl(ctx, url.Id); err != nil {
		return err
	} else {
		url = *url_
	}

	// Read the RSS feed
	if err := t.client.DoWithContext(ctx, nil, &rss, client.OptReqEndpoint(types.PtrString(url.Url))); err != nil {
		return err
	}

	// Update the feed
	var changed bool
	if feed, err := t.feed.UpsertFeed(ctx, url.Id, rss, &changed); err != nil {
		return err
	} else if !changed {
		ref.Log(ctx).With("url", url).Debug(ctx, "Feed has not changed, not updating items")
		return nil
	} else if types.PtrBool(feed.Block) {
		ref.Log(ctx).With("url", url).Debug(ctx, "Feed is blocked, not updating items")
	} else if types.PtrBool(feed.Complete) {
		ref.Log(ctx).With("url", url).Debug(ctx, "Feed is complete, not updating items")
	}

	// Update the items
	items, err := t.feed.UpsertItems(ctx, url.Id, rss)
	if err != nil {
		return err
	}

	if len(items.Body) == 0 {
		ref.Log(ctx).With("url", url).Debug(ctx, "No new items")
		return nil
	}

	ref.Log(ctx).With("url", url).Debug(ctx, "Updated items: ", items)

	// Return sucess
	return nil
}
