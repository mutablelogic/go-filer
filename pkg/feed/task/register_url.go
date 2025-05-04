package task

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	rss "github.com/mutablelogic/go-filer/pkg/rss"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) RegisterUrl(ctx context.Context, in any) error {
	var rss rss.Feed

	// Decode the payload
	var url schema.Url
	if err := t.queue.UnmarshalPayload(&url, in); err != nil {
		return err
	}

	// Read the RSS feed
	if err := t.client.DoWithContext(ctx, nil, &rss, client.OptReqEndpoint(types.PtrString(url.Url))); err != nil {
		return err
	}

	// Insert the feed
	_, err := t.feed.UpsertFeed(ctx, url.Id, rss, nil)
	if err != nil {
		return err
	}

	// Return sucess
	return nil
}
